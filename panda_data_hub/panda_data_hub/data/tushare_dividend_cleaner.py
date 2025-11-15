import calendar
from abc import ABC
from pymongo import UpdateOne
import traceback
from datetime import datetime, timedelta
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_common.utils.stock_utils import get_exchange_suffix
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.ts_utils import get_tushare_suffix
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client


class TSDividendCleaner(ABC):
    """
    股票分红数据清洗器
    从 Tushare 获取股票分红数据并清洗后存入 MongoDB
    """
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.progress_callback = None
        
        # 初始化全局 tushare 客户端
        init_tushare_client(config)
        self.pro = get_tushare_client()
    
    def set_progress_callback(self, callback):
        """设置进度回调函数"""
        self.progress_callback = callback

    def dividend_clean_daily(self):
        """
        每日清洗分红数据
        获取最近的分红公告数据
        """
        logger.info("Starting dividend data cleaning for tushare")
        date_str = datetime.now().strftime("%Y%m%d")
        
        # 获取近期分红数据（最近30天的公告）
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        end_date = datetime.now().strftime("%Y%m%d")
        
        self.clean_dividend_data(start_date, end_date)

    def dividend_clean_history(self, start_date, end_date):
        """
        历史分红数据清洗
        
        参数:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        """
        logger.info(f"Starting historical dividend data cleaning from {start_date} to {end_date}")
        self.clean_dividend_data(start_date, end_date)

    def clean_dividend_data(self, start_date, end_date):
        """
        清洗指定日期范围内的分红数据
        
        参数:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        """
        try:
            import pandas as pd
            import time
            
            # 从 Tushare 获取分红数据
            # 使用 dividend 接口获取分红送股数据
            logger.info(f"Fetching dividend data from {start_date} to {end_date}")
            
            # Tushare 的 dividend 接口字段说明：
            # ts_code: 股票代码
            # ann_date: 公告日期
            # ann_date: 公告日期
            # end_date: 分红年度
            # div_proc: 分红实施进度
            # stk_div: 每股送转（送股比例）
            # stk_bo_rate: 每股送股比例
            # stk_co_rate: 每股转增比例
            # cash_div: 每股分红（税后）
            # cash_div_tax: 每股分红（税前）
            # record_date: 股权登记日
            # ex_date: 除权除息日
            # pay_date: 派息日
            
            # 判断日期范围，决定查询策略
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            date_diff = (end_dt - start_dt).days
            
            dfs = []
            limit = 2000
            total_fetched = 0
            
            # 如果日期范围较小（<=90天），逐日查询；否则按月查询
            if date_diff <= 90:
                # 逐日查询
                dates = pd.date_range(start_dt, end_dt)
                logger.info(f"Using daily query strategy for {len(dates)} days")
                
                for idx, dt in enumerate(dates):
                    str_date = dt.strftime("%Y%m%d")
                    
                    # 更新进度
                    if self.progress_callback:
                        self.progress_callback({
                            "progress_percent": int((idx / len(dates)) * 90),  # 90% 用于数据获取
                            "current_task": f"正在获取分红数据...",
                            "current_step": f"处理日期 {str_date} ({idx+1}/{len(dates)})",
                            "processed_count": idx,
                            "total_count": len(dates)
                        })
                    
                    if (idx + 1) % 10 == 0 or idx == 0 or idx == len(dates) - 1:
                        logger.info(f"Fetching dividend data for date {str_date} ({idx+1}/{len(dates)})")
                    
                    # 对每一个日期，可能有多页记录
                    for offset in range(0, 99):
                        try:
                            df = self.pro.dividend(
                                ann_date=str_date,
                                offset=offset * limit,
                                limit=limit,
                                fields='ts_code,ann_date,end_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date'
                            )
                            
                            if df is not None and not df.empty:
                                dfs.append(df)
                                total_fetched += len(df)
                            
                            # 如果返回的记录数少于 limit，说明已经取完了
                            if df is None or len(df) < limit:
                                break
                            
                            # 添加延迟，避免被 tushare 拒绝访问
                            time.sleep(0.15)
                            
                        except Exception as e:
                            logger.warning(f"  Failed to fetch dividend data at offset {offset} for {str_date}: {str(e)}")
                            break
                    
                    # 每次日期查询后也添加延迟
                    time.sleep(0.125)
            else:
                # 按月查询（对于长时间范围）
                months = pd.date_range(start_dt, end_dt, freq='MS')  # 月初
                if end_dt not in months:
                    months = months.append(pd.DatetimeIndex([end_dt]))
                
                logger.info(f"Using monthly query strategy for {len(months)} months")
                
                for idx in range(len(months)):
                    month_start = months[idx]
                    month_end = months[idx + 1] - timedelta(days=1) if idx < len(months) - 1 else end_dt
                    
                    str_start = month_start.strftime("%Y%m%d")
                    str_end = month_end.strftime("%Y%m%d")
                    logger.info(f"Fetching dividend data for month {str_start} to {str_end} ({idx+1}/{len(months)})")
                    
                    # 按月范围查询（注意：tushare可能不支持范围查询，需要逐日）
                    month_dates = pd.date_range(month_start, month_end)
                    for dt in month_dates:
                        str_date = dt.strftime("%Y%m%d")
                        
                        for offset in range(0, 99):
                            try:
                                df = self.pro.dividend(
                                    ann_date=str_date,
                                    offset=offset * limit,
                                    limit=limit,
                                    fields='ts_code,ann_date,end_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date'
                                )
                                
                                if df is not None and not df.empty:
                                    dfs.append(df)
                                    total_fetched += len(df)
                                
                                if df is None or len(df) < limit:
                                    break
                                
                                time.sleep(0.15)
                                
                            except Exception as e:
                                logger.warning(f"  Failed to fetch dividend data at offset {offset} for {str_date}: {str(e)}")
                                break
                        
                        time.sleep(0.125)
            
            # 合并所有数据
            if not dfs:
                logger.info(f"No dividend data found for period {start_date} to {end_date}")
                return
            
            dividend_data = pd.concat(dfs, ignore_index=True)
            
            # 去重（同一个公告可能在多个日期返回）
            dividend_data = dividend_data.drop_duplicates(subset=['ts_code', 'ann_date', 'ex_date'], keep='first')
            
            logger.info(f"Fetched total {total_fetched} records, after deduplication: {len(dividend_data)} records")
            
            # 调用统一的数据处理和存储方法
            self._process_and_store_dividend_data(dividend_data)
            
            logger.info(f"Dividend data cleaning completed for period {start_date} to {end_date}")
                
        except Exception as e:
            error_msg = f"Failed to clean dividend data: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def clean_dividend_by_symbol(self, symbol):
        """
        清洗指定股票的分红数据（获取该股票的全部历史分红数据）
        
        参数:
        symbol: 股票代码，如 '600000.SH' 或 '000001.SZ'
        """
        try:
            import pandas as pd
            import time
            
            logger.info(f"Fetching dividend data for {symbol}")
            
            # 转换为 Tushare 格式
            ts_code = get_tushare_suffix(symbol)
            logger.info(f"Converted to Tushare format: {ts_code}")
            
            # 获取该股票的所有分红数据（支持分页）
            dfs = []
            limit = 2000
            total_fetched = 0
            
            # 分页获取（一个股票可能有很多历史分红记录）
            for offset in range(0, 99):  # 最多99页
                try:
                    df = self.pro.dividend(
                        ts_code=ts_code,
                        offset=offset * limit,
                        limit=limit,
                        fields='ts_code,ann_date,end_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date'
                    )
                    
                    if df is not None and not df.empty:
                        dfs.append(df)
                        total_fetched += len(df)
                        logger.info(f"  Offset {offset}: fetched {len(df)} records, total: {total_fetched}")
                    
                    # 如果返回的记录数少于 limit，说明已经取完了
                    if df is None or len(df) < limit:
                        break
                    
                    # 添加延迟，避免被 tushare 拒绝访问
                    time.sleep(0.15)
                    
                except Exception as e:
                    logger.warning(f"  Failed to fetch dividend data at offset {offset} for {symbol}: {str(e)}")
                    break
            
            # 合并所有数据
            if not dfs:
                logger.info(f"No dividend data found for {symbol}")
                return
            
            dividend_data = pd.concat(dfs, ignore_index=True)
            logger.info(f"Fetched total {total_fetched} records for {symbol}")
            
            # 调用统一的数据处理和存储方法
            self._process_and_store_dividend_data(dividend_data)
            
            logger.info(f"Successfully cleaned dividend data for {symbol}")
            
        except Exception as e:
            error_msg = f"Failed to clean dividend data for {symbol}: {str(e)}"
            logger.error(error_msg)
            raise
    
    def _process_and_store_dividend_data(self, dividend_data):
        """
        处理和存储分红数据（提取公共逻辑）
        
        参数:
        dividend_data: pandas DataFrame，包含从 tushare 获取的原始分红数据
        """
        if dividend_data is None or dividend_data.empty:
            logger.info("No data to process")
            return
        
        # 数据清洗和转换
        dividend_data = dividend_data.copy()
        
        # 转换股票代码格式
        dividend_data['symbol'] = dividend_data['ts_code'].apply(get_exchange_suffix)
        
        # 重命名字段以匹配回测系统的需求
        dividend_data = dividend_data.rename(columns={
            'ann_date': 'announcement_date',      # 公告日期
            'end_date': 'dividend_year',          # 分红年度
            'div_proc': 'process_status',         # 实施进度
            'stk_div': 'total_share_ratio',       # 每股送转总比例
            'stk_bo_rate': 'share_ratio',         # 每股送股比例（送股）
            'stk_co_rate': 'share_trans_ratio',   # 每股转增比例（转增）
            'cash_div': 'cash_div_after_tax',     # 每股分红（税后）
            'cash_div_tax': 'cash_div_before_tax',# 每股分红（税前）
            'record_date': 'record_date',         # 股权登记日
            'ex_date': 'ex_div_date',             # 除权除息日
            'pay_date': 'cash_pay_date'           # 派息日
        })
        
        # 处理空值
        # 送股比例和转增比例默认为0
        dividend_data['share_ratio'] = dividend_data['share_ratio'].fillna(0)
        dividend_data['share_trans_ratio'] = dividend_data['share_trans_ratio'].fillna(0)
        dividend_data['total_share_ratio'] = dividend_data['total_share_ratio'].fillna(0)
        
        # 现金分红默认为0
        dividend_data['cash_div_after_tax'] = dividend_data['cash_div_after_tax'].fillna(0)
        dividend_data['cash_div_before_tax'] = dividend_data['cash_div_before_tax'].fillna(0)
        
        # 计算每股分红（用于回测）
        # Tushare 的 cash_div_tax 是每股分红，但回测系统可能需要每10股分红
        # 这里保存每股分红，回测时可以根据持仓数量计算
        # 注意：Tushare 返回的是每股分红，需要根据实际情况调整
        dividend_data['unit_cash_div_tax'] = dividend_data['cash_div_before_tax']
        
        # 选择需要的字段
        desired_columns = [
            'symbol',                   # 股票代码
            'announcement_date',        # 公告日期
            'dividend_year',            # 分红年度
            'process_status',           # 实施进度
            'share_ratio',              # 送股比例
            'share_trans_ratio',        # 转增比例
            'total_share_ratio',        # 总送转比例
            'cash_div_before_tax',      # 每股分红（税前）
            'cash_div_after_tax',       # 每股分红（税后）
            'unit_cash_div_tax',        # 单位现金分红（用于回测）
            'record_date',              # 股权登记日
            'ex_div_date',              # 除权除息日
            'cash_pay_date'             # 派息日
        ]
        
        dividend_data = dividend_data[desired_columns]
        
        # 过滤掉北交所的股票
        dividend_data = dividend_data[~dividend_data['symbol'].str.contains('BJ', na=False)]
        
        # 只保留有除权除息日的记录（回测需要）
        dividend_data = dividend_data[dividend_data['ex_div_date'].notna()]
        
        logger.info(f"After filtering: {len(dividend_data)} records to be stored")
        
        if dividend_data.empty:
            logger.info("No valid dividend data to store after filtering")
            return
        
        # 确保集合存在并创建索引
        ensure_collection_and_indexes(table_name='stock_dividends')
        
        # 准备批量更新操作
        upsert_operations = []
        for record in dividend_data.to_dict('records'):
            # 使用 symbol 和 ex_div_date 作为唯一键
            upsert_operations.append(UpdateOne(
                {
                    'symbol': record['symbol'],
                    'ex_div_date': record['ex_div_date']
                },
                {'$set': record},
                upsert=True
            ))
        
        # 批量写入数据库
        if upsert_operations:
            result = self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_dividends'].bulk_write(
                upsert_operations
            )
            logger.info(
                f"Successfully upserted dividend data: "
                f"matched={result.matched_count}, "
                f"modified={result.modified_count}, "
                f"upserted={result.upserted_count}"
            )
        else:
            logger.info("No dividend data to upsert")

