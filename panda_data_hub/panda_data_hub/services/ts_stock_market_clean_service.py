from abc import ABC


import tushare as ts
from pymongo import UpdateOne
import traceback

import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_common.utils.stock_utils import get_exchange_suffix
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.ts_utils import (
    calculate_upper_limit,
    ts_is_trading_day,
    get_previous_month_dates,
    calculate_lower_limit,
    TushareTokenError,
    validate_tushare_token
)
from panda_data_hub.services.ts_namechange_clean_service import TSNamechangeCleanService

"""
       使用须知：因tushare对于接口返回数据条数具有严格限制，故无法一次拉取全量数据。此限制会导致接口运行效率偏低，请耐心等待。

       参数:
       date: 日期字符串，格式为 "YYYY-MM-DD"

       返回:
       bool: 如果是交易日返回 True，否则返回 False
       """


class StockMarketCleanTSServicePRO(ABC):
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.namechange_service = TSNamechangeCleanService(config)
        self.progress_callback = None
        try:
            # 检查 TS_TOKEN 是否存在
            ts_token = config.get('TS_TOKEN')
            if not ts_token:
                raise ValueError(
                    "TS_TOKEN 未配置。请在配置文件 panda_common/config.yaml 中设置 TS_TOKEN。\n"
                    "您可以在 https://tushare.pro/ 注册并获取 Token。"
                )
            ts.set_token(ts_token)
            self.pro = ts.pro_api()
        except Exception as e:
            error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def check_trading_day_data_completeness(self, date_str):
        """
        检查指定交易日的数据是否已存在且完整
        
        参数:
            date_str: 日期字符串，格式为 "YYYY-MM-DD"
        
        返回:
            bool: 数据完整返回 True，否则返回 False
        
        智能判断标准：
            - 该交易日有数据记录
            - 如果过去5个交易日数据充足，使用动态阈值：平均股票数 - 200
            - 如果过去交易日不足5个，使用固定阈值：3000
        """
        try:
            # 转换日期格式为数据库存储格式 YYYYMMDD
            date_formatted = date_str.replace("-", "")
            
            # 查询该交易日的数据数量
            collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_market']
            count = collection.count_documents({'date': date_formatted})
            
            # 获取过去5个交易日的股票数量，用于动态阈值计算
            previous_counts = []
            try:
                # 获取当前日期之前的交易日数据
                pipeline = [
                    {'$match': {'date': {'$lt': date_formatted}}},
                    {'$group': {'_id': '$date', 'count': {'$sum': 1}}},
                    {'$sort': {'_id': -1}},
                    {'$limit': 5}
                ]
                results = list(collection.aggregate(pipeline))
                previous_counts = [item['count'] for item in results]
            except Exception as e:
                logger.warning(f"获取过去交易日数据时出错: {str(e)}，将使用固定阈值")
            
            # 计算动态阈值
            FIXED_MIN_COUNT = 3000  # 固定最小阈值
            TOLERANCE = 200  # 允许的波动范围
            
            if len(previous_counts) >= 5:
                # 有足够的历史数据，使用动态阈值
                avg_count = sum(previous_counts) / len(previous_counts)
                min_threshold = int(avg_count - TOLERANCE)
                # 确保阈值不低于固定最小值
                min_threshold = max(min_threshold, FIXED_MIN_COUNT)
                threshold_type = "动态"
                logger.debug(f"过去5个交易日平均股票数: {avg_count:.0f}，动态阈值: {min_threshold}")
            else:
                # 历史数据不足，使用固定阈值
                min_threshold = FIXED_MIN_COUNT
                threshold_type = "固定"
                logger.debug(f"历史交易日数据不足({len(previous_counts)}个)，使用固定阈值: {min_threshold}")
            
            is_complete = count >= min_threshold
            
            if is_complete:
                logger.info(f"交易日 {date_str} 数据已存在且完整 (股票数: {count}, {threshold_type}阈值: {min_threshold})")
            else:
                logger.info(f"交易日 {date_str} 数据不完整或缺失 (股票数: {count}, {threshold_type}阈值: {min_threshold})")
            
            return is_complete
            
        except Exception as e:
            logger.error(f"检查交易日 {date_str} 数据完整性时出错: {str(e)}")
            # 出错时保守处理，返回 False 以触发重新清洗
            return False

    def stock_market_history_clean(self, start_date, end_date, force_update=False):

        logger.info("Starting market data cleaning for tushare")
        logger.info(f"强制更新模式: {'是' if force_update else '否'}")
        
        # 初始化进度信息
        if self.progress_callback:
            self.progress_callback({
                "progress_percent": 0,
                "current_task": "验证 Tushare Token...",
                "processed_count": 0,
                "total_count": 0,
                "status": "running"
            })
        
        # 在开始前验证 token（使用已有的连接，避免创建新连接）
        logger.info("验证 Tushare Token...")
        is_valid, error_message = validate_tushare_token(self.pro)
        if not is_valid:
            logger.error(f"Tushare Token 验证失败: {error_message}")
            if self.progress_callback:
                self.progress_callback({
                    "progress_percent": 0,
                    "current_task": "Token 验证失败",
                    "error_message": error_message,
                    "status": "error",
                    "processed_count": 0,
                    "total_count": 0
                })
            raise TushareTokenError(error_message)
        
        logger.info("Tushare Token 验证成功，开始数据清洗")
        
        # 转换日期格式：20250930 -> 2025-09-30
        if len(start_date) == 8 and start_date.isdigit():
            start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        if len(end_date) == 8 and end_date.isdigit():
            end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        
        logger.info(f"日期范围: {start_date} 至 {end_date}")

        # 更新进度
        if self.progress_callback:
            self.progress_callback({
                "current_task": "获取交易日列表...",
            })

        # 获取交易日
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        all_trading_days = []
        try:
            for date in date_range:
                date_str = datetime.strftime(date, "%Y-%m-%d")
                if ts_is_trading_day(date_str, self.pro):
                    all_trading_days.append(date_str)
                else:
                    logger.info(f"跳过非交易日: {date_str}")
        except TushareTokenError as e:
            # 捕获 token 错误并传递给前端
            error_msg = f"Tushare Token 错误：{str(e)}\n\n请检查您的 Tushare Token 配置。"
            logger.error(error_msg)
            if self.progress_callback:
                self.progress_callback({
                    "progress_percent": 0,
                    "current_task": "Token 错误",
                    "error_message": error_msg,
                    "status": "error",
                    "processed_count": 0,
                    "total_count": 0
                })
            raise
        
        logger.info(f"找到 {len(all_trading_days)} 个交易日")
        
        # 检查已存在的完整数据（如果不是强制更新模式）
        trading_days = []
        skipped_days = []
        force_update = True
        if force_update:
            logger.info("强制更新模式：将处理所有交易日")
            trading_days = all_trading_days
        else:
            logger.info("智能跳过模式：检查已存在的完整数据...")
            for date_str in all_trading_days:
                if self.check_trading_day_data_completeness(date_str):
                    skipped_days.append(date_str)
                else:
                    trading_days.append(date_str)
            
            logger.info(f"已跳过 {len(skipped_days)} 个已完整的交易日")
            logger.info(f"需要处理 {len(trading_days)} 个交易日")
            
            if skipped_days:
                logger.info(f"跳过的交易日: {', '.join(skipped_days[:5])}{'...' if len(skipped_days) > 5 else ''}")
        
        total_days = len(trading_days)
        processed_days = 0
        
        # 初始化进度信息
        if self.progress_callback:
            skip_info = f"（已跳过 {len(skipped_days)} 个完整交易日）" if skipped_days else ""
            self.progress_callback({
                "progress_percent": 0,
                "current_task": f"准备开始处理交易日数据{skip_info}",
                "processed_count": 0,
                "total_count": total_days,
                "current_date": "",
                "batch_info": f"总共 {len(all_trading_days)} 个交易日，需要处理 {total_days} 个{skip_info}",
                "trading_days_processed": 0,
                "trading_days_total": total_days,
            })
        # 如果没有需要处理的交易日，直接返回
        if total_days == 0:
            logger.info("所有交易日数据已完整，无需处理")
            if self.progress_callback:
                self.progress_callback({
                    "progress_percent": 100,
                    "current_task": "所有数据已存在且完整",
                    "processed_count": 0,
                    "total_count": 0,
                    "current_date": "",
                    "batch_info": f"总共 {len(all_trading_days)} 个交易日均已完整，已全部跳过",
                    "trading_days_processed": 0,
                    "trading_days_total": 0,
                    "status": "completed"
                })
            return
        
        # 根据交易日去循环
        with tqdm(total=len(trading_days), desc="Processing Trading Days") as pbar:
            # 分批处理，每批8天
            batch_size = 8
            total_batches = (len(trading_days) - 1) // batch_size + 1
            
            for i in range(0, len(trading_days), batch_size):
                current_batch = i // batch_size + 1
                batch_days = trading_days[i:i + batch_size]
                
                # 更新批次进度信息
                if self.progress_callback:
                    self.progress_callback({
                        "current_task": f"正在处理第 {current_batch}/{total_batches} 批次数据",
                        "batch_info": f"批次 {current_batch}/{total_batches} - 处理 {len(batch_days)} 个交易日",
                        "current_date": f"{batch_days[0]} 到 {batch_days[-1]}",
                        "trading_days_processed": processed_days,
                        "trading_days_total": total_days,
                    })
                
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = []
                    for date in batch_days:
                        futures.append(executor.submit(
                            self.clean_meta_market_data,
                            date_str=date
                        ))

                    # 等待当前批次的所有任务完成
                    for future_idx, future in enumerate(futures):
                        try:
                            future.result()
                            processed_days += 1
                            progress = int((processed_days / total_days) * 100)
                            
                            current_date = batch_days[future_idx] if future_idx < len(batch_days) else ""
                            
                            # 更新详细进度
                            if self.progress_callback:
                                self.progress_callback({
                                    "progress_percent": progress,
                                    "current_task": f"正在处理交易日数据",
                                    "processed_count": processed_days,
                                    "total_count": total_days,
                                    "current_date": current_date,
                                    "batch_info": f"批次 {current_batch}/{total_batches} - 已完成 {processed_days}/{total_days} 个交易日",
                                    "trading_days_processed": processed_days,
                                    "trading_days_total": total_days,
                                })
                            pbar.update(1)
                            logger.info(f"完成处理交易日: {current_date} ({processed_days}/{total_days})")
                            
                        except Exception as e:
                            processed_days += 1
                            current_date = batch_days[future_idx] if future_idx < len(batch_days) else ""
                            logger.error(f"处理交易日 {current_date} 失败: {e}")
                            
                            # 即使失败也更新进度
                            if self.progress_callback:
                                progress = int((processed_days / total_days) * 100)
                                self.progress_callback({
                                    "progress_percent": progress,
                                    "current_task": f"处理交易日 {current_date} 时出现错误",
                                    "processed_count": processed_days,
                                    "total_count": total_days,
                                    "current_date": current_date,
                                    "error_message": f"处理 {current_date} 失败: {str(e)[:100]}...",
                                })
                            pbar.update(1)

                # 批次之间添加短暂延迟，避免连接数超限
                if i + batch_size < len(trading_days):
                    logger.info(f"完成批次 {current_batch}/{total_batches}，等待10秒后继续...")
                    if self.progress_callback:
                        self.progress_callback({
                            "current_task": f"批次间等待 - 已完成 {current_batch}/{total_batches} 批次",
                            "batch_info": f"等待10秒后处理下一批次...",
                            "trading_days_processed": processed_days,
                            "trading_days_total": total_days,
                        })
                    time.sleep(10)
        
        logger.info("所有交易日数据处理完成")
        
        # 发送完成状态
        if self.progress_callback:
            skip_info = f"，跳过 {len(skipped_days)} 个已完整的交易日" if skipped_days else ""
            self.progress_callback({
                "progress_percent": 100,
                "current_task": "数据清洗已完成",
                "processed_count": total_days,
                "total_count": total_days,
                "current_date": "",
                "batch_info": f"成功处理了 {total_days} 个交易日的数据{skip_info}",
                "trading_days_processed": total_days,
                "trading_days_total": total_days,
                "status": "completed"
            })

    def clean_meta_market_data(self,date_str):
        try:
            date = date_str.replace("-", "")
            #  获取当日股票的历史行情
            price_data = self.pro.query('daily', trade_date=date)
            # 重置股票行情数据索引
            price_data.reset_index(drop=False, inplace=True)
            
            total_stocks = len(price_data)
            logger.info(f"开始处理 {date_str} 的数据，共 {total_stocks} 只股票")
            if self.progress_callback:
                self.progress_callback({
                    "stock_phase": "index_component",
                    "stock_processed": 0,
                    "stock_total": int(total_stocks),
                    "stock_progress_percent": 0,
                    "last_message": f"开始处理 {date_str} 的数据，共 {int(total_stocks)} 只股票"
                })
            
            # 洗 index_components列
            price_data['index_component'] = None

            # tushare关于中证500和中证1000这两个指数只有每月的最后一个交易日才有数据，对于沪深300成分股是每月的第一个交易日和最后一个交易日才有数据
            # 根据日期获取当月三个指数的
            mid_date,last_date = get_previous_month_dates(date_str = date)
            # 沪深300
            hs_300 = self.pro.query('index_weight', index_code='399300.SZ', start_date=mid_date, end_date=last_date)
            # 中证500
            zz_500 = self.pro.query('index_weight', index_code='000905.SH', start_date=mid_date, end_date=last_date)
            # 中证1000
            zz_1000 = self.pro.query('index_weight', index_code='000852.SH', start_date=mid_date, end_date=last_date)
            
            processed_stocks = 0
            for idx, row in price_data.iterrows():
                try:
                    component = self.clean_index_components(data_symbol=row['ts_code'], date=date,hs_300 =hs_300,zz_500 = zz_500,zz_1000 = zz_1000)
                    price_data.at[idx, 'index_component'] = component
                    processed_stocks += 1
                    
                    # 每处理100只股票记录一次进度（避免日志过多）
                    if processed_stocks % 100 == 0:
                        logger.info(f"处理指数成分股进度: {processed_stocks}/{total_stocks} ({date_str})")
                        if self.progress_callback:
                            self.progress_callback({
                                "stock_phase": "index_component",
                                "stock_processed": int(processed_stocks),
                                "stock_total": int(total_stocks),
                                "stock_progress_percent": int(processed_stocks/ max(1,total_stocks) * 100),
                                "last_message": f"指数成分: {int(processed_stocks)}/{int(total_stocks)} ({date_str})"
                            })
                except Exception as e:
                    logger.error(f"Failed to clean index for {row['ts_code']} on {date}: {str(e)}")
                    continue
            
            logger.info(f"完成指数成分股清洗: {processed_stocks}/{total_stocks} ({date_str})")
            if self.progress_callback:
                self.progress_callback({
                    "stock_phase": "name_clean",
                    "stock_processed": 0,
                    "stock_total": int(total_stocks),
                    "stock_progress_percent": 0,
                    "last_message": f"开始清洗股票名称 ({date_str})"
                })
            # 洗name列
            # 报错ERROR - Error checking if ****** on date: single positional indexer is out-of-bounds，说明该股票已经退市
            price_data['name'] = None
            # 从MongoDB获取历史名称变更信息（已预先同步，避免每次调用API）
            namechange_info = self.namechange_service.get_namechange_data(end_date=date)
            #获取目前所有股票的名称
            stock_info = self.pro.query('stock_basic')
            
            processed_names = 0
            for idx, row in price_data.iterrows():
                try:
                    stock_name = self.clean_stock_name(data_symbol=row['ts_code'], date=date,namechange_info = namechange_info,stock_info = stock_info)
                    price_data.at[idx, 'name'] = stock_name
                    processed_names += 1
                    
                    # 每处理100只股票记录一次进度
                    if processed_names % 100 == 0:
                        logger.info(f"处理股票名称进度: {processed_names}/{total_stocks} ({date_str})")
                        if self.progress_callback:
                            self.progress_callback({
                                "stock_phase": "name_clean",
                                "stock_processed": int(processed_names),
                                "stock_total": int(total_stocks),
                                "stock_progress_percent": int(processed_names/ max(1,total_stocks) * 100),
                                "last_message": f"名称清洗: {int(processed_names)}/{int(total_stocks)} ({date_str})"
                            })
                except Exception as e:
                    logger.error(f"Failed to clean name for {row['ts_code']} on {date}: {str(e)}")
                    continue
            
            logger.info(f"完成股票名称清洗: {processed_names}/{total_stocks} ({date_str})")
            price_data = price_data.drop(columns=['index','change','pct_chg','amount'])
            price_data = price_data.rename(columns={'vol': 'volume'})
            price_data = price_data.rename(columns={'trade_date': 'date'})
            price_data['ts_code'] = price_data['ts_code'].apply(get_exchange_suffix)
            price_data = price_data.rename(columns={'ts_code': 'symbol'})

            # 计算涨跌停价格时对于已经退市的股票，因无法获取当日股票名称，故无法计算涨跌停价格
            price_data['limit_up'] = None
            price_data['limit_down'] = None
            price_data['limit_up'] = price_data.apply(
                lambda row: calculate_upper_limit(stock_code = row['symbol'], prev_close = row['pre_close'], stock_name = row['name']),
                axis=1
            )
            price_data['limit_down'] = price_data.apply(
                lambda row: calculate_lower_limit(stock_code = row['symbol'], prev_close = row['pre_close'], stock_name = row['name']),
                axis=1
            )
            price_data['volume'] = price_data['volume']*100
            # 过滤掉北交所的股票
            price_data = price_data[~price_data['symbol'].str.contains('BJ')]
            final_stock_count = len(price_data)
            logger.info(f"过滤北交所后剩余 {final_stock_count} 只股票 ({date_str})")
            if self.progress_callback:
                self.progress_callback({
                    "stock_phase": "db_write",
                    "stock_processed": 0,
                    "stock_total": int(final_stock_count),
                    "stock_progress_percent": 0,
                    "last_message": f"写入数据库: 待写入 {int(final_stock_count)} 只股票 ({date_str})"
                })
            
            #重新排列
            desired_order = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'pre_close',
                             'limit_up', 'limit_down', 'index_component', 'name']
            price_data = price_data[desired_order]
            # 检索数据库索引
            ensure_collection_and_indexes(table_name='stock_market')
            # 执行插入操作
            upsert_operations = []
            for record in price_data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))
            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_market'].bulk_write(
                    upsert_operations)
                logger.info(f"成功写入数据库: {final_stock_count} 只股票的数据 ({date_str})")
                if self.progress_callback:
                    self.progress_callback({
                        "db_write_count": int(final_stock_count),
                        "stock_phase": "db_write",
                        "stock_processed": int(final_stock_count),
                        "stock_total": int(final_stock_count),
                        "stock_progress_percent": 100,
                        "last_message": f"成功写入数据库: {int(final_stock_count)} 只股票的数据 ({date_str})"
                    })

        except Exception as e:
            logger.error({e})

    def clean_index_components(self, date,data_symbol, hs_300,zz_500 ,zz_1000):
        try:
            # 首先查询沪深300
            if data_symbol in hs_300['con_code'].values:
                return '100'

            # 如果不在沪深300中，查询中证500
            if data_symbol in zz_500['con_code'].values:
                return '010'

            # 如果不在中证500中，查询中证1000
            if data_symbol in zz_1000['con_code'].values:
                return '001'

            # 如果都不在其中
            return '000'

        except Exception as e:
            logger.error(f"Error checking if {data_symbol} is in index components on {date}: {str(e)}")
            return None

    def clean_stock_name(self, data_symbol, date,namechange_info,stock_info):
        try:
            # 获取某只股票的换名历史
            valid_changes = namechange_info[(namechange_info['ann_date'] <= date) &(namechange_info['symbol'] == data_symbol)]
            if not valid_changes.empty:
                # 按开始日期排序，获取最新的变更记录
                latest_change = valid_changes.sort_values('ann_date', ascending=False).iloc[0]
                latest_symbol = latest_change['name']
                return latest_symbol
            else:
                current_info = stock_info[stock_info['ts_code'] == data_symbol]
                current_name = current_info['name'].iloc[0]
                return current_name

        except Exception as e:
            logger.error(f"Error checking if {data_symbol} on {date}: {str(e)}")
            return None  # 或者返回其他默认值



