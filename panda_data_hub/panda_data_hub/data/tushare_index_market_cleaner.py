from abc import ABC
from pymongo import UpdateOne
import traceback
from datetime import datetime, timedelta
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client


class TSIndexMarketCleaner(ABC):
    """
    指数行情数据清洗器
    负责从 Tushare 获取主要指数的日线行情数据并入库
    """

    # 主要指数列表
    MAJOR_INDICES = [
        {'ts_code': '000001.SH', 'name': '上证指数'},
        {'ts_code': '399001.SZ', 'name': '深证成指'},
        {'ts_code': '000300.SH', 'name': '沪深300'},
        {'ts_code': '000905.SH', 'name': '中证500'},
        {'ts_code': '000852.SH', 'name': '中证1000'},
        {'ts_code': '399006.SZ', 'name': '创业板指'},
        {'ts_code': '000688.SH', 'name': '科创50'},
    ]

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        # 按需初始化全局 tushare 客户端
        init_tushare_client(config)
        self.pro = get_tushare_client()

    def index_market_clean_daily(self):
        """
        每日指数行情数据清洗入口
        """
        logger.info("Starting index market data cleaning for tushare")
        # 先判断是否为交易日
        date_str = datetime.now().strftime("%Y%m%d")
        if self.is_trading_day(date_str):
            self.clean_index_market_data(date_str)
        else:
            logger.info(f"跳过非交易日: {date_str}")

    def clean_index_market_data(self, date_str):
        """
        清洗指定日期的指数行情数据

        参数:
            date_str: 日期字符串，格式为 "YYYYMMDD" 或 "YYYY-MM-DD"
        """
        try:
            date = date_str.replace("-", "")
            logger.info(f"开始清洗指数行情数据: {date}")

            # 确保集合和索引存在
            ensure_collection_and_indexes(table_name='index_market')

            upsert_operations = []
            success_count = 0
            fail_count = 0

            # 逐个获取主要指数的行情数据
            for index_info in self.MAJOR_INDICES:
                ts_code = index_info['ts_code']
                index_name = index_info['name']

                try:
                    # 获取指数日线数据
                    df = self.pro.index_daily(ts_code=ts_code, trade_date=date)

                    if df is not None and not df.empty:
                        # 处理数据
                        for _, row in df.iterrows():
                            record = {
                                'date': row['trade_date'],
                                'symbol': row['ts_code'],
                                'name': index_name,
                                'open': float(row['open']) if row['open'] is not None else None,
                                'high': float(row['high']) if row['high'] is not None else None,
                                'low': float(row['low']) if row['low'] is not None else None,
                                'close': float(row['close']) if row['close'] is not None else None,
                                'pre_close': float(row['pre_close']) if row['pre_close'] is not None else None,
                                'volume': float(row['vol']) * 100 if row['vol'] is not None else None,  # 转换为手
                            }

                            # 添加到批量操作
                            upsert_operations.append(UpdateOne(
                                {'date': record['date'], 'symbol': record['symbol']},
                                {'$set': record},
                                upsert=True
                            ))

                        success_count += 1
                        logger.info(f"成功获取指数行情: {index_name} ({ts_code}) - {date}")
                    else:
                        logger.warning(f"未获取到指数数据: {index_name} ({ts_code}) - {date}")
                        fail_count += 1

                except Exception as e:
                    logger.error(f"获取指数行情失败: {index_name} ({ts_code}) - {date}: {str(e)}")
                    fail_count += 1
                    continue

            # 批量写入数据库
            if upsert_operations:
                result = self.db_handler.mongo_client[self.config["MONGO_DB"]]['index_market'].bulk_write(
                    upsert_operations
                )
                logger.info(f"成功写入指数行情数据 - 日期: {date}, 成功: {success_count}, 失败: {fail_count}, "
                            f"插入: {result.upserted_count}, 更新: {result.modified_count}")
            else:
                logger.warning(f"无指数行情数据需要写入 - 日期: {date}")

        except Exception as e:
            logger.error(f"清洗指数行情数据失败 - 日期: {date_str}: {str(e)}\n{traceback.format_exc()}")

    def clean_index_market_history(self, start_date=None, end_date=None):
        """
        补全历史指数行情数据

        参数:
            start_date: 开始日期，格式 "YYYYMMDD" 或 "YYYY-MM-DD"，默认为2005-01-01
            end_date: 结束日期，格式 "YYYYMMDD" 或 "YYYY-MM-DD"，默认为当前日期
        """
        try:
            # 设置默认日期范围
            if start_date is None:
                start_date = "20050101"  # 默认从2005年开始
            else:
                start_date = start_date.replace("-", "")

            if end_date is None:
                end_date = datetime.now().strftime("%Y%m%d")
            else:
                end_date = end_date.replace("-", "")

            logger.info(f"开始补全指数历史行情数据: {start_date} 至 {end_date}")

            # 删除旧数据，重新开始
            try:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['index_market'].drop()
                logger.info("已删除旧的指数行情数据集合")
            except Exception as e:
                logger.warning(f"删除旧数据集合时出现警告: {str(e)}")

            # 确保集合和索引存在
            ensure_collection_and_indexes(table_name='index_market')

            total_success = 0
            total_fail = 0

            # 逐个处理每个指数
            for index_info in self.MAJOR_INDICES:
                ts_code = index_info['ts_code']
                index_name = index_info['name']

                try:
                    logger.info(f"开始获取指数历史数据: {index_name} ({ts_code})")

                    # 获取指数历史数据
                    df = self.pro.index_daily(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if df is not None and not df.empty:
                        upsert_operations = []

                        # 处理数据
                        for _, row in df.iterrows():
                            record = {
                                'date': row['trade_date'],
                                'symbol': row['ts_code'],
                                'name': index_name,
                                'open': float(row['open']) if row['open'] is not None else None,
                                'high': float(row['high']) if row['high'] is not None else None,
                                'low': float(row['low']) if row['low'] is not None else None,
                                'close': float(row['close']) if row['close'] is not None else None,
                                'pre_close': float(row['pre_close']) if row['pre_close'] is not None else None,
                                'volume': float(row['vol']) * 100 if row['vol'] is not None else None,
                            }

                            upsert_operations.append(UpdateOne(
                                {'date': record['date'], 'symbol': record['symbol']},
                                {'$set': record},
                                upsert=True
                            ))

                        # 批量写入
                        if upsert_operations:
                            result = self.db_handler.mongo_client[self.config["MONGO_DB"]]['index_market'].bulk_write(
                                upsert_operations
                            )
                            logger.info(f"成功写入指数历史数据: {index_name} ({ts_code}) - "
                                        f"记录数: {len(df)}, 插入: {result.upserted_count}, 更新: {result.modified_count}")
                            total_success += len(df)
                    else:
                        logger.warning(f"未获取到指数历史数据: {index_name} ({ts_code})")
                        total_fail += 1

                except Exception as e:
                    logger.error(f"获取指数历史数据失败: {index_name} ({ts_code}): {str(e)}")
                    total_fail += 1
                    continue

            logger.info(f"指数历史数据补全完成 - 成功记录: {total_success}, 失败指数: {total_fail}")

        except Exception as e:
            logger.error(f"补全指数历史数据失败: {str(e)}\n{traceback.format_exc()}")

    def is_trading_day(self, date):
        """
        判断传入的日期是否为股票交易日

        参数:
            date: 日期字符串，格式为 "YYYYMMDD" 或 "YYYY-MM-DD"

        返回:
            bool: 如果是交易日返回 True，否则返回 False
        """
        try:
            # 获取指定日期的交易日历信息
            cal_df = self.pro.query('trade_cal',
                                    exchange='SSE',
                                    start_date=date.replace('-', ''),
                                    end_date=date.replace('-', ''))
            # 检查是否有数据返回及该日期是否为交易日(is_open=1表示交易日)
            if not cal_df.empty and cal_df.iloc[0]['is_open'] == 1:
                return True
            return False
        except Exception as e:
            logger.error(f"检查交易日失败 {date}: {str(e)}")
            return False

