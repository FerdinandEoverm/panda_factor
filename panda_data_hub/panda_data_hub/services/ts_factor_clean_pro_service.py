from abc import ABC
from pymongo import UpdateOne
import traceback

import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
import threading
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import  ensure_collection_and_indexes
from panda_data_hub.utils.ts_utils import get_tushare_suffix
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client


class FactorCleanerTSProService(ABC):

    def __init__(self,config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.progress_callback = None
        
        # 初始化全局 tushare 客户端
        init_tushare_client(config)
        self.pro = get_tushare_client()
        
        # 创建锁以序列化 tushare API 调用，避免并发连接超限
        self.tushare_lock = threading.Lock()

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def clean_history_data(self, start_date, end_date):
        """补全历史数据"""
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        trading_days = []
        for date in date_range:
            date_str = datetime.strftime(date, "%Y-%m-%d")
            trading_days.append(date_str)
        total_days = len(trading_days)
        processed_days = 0
        with tqdm(total=len(trading_days), desc="Processing Trading Days") as pbar:
            # 使用单线程处理，避免并发导致 tushare 连接超限
            # tushare 免费版本最多允许 2 个 IP 连接，并发调用会导致超限
            for date in trading_days:
                try:
                    self.clean_daily_data(date_str=date, pbar=pbar)
                    processed_days += 1
                    progress = int((processed_days / total_days) * 100)

                    # 更新进度
                    if self.progress_callback:
                        self.progress_callback(progress)
                    pbar.update(1)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    pbar.update(1)  # 即使任务失败也更新进度条

                # 每次请求后添加短暂延迟，避免 API 限流
                time.sleep(0.5)
        logger.info("因子数据清洗全部完成！！！")

    def clean_daily_data(self, date_str, pbar):
        """补全当日数据(历史循环补充)"""
        try:
            date = date_str.replace('-', '')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], 'stock_market', query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return

            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol', 'open','high','low','close','volume']]
            data['ts_code'] = data['symbol'].apply(get_tushare_suffix)

            logger.info("正在获取市值和换手率数据......")
            # 使用锁序列化 tushare API 调用，避免并发连接超限
            with self.tushare_lock:
                factor_data = self.pro.query('daily_basic', trade_date=date,fields=['ts_code','turnover_rate','total_mv'])
            temp_data = data.merge(factor_data[['ts_code','turnover_rate','total_mv']], on='ts_code', how='left')
            temp_data = temp_data.rename(columns={'total_mv': 'market_cap'})
            temp_data = temp_data.rename(columns={'turnover_rate': 'turnover'})
            logger.info("正在获取成交额数据......")
            # 使用锁序列化 tushare API 调用，避免并发连接超限
            with self.tushare_lock:
                price_data = self.pro.query("daily", trade_date=date, fields=['ts_code', 'amount'])
            result_data = temp_data.merge(price_data[['ts_code', 'amount']], on='ts_code', how='left')
            result_data = result_data.drop(columns=['ts_code'])
            # tushare的成交额是以千元为单位的
            result_data['amount'] = result_data['amount']*1000
            result_data['market_cap'] = result_data['market_cap'] * 10000
            desired_order = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'market_cap', 'turnover','amount']
            result_data = result_data[desired_order]

            ensure_collection_and_indexes(table_name='factor_base')
            upsert_operations = []
            for record in result_data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))
            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['factor_base'].bulk_write(
                    upsert_operations)
                logger.info(f"Successfully upserted factor data for date: {date}")


        except Exception as e:
            error_msg = f"Failed to process market data for quarter : {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise
