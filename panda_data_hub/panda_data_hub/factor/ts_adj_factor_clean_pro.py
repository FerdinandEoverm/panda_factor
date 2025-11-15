import traceback
from abc import ABC
from datetime import datetime

import pandas as pd
from pymongo import UpdateOne

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client
from panda_data_hub.utils.ts_utils import get_tushare_suffix


class TSAdjFactorCleaner(ABC):

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        
        # 初始化全局 tushare 客户端
        init_tushare_client(config)
        self.pro = get_tushare_client()

    def clean_daily_adj_factor(self):
        try:
            date = datetime.now().strftime('%Y%m%d')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], 'stock_market', query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return

            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol']]
            data['ts_code'] = data['symbol'].apply(get_tushare_suffix)

            logger.info("正在获取复权因子数据......")
            adj_factor_data = self.pro.query("adj_factor", trade_date=date, fields=['ts_code', 'adj_factor'])
            result_data = data.merge(adj_factor_data[['ts_code', 'adj_factor']], on='ts_code', how='left')
            result_data = result_data.drop(columns=['ts_code'])

            # 确保集合和索引存在
            ensure_collection_and_indexes(table_name='adj_factor')

            upsert_operations = []
            for record in result_data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))

            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['adj_factor'].bulk_write(
                    upsert_operations)
                logger.info(f"Successfully upserted adj_factor data for date: {date}")

        except Exception as e:
            error_msg = f"Failed to process adj_factor data for date {date}: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise
