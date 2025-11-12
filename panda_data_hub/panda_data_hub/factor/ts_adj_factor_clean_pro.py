import traceback
from abc import ABC
from datetime import datetime

import pandas as pd
from pymongo import UpdateOne

from panda_common.handlers.database_handler import DatabaseHandler
import tushare as ts

from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.ts_utils import get_tushare_suffix


class TSAdjFactorCleaner(ABC):

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
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
