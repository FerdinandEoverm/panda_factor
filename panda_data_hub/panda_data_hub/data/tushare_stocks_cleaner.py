import traceback
from abc import ABC
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_common.utils.stock_utils import get_exchange_suffix
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client


class TSStockCleaner(ABC):

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)

        # 初始化全局 tushare 客户端
        init_tushare_client(config)
        self.pro = get_tushare_client()

    def clean_metadata(self):
        try:
            logger.info("Starting metadata cleaning for Tushare")
            basic_col = ['ts_code', 'name', 'list_date', 'delist_date']
            stocks = self.pro.query('stock_basic', list_status='L,D', fields=basic_col)
            stocks = stocks[stocks["name"] != "UNKNOWN"]
            stocks['symbol'] = stocks['ts_code'].apply(get_exchange_suffix)
            stocks = stocks.drop(columns=['ts_code'])
            stocks['expired'] = False
            desired_order = ['symbol', 'name', 'list_date', 'delist_date', 'expired']
            stocks = stocks[desired_order]

            logger.info("Updating MongoDB stocks collection")
            # 清空现有的stocks集合
            self.db_handler.mongo_delete(self.config["MONGO_DB"], 'stocks', {})
            # 将处理后的股票数据批量插入到MongoDB
            self.db_handler.mongo_insert_many(
                self.config["MONGO_DB"],
                'stocks',
                stocks.to_dict('records')
            )
            logger.info("Successfully updated stocks metadata")
        except Exception as e:
            # 错误处理：记录详细的错误信息和堆栈跟踪
            error_msg = f"Failed to clean metadata: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise  # 重新抛出异常，让上层代码处理

