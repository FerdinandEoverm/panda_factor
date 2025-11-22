import traceback
from datetime import datetime
from pymongo import UpdateOne
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.tushare_client import init_tushare_client, get_tushare_client

class TSStockInfoCleaner:
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        # 按需初始化全局 tushare 客户端
        init_tushare_client(config)
        self.pro = get_tushare_client()
        self.progress_callback = None

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def clean_stock_info_daily(self):
        """
        每日清洗 stock_info_new 表
        包含:
        1. 股票 (Type 0)
        2. 指数 (Type 1)
        3. ETF (Type 2)
        """
        logger.info("Starting stock info cleaning for tushare")
        if self.progress_callback:
            self.progress_callback({
                "progress_percent": 0,
                "status": "running",
                "current_task": "开始清洗股票基础信息...",
                "processed_count": 0,
                "total_count": 3 # Stocks, ETFs, Indices
            })
            
        try:
            self._clean_stocks()
            if self.progress_callback:
                self.progress_callback({"progress_percent": 33, "current_task": "股票信息清洗完成"})
                
            self._clean_etfs()
            if self.progress_callback:
                self.progress_callback({"progress_percent": 66, "current_task": "ETF信息清洗完成"})
                
            self._clean_indices()
            if self.progress_callback:
                self.progress_callback({
                    "progress_percent": 100, 
                    "status": "completed",
                    "current_task": "所有基础信息清洗完成"
                })
                
            logger.info("Finished stock info cleaning")
        except Exception as e:
            logger.error(f"Failed to clean stock info: {str(e)}")
            logger.error(traceback.format_exc())
            if self.progress_callback:
                self.progress_callback({
                    "status": "error",
                    "error_message": str(e),
                    "current_task": "清洗任务出错"
                })

    def _upsert_data(self, data_list):
        if not data_list:
            return
        
        ensure_collection_and_indexes(table_name='stock_info_new')
        upsert_operations = []
        for record in data_list:
            upsert_operations.append(UpdateOne(
                {'symbol': record['symbol']},
                {'$set': record},
                upsert=True
            ))
        
        if upsert_operations:
            self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_info_new'].bulk_write(upsert_operations)
            logger.info(f"Successfully upserted {len(upsert_operations)} records to stock_info_new")

    def _clean_stocks(self):
        """
        清洗股票信息 (Type 0)
        包含: L(上市), D(退市), P(暂停上市)
        """
        logger.info("Cleaning stocks info...")
        
        records = []
        now = datetime.now()
        
        # 遍历三种状态
        for status in ['L', 'D', 'P']:
            try:
                logger.info(f"Fetching stocks with status: {status}")
                df = self.pro.stock_basic(exchange='', list_status=status, 
                                        fields='ts_code,symbol,name,area,industry,market,list_date,list_status,delist_date,exchange')
                
                for _, row in df.iterrows():
                    record = {
                        'code': int(row['symbol']) if row['symbol'].isdigit() else row['symbol'],
                        'delist_date': row['delist_date'],
                        'exchange': row['exchange'],
                        'insert_time': now,
                        'list_date': int(row['list_date']) if row['list_date'] else None,
                        'list_status': row['list_status'],
                        'market': row['market'],
                        'name': row['name'],
                        'remark': row['industry'],
                        'source': 'tushare',
                        'status': 0,
                        'symbol': row['ts_code'],
                        'type': 0 # Stock
                    }
                    records.append(record)
            except Exception as e:
                logger.error(f"Failed to fetch stocks for status {status}: {str(e)}")
        
        self._upsert_data(records)

    def _clean_etfs(self):
        """
        清洗ETF信息 (Type 2)
        """
        logger.info("Cleaning ETFs info...")
        # 获取所有ETF
        df = self.pro.fund_basic(market='E', status='L', 
                               fields='ts_code,name,management,market,list_date,delist_date,issue_date,due_date,list_status')
        
        records = []
        now = datetime.now()
        for _, row in df.iterrows():
            # 提取 exchange (e.g. 513300.SH -> SH)
            parts = row['ts_code'].split('.')
            exchange = parts[1] if len(parts) > 1 else None
            code = parts[0]

            record = {
                'code': int(code) if code.isdigit() else code,
                'delist_date': row['delist_date'],
                'exchange': exchange,
                'insert_time': now,
                'list_date': int(row['list_date']) if row['list_date'] else None,
                # 'list_status': row['list_status'],
                'market': row['market'],
                'name': row['name'],
                'remark': row['management'], # 使用 management 作为 remark
                'source': 'tushare',
                'status': 0,
                'symbol': row['ts_code'],
                'type': 2 # ETF
            }
            records.append(record)
        
        self._upsert_data(records)

    def _clean_indices(self):
        """
        清洗指数信息 (Type 1)
        仅包含: 上证, 深证, 沪深300, 创业板, 中证1000
        """
        logger.info("Cleaning Indices info...")
        
        # 指定需要的指数代码
        # 000001.SH: 上证指数
        # 399001.SZ: 深证成指
        # 000300.SH: 沪深300
        # 399006.SZ: 创业板指
        # 000852.SH: 中证1000
        target_indices = ['000001.SH', '399001.SZ', '000300.SH', '399006.SZ', '000852.SH']
        ts_codes = ",".join(target_indices)

        df = self.pro.index_basic(ts_code=ts_codes, 
                                fields='ts_code,name,market,publisher,category,base_date,base_point,list_date')
        
        records = []
        now = datetime.now()
        for _, row in df.iterrows():
            parts = row['ts_code'].split('.')
            exchange = parts[1] if len(parts) > 1 else None
            code = parts[0]

            record = {
                'code': code, 
                'delist_date': None,
                'exchange': exchange,
                'insert_time': now,
                'list_date': int(row['list_date']) if row['list_date'] else None,
                'list_status': 'L',
                'market': row['market'],
                'name': row['name'],
                'remark': row['publisher'],
                'source': 'tushare',
                'status': 0,
                'symbol': row['ts_code'],
                'type': 1 # Index
            }
            records.append(record)
        
        self._upsert_data(records)
