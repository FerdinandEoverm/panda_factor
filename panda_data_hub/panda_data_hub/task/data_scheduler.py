from panda_common.config import config, logger
from panda_common.handlers.database_handler import DatabaseHandler


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import datetime

from panda_data_hub.data.tushare_stocks_cleaner import TSStockCleaner
from panda_data_hub.services.ts_stock_market_clean_service import StockMarketCleanTSServicePRO
from panda_data_hub.services.ts_financial_clean_service import FinancialCleanTSService
from panda_data_hub.services.ts_dividend_clean_service import TSDividendCleanService
from panda_data_hub.services.ts_index_market_clean_service import TSIndexMarketCleanService
from panda_data_hub.services.ts_namechange_clean_service import TSNamechangeCleanService
from panda_data_hub.services.ts_factor_valuation_clean_pro_service import FactorValuationCleanerTSProService


class DataScheduler:
    def __init__(self):
        self.config = config
        
        # Initialize database connection
        self.db_handler = DatabaseHandler(self.config)
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def _process_data(self):
        """处理数据清洗和入库 - 使用 Tushare"""
        logger.info("Processing data using Tushare")
        try:
            # 1. 清洗 stock 表当日数据（基础信息）
            stocks_cleaner = TSStockCleaner(self.config)
            stocks_cleaner.clean_metadata()

            # 2. 增量清洗股票名称变更数据（依赖基础信息）
            namechange_service = TSNamechangeCleanService(self.config)
            # 不传日期与 force_update，采用默认增量逻辑，从上次同步日期开始
            namechange_service.sync_namechange_data()

            # 3. 清洗 stock_market 表当日数据（行情，依赖名称和基础信息）
            stock_market_service = StockMarketCleanTSServicePRO(self.config)
            # 使用当天日期作为历史清洗的起止日期，相当于“当日清洗”
            today = datetime.datetime.now().strftime("%Y%m%d")
            stock_market_service.stock_market_history_clean(start_date=today, end_date=today, force_update=True)

            # 4. 清洗 index_market 表当日数据（指数行情，依赖市场基础信息）
            index_market_service = TSIndexMarketCleanService()
            index_market_service.clean_index_market_daily()
        except Exception as e:
            logger.error(f"Error _process_data: {str(e)}")

    def _process_valuation_factor(self):
        """处理估值因子数据清洗和入库 - 使用 Tushare"""
        logger.info("Processing valuation factor data using Tushare")
        try:
            valuation_service = FactorValuationCleanerTSProService(self.config)
            # 这里按日度增量方式，只补充当天的估值因子数据
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            valuation_service.clean_history_data(start_date=today_str, end_date=today_str)
            logger.info("Valuation factor daily update completed")
        except Exception as e:
            logger.error(f"Error _process_valuation_factor: {str(e)}")
    
    def _process_financial_data(self):
        """处理财务数据清洗和入库（每日更新最近2个季度） - 使用 Tushare"""
        logger.info("Processing financial data using Tushare")
        try:
            # 每日更新财务数据（最近2个季度）
            financial_service = FinancialCleanTSService(self.config)
            financial_service.financial_daily_update()
            logger.info("Financial data daily update completed")
        except Exception as e:
            logger.error(f"Error _process_financial_data: {str(e)}")
    
    def _process_dividend_data(self):
        """处理股票分红数据清洗和入库 - 使用 Tushare"""
        logger.info("Processing dividend data using Tushare")
        try:
            # 每日更新分红数据（最近30天的公告）
            dividend_service = TSDividendCleanService()
            dividend_service.clean_dividend_daily()
            logger.info("Dividend data daily update completed")
        except Exception as e:
            logger.error(f"Error _process_dividend_data: {str(e)}")
    
    def schedule_data(self):
        time = self.config["STOCKS_UPDATE_TIME"]
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )
                
        # Add scheduled task
        self.scheduler.add_job(
            self._process_data,
            trigger=trigger,
            id=f"data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        # self._process_data()
        logger.info(f"Scheduled Data")
    
    def schedule_financial_data(self):
        """调度财务数据更新任务"""
        time = self.config.get("FINANCIAL_UPDATE_TIME", "16:30")
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )
        
        # Add scheduled task for financial data
        self.scheduler.add_job(
            self._process_financial_data,
            trigger=trigger,
            id=f"financial_data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        logger.info(f"Scheduled Financial Data update at {time}")
    
    def schedule_dividend_data(self):
        """调度分红数据更新任务"""
        time = self.config.get("DIVIDEND_UPDATE_TIME", "17:00")
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )
        
        # Add scheduled task for dividend data
        self.scheduler.add_job(
            self._process_dividend_data,
            trigger=trigger,
            id=f"dividend_data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        logger.info(f"Scheduled Dividend Data update at {time}")
    
    def schedule_valuation_factor(self):
        """调度估值因子数据更新任务"""
        time = self.config.get("VALUATION_FACTOR_UPDATE_TIME", "17:20")
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )

        # Add scheduled task for valuation factor data
        self.scheduler.add_job(
            self._process_valuation_factor,
            trigger=trigger,
            id=f"valuation_factor_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        logger.info(f"Scheduled Valuation Factor update at {time}")
    
    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown() 

    def reload_schedule(self):
        """重新加载定时任务（用于配置变更后热更新）"""
        self.scheduler.remove_all_jobs()
        self.schedule_data()
        self.schedule_financial_data()
        self.schedule_dividend_data()
        self.schedule_valuation_factor()