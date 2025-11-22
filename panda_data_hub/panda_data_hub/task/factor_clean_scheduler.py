from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime
from panda_common.config import config, logger
from panda_common.handlers.database_handler import DatabaseHandler
from panda_data_hub.factor.ts_factor_clean_pro import TSFactorCleaner


class FactorCleanerScheduler():

    def __init__(self):
        self.config = config

        # 初始化数据库连接
        self.db_handler = DatabaseHandler(self.config)
        # 初始化调度器
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def _process_factor(self):
        """处理因子数据清洗 - 使用 Tushare"""
        logger.info("Processing factor data using Tushare")
        try:
            # 清洗因子数据
            factor_cleaner = TSFactorCleaner(self.config)
            factor_cleaner.clean_daily_factor()
        except Exception as e:
            logger.error(f"Error _process_factor: {str(e)}")

    def schedule_data(self):
        time = self.config["FACTOR_UPDATE_TIME"]
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )

        # 添加基础因子数据定时任务
        self.scheduler.add_job(
            self._process_factor,
            trigger=trigger,
            id=f"data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )

        # self._process_factor()
        logger.info(f"Scheduled Factor Data")

    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
    
    def reload_schedule(self):
        """重新加载定时任务（用于配置变更后热更新）"""
        self.scheduler.remove_all_jobs()
        self.schedule_data()














