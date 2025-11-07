from panda_common.config import get_config
from panda_common.logger_config import logger
from panda_data_hub.data.tushare_index_market_cleaner import TSIndexMarketCleaner


class TSIndexMarketCleanService:
    """指数行情数据清洗服务"""

    def __init__(self):
        self.config = get_config()
        self.index_market_cleaner = TSIndexMarketCleaner(self.config)
        self.progress_callback = None
    
    def set_progress_callback(self, callback):
        """设置进度回调函数"""
        self.progress_callback = callback
        # 如果cleaner有进度回调，可以在这里设置
        # self.index_market_cleaner.set_progress_callback(callback)

    def clean_index_market_daily(self):
        """
        执行每日指数行情数据清洗
        获取当日主要指数的行情数据
        """
        try:
            logger.info("开始执行每日指数行情数据清洗任务")
            self.index_market_cleaner.index_market_clean_daily()
            logger.info("每日指数行情数据清洗任务完成")
            return {"status": "success", "message": "每日指数行情数据清洗完成"}
        except Exception as e:
            error_msg = f"每日指数行情数据清洗失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

    def clean_index_market_history(self, start_date: str = None, end_date: str = None):
        """
        执行历史指数行情数据清洗
        
        参数:
        start_date: 开始日期，格式 YYYYMMDD 或 YYYY-MM-DD，默认为20050101
        end_date: 结束日期，格式 YYYYMMDD 或 YYYY-MM-DD，默认为当前日期
        """
        try:
            logger.info(f"开始执行历史指数行情数据清洗任务: {start_date or '20050101'} 到 {end_date or '当前日期'}")
            self.index_market_cleaner.clean_index_market_history(start_date, end_date)
            logger.info("历史指数行情数据清洗任务完成")
            return {
                "status": "success",
                "message": f"历史指数行情数据清洗完成: {start_date or '20050101'} 到 {end_date or '当前日期'}"
            }
        except Exception as e:
            error_msg = f"历史指数行情数据清洗失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

    def clean_index_market_by_date(self, date: str):
        """
        清洗指定日期的指数行情数据
        
        参数:
        date: 日期，格式 YYYYMMDD 或 YYYY-MM-DD
        """
        try:
            logger.info(f"开始清洗指定日期 {date} 的指数行情数据")
            self.index_market_cleaner.clean_index_market_data(date)
            logger.info(f"日期 {date} 的指数行情数据清洗完成")
            return {"status": "success", "message": f"日期 {date} 的指数行情数据清洗完成"}
        except Exception as e:
            error_msg = f"日期 {date} 的指数行情数据清洗失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

