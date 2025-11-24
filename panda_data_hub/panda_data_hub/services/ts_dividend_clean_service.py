from panda_common.config import get_config
from panda_common.logger_config import logger
from panda_data_hub.data.tushare_dividend_cleaner import TSDividendCleaner


class TSDividendCleanService:
    """股票分红数据清洗服务"""

    def __init__(self):
        self.config = get_config()
        self.dividend_cleaner = TSDividendCleaner(self.config)
        self.progress_callback = None

    def set_progress_callback(self, callback):
        """设置进度回调函数"""
        self.progress_callback = callback
        self.dividend_cleaner.set_progress_callback(callback)

    def clean_dividend_daily(self):
        """
        执行每日分红数据清洗
        获取最近30天的分红公告
        """
        try:
            logger.info("开始执行每日分红数据清洗任务")
            self.dividend_cleaner.dividend_clean_daily()
            logger.info("每日分红数据清洗任务完成")
            return {"status": "success", "message": "每日分红数据清洗完成"}
        except Exception as e:
            error_msg = f"每日分红数据清洗失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

    def clean_dividend_history(self, start_date: str, end_date: str):
        """
        执行历史分红数据清洗

        参数:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        """
        try:
            logger.info(f"开始执行历史分红数据清洗任务: {start_date} 到 {end_date}")
            self.dividend_cleaner.dividend_clean_history(start_date, end_date)
            logger.info("历史分红数据清洗任务完成")
            return {
                "status": "success",
                "message": f"历史分红数据清洗完成: {start_date} 到 {end_date}"
            }
        except Exception as e:
            error_msg = f"历史分红数据清洗失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

    def clean_dividend_by_symbol(self, symbol: str):
        """
        清洗指定股票的分红数据

        参数:
        symbol: 股票代码，如 '600000.SH'
        """
        try:
            logger.info(f"开始清洗股票 {symbol} 的分红数据")
            self.dividend_cleaner.clean_dividend_by_symbol(symbol)
            logger.info(f"股票 {symbol} 分红数据清洗完成")
            return {"status": "success", "message": f"股票 {symbol} 分红数据清洗完成"}
        except Exception as e:
            error_msg = f"股票 {symbol} 分红数据清洗失败: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}

