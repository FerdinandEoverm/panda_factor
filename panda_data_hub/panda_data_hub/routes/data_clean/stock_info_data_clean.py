from typing import Dict
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks
from panda_common.config import get_config
from panda_common.logger_config import logger
from panda_data_hub.data.tushare_stock_info_cleaner import TSStockInfoCleaner

router = APIRouter()

# 全局进度状态对象
current_progress = {
    "progress_percent": 0,
    "status": "idle",
    "current_task": "",
    "processed_count": 0,
    "total_count": 0,
    "start_time": None,
    "error_message": ""
}

@router.get('/stock_info_clean')
async def stock_info_clean(background_tasks: BackgroundTasks):
    global current_progress
    
    logger.info("收到股票基础信息清洗请求")
    
    # 重置进度状态
    current_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化任务...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "error_message": ""
    })

    def progress_callback(progress_info: dict):
        global current_progress
        for key, value in progress_info.items():
            current_progress[key] = value
        logger.debug(f"进度更新: {current_progress.get('progress_percent', 0)}% - {current_progress.get('current_task', '')}")

    def run_task():
        try:
            config = get_config()
            cleaner = TSStockInfoCleaner(config)
            cleaner.set_progress_callback(progress_callback)
            cleaner.clean_stock_info_daily()
        except Exception as e:
            logger.error(f"清洗任务失败: {e}")
            current_progress.update({
                "status": "error",
                "error_message": str(e),
                "current_task": "任务执行失败"
            })

    background_tasks.add_task(run_task)
    
    return {"message": "Stock info cleaning started"}

@router.get('/get_progress_stock_info')
async def get_progress() -> Dict:
    """获取进度"""
    return current_progress
