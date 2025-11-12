from typing import Dict
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks

from panda_common.config import get_config
from panda_common.logger_config import logger

from panda_data_hub.services.ts_index_market_clean_service import TSIndexMarketCleanService

router = APIRouter()

# 全局进度状态对象
current_progress = {
    "progress_percent": 0,
    "status": "idle",                   # idle, running, completed, error
    "current_task": "",
    "processed_count": 0,
    "total_count": 0,
    "start_time": None,
    "estimated_completion": None,
    "current_index": "",                # 当前处理的指数
    "current_date": "",                 # 当前处理的日期
    "error_message": "",
    "data_source": "tushare",
    "indices_processed": 0,             # 已处理指数数
    "indices_total": 7,                 # 总指数数（7个主要指数）
    "last_message": ""
}


@router.get('/upsert_index_market')
async def upsert_index_market(
    start_date: str, 
    end_date: str, 
    background_tasks: BackgroundTasks,
    task_type: str = "history"  # history: 历史数据补全, daily: 每日更新
):
    """
    指数行情数据清洗接口
    
    参数:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        task_type: 任务类型，history 或 daily
    """
    global current_progress
    
    logger.info(f"收到指数行情数据清洗请求: start_date={start_date}, end_date={end_date}, task_type={task_type}")
    
    # 重置进度状态
    current_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化指数行情数据清洗任务...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "estimated_completion": None,
        "current_index": "",
        "current_date": "",
        "error_message": "",
        "data_source": "tushare",
        "indices_processed": 0,
        "indices_total": 7,
        "last_message": "任务已启动"
    })

    def progress_callback(progress_info: dict):
        """进度回调函数"""
        global current_progress
        for key, value in progress_info.items():
            current_progress[key] = value
        
        logger.debug(f"进度更新: {current_progress.get('progress_percent', 0)}% - {current_progress.get('current_task', '')}")
        
        # 计算预计完成时间
        if (progress_info.get("progress_percent", 0) > 0 and 
            current_progress.get("start_time")):
            try:
                start = datetime.fromisoformat(current_progress["start_time"])
                elapsed = (datetime.now() - start).total_seconds()
                if progress_info["progress_percent"] > 0:
                    total_estimated = elapsed / (progress_info["progress_percent"] / 100)
                    remaining = total_estimated - elapsed
                    completion_time = datetime.now().timestamp() + remaining
                    current_progress["estimated_completion"] = datetime.fromtimestamp(completion_time).isoformat()
            except Exception:
                pass

    def run_task():
        """执行清洗任务"""
        global current_progress
        try:
            service = TSIndexMarketCleanService()
            service.set_progress_callback(progress_callback)
            
            if task_type == "daily":
                # 每日更新
                current_progress["current_task"] = "执行每日指数行情数据更新..."
                result = service.clean_index_market_daily()
            else:
                # 历史数据补全
                current_progress["current_task"] = "补全历史指数行情数据..."
                result = service.clean_index_market_history(start_date, end_date)
            
            if result['status'] == 'success':
                current_progress.update({
                    "status": "completed",
                    "progress_percent": 100,
                    "current_task": "指数行情数据清洗完成",
                    "last_message": result['message']
                })
                logger.info(f"指数行情数据清洗完成: {result['message']}")
            else:
                current_progress.update({
                    "status": "error",
                    "error_message": result.get('message', '未知错误'),
                    "current_task": "数据清洗失败"
                })
                logger.error(f"指数行情数据清洗失败: {result.get('message')}")
                
        except Exception as e:
            error_msg = f"数据清洗出错: {str(e)}"
            logger.error(error_msg)
            import traceback
            logger.error(traceback.format_exc())
            current_progress.update({
                "status": "error",
                "error_message": error_msg,
                "current_task": "数据清洗失败"
            })
    
    # 添加后台任务
    background_tasks.add_task(run_task)
    logger.info("指数行情数据清洗后台任务已添加")
    
    return {"message": "指数行情数据清洗任务已启动", "task_type": task_type}


@router.get('/get_progress_index_market')
async def get_progress() -> Dict:
    """获取当前指数行情数据清洗进度"""
    try:
        global current_progress
        logger.debug("收到指数行情进度查询请求")
        
        # 如果任务已完成，更新状态
        if (current_progress.get("progress_percent", 0) >= 100 and 
            current_progress.get("status") == "running"):
            current_progress["status"] = "completed"
            current_progress["current_task"] = "指数行情数据清洗已完成"
        
        return current_progress
    except Exception as e:
        error_msg = f"获取进度信息失败: {str(e)}"
        logger.error(error_msg)
        return {
            "progress_percent": 0,
            "status": "error",
            "current_task": "获取进度失败",
            "error_message": error_msg,
            "processed_count": 0,
            "total_count": 0
        }

