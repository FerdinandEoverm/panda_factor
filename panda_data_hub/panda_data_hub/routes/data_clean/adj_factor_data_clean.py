from fastapi import APIRouter, BackgroundTasks
from panda_common.config import get_config
from panda_common.logger_config import logger
from typing import Dict

from panda_data_hub.services.ts_adj_factor_clean_pro_service import AdjFactorCleanerTSProService

router = APIRouter()

# 全局进度状态
current_progress = {
    "progress_percent": 0,
    "status": "idle",
    "current_task": "",
    "error_message": ""
}

@router.get('/upsert_adj_factor_final')
async def upsert_adj_factor(start_date: str, end_date: str, background_tasks: BackgroundTasks):
    global current_progress

    logger.info(f"收到复权因子数据清洗请求: start_date={start_date}, end_date={end_date}")

    # 重置进度状态
    current_progress = {
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化复权因子数据清洗任务...",
        "error_message": ""
    }

    logger.info("使用数据源: Tushare")

    def progress_callback(progress: int):
        global current_progress
        current_progress["progress_percent"] = progress
        logger.debug(f"复权因子数据清洗进度: {progress}%")

    def run_with_error_handling(task_func, *args):
        """包装任务执行，添加错误处理"""
        global current_progress
        try:
            task_func(*args)
        except Exception as e:
            error_msg = f"复权因子数据清洗出错: {str(e)}"
            logger.error(error_msg)
            current_progress.update({
                "status": "error",
                "error_message": error_msg,
                "current_task": "复权因子数据清洗失败"
            })

    logger.info("初始化Tushare复权因子服务")
    # 动态获取最新配置，确保使用热加载后的配置
    current_config = get_config()
    adj_factor_service = AdjFactorCleanerTSProService(current_config)
    adj_factor_service.set_progress_callback(progress_callback)
    background_tasks.add_task(
        run_with_error_handling,
        adj_factor_service.clean_history_data,
        start_date,
        end_date
    )
    logger.info("Tushare复权因子后台任务已添加")

    return {"message": "Adj factor data cleaning started by tushare"}

@router.get('/get_progress_adj_factor_final')
async def get_progress() -> Dict:
    """获取当前复权因子数据清洗进度"""
    try:
        global current_progress

        # 如果任务已完成，更新状态
        if (current_progress.get("progress_percent", 0) >= 100 and
            current_progress.get("status") == "running"):
            current_progress["status"] = "completed"
            current_progress["current_task"] = "复权因子数据清洗已完成"

        logger.debug(f"返回复权因子进度信息: {current_progress}")
        return current_progress
    except Exception as e:
        error_msg = f"获取复权因子进度信息失败: {str(e)}"
        logger.error(error_msg)
        return {
            "progress_percent": 0,
            "status": "error",
            "current_task": "获取进度失败",
            "error_message": error_msg
        }
