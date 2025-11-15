from typing import Dict
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks

from panda_common.config import get_config
from panda_common.logger_config import logger

from panda_data_hub.services.ts_namechange_clean_service import TSNamechangeCleanService

router = APIRouter()

# 全局进度状态对象
current_progress = {
    "progress_percent": 0,              # 进度百分比
    "status": "idle",                   # 状态: idle, running, completed, error
    "current_task": "",                 # 当前处理的任务描述
    "processed_count": 0,               # 已处理数量
    "total_count": 0,                   # 总数量
    "start_time": None,                 # 开始时间
    "estimated_completion": None,       # 预计完成时间
    "current_date": "",                 # 当前处理的日期
    "error_message": "",                # 错误信息
    "data_source": "",                  # 数据源
    "batch_info": "",                   # 批次信息
    "sync_type": "",                    # 同步类型: full/incremental
    "last_message": ""                  # 最近一条可读的进度信息
}

@router.get('/upsert_namechange_data')
async def upsert_namechange_data(start_date: str = None, end_date: str = None, background_tasks: BackgroundTasks = None, force_update: bool = False):
    global current_progress
    
    logger.info(f"收到股票名称变更清洗请求: start_date={start_date}, end_date={end_date}, force_update={force_update}")
    
    # 重置进度状态
    current_progress.update({
        "progress_percent": 0,
        "status": "running",
        "current_task": "初始化股票名称变更清洗任务...",
        "processed_count": 0,
        "total_count": 0,
        "start_time": datetime.now().isoformat(),
        "estimated_completion": None,
        "current_date": "",
        "error_message": "",
        "data_source": "tushare",
        "batch_info": "",
        "sync_type": "full" if force_update else "incremental",
    })

    logger.info("使用数据源: Tushare")

    def progress_callback(progress_info: dict):
        """
        进度回调函数
        """
        global current_progress
        # 只更新提供的字段，保留其他字段
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
                pass  # 忽略时间计算错误

    def run_with_error_handling(task_func, *args):
        """包装任务执行，添加错误处理"""
        global current_progress
        try:
            task_func(*args)
        except Exception as e:
            error_msg = f"股票名称变更清洗出错: {str(e)}"
            logger.error(error_msg)
            current_progress.update({
                "status": "error",
                "error_message": error_msg,
                "current_task": "股票名称变更清洗失败"
            })
    
    logger.info("初始化Tushare名称变更服务")
    # 动态获取最新配置，确保使用热加载后的配置
    current_config = get_config()
    namechange_service = TSNamechangeCleanService(current_config)
    
    # 设置进度回调（如果服务支持）
    if hasattr(namechange_service, 'set_progress_callback'):
        namechange_service.set_progress_callback(progress_callback)
    
    background_tasks.add_task(
        run_with_error_handling,
        namechange_service.sync_namechange_data,
        start_date,
        end_date,
        force_update
    )
    logger.info("Tushare名称变更后台任务已添加")
    
    return {"message": "Namechange data cleaning started by tushare"}


@router.get('/get_progress_namechange')
async def get_progress() -> Dict:
    """获取当前股票名称变更清洗进度"""
    try:
        global current_progress
        logger.info("收到名称变更进度查询请求")
        
        # 如果任务已完成，更新状态
        if (current_progress.get("progress_percent", 0) >= 100 and 
            current_progress.get("status") == "running"):
            current_progress["status"] = "completed"
            current_progress["current_task"] = "股票名称变更清洗已完成"
        
        logger.info(f"返回名称变更进度信息: status={current_progress.get('status')}, percent={current_progress.get('progress_percent')}%, task={current_progress.get('current_task')}")
        return current_progress
    except Exception as e:
        error_msg = f"获取名称变更进度信息失败: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return {
            "progress_percent": 0,
            "status": "error",
            "current_task": "获取进度失败",
            "error_message": error_msg,
            "processed_count": 0,
            "total_count": 0
        }


@router.get('/query_namechange_data')
async def query_namechange_data(symbol: str = None, end_date: str = None):
    """
    查询股票名称变更数据
    支持按个股查询
    """
    try:
        logger.info(f"收到名称变更数据查询请求: symbol={symbol}, end_date={end_date}")
        
        current_config = get_config()
        namechange_service = TSNamechangeCleanService(current_config)
        
        # 获取数据
        data = namechange_service.get_namechange_data(end_date)
        
        # 如果指定了股票代码，过滤数据
        if symbol:
            data = data[data['symbol'] == symbol]
        
        # 转换为字典列表返回
        result = data.to_dict('records') if not data.empty else []
        
        logger.info(f"返回 {len(result)} 条名称变更记录")
        return {
            "success": True,
            "data": result,
            "count": len(result)
        }
        
    except Exception as e:
        error_msg = f"查询名称变更数据失败: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return {
            "success": False,
            "error_message": error_msg,
            "data": [],
            "count": 0
        }
