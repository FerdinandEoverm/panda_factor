from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict
from datetime import datetime
from panda_common.logger_config import logger
from panda_data_hub.services.ts_dividend_clean_service import TSDividendCleanService

# 创建路由
router = APIRouter()

# 全局进度状态对象
dividend_progress = {
    "progress_percent": 0,
    "status": "idle",  # idle, running, completed, error
    "current_task": "",
    "current_step": "",
    "processed_count": 0,
    "total_count": 0,
    "start_time": None,
    "end_time": None,
    "error_message": "",
    "date_range": "",
    "symbol": "",
}


# 请求体模型
class DividendHistoryRequest(BaseModel):
    start_date: str
    end_date: str


class DividendSymbolRequest(BaseModel):
    symbol: str


@router.post("/dividend_clean/daily")
async def clean_dividend_daily(background_tasks: BackgroundTasks):
    """
    每日分红数据清洗接口
    获取最近30天的分红公告数据
    """
    global dividend_progress
    
    try:
        logger.info("收到每日分红数据清洗请求")
        
        # 重置进度状态
        dividend_progress.update({
            "progress_percent": 0,
            "status": "running",
            "current_task": "正在启动每日分红数据清洗（最近30天）...",
            "current_step": "",
            "processed_count": 0,
            "total_count": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "error_message": "",
            "date_range": "最近30天",
            "symbol": "",
        })
        
        # 设置进度回调
        def progress_callback(progress_info: dict):
            global dividend_progress
            dividend_progress.update(progress_info)

        # 在后台运行清洗任务（延迟创建服务实例）
        def _run_daily_clean_task_inner():
            global dividend_progress
            try:
                service = TSDividendCleanService()
                service.set_progress_callback(progress_callback)
                result = service.clean_dividend_daily()
                if result["status"] == "success":
                    dividend_progress.update({
                        "status": "completed",
                        "progress_percent": 100,
                        "current_task": "每日分红数据清洗完成",
                        "end_time": datetime.now().isoformat()
                    })
                else:
                    dividend_progress.update({
                        "status": "error",
                        "error_message": result.get("message", "未知错误"),
                        "end_time": datetime.now().isoformat()
                    })
            except Exception as e:
                logger.error(f"每日清洗任务执行失败: {str(e)}")
                dividend_progress.update({
                    "status": "error",
                    "error_message": str(e),
                    "end_time": datetime.now().isoformat()
                })

        background_tasks.add_task(_run_daily_clean_task_inner)
        
        return {
            "status": "success",
            "message": "每日分红数据清洗任务已启动",
            "date_range": "最近30天"
        }
        
    except Exception as e:
        logger.error(f"每日分红数据清洗接口异常: {str(e)}")
        dividend_progress.update({
            "status": "error",
            "error_message": str(e)
        })
        raise HTTPException(status_code=500, detail=f"接口调用失败: {str(e)}")


@router.post("/dividend_clean/history")
async def clean_dividend_history(request: DividendHistoryRequest, background_tasks: BackgroundTasks):
    """
    历史分红数据清洗接口
    
    请求体示例:
    {
        "start_date": "20200101",
        "end_date": "20231231"
    }
    """
    global dividend_progress
    
    try:
        logger.info(f"收到历史分红数据清洗请求: {request.start_date} 到 {request.end_date}")
        
        # 重置进度状态
        dividend_progress.update({
            "progress_percent": 0,
            "status": "running",
            "current_task": f"正在启动历史分红数据清洗...",
            "current_step": "",
            "processed_count": 0,
            "total_count": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "error_message": "",
            "date_range": f"{request.start_date} - {request.end_date}",
            "symbol": "",
        })
        
        # 设置进度回调
        def progress_callback(progress_info: dict):
            global dividend_progress
            dividend_progress.update(progress_info)
        
        # 在后台运行清洗任务（延迟创建服务实例）
        def _run_history_clean_task_inner(start_date: str, end_date: str):
            global dividend_progress
            try:
                service = TSDividendCleanService()
                service.set_progress_callback(progress_callback)
                result = service.clean_dividend_history(start_date, end_date)
                if result["status"] == "success":
                    dividend_progress.update({
                        "status": "completed",
                        "progress_percent": 100,
                        "current_task": "历史分红数据清洗完成",
                        "end_time": datetime.now().isoformat()
                    })
                else:
                    dividend_progress.update({
                        "status": "error",
                        "error_message": result.get("message", "未知错误"),
                        "end_time": datetime.now().isoformat()
                    })
            except Exception as e:
                logger.error(f"历史清洗任务执行失败: {str(e)}")
                dividend_progress.update({
                    "status": "error",
                    "error_message": str(e),
                    "end_time": datetime.now().isoformat()
                })
        
        background_tasks.add_task(
            _run_history_clean_task_inner,
            request.start_date,
            request.end_date
        )
        
        return {
            "status": "success",
            "message": "历史分红数据清洗任务已启动",
            "date_range": f"{request.start_date} - {request.end_date}"
        }
        
    except Exception as e:
        logger.error(f"历史分红数据清洗接口异常: {str(e)}")
        dividend_progress.update({
            "status": "error",
            "error_message": str(e)
        })
        raise HTTPException(status_code=500, detail=f"接口调用失败: {str(e)}")


@router.post("/dividend_clean/symbol")
async def clean_dividend_by_symbol(request: DividendSymbolRequest, background_tasks: BackgroundTasks):
    """
    按股票代码清洗分红数据接口
    
    请求体示例:
    {
        "symbol": "600000.SH"
    }
    """
    global dividend_progress
    
    try:
        logger.info(f"收到股票 {request.symbol} 的分红数据清洗请求")
        
        # 重置进度状态
        dividend_progress.update({
            "progress_percent": 0,
            "status": "running",
            "current_task": f"正在启动股票 {request.symbol} 的分红数据清洗...",
            "current_step": "",
            "processed_count": 0,
            "total_count": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "error_message": "",
            "date_range": "",
            "symbol": request.symbol,
        })
        
        # 设置进度回调
        def progress_callback(progress_info: dict):
            global dividend_progress
            dividend_progress.update(progress_info)
        
        # 在后台运行清洗任务（延迟创建服务实例）
        def _run_symbol_clean_task_inner(symbol: str):
            global dividend_progress
            try:
                service = TSDividendCleanService()
                service.set_progress_callback(progress_callback)
                result = service.clean_dividend_by_symbol(symbol)
                if result["status"] == "success":
                    dividend_progress.update({
                        "status": "completed",
                        "progress_percent": 100,
                        "current_task": f"股票 {symbol} 的分红数据清洗完成",
                        "end_time": datetime.now().isoformat()
                    })
                else:
                    dividend_progress.update({
                        "status": "error",
                        "error_message": result.get("message", "未知错误"),
                        "end_time": datetime.now().isoformat()
                    })
            except Exception as e:
                logger.error(f"按股票清洗任务执行失败: {str(e)}")
                dividend_progress.update({
                    "status": "error",
                    "error_message": str(e),
                    "end_time": datetime.now().isoformat()
                })
        
        background_tasks.add_task(
            _run_symbol_clean_task_inner,
            request.symbol
        )
        
        return {
            "status": "success",
            "message": f"股票 {request.symbol} 的分红数据清洗任务已启动",
            "symbol": request.symbol
        }
        
    except Exception as e:
        logger.error(f"按股票清洗分红数据接口异常: {str(e)}")
        dividend_progress.update({
            "status": "error",
            "error_message": str(e)
        })
        raise HTTPException(status_code=500, detail=f"接口调用失败: {str(e)}")


@router.get("/get_dividend_progress")
async def get_dividend_progress() -> Dict:
    """获取分红数据清洗进度"""
    global dividend_progress
    
    # 如果任务已完成，更新状态
    if (dividend_progress.get("progress_percent", 0) >= 100 and 
        dividend_progress.get("status") == "running"):
        dividend_progress["status"] = "completed"
        dividend_progress["current_task"] = "分红数据清洗已完成"
        if not dividend_progress.get("end_time"):
            dividend_progress["end_time"] = datetime.now().isoformat()
    
    return dividend_progress


@router.get("/dividend_clean/status")
async def get_dividend_clean_status():
    """
    获取分红数据清洗服务状态
    """
    try:
        # 可以返回一些统计信息，比如数据库中的分红记录数量等
        return {
            "status": "success",
            "message": "分红数据清洗服务运行正常"
        }
    except Exception as e:
        logger.error(f"获取分红数据清洗状态异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取状态失败: {str(e)}")


