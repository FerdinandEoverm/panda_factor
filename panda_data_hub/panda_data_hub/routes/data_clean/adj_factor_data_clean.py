from fastapi import APIRouter
from typing import Dict

router = APIRouter()


@router.get('/upsert_adj_factor_final')
async def upsert_adj_factor(start_date: str, end_date: str) -> Dict:
    """复权因子清洗任务已下线，接口保留仅用于兼容，将直接返回禁用状态"""
    return {
        "success": False,
        "status": "disabled",
        "message": "复权因子清洗任务已移除，请使用 close/pre_close 自行计算复权价格"
    }


@router.get('/get_progress_adj_factor_final')
async def get_progress() -> Dict:
    """复权因子清洗任务已下线，始终返回 idle/disabled 状态"""
    return {
        "progress_percent": 0,
        "status": "disabled",
        "current_task": "复权因子清洗任务已移除",
        "error_message": ""
    }
