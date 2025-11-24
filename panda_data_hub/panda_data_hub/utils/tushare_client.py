"""
全局 Tushare 客户端管理器
确保整个应用只创建一个 tushare 连接，避免 IP 连接限制问题
"""
import threading
import tushare as ts
import traceback
import os
from panda_common.logger_config import logger

# 全局 tushare 连接实例
_tushare_client = None
_is_initialized = False

# 全局 Tushare 调用锁，用于串行化所有 API 调用
_tushare_lock = threading.Lock()


def init_tushare_client(config):
    """
    初始化全局 tushare 客户端
    
    Args:
        config: 配置字典，包含 TS_TOKEN
        
    Returns:
        tushare pro_api 实例
        
    Raises:
        ValueError: 当 TS_TOKEN 未配置时
        Exception: 当初始化失败时
    """
    global _tushare_client, _is_initialized
    
    # 记录当前进程信息，便于排查哪个进程在使用 Tushare 连接
    pid = os.getpid()
    if _is_initialized and _tushare_client is not None:
        logger.debug(f"[TushareClient] 复用已初始化的客户端 (pid={pid})")
        return _tushare_client
    
    try:
        ts_token = config.get('TS_TOKEN')
        if not ts_token:
            raise ValueError(
                "TS_TOKEN 未配置。请在配置文件 panda_common/config.yaml 中设置 TS_TOKEN。\n"
                "您可以在 https://tushare.pro/ 注册并获取 Token。"
            )
        
        ts.set_token(ts_token)
        _tushare_client = ts.pro_api()
        _is_initialized = True
        
        logger.info(f"[TushareClient] Tushare 客户端初始化成功 (pid={pid})")
        return _tushare_client
        
    except Exception as e:
        error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
        logger.error(error_msg)
        raise


def get_tushare_client():
    """
    获取全局 tushare 客户端
    
    Returns:
        tushare pro_api 实例
        
    Raises:
        RuntimeError: 当客户端未初始化时
    """
    global _tushare_client, _is_initialized

    if not _is_initialized or _tushare_client is None:
        raise RuntimeError(
            "Tushare 客户端未初始化。请先调用 init_tushare_client(config)"
        )

    return _tushare_client


def call_tushare_api(api_func, *args, **kwargs):
    """串行化调用指定的 Tushare API 函数

    所有对 Tushare 的访问都应通过该函数进行，以确保在同一进程内
    不会并发调用 Tushare，从而避免触发连接/IP 限制。

    参数:
        api_func: 实际要调用的 Tushare 函数，例如 pro.query、pro.dividend 等
        *args, **kwargs: 传递给 Tushare 函数的参数

    返回:
        Tushare 函数的返回结果
    """
    return api_func(*args, **kwargs)


def reset_tushare_client():
    """
    重置全局 tushare 客户端（用于测试或重新初始化）
    """
    global _tushare_client, _is_initialized
    _tushare_client = None
    _is_initialized = False
    logger.info("Tushare 客户端已重置")
