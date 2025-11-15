import logging
import calendar
from datetime import datetime
import tushare as ts

# 设置日志记录器
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def calculate_upper_limit(stock_code, prev_close, stock_name):
    """
    根据股票代码、前收盘价和股票名称计算涨停价格。

    Args:
        stock_code (str): 股票代码 (纯数字部分，如 '600000').
        prev_close (float): 前一交易日收盘价.
        stock_name (str): 股票名称 (用于判断ST).

    Returns:
        float or None: 涨停价格，如果无法确定则返回 None.
    """
    if not isinstance(prev_close, (int, float)) or prev_close <= 0:
        logger.warning(f"无效的前收盘价: {prev_close} for stock {stock_code}")
        return None
    # 对于历史行情中已经退市的股票，其股票名称为Nan
    if stock_name is None:
        return None

    market_type = get_stock_market_type(stock_code)

    if market_type == 'main':
        # 主板: 正常 ±10%, ST ±5%
        limit_factor = 1.05 if 'ST' in stock_name or '*ST' in stock_name else 1.10
        return round(prev_close * limit_factor, 2) # 保留两位小数
    elif market_type in ('star', 'chinext'):
        # 科创板/创业板: ±20%
        return round(prev_close * 1.20, 2)
    elif market_type == 'bse':
        # 北交所: ±30%
        return round(prev_close * 1.30, 2)
    else:
        logger.warning(f"无法识别的股票代码 {stock_code}，无法计算涨停价。")
        return None

def calculate_lower_limit(stock_code, prev_close, stock_name):
    """
    根据股票代码、前收盘价和股票名称计算跌停价格。

     Args:
        stock_code (str): 股票代码 (纯数字部分，如 '600000').
        prev_close (float): 前一交易日收盘价.
        stock_name (str): 股票名称 (用于判断ST).

    Returns:
        float or None: 跌停价格，如果无法确定则返回 None.
    """
    if not isinstance(prev_close, (int, float)) or prev_close <= 0:
        logger.warning(f"无效的前收盘价: {prev_close} for stock {stock_code}")
        return None

    if stock_name is None:
        return None

    market_type = get_stock_market_type(stock_code)

    if market_type == 'main':
        # 主板: 正常 ±10%, ST ±5%
        limit_factor = 0.95 if 'ST' in stock_name or '*ST' in stock_name else 0.90
        return round(prev_close * limit_factor, 2) # 保留两位小数
    elif market_type in ('star', 'chinext'):
        # 科创板/创业板: ±20%
        return round(prev_close * 0.80, 2)
    elif market_type == 'bse':
         # 北交所: ±30%
        return round(prev_close * 0.70, 2)
    else:
        logger.warning(f"无法识别的股票代码 {stock_code}，无法计算跌停价。")
        return None

def get_stock_market_type(stock_code):
    """
    根据股票代码前缀判断市场和板块类型。

    返回: 'main', 'star', 'chinext', 'bse', or None
    """
    # 主板 (上海 + 深圳 A/B股)
    if stock_code.startswith(("600", "601", "603", "605", "900", "000", "001", "002", "003", "200", "201")):
        return 'main'
    # 科创板 (上海)
    elif stock_code.startswith(("688", "689")):
        return 'star'
    # 创业板 (深圳)
    elif stock_code.startswith(("300", "301", "302")):
        return 'chinext'
    # 北交所
    elif stock_code.startswith(("43", "83", "87","920")):
        return 'bse'
    else:
        return None

class TushareTokenError(Exception):
    """Tushare Token 相关错误的专用异常"""
    pass


def ts_is_trading_day(date, pro=None):
    """
    判断传入的日期是否为股票交易日

    参数:
    date: 日期字符串，格式为 "YYYY-MM-DD"
    pro: 可选的 tushare pro_api 实例。如果提供，则使用该实例，避免创建新连接

    返回:
    bool: 如果是交易日返回 True，否则返回 False
    
    抛出:
    TushareTokenError: 当 token 无效或认证失败时
    """
    try:
        # 如果没有提供 pro 实例，则创建一个新的（仅在必要时）
        if pro is None:
            from panda_common.config import get_config
            config = get_config()
            ts_token = config.get('TS_TOKEN')
            if not ts_token:
                raise ValueError(
                    "TS_TOKEN 未配置。请在配置文件 panda_common/config.yaml 中设置 TS_TOKEN。\n"
                    "您可以在 https://tushare.pro/ 注册并获取 Token。"
                )
            ts.set_token(ts_token)
            pro = ts.pro_api()
        
        # 获取指定日期的交易日历信息
        cal_df = pro.query('trade_cal',
                            exchange='SSE',
                            start_date=date.replace('-', ''),
                            end_date=date.replace('-', ''))
        # 检查是否有数据返回及该日期是否为交易日(is_open=1表示交易日)
        if not cal_df.empty and cal_df.iloc[0]['is_open'] == 1:
            return True
        return False
    except Exception as e:
        error_msg = str(e).lower()
        # 检测 token 相关错误
        if any(keyword in error_msg for keyword in ['token', '认证', '权限', 'auth', 'permission']):
            logger.error(f"Tushare Token 错误 {date}: {str(e)}")
            raise TushareTokenError(f"Tushare Token 无效或认证失败: {str(e)}")
        else:
            logger.error(f"检查交易日失败 {date}: {str(e)}")
            return False


def validate_tushare_token(pro=None):
    """
    验证 Tushare token 是否有效
    
    参数:
    pro: 可选的 tushare pro_api 实例。如果提供，则使用该实例进行验证，避免创建新连接
    
    返回:
    tuple: (is_valid: bool, error_message: str)
    """
    try:
        from panda_common.config import get_config
        config = get_config()
        
        ts_token = config.get('TS_TOKEN')
        if not ts_token:
            return False, "未配置 Tushare Token，请在配置文件中设置 TS_TOKEN"
        
        # 如果没有提供 pro 实例，则创建一个新的（仅在必要时）
        if pro is None:
            ts.set_token(ts_token)
            pro = ts.pro_api()
        
        # 尝试调用一个简单的 API 来验证 token
        from datetime import datetime
        today = datetime.now().strftime('%Y%m%d')
        test_df = pro.query('trade_cal', exchange='SSE', start_date=today, end_date=today)
        
        if test_df is not None:
            logger.info("Tushare Token 验证成功")
            return True, ""
        else:
            return False, "Token 验证失败：API 返回空数据"
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Tushare Token 验证失败: {error_msg}")
        
        # 提供更友好的错误提示
        if 'token' in error_msg.lower() or '不对' in error_msg or '请确认' in error_msg:
            return False, f"Token 无效：{error_msg}\n\n请检查配置文件中的 TS_TOKEN 是否正确。您可以在 Tushare 官网（https://tushare.pro/）的个人中心查看您的 Token。"
        elif '权限' in error_msg or 'permission' in error_msg.lower():
            return False, f"权限不足：{error_msg}\n\n您的 Token 权限可能不足，请升级 Tushare 账户权限。"
        else:
            return False, f"Token 验证失败：{error_msg}"


def get_previous_month_dates(date_str):
    """
    根据日期字符串获取上个月的中间日和最后一日
    参数格式：'YYYYMMDD' (如 '20170101')
    返回值：(中间日字符串, 最后一日字符串)
    """
    # 转换为datetime对象
    date = datetime.strptime(date_str, "%Y%m%d")
    year = date.year
    month = date.month

    # 计算上个月的年份和月份
    if month == 1:
        prev_year = year - 1
        prev_month = 12
    else:
        prev_year = year
        prev_month = month - 1

    # 获取上个月最后一天
    last_day = calendar.monthrange(prev_year, prev_month)[1]
    last_date = f"{prev_year}{prev_month:02d}{last_day:02d}"

    # 计算中间日（向上取整）
    middle_day = (last_day + 1) // 2
    middle_date = f"{prev_year}{prev_month:02d}{middle_day:02d}"

    return middle_date, last_date


def get_tushare_suffix(code):
    code = code.split('.')[0]
    if code.startswith(("600", "601", "603", "688", "689", "605", "900")):
        return f"{code}.SH"  # 上海证券交易所
    elif code.startswith(("000", "001", "300", "200", "002", "301", "201", "003", "302")):
        return f"{code}.SZ"  # 深圳证券交易所
    elif code.startswith(("43", "83", "87", "920")):
        return f"{code}.BJ"  # 北京证券交易所
    else:
        return "UNKNOWN"