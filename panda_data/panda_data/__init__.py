# panda_data/__init__.py
import logging
from typing import Optional, List, Union

import pandas as pd

from panda_common.config import get_config

# 延迟导入，避免循环依赖和初始化错误
_config = None
_factor = None
_market_data = None
_market_min_data = None
_financial_data_reader = None # 新增

# 明确导出的公共接口
__all__ = [
    'init',
    'get_all_symbols',
    'get_factor',
    'get_custom_factor',
    'get_factor_by_name',
    'get_stock_instruments',
    'get_market_min_data',
    'get_market_data',
    'get_available_market_fields',
    'get_st_stocks_by_date',
    'get_stocks_by_listing_days',
    'get_stocks_by_listing_date',
    'get_non_limit_stocks',
    'get_financial_data',
    'get_latest_financial_data',
    'get_financial_data_by_quarter',
    'get_financial_time_series',
    'get_financial_cross_section',
    'get_financial_all_symbols',
]


def init(configPath: Optional[str] = None) -> None:
    """
    Initialize the panda_data package with configuration

    Args:
        config_path: Path to the config file. If None, will use default config from panda_common.config
    """
    global _config, _factor, _market_data, _market_min_data,_financial_data_reader

    try:
        # 延迟导入，避免在模块加载时就导入
        from .factor.factor_reader import FactorReader
        from .market_data.market_data_reader import MarketDataReader
        from .market_data.market_stock_cn_minute_reader import MarketStockCnMinReaderV3
        from .financial.financial_data_reader import FinancialDataReader # 新增
        
        # 使用panda_common中的配置
        _config = get_config()

        if not _config:
            raise RuntimeError("Failed to load configuration from panda_common")

        _factor = FactorReader(_config)
        _market_data = MarketDataReader(_config)
        _market_min_data = MarketStockCnMinReaderV3(_config)
        _financial_data_reader = FinancialDataReader(_config) # 新增
    except Exception as e:
        raise RuntimeError(f"Failed to initialize panda_data: {str(e)}")


def get_all_symbols() -> pd.DataFrame:
    if _market_min_data is None:
        raise RuntimeError("Please call init() before using any functions")
    return _market_min_data.get_all_symbols()


def get_factor(
        factors: Union[str, List[str]],
        start_date: str,
        end_date: str,
        symbols: Optional[Union[str, List[str]]] = None,
        index_component: Optional[str] = None,
        type: Optional[str] = 'stock'
) -> Optional[pd.DataFrame]:
    """
    Get factor data for given symbols and date range

    Args:
        factors: List of factor names to retrieve or single factor name
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        symbols: Optional list of symbols or single symbol. If None, returns all symbols

    Returns:
        pandas DataFrame with factor data, or None if no data found
    """
    if _factor is None:
        raise RuntimeError("Please call init() before using any functions")

    return _factor.get_factor(symbols, factors, start_date, end_date, index_component, type)


def get_custom_factor(
        factor_logger: logging.Logger,
        user_id: int,
        factor_name: str,
        start_date: str,
        end_date: str,
        symbol_type: Optional[str] = 'stock'
) -> Optional[pd.DataFrame]:
    """
    Get factor data for given symbols and date range

    Args:
        factors: List of factor names to retrieve or single factor name
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        symbols: Optional list of symbols or single symbol. If None, returns all symbols

    Returns:
        pandas DataFrame with factor data, or None if no data found
    """
    if _factor is None:
        raise RuntimeError("Please call init() before using any functions")

    return _factor.get_custom_factor(factor_logger, user_id, factor_name, start_date, end_date, symbol_type)

def get_factor_by_name(factor_name, start_date, end_date):
    if _factor is None:
        raise RuntimeError("Please call init() before using any functions")
    return _factor.get_factor_by_name(factor_name, start_date, end_date)


"""获取所有股票代码"""


def get_stock_instruments() -> pd.DataFrame:
    stocks = _market_min_data.get_stock_instruments()
    return pd.DataFrame(stocks)


def get_market_min_data(
        start_date: str,
        end_date: str,
        symbol: Optional[str] = None,
        fields: Optional[Union[str, List[str]]] = None
) -> Optional[pd.DataFrame]:
    """
    Get market data for given symbols and date range

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        symbols: Optional list of symbols or single symbol. If None, returns all symbols
        fields: Optional list of fields to retrieve (e.g., ['open', 'close', 'volume']).
               If None, returns all available fields

    Returns:
        pandas DataFrame with market data, or None if no data found
    """
    if _market_min_data is None:
        raise RuntimeError("Please call init() before using any functions")

    return _market_min_data.get_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        fields=fields
    )


def get_market_data(
        start_date: str,
        end_date: str,
        indicator="000985",
        st=True,
        symbols: Optional[Union[str, List[str]]] = None,
        fields: Optional[Union[str, List[str]]] = None
) -> Optional[pd.DataFrame]:
    """
    Get market data for given symbols and date range

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        symbols: Optional list of symbols or single symbol. If None, returns all symbols
        fields: Optional list of fields to retrieve (e.g., ['open', 'close', 'volume']).
               If None, returns all available fields

    Returns:
        pandas DataFrame with market data, or None if no data found
    """
    if _market_data is None:
        raise RuntimeError("Please call init() before using any functions")

    return _market_data.get_market_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        indicator=indicator,
        st=st,
        fields=fields
    )


def get_available_market_fields() -> List[str]:
    """
    Get all available fields in the stock_market collection

    Returns:
        List of available field names
    """
    if _market_data is None:
        raise RuntimeError("Please call init() before using any functions")

    return _market_data.get_available_fields()


def get_st_stocks_by_date(date: str) -> Optional[List[str]]:
    """
    获取指定日期的所有 ST 股票
    
    参数:
        date (str): 参考日期，格式为 YYYYMMDD (例如: "20250115")
        
    返回:
        List[str]: ST 股票代码列表，如果没有找到则返回 None
    """
    if _market_data is None:
        raise RuntimeError("请先调用 init() 函数进行初始化")
    
    return _market_data.get_st_stocks_by_date(date)


def get_stocks_by_listing_days(date: str, min_trading_days: int = 250) -> Optional[List[str]]:
    """
    获取上市天数满足条件的股票列表
    
    根据上市日期和参考日期计算上市天数，返回满足最小上市天数要求的股票列表。
    
    参数:
        date (str): 参考日期，格式为 YYYYMMDD (例如: "20250115")
        min_trading_days (int): 最小上市天数，默认为 250 天
        
    返回:
        List[str]: 满足条件的股票代码列表，如果没有找到则返回 None
    """
    if _market_data is None:
        raise RuntimeError("请先调用 init() 函数进行初始化")
    
    return _market_data.get_stocks_by_listing_days(date, min_trading_days)


def get_stocks_by_listing_date(date: str, max_days_since_listing: Optional[int] = None) -> Optional[List[str]]:
    """
    根据上市日期过滤股票列表
    
    返回在参考日期之前上市的股票，可选择限制最大上市天数。
    
    参数:
        date (str): 参考日期，格式为 YYYYMMDD (例如: "20250115")
        max_days_since_listing (int, 可选): 最大上市天数限制
            - 如果为 None: 返回所有在参考日期之前上市的股票
            - 如果设置值: 返回在参考日期前 N 天内上市的股票 (例如: 250 表示最近 250 天内上市的股票)
        
    返回:
        List[str]: 满足条件的股票代码列表，如果没有找到则返回 None
    """
    if _market_data is None:
        raise RuntimeError("请先调用 init() 函数进行初始化")
    
    return _market_data.get_stocks_by_listing_date(date, max_days_since_listing)


def get_non_limit_stocks(date: str, limit_rate: float = 0.095, symbols: Optional[Union[str, List[str]]] = None) -> Optional[List[str]]:
    """获取指定日期未触及涨跌停的股票列表（近似过滤）

    参数:
        date (str): 交易日期，格式为 YYYYMMDD
        limit_rate (float): 涨跌停幅度，普通股一般为 0.10，ST 股为 0.05，创业板/科创板为 0.20。
                            这里只用于近似判断，可根据策略需求进行调整。
        symbols (Union[str, List[str]], 可选): 股票代码或代码列表；为 None 时在当前可用股票全集上判断。

    返回:
        List[str]: 未涨停且未跌停的股票代码列表；如无数据或全部为涨跌停，则返回 None。
    """
    if _market_data is None:
        raise RuntimeError("请先调用 init() 函数进行初始化")

    if isinstance(symbols, str):
        symbols = [symbols]

    return _market_data.get_non_limit_stocks(date=date, limit_rate=limit_rate, symbols=symbols)


def get_financial_data(
    symbols: Optional[Union[str, List[str]]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fields: Optional[Union[str, List[str]]] = None,
    data_type: str = 'indicator',
    date_type: str = 'ann_date'
) -> Optional[pd.DataFrame]:
    if _financial_data_reader is None:
        raise RuntimeError("Please call init() before using any financial functions")
    return _financial_data_reader.get_financial_data(
        symbols=symbols, start_date=start_date, end_date=end_date,
        fields=fields, data_type=data_type, date_type=date_type
    )

def get_latest_financial_data(
    symbols: Optional[Union[str, List[str]]] = None,
    fields: Optional[Union[str, List[str]]] = None,
    data_type: str = 'indicator',
    as_of_date: Optional[str] = None
) -> Optional[pd.DataFrame]:
    if _financial_data_reader is None:
        raise RuntimeError("Please call init() before using any financial functions")
    return _financial_data_reader.get_latest_financial_data(
        symbols=symbols, fields=fields, data_type=data_type, as_of_date=as_of_date
    )

def get_financial_data_by_quarter(
    symbols: Optional[Union[str, List[str]]] = None,
    quarters: Optional[Union[str, List[str]]] = None,
    fields: Optional[Union[str, List[str]]] = None,
    data_type: str = 'indicator'
) -> Optional[pd.DataFrame]:
    if _financial_data_reader is None:
        raise RuntimeError("Please call init() before using any financial functions")
    return _financial_data_reader.get_financial_data_by_quarter(
        symbols=symbols, quarters=quarters, fields=fields, data_type=data_type
    )

def get_financial_time_series(
    symbol: str,
    fields: Optional[Union[str, List[str]]],
    data_type: str = 'indicator',
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_type: str = 'ann_date'
) -> Optional[pd.DataFrame]:
    if _financial_data_reader is None:
        raise RuntimeError("Please call init() before using any financial functions")
    return _financial_data_reader.get_financial_time_series(
        symbol=symbol, fields=fields, data_type=data_type,
        start_date=start_date, end_date=end_date, date_type=date_type
    )

def get_financial_cross_section(
    date: str,
    fields: Optional[Union[str, List[str]]],
    data_type: str = 'indicator',
    date_type: str = 'ann_date',
    symbols: Optional[Union[str, List[str]]] = None
) -> Optional[pd.DataFrame]:
    if _financial_data_reader is None:
        raise RuntimeError("Please call init() before using any financial functions")
    return _financial_data_reader.get_financial_cross_section(
        date=date, fields=fields, data_type=data_type,
        date_type=date_type, symbols=symbols
    )

def get_financial_all_symbols() -> List[str]:
    if _financial_data_reader is None:
        raise RuntimeError("Please call init() before using any financial functions")
    return _financial_data_reader.get_all_symbols()