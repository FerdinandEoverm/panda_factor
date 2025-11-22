from fastapi import APIRouter, Query

from panda_common.config import config
from panda_data_hub.services.query.stock_statistic_service import StockStatisticQuery

router = APIRouter()

@router.get('/data_query')
async def data_query(tables_name : str ,
                     start_date : str ,
                     end_date : str,
                     page: int = Query(default=1, ge=1, description="页码"),
                     page_size: int = Query(default=10, ge=1, le=100, description="每页数量"),
                     sort_field: str = Query(default="created_at", description="排序字段，支持created_at、return_ratio、sharpe_ratio、maximum_drawdown、IC、IR"),
                     sort_order: str = Query(default="desc", description="排序方式，asc升序，desc降序")
                     ):
    """ 根据表名和起止时间获取统计数据 """
    service = StockStatisticQuery(config)
    result_data = service.get_stock_statistic(tables_name,start_date,end_date,page, page_size,sort_field, sort_order)

    return result_data

@router.get('/get_trading_days')
async def get_trading_days(
        start_date: str ,
        end_date: str ,
):
    service = StockStatisticQuery(config)
    result_data = service.get_trading_days(start_date,end_date)
    return result_data

@router.get('/count_collection')
async def count_collection(collection: str):
    """查询指定集合的记录总数"""
    from panda_common.handlers.database_handler import DatabaseHandler
    
    # 允许查询的集合列表（白名单）
    allowed_collections = [
        'stock_market',
        'factor_base',
        'valuation_factor',
        'financial_indicator',
        'financial_income',
        'financial_balance',
        'financial_cashflow',
        'stock_dividends'
    ]
    
    if collection not in allowed_collections:
        return {"error": "不允许查询的集合", "count": 0}
    
    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            collection
        )
        count = mongo_collection.count_documents({})
        return {"collection": collection, "count": count}
    except Exception as e:
        return {"error": str(e), "count": 0}

@router.get('/financial_stats_by_quarter')
async def financial_stats_by_quarter():
    """按季度统计财务数据（最近4个季度）"""
    from panda_common.handlers.database_handler import DatabaseHandler
    from datetime import datetime
    
    try:
        db_handler = DatabaseHandler(config)
        
        # 财务数据表列表
        financial_tables = {
            'financial_indicator': '财务指标',
            'financial_income': '利润表',
            'financial_balance': '资产负债表',
            'financial_cashflow': '现金流量表'
        }
        
        # 生成最近4个季度（从当前往前推）
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # 确定当前季度
        if current_month <= 3:
            current_quarter = 1
        elif current_month <= 6:
            current_quarter = 2
        elif current_month <= 9:
            current_quarter = 3
        else:
            current_quarter = 4
        
        # 生成最近4个季度的报告期（YYYYMMDD格式，季度末日期）
        quarters = []
        year = current_year
        quarter = current_quarter
        
        for i in range(4):
            if quarter == 1:
                end_date = f"{year}0331"
            elif quarter == 2:
                end_date = f"{year}0630"
            elif quarter == 3:
                end_date = f"{year}0930"
            else:
                end_date = f"{year}1231"
            
            quarters.append({
                'end_date': end_date,
                'label': f"{year}Q{quarter}"
            })
            
            # 往前推一个季度
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1
        
        # 查询每个表在每个季度的数据量
        result = []
        for table_name, table_label in financial_tables.items():
            mongo_collection = db_handler.get_mongo_collection(
                config["MONGO_DB"],
                table_name
            )
            
            quarter_stats = []
            total_count = 0
            
            for quarter_info in quarters:
                count = mongo_collection.count_documents({
                    "end_date": quarter_info['end_date']
                })
                quarter_stats.append({
                    'quarter': quarter_info['label'],
                    'end_date': quarter_info['end_date'],
                    'count': count
                })
                total_count += count
            
            result.append({
                'table': table_name,
                'label': table_label,
                'quarters': quarter_stats,
                'total': total_count
            })
        
        return {
            'success': True,
            'data': result,
            'quarters': [q['label'] for q in quarters]
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': []
        }

@router.get('/query_financial_data')
async def query_financial_data(
    table_name: str = Query(..., description="财务数据表名：financial_indicator, financial_income, financial_balance, financial_cashflow"),
    end_date: str = Query(..., description="报告期（YYYYMMDD格式，如20231231）"),
    symbol: str = Query(None, description="股票代码（可选，如000001.SZ）"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量")
):
    """查询指定报告期、指定股票、指定表的财务数据"""
    from panda_common.handlers.database_handler import DatabaseHandler
    import math
    
    # 允许查询的表列表（白名单）
    allowed_tables = [
        'financial_indicator',
        'financial_income',
        'financial_balance',
        'financial_cashflow'
    ]
    
    if table_name not in allowed_tables:
        return {
            'success': False,
            'error': '不允许查询的数据表',
            'data': [],
            'pagination': {}
        }
    
    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            table_name
        )
        
        # 构建查询条件
        query_filter = {"end_date": end_date}
        if symbol:
            query_filter["symbol"] = symbol
        
        # 查询总数
        total_count = mongo_collection.count_documents(query_filter)
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size
        
        # 分页查询
        skip = (page - 1) * page_size
        cursor = mongo_collection.find(query_filter).skip(skip).limit(page_size)
        
        # 转换结果并处理NaN值
        data = []
        for doc in cursor:
            # 移除MongoDB的_id字段
            if '_id' in doc:
                del doc['_id']
            
            # 处理NaN和Inf值，将它们转换为None
            cleaned_doc = {}
            for key, value in doc.items():
                if isinstance(value, float):
                    # 检查是否是NaN或Inf
                    if math.isnan(value) or math.isinf(value):
                        cleaned_doc[key] = None
                    else:
                        cleaned_doc[key] = value
                else:
                    cleaned_doc[key] = value
            
            data.append(cleaned_doc)
        
        return {
            'success': True,
            'data': data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total_count,
                'total_pages': total_pages
            },
            'query': {
                'table_name': table_name,
                'end_date': end_date,
                'symbol': symbol
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': [],
            'pagination': {}
        }

@router.get('/dividend_stats_by_year')
async def dividend_stats_by_year():
    """按年度统计分红数据（最近5年）"""
    from panda_common.handlers.database_handler import DatabaseHandler
    from datetime import datetime
    
    try:
        db_handler = DatabaseHandler(config)
        
        # 生成最近5年的年份
        current_year = datetime.now().year
        years = [str(year) for year in range(current_year, current_year - 5, -1)]
        
        # 查询分红数据集合
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'stock_dividends'
        )
        
        # 按年度统计
        year_stats = []
        total_count = 0
        
        for year in years:
            # 统计该年度的分红记录（按除权除息日的年份）
            count = mongo_collection.count_documents({
                "ex_div_date": {"$regex": f"^{year}"}
            })
            year_stats.append({
                'year': year,
                'count': count
            })
            total_count += count
        
        # 统计总记录数
        all_count = mongo_collection.count_documents({})
        
        return {
            'success': True,
            'data': {
                'years': year_stats,
                'total': total_count,
                'all_count': all_count
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': {}
        }

@router.get('/query_dividend_data')
async def query_dividend_data(
    year: str = Query(None, description="除权除息年份（可选，如2024）"),
    symbol: str = Query(None, description="股票代码（可选，如000001.SZ）"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量")
):
    """查询分红数据"""
    from panda_common.handlers.database_handler import DatabaseHandler
    import math
    
    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'stock_dividends'
        )
        
        # 构建查询条件
        query_filter = {}
        
        # 如果指定了年份，按除权除息日的年份查询
        if year:
            query_filter["ex_div_date"] = {"$regex": f"^{year}"}
        
        # 如果指定了股票代码
        if symbol:
            query_filter["symbol"] = symbol
        
        # 查询总数
        total_count = mongo_collection.count_documents(query_filter)
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        # 分页查询，按除权除息日倒序排列
        skip = (page - 1) * page_size
        cursor = mongo_collection.find(query_filter).sort("ex_div_date", -1).skip(skip).limit(page_size)
        
        # 转换结果并处理NaN值
        data = []
        for doc in cursor:
            # 移除MongoDB的_id字段
            if '_id' in doc:
                del doc['_id']
            
            # 处理NaN和Inf值，将它们转换为None
            cleaned_doc = {}
            for key, value in doc.items():
                if isinstance(value, float):
                    # 检查是否是NaN或Inf
                    if math.isnan(value) or math.isinf(value):
                        cleaned_doc[key] = None
                    else:
                        cleaned_doc[key] = value
                else:
                    cleaned_doc[key] = value
            
            data.append(cleaned_doc)
        
        return {
            'success': True,
            'data': data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total_count,
                'total_pages': total_pages
            },
            'query': {
                'year': year,
                'symbol': symbol
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': [],
            'pagination': {}
        }

@router.get('/query_index_market_data')
async def query_index_market_data(
    index_code: str = Query(None, description="指数代码（可选，如000001.SH上证指数、399001.SZ深证成指）"),
    start_date: str = Query(None, description="开始日期（可选，YYYYMMDD格式）"),
    end_date: str = Query(None, description="结束日期（可选，YYYYMMDD格式）"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量")
):
    """查询指数行情数据"""
    from panda_common.handlers.database_handler import DatabaseHandler
    import math
    
    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'index_market'
        )
        
        # 构建查询条件
        query_filter = {}
        
        # 如果指定了指数代码
        if index_code:
            query_filter["symbol"] = index_code
        
        # 如果指定了日期范围
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            if date_filter:
                query_filter["trade_date"] = date_filter
        
        # 查询总数
        total_count = mongo_collection.count_documents(query_filter)
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        # 分页查询，按交易日期倒序排列
        skip = (page - 1) * page_size
        cursor = mongo_collection.find(query_filter).sort("trade_date", -1).skip(skip).limit(page_size)
        
        # 转换结果并处理NaN值
        data = []
        for doc in cursor:
            # 移除MongoDB的_id字段
            if '_id' in doc:
                del doc['_id']
            
            # 处理NaN和Inf值，将它们转换为None
            cleaned_doc = {}
            for key, value in doc.items():
                if isinstance(value, float):
                    # 检查是否是NaN或Inf
                    if math.isnan(value) or math.isinf(value):
                        cleaned_doc[key] = None
                    else:
                        cleaned_doc[key] = value
                else:
                    cleaned_doc[key] = value
            
            data.append(cleaned_doc)
        
        return {
            'success': True,
            'data': data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total_count,
                'total_pages': total_pages
            },
            'query': {
                'index_code': index_code,
                'start_date': start_date,
                'end_date': end_date
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': [],
            'pagination': {}
        }

@router.get('/adj_factor_stats_by_year')
async def adj_factor_stats_by_year():
    """按年度统计复权因子数据（最近5年）"""
    from panda_common.handlers.database_handler import DatabaseHandler
    from datetime import datetime
    
    try:
        db_handler = DatabaseHandler(config)
        
        # 生成最近5年的年份
        current_year = datetime.now().year
        years = [str(year) for year in range(current_year, current_year - 5, -1)]
        
        # 查询复权因子数据集合
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'adj_factor'
        )
        
        # 按年度统计
        year_stats = []
        total_count = 0
        
        for year in years:
            # 统计该年度的复权因子记录（按日期的年份）
            count = mongo_collection.count_documents({
                "date": {"$regex": f"^{year}"}
            })
            year_stats.append({
                'year': year,
                'count': count
            })
            total_count += count
        
        # 统计总记录数
        all_count = mongo_collection.count_documents({})
        
        return {
            'success': True,
            'data': {
                'years': year_stats,
                'total': total_count,
                'all_count': all_count
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': {}
        }

@router.get('/query_stock_market_data')
async def query_stock_market_data(
    year: str = Query(None, description="年份（可选，如2024）"),
    symbol: str = Query(None, description="股票代码（可选，如000001.SZ）"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量")
):
    """查询股票行情数据"""
    from panda_common.handlers.database_handler import DatabaseHandler
    import math
    
    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'stock_market'
        )
        
        # 构建查询条件
        query_filter = {}
        
        # 如果指定了年份，按交易日期的年份查询
        if year:
            query_filter["trade_date"] = {"$regex": f"^{year}"}
        
        # 如果指定了股票代码
        if symbol:
            query_filter["symbol"] = symbol
        
        # 查询总数
        total_count = mongo_collection.count_documents(query_filter)
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        # 分页查询，按交易日期倒序排列
        skip = (page - 1) * page_size
        cursor = mongo_collection.find(query_filter).sort("trade_date", -1).skip(skip).limit(page_size)
        
        # 转换结果并处理NaN值
        data = []
        for doc in cursor:
            # 移除MongoDB的_id字段
            if '_id' in doc:
                del doc['_id']
            
            # 处理NaN和Inf值，将它们转换为None
            cleaned_doc = {}
            for key, value in doc.items():
                if isinstance(value, float):
                    # 检查是否是NaN或Inf
                    if math.isnan(value) or math.isinf(value):
                        cleaned_doc[key] = None
                    else:
                        cleaned_doc[key] = value
                else:
                    cleaned_doc[key] = value
            
            data.append(cleaned_doc)
        
        return {
            'success': True,
            'data': data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total_count,
                'total_pages': total_pages
            },
            'query': {
                'year': year,
                'symbol': symbol
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': [],
            'pagination': {}
        }

@router.get('/stock_market_stats_by_year')
async def stock_market_stats_by_year():
    """按年度统计股票行情数据（最近5年）"""
    from panda_common.handlers.database_handler import DatabaseHandler
    from datetime import datetime
    
    try:
        db_handler = DatabaseHandler(config)
        
        # 生成最近5年的年份
        current_year = datetime.now().year
        years = [str(year) for year in range(current_year, current_year - 5, -1)]
        
        # 查询股票行情数据集合
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'stock_market'
        )
        
        # 按年度统计
        year_stats = []
        total_count = 0
        
        for year in years:
            # 统计该年度的股票行情记录（按交易日期的年份）
            count = mongo_collection.count_documents({
                "trade_date": {"$regex": f"^{year}"}
            })
            year_stats.append({
                'year': year,
                'count': count
            })
            total_count += count
        
        # 统计总记录数
        all_count = mongo_collection.count_documents({})
        
        return {
            'success': True,
            'data': {
                'years': year_stats,
                'total': total_count,
                'all_count': all_count
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': {}
        }

@router.get('/query_valuation_factor_data')
async def query_valuation_factor_data(
    symbol: str = Query(None, description="股票代码（可选，如000001.SZ）"),
    start_date: str = Query(None, description="开始日期（可选，YYYYMMDD格式）"),
    end_date: str = Query(None, description="结束日期（可选，YYYYMMDD格式）"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=20, ge=1, le=100, description="每页数量")
):
    """查询估值因子数据"""
    from panda_common.handlers.database_handler import DatabaseHandler
    import math

    try:
        db_handler = DatabaseHandler(config)
        mongo_collection = db_handler.get_mongo_collection(
            config["MONGO_DB"],
            'factor_base'
        )

        # 构建查询条件
        query_filter = {}

        # 如果指定了股票代码
        if symbol:
            query_filter["symbol"] = symbol

        # 如果指定了日期范围
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            if date_filter:
                query_filter["date"] = date_filter

        # 查询总数
        total_count = mongo_collection.count_documents(query_filter)

        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1

        # 分页查询，按日期倒序排列
        skip = (page - 1) * page_size
        cursor = mongo_collection.find(query_filter).sort("date", -1).skip(skip).limit(page_size)

        # 转换结果并处理NaN值
        data = []
        for doc in cursor:
            # 移除MongoDB的_id字段
            if '_id' in doc:
                del doc['_id']

            # 处理NaN和Inf值，将它们转换为None
            cleaned_doc = {}
            for key, value in doc.items():
                if isinstance(value, float):
                    # 检查是否是NaN或Inf
                    if math.isnan(value) or math.isinf(value):
                        cleaned_doc[key] = None
                    else:
                        cleaned_doc[key] = value
                else:
                    cleaned_doc[key] = value

            data.append(cleaned_doc)

        return {
            'success': True,
            'data': data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total_count,
                'total_pages': total_pages
            },
            'query': {
                'symbol': symbol,
                'start_date': start_date,
                'end_date': end_date
            }
        }

    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'data': [],
            'pagination': {}
        }
