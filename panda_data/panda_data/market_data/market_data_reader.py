from math import e
import pandas as pd
import time
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any


class MarketDataReader:
    def __init__(self, config):
        self.config = config
        # Initialize DatabaseHandler
        self.db_handler = DatabaseHandler(config)
        self.all_symbols = self.get_all_symbols()

    def _chunk_date_range(self, start_date: str, end_date: str, chunk_months: int = 3) -> List[tuple]:
        """
        Split date range into smaller chunks for parallel processing.
        If start_date equals end_date, returns a single chunk with the same date.

        Args:
            start_date (str): Start date in YYYYMMDD format
            end_date (str): End date in YYYYMMDD format
            chunk_months (int): Number of months per chunk

        Returns:
            List[tuple]: List of (chunk_start, chunk_end) date tuples
        """
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")

        # Handle single day case
        if start == end:
            return [(start_date, end_date)]

        chunks = []
        chunk_start = start

        while chunk_start <= end:  # Changed from < to <= to include end date
            # Calculate chunk_end
            chunk_end = min(
                chunk_start + timedelta(days=chunk_months * 30 - 1),  # Subtract 1 to not overlap with next chunk
                end
            )

            chunks.append((
                chunk_start.strftime("%Y%m%d"),
                chunk_end.strftime("%Y%m%d")
            ))

            # If we've reached or passed the end date, break
            if chunk_end >= end:
                break

            # Move to next chunk start
            chunk_start = chunk_end + timedelta(days=1)

        return chunks

    def _get_chunk_data(self, chunk_dates: tuple, query_params: Dict[str, Any], type: Optional[str] = 'stock') -> \
    Optional[pd.DataFrame]:
        """
        Get data for a specific date chunk
        """
        start_date, end_date = chunk_dates
        symbols = query_params['symbols']
        fields = query_params['fields']
        indicator = query_params['indicator']
        st = query_params['st']

        # Build query for this chunk
        query = {}
        
        # Add symbols filter if symbols are specified
        if symbols:
            query["symbol"] = {"$in": symbols}
        
        # Add date filter
        if start_date == end_date:
            # 如果是同一天，直接精确匹配
            query["date"] = start_date
        else:
            # 如果是不同日期，使用范围查询
            query["date"] = {
                "$gte": start_date,
                "$lte": end_date
            }

        if indicator != "000985":
            if indicator == "000300":
                query["index_component"] = "100"
            elif indicator == "000905":
                query["index_component"] = "010"
            elif indicator == "000852":
                query["index_component"] = "001"
        if not st:
            query["name"] = {"$not": {"$regex": "ST"}}
        # 构建投影
        projection = None
        if fields:
            projection = {field: 1 for field in fields + ['date', 'symbol']}
            if '_id' not in fields:
                projection['_id'] = 0

        # 估算每条记录的大小并设置合适的batch_size
        estimated_doc_size = len(fields) * 20 if fields else 200  # 假设每个字段平均20字节
        target_batch_size = min(
            max(
                int(10 * 1024 * 1024 / estimated_doc_size),  # 10MB / 每条记录大小
                2000  # 最小batch_size
            ),
            10000  # 最大batch_size
        )
        if type == 'future':
            query["$expr"] = {
                "$eq": [
                    "$symbol",
                    {"$concat": ["$underlying_symbol", "88"]}
                ]
            }
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "future_market"
            )
        else:
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stock_market"
            )
        cursor = collection.find(
            query,
            projection=projection
        ).batch_size(target_batch_size)

        chunk_df = pd.DataFrame(list(cursor))
        if chunk_df.empty:
            return None

        if '_id' in chunk_df.columns:
            chunk_df = chunk_df.drop(columns=['_id'])

        return chunk_df

    def get_market_data(self, symbols=None, start_date=None, end_date=None, indicator="000985", st=True, fields=None,
                        type: Optional[str] = 'stock'):
        """
        Get market data for given symbols and date range using parallel processing

        Args:
            symbols: List of stock symbols or single symbol
            fields: List of fields to retrieve (e.g., ['open', 'close', 'volume'])
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            indicator: Index code for filtering
            st: Whether to include ST stocks

        Returns:
            pandas DataFrame with market data
        """
        start_time = time.time()

        # 参数验证
        if start_date is None or end_date is None:
            logger.error("start_date and end_date must be provided")
            return None

        # Convert parameters to list if they're not already
        if isinstance(symbols, str):
            symbols = [symbols]
        if isinstance(fields, str):
            fields = [fields]
        if fields is None:
            fields = []

        if symbols is None:
            symbols = self.all_symbols

        # 准备查询参数
        query_params = {
            'symbols': symbols,
            'fields': fields,
            'indicator': indicator,
            'st': st
        }

        # 将日期范围分成多个小块
        date_chunks = self._chunk_date_range(str(start_date), str(end_date))
        print(date_chunks)

        # 使用线程池并行处理每个块
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(self._get_chunk_data, chunk, query_params, str(type))
                for chunk in date_chunks
            ]

            for future in concurrent.futures.as_completed(futures):
                chunk_df = future.result()
                if chunk_df is not None and not chunk_df.empty:
                    dfs.append(chunk_df)

        if not dfs:
            logger.warning(f"No market data found for the specified parameters")
            return None

        # 合并所有数据块
        final_df = pd.concat(dfs, ignore_index=True)

        end_time = time.time()
        logger.info(f"Market data query and conversion took {end_time - start_time:.2f} seconds")

        return final_df

    def get_st_stocks_by_date(self, date: str) -> Optional[List[str]]:
        """
        Get all ST stocks for a specific date
        
        Args:
            date (str): Date in YYYYMMDD format
            
        Returns:
            List[str]: List of ST stock symbols for the given date, or None if no ST stocks found
        """
        try:
            # Build query for ST stocks on the specified date
            query = {
                "date": date,
                "name": {"$regex": "ST"}  # Match stocks with "ST" in their name
            }
            
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stock_market"
            )
            
            # Get distinct symbols matching the query
            st_symbols = collection.distinct("symbol", query)
            
            if not st_symbols:
                logger.warning(f"No ST stocks found for date {date}")
                return None
            
            logger.info(f"Found {len(st_symbols)} ST stocks for date {date}")
            return st_symbols
            
        except Exception as e:
            logger.error(f"Error querying ST stocks for date {date}: {str(e)}")
            return None

    def get_stocks_by_listing_days(self, date: str, min_trading_days: int = 250) -> Optional[List[str]]:
        """
        获取上市天数满足条件的股票列表
        
        根据上市日期和参考日期计算上市天数，返回满足最小上市天数要求的股票列表。
        
        参数:
            date (str): 参考日期，格式为 YYYYMMDD (例如: "20250115")
            min_trading_days (int): 最小上市天数，默认为 250 天
            
        返回:
            List[str]: 满足条件的股票代码列表，如果没有找到则返回 None
        """
        try:
            # 获取 stocks 集合
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stocks"
            )
            
            # 将参考日期转换为 datetime 对象用于计算
            ref_date = datetime.strptime(date, "%Y%m%d")
            
            # 查询所有股票信息
            qualified_symbols = []
            
            # 遍历所有股票，检查上市天数
            for stock in collection.find({}):
                symbol = stock.get("symbol")
                list_date = stock.get("list_date")
                
                if not symbol or not list_date:
                    continue
                
                try:
                    # 将上市日期转换为 datetime 对象
                    listing_date = datetime.strptime(str(list_date), "%Y%m%d")
                    
                    # 计算上市天数
                    days_since_listing = (ref_date - listing_date).days
                    
                    # 检查是否满足最小上市天数要求
                    if days_since_listing >= min_trading_days:
                        qualified_symbols.append(symbol)
                        
                except ValueError as e:
                    logger.warning(f"股票 {symbol} 的上市日期格式错误: {list_date}, 错误: {str(e)}")
                    continue
            
            if not qualified_symbols:
                logger.warning(f"在 {date} 没有找到上市天数 >= {min_trading_days} 的股票")
                return None
            
            logger.info(f"在 {date} 找到 {len(qualified_symbols)} 只上市天数 >= {min_trading_days} 的股票")
            return qualified_symbols
            
        except Exception as e:
            logger.error(f"查询上市天数满足条件的股票时出错 (日期: {date}): {str(e)}")
            return None

    def get_stocks_by_listing_date(self, date: str, max_days_since_listing: Optional[int] = None) -> Optional[List[str]]:
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
        try:
            # 获取 stocks 集合
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stocks"
            )
            
            # 将参考日期转换为 datetime 对象用于计算
            ref_date = datetime.strptime(date, "%Y%m%d")
            
            # 查询所有股票信息
            qualified_symbols = []
            
            # 遍历所有股票，检查上市日期
            for stock in collection.find({}):
                symbol = stock.get("symbol")
                list_date = stock.get("list_date")
                
                if not symbol or not list_date:
                    continue
                
                try:
                    # 将上市日期转换为 datetime 对象
                    listing_date = datetime.strptime(str(list_date), "%Y%m%d")
                    
                    # 检查上市日期是否不晚于参考日期
                    if listing_date <= ref_date:
                        # 如果设置了最大上市天数限制，进行额外检查
                        if max_days_since_listing is not None:
                            days_since_listing = (ref_date - listing_date).days
                            if days_since_listing <= max_days_since_listing:
                                qualified_symbols.append(symbol)
                        else:
                            # 没有最大天数限制，直接添加
                            qualified_symbols.append(symbol)
                            
                except ValueError as e:
                    logger.warning(f"股票 {symbol} 的上市日期格式错误: {list_date}, 错误: {str(e)}")
                    continue
            
            if not qualified_symbols:
                logger.warning(f"在 {date} 没有找到满足上市日期条件的股票")
                return None
            
            logger.info(f"在 {date} 找到 {len(qualified_symbols)} 只满足上市日期条件的股票")
            return qualified_symbols
            
        except Exception as e:
            logger.error(f"查询上市日期满足条件的股票时出错 (日期: {date}): {str(e)}")
            return None

    def get_non_limit_stocks(self, date: str, limit_rate: float = 0.095, symbols: Optional[List[str]] = None) -> Optional[List[str]]:
        """获取指定日期未触及涨跌停的股票列表

        Args:
            date: 交易日期，格式为 YYYYMMDD
            limit_rate: 涨跌停幅度，普通股一般为 0.10，ST 股为 0.05，创业板/科创板为 0.20。
                        这里用于近似判断，可根据策略需要调整。
            symbols: 可选的股票代码列表；如果为 None，则在当前 universe 中所有股票上判断。

        Returns:
            未涨停且未跌停的股票代码列表；如无数据或全部为涨跌停，则返回 None。
        """
        try:
            fields = ["open", "high", "low", "close", "pre_close"]

            df = self.get_market_data(
                symbols=symbols,
                start_date=date,
                end_date=date,
                fields=fields,
            )

            if df is None or df.empty:
                logger.warning(f"在 {date} 未查询到行情数据，无法判断涨跌停")
                return None

            # 仅保留需要的列，确保包含 symbol
            if "symbol" not in df.columns:
                logger.error("行情数据中缺少 symbol 字段，无法判断涨跌停")
                return None

            # 过滤掉缺失关键价格字段的记录
            df = df.dropna(subset=["close", "pre_close"])
            if df.empty:
                logger.warning(f"在 {date} 有效价格数据为空，无法判断涨跌停")
                return None

            # 计算简单日收益率
            df["ret"] = df["close"] / df["pre_close"] - 1

            # 采用略低于限制幅度的阈值做近似过滤，避免由于价格精度导致的误判
            threshold = limit_rate * 0.98
            abs_ret = df["ret"].abs()

            # 近似认为 |涨跌幅| 大于等于 threshold 的为涨跌停
            is_limit = abs_ret >= threshold
            non_limit_df = df[~is_limit]

            if non_limit_df.empty:
                logger.info(f"在 {date} 所有股票近似视为涨跌停，没有可用标的")
                return None

            symbols_list = sorted(non_limit_df["symbol"].unique().tolist())
            logger.info(f"在 {date} 过滤后得到 {len(symbols_list)} 只未触及涨跌停股票")
            return symbols_list

        except Exception as e:
            logger.error(f"在 {date} 过滤涨跌停股票时出错: {str(e)}")
            return None

    def get_all_symbols(self):
        """Get all unique symbols using distinct command"""
        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"],
            "stock_market"
        )
        return collection.distinct("symbol")