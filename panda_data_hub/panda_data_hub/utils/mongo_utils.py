from panda_common.config import config
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger


def ensure_collection_and_indexes(table_name):
    """ 确保集合存在并创建所需的索引 """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        
        # 特殊处理 stock_dividends 表
        if collection_name == 'stock_dividends':
            return ensure_collection_and_indexes_dividend(table_name)
        
        # 特殊处理 index_market 表
        if collection_name == 'index_market':
            return ensure_collection_and_indexes_index_market(table_name)
        
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建集合 {collection_name}")
        # 获取集合对象
        collection = db[collection_name]
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        # 检查是否已存在所需的索引
        if 'symbol_date_idx' not in existing_indexes:
            # 创建复合索引
            collection.create_index(
                [
                    ('symbol', 1),
                    ('date', 1)
                ],
                name='symbol_date_idx',  # 指定索引名称
                background=True  # 后台创建索引，不阻塞其他数据库操作
            )
            logger.info("成功创建索引 symbol_date_idx")
        else:
            logger.info("索引 symbol_date_idx 已存在")

    except Exception as e:
        logger.error(f"创建集合或索引失败: {str(e)}")
        raise  # 抛出异常，因为这是初始化的关键步骤


def ensure_collection_and_indexes_tm(table_name):
    """ 确保集合存在并创建所需的索引 """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建集合 {collection_name}")
        # 获取集合对象
        collection = db[collection_name]
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        # 检查是否已存在所需的索引
        if 'symbol_1_date_1' not in existing_indexes:
            # 创建复合索引
            collection.create_index(
                [
                    ('symbol', 1),
                    ('date', 1)
                ],
                name='symbol_1_date_1',  # 指定索引名称
                background=True  # 后台创建索引，不阻塞其他数据库操作
            )
            logger.info("成功创建索引 symbol_1_date_1")
        else:
            logger.info("索引 symbol_1_date_1 已存在")
    except Exception as e:
        logger.error(f"创建集合或索引失败: {str(e)}")
        raise  # 抛出异常，因为这是初始化的关键步骤


def ensure_collection_and_indexes_financial(table_name):
    """
    确保财务数据集合存在并创建所需的索引
    财务数据表使用 (symbol, end_date, ann_date) 复合索引
    """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建财务数据集合 {collection_name}")
        
        # 获取集合对象
        collection = db[collection_name]
        
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        
        # 创建主要的复合索引：symbol + end_date + ann_date
        index_name = 'symbol_end_date_ann_date_idx'
        if index_name not in existing_indexes:
            collection.create_index(
                [
                    ('symbol', 1),
                    ('end_date', 1),
                    ('ann_date', 1)
                ],
                name=index_name,
                background=True
            )
            logger.info(f"成功创建财务数据索引 {index_name}")
        else:
            logger.info(f"财务数据索引 {index_name} 已存在")
        
        # 创建辅助索引：ann_date（用于按公告日期查询）
        ann_date_index_name = 'ann_date_idx'
        if ann_date_index_name not in existing_indexes:
            collection.create_index(
                [('ann_date', 1)],
                name=ann_date_index_name,
                background=True
            )
            logger.info(f"成功创建财务数据索引 {ann_date_index_name}")
        else:
            logger.info(f"财务数据索引 {ann_date_index_name} 已存在")
        
        # 创建辅助索引：end_date（用于按报告期查询）
        end_date_index_name = 'end_date_idx'
        if end_date_index_name not in existing_indexes:
            collection.create_index(
                [('end_date', 1)],
                name=end_date_index_name,
                background=True
            )
            logger.info(f"成功创建财务数据索引 {end_date_index_name}")
        else:
            logger.info(f"财务数据索引 {end_date_index_name} 已存在")
        
    except Exception as e:
        logger.error(f"创建财务数据集合或索引失败: {str(e)}")
        raise


def ensure_collection_and_indexes_dividend(table_name):
    """
    确保股票分红数据集合存在并创建所需的索引
    股票分红数据表使用 (symbol, ex_div_date) 作为复合唯一索引
    """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建分红数据集合 {collection_name}")
        
        # 获取集合对象
        collection = db[collection_name]
        
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        
        # 创建主要的复合索引：symbol + ex_div_date（除权除息日）
        # 这个索引用于回测系统快速查询某只股票在特定日期的分红信息
        index_name = 'symbol_ex_div_date_idx'
        if index_name not in existing_indexes:
            collection.create_index(
                [
                    ('symbol', 1),
                    ('ex_div_date', 1)
                ],
                name=index_name,
                unique=True,  # 设置为唯一索引，确保同一股票在同一除权日只有一条记录
                background=True
            )
            logger.info(f"成功创建分红数据索引 {index_name}")
        else:
            logger.info(f"分红数据索引 {index_name} 已存在")
        
        # 创建辅助索引：ex_div_date（用于按除权日期查询所有分红股票）
        ex_div_date_index_name = 'ex_div_date_idx'
        if ex_div_date_index_name not in existing_indexes:
            collection.create_index(
                [('ex_div_date', 1)],
                name=ex_div_date_index_name,
                background=True
            )
            logger.info(f"成功创建分红数据索引 {ex_div_date_index_name}")
        else:
            logger.info(f"分红数据索引 {ex_div_date_index_name} 已存在")
        
        # 创建辅助索引：symbol（用于查询某只股票的所有分红记录）
        symbol_index_name = 'symbol_idx'
        if symbol_index_name not in existing_indexes:
            collection.create_index(
                [('symbol', 1)],
                name=symbol_index_name,
                background=True
            )
            logger.info(f"成功创建分红数据索引 {symbol_index_name}")
        else:
            logger.info(f"分红数据索引 {symbol_index_name} 已存在")
        
        # 创建辅助索引：announcement_date（用于查询某日公告的分红）
        ann_date_index_name = 'announcement_date_idx'
        if ann_date_index_name not in existing_indexes:
            collection.create_index(
                [('announcement_date', 1)],
                name=ann_date_index_name,
                background=True
            )
            logger.info(f"成功创建分红数据索引 {ann_date_index_name}")
        else:
            logger.info(f"分红数据索引 {ann_date_index_name} 已存在")
        
    except Exception as e:
        logger.error(f"创建分红数据集合或索引失败: {str(e)}")
        raise


def ensure_collection_and_indexes_index_market(table_name):
    """
    确保指数行情数据集合存在并创建所需的索引
    指数行情数据表使用 (symbol, trade_date) 作为复合唯一索引
    """
    try:
        # 获取数据库对象
        db = DatabaseHandler(config).mongo_client[config["MONGO_DB"]]
        collection_name = table_name
        
        # 检查集合是否存在
        if collection_name not in db.list_collection_names():
            # 创建集合
            db.create_collection(collection_name)
            logger.info(f"成功创建指数行情数据集合 {collection_name}")
        
        # 获取集合对象
        collection = db[collection_name]
        
        # 获取现有的索引信息
        existing_indexes = collection.index_information()
        
        # 删除旧的索引（如果存在）
        old_index_names = ['ts_code_date_idx', 'date_idx', 'ts_code_idx']
        for old_index_name in old_index_names:
            if old_index_name in existing_indexes:
                try:
                    collection.drop_index(old_index_name)
                    logger.info(f"成功删除旧索引 {old_index_name}")
                except Exception as e:
                    logger.warning(f"删除旧索引 {old_index_name} 失败: {str(e)}")
        
        # 重新获取索引信息
        existing_indexes = collection.index_information()
        
        # 创建主要的复合索引：symbol + trade_date
        # 这个索引用于快速查询某个指数在特定日期的行情数据
        index_name = 'symbol_trade_date_idx'
        if index_name not in existing_indexes:
            collection.create_index(
                [
                    ('symbol', 1),
                    ('trade_date', 1)
                ],
                name=index_name,
                unique=True,  # 设置为唯一索引，确保同一指数在同一日期只有一条记录
                background=True
            )
            logger.info(f"成功创建指数行情数据索引 {index_name}")
        else:
            logger.info(f"指数行情数据索引 {index_name} 已存在")
        
        # 创建辅助索引：trade_date（用于按日期查询所有指数的行情）
        date_index_name = 'trade_date_idx'
        if date_index_name not in existing_indexes:
            collection.create_index(
                [('trade_date', 1)],
                name=date_index_name,
                background=True
            )
            logger.info(f"成功创建指数行情数据索引 {date_index_name}")
        else:
            logger.info(f"指数行情数据索引 {date_index_name} 已存在")
        
        # 创建辅助索引：symbol（用于查询某个指数的所有历史行情）
        symbol_index_name = 'symbol_idx'
        if symbol_index_name not in existing_indexes:
            collection.create_index(
                [('symbol', 1)],
                name=symbol_index_name,
                background=True
            )
            logger.info(f"成功创建指数行情数据索引 {symbol_index_name}")
        else:
            logger.info(f"指数行情数据索引 {symbol_index_name} 已存在")
        
    except Exception as e:
        logger.error(f"创建指数行情数据集合或索引失败: {str(e)}")
        raise