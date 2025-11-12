"""
删除指数行情数据集合
用于清空 index_market 表的所有数据和索引
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from panda_common.config import get_config
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger


def drop_index_market_collection():
    """删除指数行情数据集合"""
    try:
        config = get_config()
        db_handler = DatabaseHandler(config)
        db = db_handler.mongo_client[config["MONGO_DB"]]
        
        collection_name = 'index_market'
        
        logger.info("=" * 60)
        logger.info(f"准备删除集合: {collection_name}")
        logger.info("=" * 60)
        
        # 检查集合是否存在
        if collection_name in db.list_collection_names():
            # 统计记录数
            record_count = db[collection_name].count_documents({})
            logger.info(f"当前记录数: {record_count}")
            
            # 显示索引信息
            indexes = db[collection_name].index_information()
            logger.info(f"当前索引数: {len(indexes)}")
            for idx_name in indexes.keys():
                logger.info(f"  - {idx_name}")
            
            # 删除集合
            logger.info(f"\n正在删除集合 {collection_name}...")
            db[collection_name].drop()
            logger.info(f"✓ 成功删除集合 {collection_name}")
            
        else:
            logger.info(f"集合 {collection_name} 不存在，无需删除")
        
        # 验证删除结果
        if collection_name not in db.list_collection_names():
            logger.info(f"\n验证: 集合 {collection_name} 已不存在")
            logger.info("=" * 60)
            logger.info("删除完成！")
            logger.info("=" * 60)
            return True
        else:
            logger.error(f"删除失败: 集合 {collection_name} 仍然存在")
            return False
        
    except Exception as e:
        logger.error(f"删除集合失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = drop_index_market_collection()
    sys.exit(0 if success else 1)

