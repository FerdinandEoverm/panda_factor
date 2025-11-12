"""
指数行情数据迁移脚本
用于修复字段名不一致导致的数据问题
- 删除 trade_date 为 null 的错误记录
- 删除旧的错误索引
- 重建正确的索引
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from panda_common.config import get_config
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes


def migrate_index_market_data():
    """
    迁移指数行情数据
    1. 删除有问题的数据（trade_date 为 null 的记录）
    2. 删除旧索引
    3. 创建新索引
    """
    try:
        config = get_config()
        db_handler = DatabaseHandler(config)
        db = db_handler.mongo_client[config["MONGO_DB"]]
        collection = db['index_market']
        
        logger.info("=" * 60)
        logger.info("开始迁移指数行情数据")
        logger.info("=" * 60)
        
        # 1. 统计当前数据情况
        total_count = collection.count_documents({})
        logger.info(f"当前总记录数: {total_count}")
        
        # 统计有问题的记录
        null_trade_date_count = collection.count_documents({'trade_date': None})
        logger.info(f"trade_date 为 null 的记录数: {null_trade_date_count}")
        
        null_date_count = collection.count_documents({'date': None})
        logger.info(f"date 为 null 的记录数: {null_date_count}")
        
        # 2. 删除 trade_date 为 null 的记录（这些是错误数据）
        if null_trade_date_count > 0:
            logger.info(f"正在删除 {null_trade_date_count} 条 trade_date 为 null 的错误记录...")
            result = collection.delete_many({'trade_date': None})
            logger.info(f"成功删除 {result.deleted_count} 条错误记录")
        
        # 3. 删除可能存在的错误记录（date 为 null）
        if null_date_count > 0:
            logger.info(f"正在删除 {null_date_count} 条 date 为 null 的错误记录...")
            result = collection.delete_many({'date': None})
            logger.info(f"成功删除 {result.deleted_count} 条错误记录")
        
        # 4. 获取当前索引信息
        logger.info("\n当前索引列表:")
        existing_indexes = collection.index_information()
        for idx_name, idx_info in existing_indexes.items():
            logger.info(f"  - {idx_name}: {idx_info.get('key', [])}")
        
        # 5. 删除旧的错误索引
        old_indexes_to_delete = ['symbol_trade_date_idx', 'trade_date_idx', 'ts_code_date_idx', 'date_idx', 'ts_code_idx']
        logger.info("\n正在删除旧索引...")
        for idx_name in old_indexes_to_delete:
            if idx_name in existing_indexes:
                try:
                    collection.drop_index(idx_name)
                    logger.info(f"  ✓ 成功删除索引: {idx_name}")
                except Exception as e:
                    logger.warning(f"  ✗ 删除索引 {idx_name} 失败: {str(e)}")
        
        # 6. 重建正确的索引
        logger.info("\n正在重建索引...")
        ensure_collection_and_indexes(table_name='index_market')
        
        # 7. 验证新索引
        logger.info("\n新索引列表:")
        existing_indexes = collection.index_information()
        for idx_name, idx_info in existing_indexes.items():
            logger.info(f"  - {idx_name}: {idx_info.get('key', [])}")
        
        # 8. 统计最终数据情况
        final_count = collection.count_documents({})
        logger.info(f"\n最终总记录数: {final_count}")
        logger.info(f"删除的记录数: {total_count - final_count}")
        
        logger.info("=" * 60)
        logger.info("指数行情数据迁移完成！")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"迁移失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = migrate_index_market_data()
    sys.exit(0 if success else 1)

