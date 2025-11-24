#!/usr/bin/env python3
"""
股票名称变更数据同步调度脚本

此脚本用于定期同步股票名称变更数据到MongoDB，
以便股票市场数据清洗服务可以高效地获取历史名称变更信息。

使用方法:
python namechange_sync_scheduler.py

或者作为定时任务运行:
# 每天早上8点执行增量同步
0 8 * * * /usr/bin/python3 /path/to/namechange_sync_scheduler.py

# 每月1号早上9点执行全量同步
0 9 1 * * /usr/bin/python3 /path/to/namechange_sync_scheduler.py --force-update
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from panda_common.config import config
from panda_data_hub.services.ts_namechange_clean_service import TSNamechangeCleanService
from panda_common.logger_config import logger


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='股票名称变更数据同步调度脚本')
    parser.add_argument('--force-update', action='store_true',
                       help='强制全量更新所有历史数据（默认为增量更新）')
    parser.add_argument('--start-date', type=str,
                       help='指定开始日期（格式：YYYYMMDD）')
    parser.add_argument('--end-date', type=str,
                       help='指定结束日期（格式：YYYYMMDD）')

    args = parser.parse_args()

    try:
        logger.info("=" * 60)
        logger.info("开始执行股票名称变更数据同步任务")
        logger.info(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"强制更新: {'是' if args.force_update else '否'}")
        logger.info(f"开始日期: {args.start_date or '自动'}")
        logger.info(f"结束日期: {args.end_date or '自动'}")
        logger.info("=" * 60)

        # 创建服务实例
        namechange_service = TSNamechangeCleanService(config)

        # 执行同步
        start_time = datetime.now()
        namechange_service.sync_namechange_data(
            start_date=args.start_date,
            end_date=args.end_date,
            force_update=args.force_update
        )
        end_time = datetime.now()

        # 计算执行时间
        execution_time = end_time - start_time
        logger.info("=" * 60)
        logger.info("股票名称变更数据同步任务执行完成")
        logger.info(f"总执行时间: {execution_time}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"股票名称变更数据同步任务执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
