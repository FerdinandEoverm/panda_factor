from abc import ABC
import tushare as ts
import traceback
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes_namechange
from panda_data_hub.utils.ts_utils import validate_tushare_token


class TSNamechangeCleanService(ABC):
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.progress_callback = None

    def set_progress_callback(self, callback):
        """设置进度回调函数"""
        self.progress_callback = callback

    def _update_progress(self, progress_info):
        """更新进度信息"""
        if self.progress_callback:
            self.progress_callback(progress_info)

    def sync_namechange_data(self, start_date=None, end_date=None, force_update=False):
        """
        同步股票名称变更数据到MongoDB

        参数:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        force_update: 是否强制全量更新
        """
        logger.info("开始同步股票名称变更数据")
        logger.info(f"强制更新模式: {'是' if force_update else '否'}")
        
        # 初始化进度
        sync_type = "full" if force_update else "incremental"
        self._update_progress({
            "progress_percent": 0,
            "current_task": "初始化股票名称变更清洗任务...",
            "sync_type": sync_type,
            "processed_count": 0,
            "total_count": 0
        })
        
        try:
            # ts_token = self.config.get('TS_TOKEN')
            # if not ts_token:
            #     raise ValueError(
            #         "TS_TOKEN 未配置。请在配置文件 panda_common/config.yaml 中设置 TS_TOKEN。\n"
            #         "您可以在 https://tushare.pro/ 注册并获取 Token。"
            #     )
            # ts.set_token(ts_token)
            self.pro = ts.pro_api()
        except Exception as e:
            error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            self._update_progress({
                "status": "error",
                "current_task": "Tushare 初始化失败",
                "error_message": error_msg
            })
            raise

        # 验证 Token（使用已有的连接，避免创建新连接）
        self._update_progress({
            "progress_percent": 10,
            "current_task": "验证 Tushare Token 有效性..."
        })
        
        is_valid, error_message = validate_tushare_token(self.pro)
        if not is_valid:
            logger.error(f"Tushare Token 验证失败: {error_message}")
            self._update_progress({
                "status": "error",
                "current_task": "Token 验证失败",
                "error_message": error_message
            })
            raise Exception(error_message)

        # 设置日期范围
        self._update_progress({
            "progress_percent": 15,
            "current_task": "设置日期范围..."
        })
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        if start_date is None:
            if force_update:
                # 全量更新时，从一个较早的日期开始
                start_date = "19900101"
            else:
                # 增量更新时，从最近的同步日期开始
                start_date = self._get_last_sync_date()

        logger.info(f"同步日期范围: {start_date} 至 {end_date}")

        # 获取数据
        self._update_progress({
            "progress_percent": 20,
            "current_task": f"从 Tushare 获取名称变更数据 ({start_date} - {end_date})..."
        })
        
        namechange_data = self._fetch_namechange_data(start_date, end_date, force_update)
        if namechange_data.empty:
            logger.info("没有新的名称变更数据需要同步")
            self._update_progress({
                "progress_percent": 100,
                "current_task": "没有新的名称变更数据需要同步",
                "status": "completed"
            })
            return

        # 保存到数据库（全量更新时数据已分批保存，跳过此步骤）
        if not force_update:
            self._update_progress({
                "progress_percent": 60,
                "current_task": f"保存 {len(namechange_data)} 条名称变更记录到数据库..."
            })
            
            self._save_namechange_data(namechange_data, force_update)
        else:
            logger.info("全量更新模式：数据已分批保存完成")
            self._update_progress({
                "progress_percent": 95,
                "current_task": "全量更新完成，数据已分批保存"
            })

        # 更新同步记录
        self._update_progress({
            "progress_percent": 90,
            "current_task": "更新同步记录..."
        })
        
        self._update_sync_record(end_date)

        logger.info(f"成功同步名称变更记录")

        # 完成
        if force_update:
            self._update_progress({
                "progress_percent": 100,
                "current_task": f"股票名称变更全量清洗完成，数据已分批保存",
                "status": "completed"
            })
        else:
            self._update_progress({
                "progress_percent": 100,
                "current_task": f"股票名称变更增量清洗完成，共处理 {len(namechange_data)} 条记录",
                "processed_count": len(namechange_data),
                "total_count": len(namechange_data),
                "status": "completed"
            })

    def _fetch_namechange_data(self, start_date, end_date, force_update=False):
        """
        从 tushare 获取股票名称变更数据
        
        参数:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        force_update: 是否强制全量更新
        
        返回:
        pandas.DataFrame: 名称变更数据
        """
        try:
            all_data = []
            import time
            if force_update:
                # 全量更新：遍历所有股票获取名称变更数据
                logger.info("全量更新模式：获取股票列表...")
                
                # 获取所有股票列表
                stock_list = self.pro.query('stock_basic', exchange='', list_status='L,D', 
                                           fields='ts_code,symbol,name,area,industry,market,list_date')
                logger.info(f"获取到 {len(stock_list)} 只股票")
                
                # 确保集合和索引存在
                ensure_collection_and_indexes_namechange('stock_namechange')
                collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_namechange']
                
                self._update_progress({
                    "current_task": f"开始遍历 {len(stock_list)} 只股票获取名称变更数据...",
                    "total_count": len(stock_list),
                    "processed_count": 0
                })

                # 每10个股票处理一次，并保存到数据库
                batch_size = 10
                total_processed = 0
                
                for i in range(0, len(stock_list), batch_size):
                    batch_stocks = stock_list.iloc[i:i+batch_size]
                    batch_data = []
                    
                    for _, stock in batch_stocks.iterrows():
                        try:
                            # 检查数据库中是否已有该股票的数据
                            existing_count = collection.count_documents({'symbol': stock['ts_code']})

                            if existing_count > 0:
                                logger.info(f"股票 {stock['ts_code']} 已有 {existing_count} 条记录，跳过获取")
                                continue
                            
                            # 获取单只股票的名称变更数据
                            stock_data = self.pro.namechange(ts_code=stock['ts_code'])
                            time.sleep(0.5)  # 避免请求过快
                            
                            if not stock_data.empty:
                                # 按照参考代码进行去重：按 ts_code 和 start_date 去重，保留最早的记录
                                stock_data = stock_data.sort_values(by='end_date', ascending=True).drop_duplicates(['ts_code', 'start_date'], keep='first')
                                batch_data.append(stock_data)
                                logger.info(f"获取股票 {stock['ts_code']} 的 {len(stock_data)} 条记录")
                                
                        except Exception as e:
                            logger.warning(f"获取股票 {stock['ts_code']} 名称变更数据失败: {str(e)}")
                            continue
                    
                    # 保存当前批次数据到数据库
                    if batch_data:
                        batch_combined = pd.concat(batch_data, ignore_index=True)
                        # 重命名 ts_code 为 symbol
                        batch_combined.rename(columns={'ts_code': 'symbol'}, inplace=True)
                        
                        # 准备保存数据
                        docs_to_insert = []
                        for _, row in batch_combined.iterrows():
                            doc = {
                                'symbol': row['symbol'],
                                'name': row['name'],
                                'start_date': row.get('start_date'),
                                'end_date': row.get('end_date'),
                                'ann_date': row.get('ann_date'),
                                'change_reason': row.get('change_reason'),
                                'updated_at': datetime.now()
                            }
                            docs_to_insert.append(doc)
                        
                        # 批量插入数据库
                        if docs_to_insert:
                            try:
                                insert_result = collection.insert_many(docs_to_insert)
                                batch_count = len(insert_result.inserted_ids)
                                total_processed += batch_count
                                logger.info(f"批次 {i//batch_size + 1}: 成功保存 {batch_count} 条记录到数据库")
                            except Exception as e:
                                logger.error(f"批次 {i//batch_size + 1}: 保存数据失败: {str(e)}")
                    
                    # 更新进度
                    processed = min(i + batch_size, len(stock_list))
                    progress = 20 + (processed / len(stock_list)) * 70  # 20% - 90%
                    self._update_progress({
                        "progress_percent": progress,
                        "current_task": f"已处理 {processed}/{len(stock_list)} 只股票，累计保存 {total_processed} 条记录...",
                        "processed_count": processed,
                        "total_count": len(stock_list)
                    })
                
                # 返回空DataFrame，因为数据已经分批保存
                logger.info(f"全量更新完成，累计保存 {total_processed} 条记录")
                return pd.DataFrame()
                
            else:
                # 增量更新：按月批量获取数据
                logger.info("增量更新模式：按月批量获取数据...")
                current_start = start_date
                
                while current_start <= end_date:
                    try:
                        # 按月获取数据
                        current_end = self._get_month_end(current_start)
                        if current_end > end_date:
                            current_end = end_date
                        
                        logger.info(f"获取 {current_start} 到 {current_end} 的名称变更数据")
                        
                        # 获取月度数据
                        monthly_data = self.pro.query('namechange', start_date=current_start, end_date=current_end)
                        
                        if not monthly_data.empty:
                            # 对增量数据也进行去重处理
                            monthly_data = monthly_data.sort_values(by='end_date', ascending=True).drop_duplicates(['ts_code', 'start_date'], keep='first')
                            all_data.append(monthly_data)
                            logger.info(f"获取到 {current_start}-{current_end} 的 {len(monthly_data)} 条记录")
                        
                        # 更新进度
                        days_processed = (datetime.strptime(current_end, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")).days + 1
                        total_days = (datetime.strptime(end_date, "%Y%m%d") - datetime.strptime(start_date, "%Y%m%d")).days + 1
                        progress = 20 + (days_processed / total_days) * 50  # 20% - 70%
                        self._update_progress({
                            "progress_percent": progress,
                            "current_task": f"获取 {current_start}-{current_end} 的名称变更数据...",
                            "processed_count": len(all_data),
                            "total_count": total_days
                        })
                        
                        # 移动到下个月
                        if current_end == end_date:
                            break
                        next_month = datetime.strptime(current_end, "%Y%m%d") + timedelta(days=32)
                        next_month = next_month.replace(day=1)
                        current_start = next_month.strftime("%Y%m%d")
                        
                    except Exception as e:
                        logger.warning(f"获取 {current_start}-{current_end} 数据失败: {str(e)}")
                        # 移动到下个月
                        next_month = datetime.strptime(current_start, "%Y%m%d") + timedelta(days=32)
                        next_month = next_month.replace(day=1)
                        current_start = next_month.strftime("%Y%m%d")
                        continue
            
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                # 最终去重：确保数据唯一性
                combined_data = combined_data.sort_values(by='end_date', ascending=True).drop_duplicates(['ts_code', 'start_date'], keep='first')
                # 重命名 ts_code 为 symbol
                combined_data.rename(columns={'ts_code': 'symbol'}, inplace=True)
                logger.info(f"总共获取到 {len(combined_data)} 条唯一记录")
                return combined_data
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取名称变更数据失败: {str(e)}")
            raise

    def _get_month_end(self, date_str):
        """
        获取指定日期所在月的最后一天
        """
        date = datetime.strptime(date_str, "%Y%m%d")
        # 获取下个月的第1天，然后减去1天
        if date.month == 12:
            next_month = date.replace(year=date.year + 1, month=1, day=1)
        else:
            next_month = date.replace(month=date.month + 1, day=1)
        last_day = next_month - timedelta(days=1)
        return last_day.strftime("%Y%m%d")

    def _save_namechange_data(self, data, force_update=False):
        """
        保存名称变更数据到MongoDB
        """
        try:
            # 确保集合和索引存在
            ensure_collection_and_indexes_namechange('stock_namechange')

            collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_namechange']

            if force_update:
                # 强制更新模式：先删除所有数据，然后直接插入
                self._update_progress({
                    "progress_percent": 65,
                    "current_task": "强制更新模式：删除现有数据..."
                })
                
                delete_result = collection.delete_many({})
                logger.info(f"强制更新模式：已删除 {delete_result.deleted_count} 条现有记录")
                
                # 直接插入所有数据
                self._update_progress({
                    "progress_percent": 75,
                    "current_task": f"准备插入 {len(data)} 条名称变更数据..."
                })
                
                docs_to_insert = []
                for i, (_, row) in enumerate(tqdm(data.iterrows(), total=len(data), desc="准备插入名称变更数据")):
                    try:
                        # 完整保存所有原始字段，只重命名 ts_code 为 symbol
                        doc = {
                            'symbol': row['symbol'],
                            'name': row['name'],
                            'start_date': row.get('start_date'),  # 开始日期
                            'end_date': row.get('end_date'),      # 结束日期
                            'ann_date': row.get('ann_date'),      # 公告日期
                            'change_reason': row.get('change_reason'),  # 变更原因
                            'updated_at': datetime.now()
                        }
                        docs_to_insert.append(doc)
                        
                        # 更新进度
                        if i % 100 == 0:
                            progress = 75 + (i / len(data)) * 20  # 75% - 95%
                            self._update_progress({
                                "progress_percent": progress,
                                "current_task": f"准备数据 {i}/{len(data)}...",
                                "processed_count": i,
                                "total_count": len(data)
                            })
                    except Exception as e:
                        logger.error(f"处理名称变更记录失败: {row.to_dict()}, 错误: {str(e)}")
                        continue

                if docs_to_insert:
                    self._update_progress({
                        "progress_percent": 95,
                        "current_task": f"插入 {len(docs_to_insert)} 条名称变更数据到数据库..."
                    })
                    
                    insert_result = collection.insert_many(docs_to_insert)
                    logger.info(f"成功插入 {len(insert_result.inserted_ids)} 条名称变更记录到数据库")
                return

            # 准备批量插入操作（增量模式）
            self._update_progress({
                "progress_percent": 65,
                "current_task": f"准备批量保存 {len(data)} 条名称变更数据..."
            })
            
            docs_to_insert = []
            processed_count = 0

            for i, (_, row) in enumerate(tqdm(data.iterrows(), total=len(data), desc="保存名称变更数据")):
                try:
                    # 完整保存所有原始字段，只重命名 ts_code 为 symbol
                    doc = {
                        'symbol': row['symbol'],
                        'name': row['name'],
                        'start_date': row.get('start_date'),  # 开始日期
                        'end_date': row.get('end_date'),      # 结束日期
                        'ann_date': row.get('ann_date'),      # 公告日期
                        'change_reason': row.get('change_reason'),  # 变更原因
                        'updated_at': datetime.now()
                    }

                    # 在增量模式下，检查是否已存在相同的记录
                    existing = collection.find_one({
                        'symbol': doc['symbol'],
                        'name': doc['name'],
                        'ann_date': doc['ann_date']
                    })
                    
                    if not existing:
                        docs_to_insert.append(doc)
                        processed_count += 1

                    # 每1000条批量插入一次
                    if len(docs_to_insert) >= 1000:
                        if docs_to_insert:
                            collection.insert_many(docs_to_insert)
                            logger.info(f"批量插入了 {len(docs_to_insert)} 条新记录")
                        docs_to_insert = []

                    # 更新进度
                    if i % 100 == 0:
                        progress = 65 + (i / len(data)) * 25  # 65% - 90%
                        self._update_progress({
                            "progress_percent": progress,
                            "current_task": f"处理数据 {i}/{len(data)}，新增 {processed_count} 条...",
                            "processed_count": processed_count,
                            "total_count": len(data)
                        })

                except Exception as e:
                    logger.error(f"处理名称变更记录失败: {row.to_dict()}, 错误: {str(e)}")
                    continue

            # 处理剩余的记录
            if docs_to_insert:
                collection.insert_many(docs_to_insert)
                logger.info(f"批量插入了剩余的 {len(docs_to_insert)} 条新记录")

            logger.info(f"增量更新完成，新增 {processed_count} 条名称变更记录")

        except Exception as e:
            logger.error(f"保存名称变更数据失败: {str(e)}")
            self._update_progress({
                "status": "error",
                "current_task": "保存数据失败",
                "error_message": str(e)
            })
            raise

    def _get_last_sync_date(self):
        """
        获取最后一次同步的日期
        """
        try:
            collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['sync_records']
            record = collection.find_one({'data_type': 'namechange'})

            if record and 'last_sync_date' in record:
                # 从最后同步日期前一天开始，避免重复
                last_date = datetime.strptime(record['last_sync_date'], "%Y%m%d")
                prev_date = last_date - timedelta(days=1)
                return prev_date.strftime("%Y%m%d")
            else:
                # 如果没有同步记录，从3个月前开始
                three_months_ago = datetime.now() - timedelta(days=90)
                return three_months_ago.strftime("%Y%m%d")

        except Exception as e:
            logger.warning(f"获取最后同步日期失败: {str(e)}，使用默认日期")
            # 默认从3个月前开始
            three_months_ago = datetime.now() - timedelta(days=90)
            return three_months_ago.strftime("%Y%m%d")

    def _update_sync_record(self, sync_date):
        """
        更新同步记录
        """
        try:
            collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['sync_records']

            collection.update_one(
                {'data_type': 'namechange'},
                {
                    '$set': {
                        'last_sync_date': sync_date,
                        'updated_at': datetime.now()
                    }
                },
                upsert=True
            )

            logger.info(f"更新同步记录: namechange 数据同步至 {sync_date}")

        except Exception as e:
            logger.error(f"更新同步记录失败: {str(e)}")

    def get_namechange_data(self, end_date=None):
        """
        从MongoDB获取名称变更数据，用于股票市场数据清洗

        参数:
        end_date: 结束日期，格式 YYYYMMDD

        返回:
        pandas.DataFrame: 名称变更数据
        """
        try:
            if end_date is None:
                end_date = datetime.now().strftime("%Y%m%d")

            collection = self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_namechange']

            # 查询指定日期之前的所有名称变更记录
            query = {'ann_date': {'$lte': end_date}}

            cursor = collection.find(query).sort('ann_date', 1)  # 按公告日期升序排列

            # 转换为DataFrame
            data = list(cursor)
            if data:
                df = pd.DataFrame(data)
                # 保留所有原始字段，按重要性排序
                columns_order = ['symbol', 'ann_date', 'name', 'start_date', 'end_date', 'change_reason', 'updated_at']
                # 只保留存在的字段
                existing_columns = [col for col in columns_order if col in df.columns]
                if existing_columns:
                    df = df[existing_columns]
                logger.info(f"从数据库获取到 {len(df)} 条名称变更记录")
                return df
            else:
                logger.info("数据库中没有名称变更记录")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"从数据库获取名称变更数据失败: {str(e)}")
            # 返回空的DataFrame以避免程序崩溃
            return pd.DataFrame()
