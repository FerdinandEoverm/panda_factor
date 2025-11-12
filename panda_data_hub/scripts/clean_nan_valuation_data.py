import pandas as pd
from datetime import datetime
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.services.ts_factor_valuation_clean_pro_service import FactorValuationCleanerTSProService
from panda_common.config import get_config


def run_nan_valuation_cleaner():
    logger.info("Starting N/A valuation data cleaner...")

    # Load config
    config = get_config()

    # Initialize service
    valuation_cleaner = FactorValuationCleanerTSProService(config)

    # Query for 600519.SH data
    db_handler = DatabaseHandler(config)
    collection = db_handler.mongo_client[config["MONGO_DB"]]['factor_base']
    
    # Find records for 600519.SH where pe_ttm is None or "N/A"
    # Assuming 'factor_base' stores the valuation data
    # And 'date' field is in 'YYYYMMDD' format
    nan_records = collection.find({
        "symbol": "600519.SH",
        "$or": [
            {"pe_ttm": {"$in": [None, "N/A", "nan", "NaN"]}},
            {"ps_ttm": {"$in": [None, "N/A", "nan", "NaN"]}},
            {"dv_ttm": {"$in": [None, "N/A", "nan", "NaN"]}},
            {"circ_mv": {"$in": [None, "N/A", "nan", "NaN"]}}
        ]
    }).sort("date", 1)  # Sort by date ascending to process older data first

    dates_to_clean = []
    for record in nan_records:
        date_str = record['date']
        # Convert 'YYYYMMDD' to 'YYYY-MM-DD' for clean_daily_valuation_data
        formatted_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        if formatted_date not in dates_to_clean:
            dates_to_clean.append(formatted_date)
    
    if not dates_to_clean:
        logger.info("No N/A valuation data found for 600519.SH. Exiting.")
        return

    dates_to_clean = [i for i in  dates_to_clean if i >'2024-11-01']
    print(dates_to_clean)
    logger.info(f"Found {len(dates_to_clean)} dates with N/A valuation data for 600519.SH. Cleaning now...")
    import time
    for date_to_clean in dates_to_clean:
        logger.info(f"Cleaning valuation data for date: {date_to_clean}")
        try:
            valuation_cleaner.clean_daily_valuation_data(date_str=date_to_clean, pbar=None)
        except Exception as e:
            logger.error(f"Error cleaning data for {date_to_clean}: {e}")
        time.sleep(3)
    logger.info("N/A valuation data cleaner finished.")


if __name__ == "__main__":
    run_nan_valuation_cleaner()
