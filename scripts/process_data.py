import sys
import logging
import pandas as pd
from pathlib import Path
from utils import configure_logger
from typing import Optional
import argparse
from database import RawMatch, SessionLocal


PROCESSED_DATA_PATH = Path("data/processed/processed_data.csv")
LOG_FILE_NAME = "processed_data.log"
FAILURE_THRESHOLD_RATIO = 0.1
    
logger = logging.getLogger(__name__)

def process_match_data(match_id: str, match_data: dict) -> Optional[pd.DataFrame]:
    try:
        participants_list = match_data["info"]["participants"]
        participants_df = pd.DataFrame(participants_list)
        participants_df["matchId"] = match_id
        return participants_df

    except Exception as e:
        logger.exception(f"Failed to process match data: {e}")
        return None
    
def parse_args():
    parser = argparse.ArgumentParser(description="Script for processing data from SQL")
    parser.add_argument("--output_file", default=PROCESSED_DATA_PATH, type=Path)
    return parser.parse_args()


def main(output_file: Path) -> int:
    db = SessionLocal()
    
    
    try:
        query = db.query(RawMatch).yield_per(100)
        valid_dfs = []
        total = 0
        failure_count = 0

        for row in query:
            total += 1
            processed_data = process_match_data(row.match_id, row.data)
            
            if processed_data is None:
                
                logger.warning(f"Match data failed to process: {row.match_id}")
                failure_count += 1
            else:
                valid_dfs.append(processed_data)
        
        
        if total == 0:
             logger.warning(f"Database is empty. Run ingest_data.py first.")
             return 0
        
        failure_ratio = failure_count / total

        if failure_ratio >= FAILURE_THRESHOLD_RATIO:
            logger.critical(f"Failure threshold exceeded: {FAILURE_THRESHOLD_RATIO:.2%} | Actual: {failure_ratio:.2%}")
            return 1
            
        if not valid_dfs:
            logger.warning(f"DATA ANOMALY: Zero records were successfully processed.")
            return 0
            
        final_df = pd.concat(valid_dfs, ignore_index=True)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        final_df.to_csv(output_file, encoding="utf-8", index=False)
        logger.info(f"There were {len(valid_dfs)} files saved to {output_file}")
        return 0
    
    finally:
        db.close()


if __name__ == "__main__":
    args = parse_args()
    configure_logger(LOG_FILE_NAME)
    sys.exit(main(args.output_file))



        