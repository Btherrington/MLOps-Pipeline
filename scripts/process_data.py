import json
import sys
import logging
import pandas as pd
from pathlib import Path
from utils import configure_logger
from typing import Optional
import argparse

RAW_DATA_DIRECTORY = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed/processed_data.csv")
LOG_FILE_NAME = "processed_data.log"
FAILURE_THRESHOLD_RATIO = 0.1

logger = logging.getLogger(__name__)

def process_match_file(file_path: Path) -> Optional[pd.DataFrame]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            processed_data = json.load(f)
            participants_list = processed_data["info"]["participants"]
            participants_df = pd.DataFrame(participants_list)
            participants_df["matchId"] = processed_data["metadata"]["matchId"]
            return participants_df

    except (FileNotFoundError, KeyError,json.JSONDecodeError, TypeError) as e:
        logger.exception(f"Failed to process file: {file_path}. Error during data extraction or transformation")
        return None
    
def parse_args():
    parser = argparse.ArgumentParser(description = "Script for processing data from ingest_data.py")
    parser.add_argument("--input_dir", default=RAW_DATA_DIRECTORY, type=Path)
    parser.add_argument("--output_file", default=PROCESSED_DATA_PATH, type=Path)
    return parser.parse_args()


def main(input_dir: Path, output_file: Path) -> int:
    files = list(input_dir.glob("*.json"))
    if not files:
        logger.warning(f"No files were inputed from directory: {input_dir}")
        return 0
    valid_df = []
    failure_count = 0

    for file in files:
        
        df = process_match_file(file)
        if df is None:
            failure_count += 1
            logger.warning(f"Skipping file: {file} due to processing error")
        
        else:
            valid_df.append(df)
        
    total_files = len(files)

    failure_ratio = failure_count / total_files

    if failure_ratio >= FAILURE_THRESHOLD_RATIO:
        logger.critical(F"Failure threshold exceeded: {FAILURE_THRESHOLD_RATIO: .2%} | Actual failure rate: {failure_ratio: .2%} | Number of failed/total files: {failure_count}/{total_files}")
        return 1
    if not valid_df:
        logger.warning(f"DATA ANOMALY: Zero records were successfully processed. No output file created. (Note: Failure ratio was within configured threshold).")
        return 0
        
    final_df = pd.concat(valid_df, ignore_index=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    final_df.to_csv(output_file, encoding = "utf-8", index=False)
    logger.info(f"There was {len(valid_df)} files saved to {output_file}")
    return 0


if __name__ == "__main__":
    args = parse_args()
    configure_logger(LOG_FILE_NAME)
    sys.exit(main(args.input_dir, args.output_file))



        