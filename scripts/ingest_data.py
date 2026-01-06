import os
import json
from requests import Session
from requests.exceptions import RequestException
import time
from dotenv import load_dotenv
import logging
from utils import configure_logger
import sys
import argparse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log, wait_random
from typing import Optional, Any, Union
from pathlib import Path
from database import RawMatch, init_db, SessionLocal
from sqlalchemy.exc import IntegrityError

MAX_API_RETRIES = 5
RETRY_WAIT_MIN_SECONDS = 2
RETRY_WAIT_MAX_SECONDS = 30
LOG_FILE_NAME = "ingest.log"
FAILURE_RATIO_ALLOWED = 0.1
TIME_SLEEP_NUMBER = 1.5
DEFAULT_RAW_DATA_DIRECTORY = Path("data/raw")
TIMEOUT = 10

#Creates a logger instance, __name__ parameter logs with the name of the current file
logger = logging.getLogger(__name__)


def save_match(match_id, match_data):
    db = SessionLocal()
    try:
        match = RawMatch(match_id=match_id, data=match_data)
        db.merge(match)
        db.commit()
    finally:
        db.close()

#Retry decorator, before_sleep_log, logs a message before sleeping and retrying, rest is max retries, how long to wait, and solving "Thundering Herd" problem
@retry(
        before_sleep = before_sleep_log(logger, logging.WARNING), 
        stop = stop_after_attempt(MAX_API_RETRIES), 
        wait = wait_exponential(min = RETRY_WAIT_MIN_SECONDS, max = RETRY_WAIT_MAX_SECONDS) + wait_random(min=0, max=2), 
        #If the exception type is a network type issue then retry, not if code error
        retry = retry_if_exception_type(RequestException)
        )
#Function that takes the session and url as parameter, type of each are Session and Str respectiviely. 
def _fetch_from_api(session: Session, url: str)-> Union[dict[str, Any], list[Any]]:
    #Session keeps the line open, gets the url, if it takes longer then 10s then cut the hanging request
    r = session.get(url, timeout=TIMEOUT)
    #Raise an error for network request error
    r.raise_for_status()
    return r.json()
    


def get_puuid(game_name: str, tagline: str, region:str, session: Session) -> Optional[str]:
    puuid_api_url = f"https://{region}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tagline}"
    try:
        puuid_data = _fetch_from_api(session, puuid_api_url)
        if isinstance(puuid_data, dict):
            return puuid_data.get("puuid")
        else:
            logger.error(f"puuid_data output was expected to be a dictionary, was a {type(puuid_data)}. URL input: {puuid_api_url}")
            return None
    except RequestException as e:
        logger.exception(f"Failed to pull puuid after {MAX_API_RETRIES} retries. Inputs are regions :{region}, game_name: {game_name}, tagline: {tagline}, URL: {puuid_api_url}")
        return None
    
    

def get_match_ids(puuid: str, region:str, session: Session) -> Optional[list[str]]:
    match_api_url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&start=0&count=20"
    try:
        match_id_data = _fetch_from_api(session, match_api_url)
        if isinstance(match_id_data, list):
            return match_id_data
        else:
            logger.error(f"match_id_data output was expected to be a list, was a {type(match_id_data)}. URL input: {match_api_url}")
            return None
    except RequestException as e:
        logger.exception(f"Failed to pull match ids after {MAX_API_RETRIES} retries. Inputs are regions :{region}, puuid: {puuid}, URL: {match_api_url}")
        return None

def get_match_data(region: str, match_id: str, session: Session) -> Optional[dict[str, Any]]:
    overall_match_data_api_url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    try:
        overall_match_data = _fetch_from_api(session, overall_match_data_api_url)
        if isinstance(overall_match_data, dict):
            return overall_match_data
        else:
            logger.error(f"overall_match_data output was expected to be a dictionary, was a {type(overall_match_data)}. URL input: {overall_match_data_api_url}")
            return None
    except RequestException as e:
        logger.exception(f"Failed to pull match data after {MAX_API_RETRIES} retries. Inputs are regions :{region}, match_id: {match_id}, URL: {overall_match_data_api_url}")
        return None
    
def parse_args():
    parser = argparse.ArgumentParser(description = "Script for ingesting data from Riot API")
    parser.add_argument("--game-name", required=True)
    parser.add_argument("--tag-line", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--output_dir", default=DEFAULT_RAW_DATA_DIRECTORY, type=Path)
    return parser.parse_args()

def main(args: argparse.Namespace, api_key: str) -> int:
    init_db()
    with Session() as session:
        session.headers["X-Riot-Token"] = api_key

        args.output_dir.mkdir(parents=True ,exist_ok=True)
        logger.info("Attempting to retrieve puuid")
        player_puuid = get_puuid(args.game_name, args.tag_line, args.region, session)
        if not player_puuid:
            logger.error(f"Unsuccessful pull of player id, Game name: {args.game_name}, Tagline: {args.tag_line}, Region: {args.region}")
            return 1 
        logger.info(f"Successful player id pull")

        match_ids = get_match_ids(player_puuid, args.region, session)
        if match_ids is None:
            logger.error(f"Unable to pull match id from API, inputs; player_puuid: {player_puuid} and REGION: {args.region}, script is stopping.")
            return 1


        total_failures = 0
        number_of_match_ids = (len(match_ids))
        logger.info(f"There is {number_of_match_ids} match ids being pulled")
        if number_of_match_ids == 0:
            logger.warning(f"API data anomaly detected, API returned 0 matches. No new data to process")
            return 0

        for match_id in match_ids:
            time.sleep(TIME_SLEEP_NUMBER)
            single_match_data = get_match_data(args.region, match_id, session)
            if not single_match_data:
                logger.warning(f"Data point was missing {match_id}, script continuing")
                total_failures += 1
                continue
            try:
                save_match(match_id, single_match_data)
            except (Exception) as e:
                logger.exception(f"Failed to save match to db: {e}")
                total_failures += 1
                continue
    
        actual_failure_ratio = total_failures/number_of_match_ids
        if actual_failure_ratio >= FAILURE_RATIO_ALLOWED:
            logger.critical(f"Failure threshold exceeded: {FAILURE_RATIO_ALLOWED: .2%} | Actual error percent = {actual_failure_ratio: .2%} | Raw count (total failed and total number of matches): ({total_failures}/{number_of_match_ids})")
            return 1
        else:
            success_count = number_of_match_ids - total_failures
            logger.info(f"Ingestion complete, successfully saved {success_count} matches to {args.output_dir}")
            return 0
                
        
        


if __name__ == "__main__":
    args = parse_args()
    configure_logger(LOG_FILE_NAME)
    load_dotenv()
    api_key = os.getenv("RIOT_API_KEY")
    if api_key is None:
        logger.critical("Critical error, missing env variable: RIOT_API_KEY")
        sys.exit(1)
    
    sys.exit(main(args, api_key))
