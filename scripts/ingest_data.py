import os
import json
import requests
import time
from dotenv import load_dotenv
import logging
from utils import configure_logger
import sys

LOG_FILE_NAME = "ingest.log"
FAILURE_RATIO_ALLOWED = 0.1
TIME_SLEEP_NUMBER = 1.5
RAW_DIRECTORY_PATH = os.path.join("data", "raw")

logger = logging.getLogger(__name__)

load_dotenv()


API_KEY = os.getenv("RIOT_API_KEY")
GAME_NAME = "Drip"
TAGLINE = "Drip2"
REGION = "americas"

def get_puuid(game_name, tagline):
    api_url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tagline}?api_key={API_KEY}"
    try:
        r = requests.get(api_url)
        r.raise_for_status()
        pulled_api_data = r.json()
        return pulled_api_data.get("puuid")
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request unsuccessful: {e}")
        return None
    
    

def get_match_ids(puuid, region):
    match_api_url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?api_key={API_KEY}"
    try:
        match_response = requests.get(match_api_url)
        match_response.raise_for_status()
        match_id_data = match_response.json()
        return match_id_data
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request unsuccessful: {e}")
        return None

def get_match_data(region, match_id):
    match_data_url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={API_KEY}"
    try:
        match_data_response = requests.get(match_data_url)
        match_data_response.raise_for_status()
        match_data = match_data_response.json()
        return match_data
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request unsuccessful: {e}")
        return None

def main():
    os.makedirs(RAW_DIRECTORY_PATH ,exist_ok=True)
    logger.info("Attempting to retrieve puuid")
    player_puuid = get_puuid(GAME_NAME,TAGLINE)
    if not player_puuid:
        logger.error(f"Unsuccessful pull of player id, GAME_NAME: {GAME_NAME}, TAGLINE: {TAGLINE}")
        return 1 
    logger.info(f"Successful player id pull")

    match_ids = get_match_ids(player_puuid, REGION)
    if match_ids is None:
        logger.error(f"Unable to pull match id from API, inputs; player_puuid: {player_puuid} and REGION: {REGION}, script is stopping.")
        return 1


    total_failures = 0
    number_of_match_ids = (len(match_ids))
    logger.info(f"There is {number_of_match_ids} match ids being pulled")
    if number_of_match_ids == 0:
        logger.warning(f"API data anomaly detected, API returned 0 matches. No new data to process")
        return 0

    for match_id in match_ids:
        time.sleep(TIME_SLEEP_NUMBER)
        single_match_data = get_match_data(REGION, match_id)
        filename = (f"{match_id}.json")
        file_path = os.path.join(RAW_DIRECTORY_PATH, filename)
        if not single_match_data:
            logger.warning(f"Data point was missing {match_id}, script continuing")
            total_failures += 1
            continue

        try:
            with open(file_path,"w") as f:
                json.dump(single_match_data,f, indent=4)
        except (IOError, PermissionError) as e:
            logger.exception(f"Failed to write json match file: {e}")
            total_failures += 1
            continue
    actual_failure_ratio = total_failures/number_of_match_ids
    if actual_failure_ratio >= FAILURE_RATIO_ALLOWED:
        logger.critical(f"Failure threshold exceeded: {FAILURE_RATIO_ALLOWED: .2%} | Actual error percent = {actual_failure_ratio: .2%} | Raw count (total failed and total number of matches): ({total_failures}/{number_of_match_ids})")
        return 1
    else:
        return 0

        
        
        


if __name__ == "__main__":
    configure_logger(LOG_FILE_NAME)
    sys.exit(main())
