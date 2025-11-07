import os
import json
import requests
import time
from dotenv import load_dotenv
import logging
from utils import configure_logger
import sys

LOG_FILE_NAME = "ingest.log"

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
    os.makedirs("data/raw",exist_ok=True)
    logger.info("Attempting to retrieve puuid")
    player_puuid = get_puuid(GAME_NAME,TAGLINE)
    if not player_puuid:
        logger.error(f"Unsuccessful pull of player id: {player_puuid}")
        return 1 
    logger.info(f"Successful player id pull")
    match_ids = get_match_ids(player_puuid, REGION)
    if not match_ids:
        logger.error(f"unsuccessful pull of match id: {match_ids}")
        return 1
    file_write_failures = 0
    number_of_match_ids = (len(match_ids))
    logger.info(f"There is {number_of_match_ids} match ids being pulled")
    for match_id in match_ids:
        single_match_data = get_match_data(REGION, match_id)
        time.sleep(1.5)
        if not single_match_data:
            logger.warning(f"Data point was missing, script continuing")
            continue
        file_path = f"data/raw/{match_id}.json"
        try:
            with open(file_path,"w") as f:
                json.dump(single_match_data,f, indent=4)
        except (IOError, PermissionError) as e:
            logger.exception(f"Failed to write json match file: {e}")
            file_write_failures += 1
            continue
    if file_write_failures/number_of_match_ids >= 0.1:
        logger.critical(f"More than 10% of matches were not pulled")
        return 1
    else:
        return 0

        
        
        


if __name__ == "__main__":
    configure_logger(LOG_FILE_NAME)
    sys.exit(main())
