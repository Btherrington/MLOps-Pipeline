
import sys
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.model_selection import train_test_split
import requests
import json
import logging
from logging import FileHandler, Formatter, StreamHandler


LOG_FILE_NAME = "train_model.log"

logger = logging.getLogger(__name__)

def configure_logger(log_filename):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_filename)
    formatter_instance = Formatter(fmt= "{asctime} - {levelname} - {name} - {message}", datefmt="%m/%d/%Y", style="{")
    file_handler.setFormatter(formatter_instance)
    stream_handler.setFormatter(formatter_instance)
    if not root_logger.handlers:
        root_logger.addHandler(stream_handler)
        root_logger.addHandler(file_handler)



def get_champion_list():
    latest_version = get_latest_version()
    if latest_version is None:
        return None
    else:
        data_dragon_url_champions = f"https://ddragon.leagueoflegends.com/cdn/{latest_version}/data/en_US/champion.json"
        try:
            r_champions = requests.get(data_dragon_url_champions)
            r_champions.raise_for_status()
            current_champions = r_champions.json()
            current_champions_data_key = current_champions.get("data")
            if not current_champions_data_key:
                logger.error("Error in getting champions from data key.")
                return None
            final_champion_list = sorted(current_champions_data_key.keys())
            logger.info(f"Successfully retrieved {len(final_champion_list)}")
            return final_champion_list
        except (json.JSONDecodeError, requests.exceptions.RequestException) as e:
            logger.exception("Error in getting champion list")
            return None





def get_latest_version():
    data_dragon_url_version = f"https://ddragon.leagueoflegends.com/api/versions.json"
    try:
        r_version = requests.get(data_dragon_url_version)
        r_version.raise_for_status()
        current_version = r_version.json()[0]
        return current_version
    except (json.JSONDecodeError, IndexError, requests.exceptions.RequestException) as e:
        logger.exception("Error in getting latest version")
        return None
        








csv_file_path = "data/processed/match_ouput.csv"

match_output_df = pd.read_csv(csv_file_path)

blue_team_df = match_output_df[match_output_df["teamId"] == 100]
red_team_df = match_output_df[match_output_df["teamId"] == 200]

blue_team_champs = blue_team_df.groupby("matchId")["championName"].apply(list)
red_team_champs = red_team_df.groupby("matchId")["championName"].apply(list)

champion_df = pd.DataFrame({"Blue_team_champs": blue_team_champs, "Red_team_champs": red_team_champs})

win_df = blue_team_df.groupby("matchId")["win"].first()

champion_win_df = champion_df.join(win_df)

final_model_df = champion_win_df.rename(columns = {"win": "blue_team_win"})

X = final_model_df[["Blue_team_champs", "Red_team_champs"]]
y = final_model_df["blue_team_win"]

X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.2, random_state=42)

mlb = MultiLabelBinarizer()

full_champion_list = pd.concat([X["Blue_team_champs"], X["Red_team_champs"]])

mlb.fit(full_champion_list)
print(X_train)
blue_encoder_train = mlb.transform(X_train["Blue_team_champs"])
blue_encoder_test = mlb.transform(X_test["Blue_team_champs"])
red_encoder_train = mlb.transform(X_train["Red_team_champs"])
red_encoder_test = mlb.transform(X_test["Red_team_champs"])

print(blue_encoder_train)

def create_encoded_dataframe(data,index,columns):
    return pd.DataFrame(data=data, index=index, columns=columns)



if __name__ == "__main__":
    configure_logger(LOG_FILE_NAME)
    logger.info("Script started")
    get_champion_list()
    logger.info("Script finished")
     


