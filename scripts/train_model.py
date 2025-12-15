import sys
import requests
from requests import Session
import logging
import argparse
from pathlib import Path
import pandas as pd
import joblib
from typing import Optional, Any, Union
from requests.exceptions import RequestException
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn
from utils import configure_logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log, wait_random
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler


DEFAULT_DATA_PATH = Path("data/processed/processed_data.csv")
MODEL_SAVE_DIR = Path("models")
LOG_FILE_NAME = "train_model.log"
RANDOM_STATE = 42
DRAGON_VERSION_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
DRAGON_CHAMPION_URL = "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
NUMERIC_FEATURES = [
    "kills", "deaths", "assists", 
    "totalDamageDealtToChampions", "totalMinionsKilled", 
    "goldEarned", "dragonKills", "baronKills"
]
CATEGORICAL_FEATURES = ["championName", "teamPosition"]
TARGET_COLUMN = "win"
MAX_API_RETRIES = 5
RETRY_WAIT_MIN_SECONDS = 2
RETRY_WAIT_MAX_SECONDS = 30
TIMEOUT = 10
MAX_ITERATION = 1000



logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description = "Script for training the model using the processed data csv.")
    parser.add_argument("--input_file", type=Path, default=DEFAULT_DATA_PATH)
    parser.add_argument("--output_dir", type=Path, default=MODEL_SAVE_DIR)
    parser.add_argument("--random_state", type=int, default=RANDOM_STATE)
    return parser.parse_args()
@retry(
        before_sleep = before_sleep_log(logger, logging.WARNING),
        stop = stop_after_attempt(MAX_API_RETRIES),
        wait = wait_exponential(min = RETRY_WAIT_MIN_SECONDS, max = RETRY_WAIT_MAX_SECONDS) + wait_random(min=0, max=2),
        retry = retry_if_exception_type(RequestException)
)
def _fetch_from_api(session: Session, url: str)-> Union[dict[str, Any], list[Any]]:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_champion_list(session: Session):
    latest_version = _fetch_from_api(session, DRAGON_VERSION_URL)[0]
    champion_url = DRAGON_CHAMPION_URL.format(version=latest_version)
    champions = _fetch_from_api(session, champion_url)
    return sorted(list(champions["data"].keys()))



def main(args: argparse.Namespace) -> int:
    logger.info("Starting model training...")
    with requests.Session() as session:
        known_champions = get_champion_list(session)
    initial_df = pd.read_csv(args.input_file)
    y = initial_df[TARGET_COLUMN]
    X = initial_df.drop(columns=[TARGET_COLUMN, "matchId"], errors="ignore")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=args.random_state, stratify=y)
    mlflow.set_experiment("LoL Win Predictor")
    with mlflow.start_run():
        mlflow.log_params(vars(args))
        numeric_transformer = StandardScaler()
        categorical_transformer = OneHotEncoder(categories=[known_champions,["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]], handle_unknown="ignore")
        preprocessor = ColumnTransformer(transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURES),
            ("cat", categorical_transformer, CATEGORICAL_FEATURES)])
        pipeline = Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(random_state=args.random_state, max_iter=MAX_ITERATION))
        ])
        pipeline.fit(X_train, y_train)
        score = pipeline.score(X_test, y_test)
        mlflow.log_metric("accuracy", score)
        mlflow.sklearn.log_model(pipeline, "model")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        file_path = args.output_dir / "model_pipeline.joblib"
        joblib.dump(pipeline, file_path)
        logger.info(f"File saved to {file_path}")
        logger.info(f"Training complete. Accuracy: {score:.4f}")
        return 0




if __name__ == "__main__":
    args = parse_args()
    configure_logger(LOG_FILE_NAME)
    try:
        sys.exit(main(args))
    except Exception as e:
        logger.exception("Error exception in training process")
        sys.exit(1)
        










