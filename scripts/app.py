from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
from pathlib import Path

MODEL_PATH = Path("models/model_pipeline.joblib")

#Start fast api instance, put into app variable
app = FastAPI()

#Loads my model pipeline specified in the constant MODEL_PATH and sets it to a variable (joblib.load works with any separator)
pipeline = joblib.load(MODEL_PATH)

#Creates a custom class that validates my numeric and categorical features used in my model with the correct type hint
class MatchStats(BaseModel):
    #numeric features
    kills: int
    deaths: int
    assists: int
    totalDamageDealtToChampions: int
    totalMinionsKilled: int
    goldEarned:int
    dragonKills: int
    baronKills:int
    #categorical features
    championName: str
    teamPosition: str

#Creates a decorator that goes to the specific route /predict
@app.post("/predict")

#Creates a function, that takes data as a parameter, and matches the type hints to the correct parts
def predict(data: MatchStats):
    #Dumps the user input via the FastAPI connection into a dataframe
    data_df = pd.DataFrame([data.model_dump()])
    #Calls a prediction on my pipeline, with the incoming data as parameter, index of 0 catches the first index, either 0 or 1
    prediction = pipeline.predict(data_df)[0]
    #Gives probabilty of correctness returning both numbers
    probability = pipeline.predict_proba(data_df)[0][1]
    #Return a dictionary of both answers making the prediction a whole number (int) and the probabilty a decimal (float)
    return {
        "Prediction": int(prediction),
        "Probability": float(probability)
    }