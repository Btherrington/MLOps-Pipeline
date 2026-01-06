# League Win Predictor Pipeline

Pulls and ingests Riot API data to serve for a win predictor. Uses Postgres, trains the model and uses FastAPI for web connection. Built to learn how to accomplish a full end to end pipeline.

## How It Was Made

**Tech stack:** Python, Postgres, Docker, MLflow, FastAPI, Scikit-learn

There are four parts:

1. **Ingestion** (ingest_data.py) - Pulls match data with argument parsing, logging, retry/exponential backoff plus jitter to handle the Thundering Herd problem.
2. **Processing** (process_data.py) - Streams rows from the database using yield_per to limit memory usage, extracts the necessary stats and sends to training.
3. **Training** (train_model.py) - Logistic regression model using sklearn, MLflow to track experiments, saves to a joblib file.
4. **Serving** (app.py) - FastAPI endpoint for predictions. Everything runs in Docker.