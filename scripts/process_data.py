import json
import os
import glob
import pandas as pd

def process_match_file(file_path):
    try:
        with open(file_path,"r") as f:
            processed_data = json.load(f)
        participants_list = processed_data["info"]["participants"]
        participants_df = pd.DataFrame(participants_list)
        participants_df["matchId"] = processed_data["metadata"]["matchId"]
        return participants_df

    except (FileNotFoundError, KeyError, json.JSONDecodeError, TypeError) as e:
        print(f"An error occured while processing {file_path}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    caught_data = []
    raw_file_paths = glob.glob("data/raw/NA1*.json")
    for file_path in raw_file_paths:
        single_match_data = process_match_file(file_path)
        if not single_match_data.empty:
            caught_data.append(single_match_data)
        else:
            print("Dataframe is empty")

    final_data = pd.concat(caught_data)



    print(final_data)


    final_data.to_csv("data/processed/match_ouput.csv", encoding = "utf=8", index = False)



        