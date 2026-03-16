import os
import pandas as pd
import json


def save_to_bronze(data, table_name):

    path = "data/bronze"
    os.makedirs(path, exist_ok=True)


    #raw JSON 저장
    json_path = f"{path}/{table_name}_raw.json"
    with open(json_path, "w") as f:
        json.dump(data, f)

    #parquet 저장
    df = pd.DataFrame(data)
    parquet_path = f"{path}/{table_name}.parquet"
    df.to_parquet(parquet_path)

    print(f"Saved {table_name} to bronze layer")
    print(f"JSON: {json_path}")
    print(f"Parquet: {parquet_path}")