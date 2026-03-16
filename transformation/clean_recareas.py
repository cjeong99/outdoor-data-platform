import pandas as pd
import os

def run():

    df = pd.read_parquet("data/bronze/recareas.parquet")

    df_clean = df[[
        "RecAreaID",
        "RecAreaName",
        "RecAreaDescription",
        "RecAreaLatitude",
        "RecAreaLongitude",
        "RecAreaPhone",
        "RecAreaEmail",
        "Reservable",
        "Enabled",
        "LastUpdatedDate"
    ]]

    df_clean = df_clean.rename(columns={
        "RecAreaID": "recarea_id",
        "RecAreaName": "recarea_name",
        "RecAreaDescription": "description",
        "RecAreaLatitude": "latitude",
        "RecAreaLongitude": "longitude",
        "RecAreaPhone": "phone",
        "RecAreaEmail": "email",
        "Reservable": "reservable",
        "Enabled": "enabled",
        "LastUpdatedDate": "last_updated"
    })

    os.makedirs("data/silver", exist_ok=True)

    df_clean.to_parquet("data/silver/recareas.parquet")

    print("recareas silver saved")


if __name__ == "__main__":
    run()
