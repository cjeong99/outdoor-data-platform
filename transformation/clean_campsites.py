import pandas as pd
import os

def run():

    df = pd.read_parquet("data/bronze/campsites.parquet")

    df_clean = df[[
        "CampsiteID",
        "FacilityID",
        "CampsiteName",
        "CampsiteType",
        "TypeOfUse",
        "Loop",
        "CampsiteAccessible",
        "CampsiteLatitude",
        "CampsiteLongitude",
        "CreatedDate",
        "LastUpdatedDate"
    ]]

    df_clean = df_clean.rename(columns={
        "CampsiteID": "campsite_id",
        "FacilityID": "facility_id",
        "CampsiteName": "campsite_name",
        "CampsiteType": "campsite_type",
        "TypeOfUse": "type_of_use",
        "Loop": "loop",
        "CampsiteAccessible": "accessible",
        "CampsiteLatitude": "latitude",
        "CampsiteLongitude": "longitude",
        "CreatedDate": "created_date",
        "LastUpdatedDate": "last_updated"
    })

    os.makedirs("data/silver", exist_ok=True)

    df_clean.to_parquet("data/silver/campsites.parquet")

    print("campsites silver saved")


if __name__ == "__main__":
    run()
