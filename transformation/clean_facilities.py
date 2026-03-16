import pandas as pd
import os

def run():

    df = pd.read_parquet("data/bronze/facilities.parquet")

    df_clean = df[[
        "FacilityID",
        "ParentRecAreaID",
        "FacilityName",
        "FacilityTypeDescription",
        "FacilityDescription",
        "FacilityLatitude",
        "FacilityLongitude",
        "FacilityPhone",
        "FacilityEmail",
        "Reservable",
        "Enabled",
        "LastUpdatedDate"
    ]]

    df_clean = df_clean.rename(columns={
        "FacilityID": "facility_id",
        "ParentRecAreaID": "recarea_id",
        "FacilityName": "facility_name",
        "FacilityTypeDescription": "facility_type",
        "FacilityDescription": "description",
        "FacilityLatitude": "latitude",
        "FacilityLongitude": "longitude",
        "FacilityPhone": "phone",
        "FacilityEmail": "email",
        "Reservable": "reservable",
        "Enabled": "enabled",
        "LastUpdatedDate": "last_updated"
    })

    os.makedirs("data/silver", exist_ok=True)

    df_clean.to_parquet("data/silver/facilities.parquet")

    print("facilities silver saved")


if __name__ == "__main__":
    run()
