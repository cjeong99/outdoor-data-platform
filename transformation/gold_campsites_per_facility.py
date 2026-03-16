import pandas as pd
import os

def run():

    campsites = pd.read_parquet("data/silver/campsites.parquet")
    facilities = pd.read_parquet("data/silver/facilities.parquet")

    df = campsites.groupby("facility_id").size().reset_index(name="total_campsites")

    df = df.merge(
        facilities[["facility_id", "facility_name"]],
        on="facility_id",
        how="left"
    )

    os.makedirs("data/gold", exist_ok=True)

    df.to_parquet("data/gold/campsites_per_facility.parquet")

    print("campsites_per_facility saved")


if __name__ == "__main__":
    run()
