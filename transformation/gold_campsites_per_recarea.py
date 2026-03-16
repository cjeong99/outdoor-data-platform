import pandas as pd
import os

def run():

    campsites = pd.read_parquet("data/silver/campsites.parquet")
    facilities = pd.read_parquet("data/silver/facilities.parquet")
    recareas = pd.read_parquet("data/silver/recareas.parquet")

    df = campsites.merge(
        facilities[["facility_id", "recarea_id"]],
        on="facility_id",
        how="left"
    )

    df = df.groupby("recarea_id").size().reset_index(name="total_campsites")

    df = df.merge(
        recareas[["recarea_id", "recarea_name"]],
        on="recarea_id",
        how="left"
    )

    os.makedirs("data/gold", exist_ok=True)

    df.to_parquet("data/gold/campsites_per_recarea.parquet")

    print("campsites_per_recarea saved")


if __name__ == "__main__":
    run()
