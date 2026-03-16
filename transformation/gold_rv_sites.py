import pandas as pd
import os

def run():

    equipment = pd.read_parquet("data/silver/campsite_equipment.parquet")

    df = equipment[equipment["equipment_name"] == "RV"]

    os.makedirs("data/gold", exist_ok=True)

    df.to_parquet("data/gold/rv_friendly_sites.parquet")

    print("rv_friendly_sites saved")


if __name__ == "__main__":
    run()
