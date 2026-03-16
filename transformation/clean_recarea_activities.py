import pandas as pd
import os
#No Data From API, just transforming the data from bronze to silver
def run():

    df = pd.read_parquet("data/bronze/recareas.parquet")

    rows = []

    for _, row in df.iterrows():

        recarea_id = row["RecAreaID"]

        activities = row.get("ACTIVITY", [])

        if activities:
            for act in activities:

                rows.append({
                    "recarea_id": recarea_id,
                    "activity_id": act.get("ActivityID"),
                    "activity_name": act.get("ActivityName")
                })

    df_act = pd.DataFrame(rows)

    os.makedirs("data/silver", exist_ok=True)

    df_act.to_parquet("data/silver/recarea_activities.parquet")

    print("recarea_activities saved")


if __name__ == "__main__":
    run()
