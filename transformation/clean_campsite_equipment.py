import os
import pandas as pd


def run():
    #read parquet file from bronze layer
    df = pd.read_parquet("data/bronze/campsites.parquet")
    #explode the equipment column to have one row per equipment item
    df_dq = df[["CampsiteID","PERMITTEDEQUIPMENT"]]
    df_dq = df_dq.explode("PERMITTEDEQUIPMENT")
    #keep only rows where PERMITTEDEQUIPMENT is not null
    df_dq = df_dq.dropna(subset=["PERMITTEDEQUIPMENT"])
    #rename columns to be lowercase and snake_Case
    df_dq = df_dq.rename(columns={"CampsiteID": "campsite_id"})

    df_dq["equipment_name"] = df_dq["PERMITTEDEQUIPMENT"].apply(lambda x: x.get("EquipmentName"))
    df_dq["max_length"] = df_dq["PERMITTEDEQUIPMENT"].apply(lambda x: x.get("MaxLength"))                     
    #keep only campsite_id, equipment_name. max_lengnth
    df_dq = df_dq[["campsite_id", "equipment_name", "max_length"]]
    #create a destination folder
    os.makedirs("data/silver", exist_ok=True)
    #save the cleaned data to silver layer as parquet file

    df_dq.to_parquet("data/silver/campsite_equipment.parquet", index=False)
    print("campsite_equipment saved")





if __name__ == "__main__":
    run()