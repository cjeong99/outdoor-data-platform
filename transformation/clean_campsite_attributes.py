import pandas as pd
import os

def run():

    # Load the campsites dataset from the bronze layer
    # This parquet file contains raw API data including nested attributes
    df = pd.read_parquet("data/bronze/campsites.parquet")

    # Select only the columns we need for this transformation
    # CampsiteID identifies the campsite
    # ATTRIBUTES contains a list of attribute dictionaries
    df_attr = df[["CampsiteID", "ATTRIBUTES"]]

    # Expand the ATTRIBUTES list so each attribute becomes its own row
    # Example: [Water, Electric] -> two separate rows
    df_attr = df_attr.explode("ATTRIBUTES")

    # Remove rows where attributes are missing or null
    df_attr = df_attr.dropna(subset=["ATTRIBUTES"])

    # Extract fields from the attribute dictionary
    # Each value in ATTRIBUTES is a dictionary like:
    # {"AttributeID":1, "AttributeName":"Water", "AttributeValue":"Yes"}

    df_attr["attribute_id"] = df_attr["ATTRIBUTES"].apply(lambda x: x.get("AttributeID"))
    df_attr["attribute_name"] = df_attr["ATTRIBUTES"].apply(lambda x: x.get("AttributeName"))
    df_attr["attribute_value"] = df_attr["ATTRIBUTES"].apply(lambda x: x.get("AttributeValue"))

    # Rename CampsiteID to snake_case for consistency with warehouse naming conventions
    df_attr = df_attr.rename(columns={
        "CampsiteID": "campsite_id"
    })

    # Keep only the final cleaned columns for the output dataset
    df_attr = df_attr[[
        "campsite_id",
        "attribute_id",
        "attribute_name",
        "attribute_value"
    ]]

    # Ensure the silver directory exists
    os.makedirs("data/silver", exist_ok=True)

    # Save the cleaned attribute dataset to the silver layer
    df_attr.to_parquet("data/silver/campsite_attributes.parquet")

    print("campsite_attributes saved")


# Run the pipeline only if this file is executed directly
if __name__ == "__main__":
    run()