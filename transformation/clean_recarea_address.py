import pandas as pd
import os
#No Data From API, just transforming the data from bronze to silver
def run():

    df = pd.read_parquet("data/bronze/recareas.parquet")

    rows = []

    for _, row in df.iterrows():

        recarea_id = row["RecAreaID"]

        address = row.get("RECAREAADDRESS", [])

        if address:
            for addr in address:

                rows.append({
                    "recarea_id": recarea_id,
                    "city": addr.get("City"),
                    "state": addr.get("AddressStateCode"),
                    "postal_code": addr.get("PostalCode"),
                    "country": addr.get("AddressCountryCode")
                })

    df_addr = pd.DataFrame(rows)

    os.makedirs("data/silver", exist_ok=True)

    df_addr.to_parquet("data/silver/recarea_addresses.parquet")

    print("recarea_addresses saved")


if __name__ == "__main__":
    run()
