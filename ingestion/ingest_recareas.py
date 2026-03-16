import os
import requests
import pandas as pd
from bronze_utils import save_to_bronze
from api_clinet import fetch_all

#API_KEY = os.getenv("API_KEY")
def run():

    print("Fetching recreation areas...")

    data = fetch_all("recareas")

    save_to_bronze(data, "recareas")


if __name__ == "__main__":
    run()
