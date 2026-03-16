import os
import requests
import pandas as pd
from bronze_utils import save_to_bronze
from api_clinet import fetch_all

def run():

    print("Fetching facilities...")

    data = fetch_all("facilities")

    save_to_bronze(data, "facilities")


if __name__ == "__main__":
    run()
