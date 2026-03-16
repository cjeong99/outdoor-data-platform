import os
import requests
import pandas as pd
from bronze_utils import save_to_bronze
from api_clinet import fetch_all

def run():

    print("Fetching campsites...")

    data = fetch_all("campsites")

    save_to_bronze(data, "campsites")

if __name__ == "__main__":
    run()
