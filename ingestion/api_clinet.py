import os
from time import time
from urllib import response
import requests
from dotenv import load_dotenv
import time

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://ridb.recreation.gov/api/v1/"
MAX_OFFSET = 15000

def fetch_all(endpoint, limit=50):

    offset = 0
    all_records = []

    while True:

        url = f"{BASE_URL}/{endpoint}"
        params = {
            "apikey": API_KEY,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params, timeout=30)
        time.sleep(0.2)

        if response.status_code != 200:
            raise Exception("API request failed")

        data = response.json()["RECDATA"]

        if not data:
            break
        
        if offset > MAX_OFFSET:
            break


        all_records.extend(data)

        print(f"Fetched {len(data)} records (offset={offset})")
        offset += limit

    return all_records
