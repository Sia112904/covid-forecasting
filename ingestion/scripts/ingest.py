import requests
import pandas as pd
import logging
import os
import json
from datetime import datetime
from dateutil import parser

# Ensure directories exist
os.makedirs("ingestion/raw", exist_ok=True)
os.makedirs("ingestion/logs", exist_ok=True)

LOG_FILE = "ingestion/logs/ingestion.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Ingestion script started")

API_SOURCES = {
    "disease_sh": "https://disease.sh/v3/covid-19/historical/all?lastdays=all",
    "covid_tracking": "https://api.covidtracking.com/v1/us/daily.json"
}

def fetch_api_data(name, url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully fetched data from {name}")
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch data from {name}: {e}")
        return None

def normalize_date(date_value):
    try:
        return parser.parse(str(date_value)).date().isoformat()
    except Exception:
        return None

def normalize_region(region):
    if region is None:
        return "UNKNOWN"
    return str(region).upper().strip()

def process_disease_sh(data):
    records = []
    cases = data.get("cases", {})

    for date, value in cases.items():
        records.append({
            "date": normalize_date(date),
            "region": "US",
            "cases": value
        })

    return pd.DataFrame(records)

def process_covid_tracking(data):
    records = []

    for row in data:
        records.append({
            "date": normalize_date(row.get("date")),
            "region": normalize_region(row.get("state")),
            "cases": row.get("positive")
        })

    return pd.DataFrame(records)

def store_raw_snapshot(source_name, raw_data):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"ingestion/raw/{source_name}_{timestamp}.json"

    try:
        with open(filename, "w") as f:
            json.dump(raw_data, f)
        logging.info(f"Raw snapshot saved: {filename}")
    except Exception as e:
        logging.error(f"Failed to save raw snapshot for {source_name}: {e}")

def main():
    for source, url in API_SOURCES.items():
        raw_data = fetch_api_data(source, url)

        if raw_data is None:
            continue

        store_raw_snapshot(source, raw_data)

        try:
            if source == "disease_sh":
                df = process_disease_sh(raw_data)
            elif source == "covid_tracking":
                df = process_covid_tracking(raw_data)

            logging.info(f"{source} processed with {len(df)} records")

        except Exception as e:
            logging.error(f"Processing failed for {source}: {e}")

if __name__ == "__main__":
    main()


