"""Module to fetch data from INPI and INSEE APIs and save to CSV files."""

import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env


def fetch_all_inpi_data():
    """
    Fetch all company financial ratios data from the INPI BCE dataset using pagination.

    Returns a list of company records.
    """
    base_url = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/ratios_inpi_bce/records"
    all_records = []

    offset = 0
    limit = 100
    while offset < 1:  # Put True to fetch all data
        params = {
            "limit": limit,
            "offset": offset,
        }

        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Failed at offset {offset} with status code: {response.status_code}")
            break

        data = response.json()
        records = data.get("results", [])
        if not records:
            break
        all_records.extend(records)
        offset += limit
        time.sleep(0.5)  # Rate-limiting

    return all_records


def fetch_insee_data_by_siren(df_inpi):
    """
    Fetch detailed INSEE data for each unique SIREN in the given INPI DataFrame.

    Returns a list of INSEE records.
    """
    api_key = os.getenv("INSEE_API_KEY")
    if not api_key:
        raise ValueError("INSEE_API_KEY not found in environment variables.")

    headers = {
        "X-INSEE-Api-Key-Integration": api_key,
    }

    siren_list = df_inpi["siren"].dropna().unique()
    all_insee_data = []

    for siren in siren_list:
        url = f"https://api.insee.fr/api-sirene/3.11/siren/{siren}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            unite_legale = json_data.get("uniteLegale")
            if unite_legale:
                all_insee_data.append(unite_legale)
        else:
            print(f"Failed to fetch INSEE data for SIREN {siren}: {response.status_code}")

        time.sleep(2)  # To avoid hitting rate limits

    return all_insee_data


def save_data_to_csv(data, path):
    """Save a list of dictionaries or DataFrame to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)
