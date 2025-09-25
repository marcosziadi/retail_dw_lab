import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


RAW_PATH = Path("raw")
STAGING_PATH = Path("staging")


def clean_locations() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor(RAW_PATH, STAGING_PATH)

    c_addresses = extractor.load_csv(RAW_PATH, "customer_addresses")

    unique_locations = c_addresses[["city","province","country_code"]].drop_duplicates()

    unique_locations["location_id"] = range(1, len(unique_locations) + 1)

    locations = pd.merge(
        c_addresses,
        unique_locations,
        on = ["city","province","country_code"]
    )

    locations_clean = locations[[
        "location_id",
        "address_id",
        "city",
        "province",
        "country_code"
        ]].copy()

    locations_clean.to_csv("staging/locations_clean.csv", index=False)
    print("locations_clean.csv created!")