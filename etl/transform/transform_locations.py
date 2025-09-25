import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_locations(c_addresses: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    # Columns required for the transformation
    required_cols = ["address_id", "city", "province", "country_code"]
    missing = [col for col in required_cols if col not in c_addresses.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Keep only important columns
    customers_trimmed = c_addresses[required_cols].copy()

    # Generate unique locations
    unique_locations = (
        customers_trimmed[["city", "province", "country_code"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    unique_locations.insert(0, "location_id", range(1, len(unique_locations) + 1))

    # Join back to create mapping (address_id → location_id)
    locations_with_keys = pd.merge(
        customers_trimmed,
        unique_locations,
        on=["city", "province", "country_code"],
        how="left"
    )

    location_customer_address_mapping = locations_with_keys[["address_id", "location_id"]]

    # Final dimension table (clean)
    locations_clean = (
        unique_locations[["location_id", "city", "province", "country_code"]]
        .sort_values("location_id")
        .reset_index(drop=True)
    )

    locations_clean.to_csv("staging/locations_clean.csv", index=False)
    print("locations_clean.csv created!")
    
    location_customer_address_mapping.to_csv("staging/mappings/location_customer_address_mapping.csv")