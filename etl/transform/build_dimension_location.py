import pandas as pd
from pathlib import Path
from etl.extract import CSVExtractor

STAGING_PATH = Path("staging")

def build_dim_location() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    customer_addresses_clean = extractor.load_csv(STAGING_PATH, "customer_addresses_clean")
    
    only_shipping = customer_addresses_clean["address_type"] == "shipping"
    shipping_addresses = customer_addresses_clean[only_shipping]
    
    unique_cities = shipping_addresses[["city", "province", "country_code"]].drop_duplicates()
    unique_cities["location_key"] = range(1, len(unique_cities) + 1)

    dim_location = (
        unique_cities[[
            "location_key",
            "city",
            "province",
            "country_code"
        ]].copy()
    )

    dim_location.to_csv("warehouse/dim_location.csv", index=False)
    print("dim_location.csv created!")

    address_city_mapping = (
        shipping_addresses.merge(unique_cities, on = ["city", "province", "country_code"], how = "left")
                          .drop(columns = ["city","province","country_code", "address_type"])
        )
    
    address_city_mapping.to_csv("staging/address_city_mapping.csv", index=False)
    print("address_city_mapping.csv created!")
