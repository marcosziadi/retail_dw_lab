import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_customer_addresses() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    customer_addresses = extractor.load_csv(RAW_PATH, "customer_addresses")
    
    important_columns = ["address_id", "address_type", "city", "province", "country_code"]
    customer_addresses_clean = customer_addresses[important_columns].copy()


    customer_addresses_clean.to_csv("staging/customer_addresses_clean.csv", index=False)
    print("customer_addresses_clean.csv created!")