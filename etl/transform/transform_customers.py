import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_customers() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    customers = extractor.load_csv(RAW_PATH, "customers")
    
    customers_clean = customers.copy()

    customers_clean.to_csv("staging/customers_clean.csv", index=False)
    print("customers_clean.csv created!")