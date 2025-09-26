import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_products() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    products = extractor.load_csv(RAW_PATH, "products")

    products_clean = products.copy()

    products_clean.to_csv("staging/products_clean.csv", index=False)
    print("products_clean.csv created!")