import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_order_items() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    order_items = extractor.load_csv(RAW_PATH, "order_items")
    
    order_items_clean = (
        order_items
        .drop(columns = ["unit_price"])
        .copy()
    )

    order_items_clean.to_csv("staging/order_items_clean.csv", index=False)
    print("order_items_clean.csv created!")