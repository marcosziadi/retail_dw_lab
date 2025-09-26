import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_order_marketing() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    order_marketing = extractor.load_csv(RAW_PATH, "order_marketing")
    
    important_columns = ["order_id","campaign_id"]
    order_marketing_trimmed = order_marketing[important_columns]


    order_marketing_clean = order_marketing_trimmed[order_marketing_trimmed["campaign_id"].notnull()]

    order_marketing_clean.to_csv("staging/order_marketing_clean.csv", index=False)
    print("order_marketing_clean.csv created!")