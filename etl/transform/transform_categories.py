import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_categories() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    categories = extractor.load_csv(RAW_PATH, "categories")
    
    categories_clean = (
        categories.merge(categories.add_prefix("parent_"), on = "parent_category_id", how = "left")
                .drop(columns = ["parent_category_id","parent_parent_category_id"])
                .rename(columns = {"category_name": "category"})
    )

    categories_clean.to_csv("staging/categories_clean.csv", index=False)
    print("categories_clean.csv created!")