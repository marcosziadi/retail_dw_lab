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
    categories = extractor.load_csv(RAW_PATH, "categories")

    categories_clean = (
        categories.merge(categories.add_prefix("parent_"), on = "parent_category_id", how = "left")
                .drop(columns = ["parent_category_id","parent_parent_category_id"])
                .rename(columns = {"category_name": "category"})
    )

    products_clean = (
        products.merge(categories_clean, on = "category_id", how = "left")
                .drop(columns = ["category_id"])
    )

    products_clean.to_csv("staging/products_clean.csv", index=False)
    print("products_clean.csv created!")