import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


RAW_PATH = Path("raw")
STAGING_PATH = Path("staging")


def clean_products() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor(RAW_PATH, STAGING_PATH)

    products = extractor.load_csv(RAW_PATH, "products")
    categories = extractor.load_csv(RAW_PATH, "categories")

    categories_with_parents_name = pd.merge(
        categories,
        categories.add_prefix("parent_"),
        on="parent_category_id",
        how="left"
    )[["category_id", "category_name", "parent_category_name"]]

    products_clean = pd.merge(
        products,
        categories_with_parents_name,
        on = "category_id",
        how = "left"
    ).drop(columns=["category_id"])

    products_clean.to_csv("staging/products_clean.csv", index=False)

    print("products_clean.csv created!")