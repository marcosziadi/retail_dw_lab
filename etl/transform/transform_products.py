import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_products(products: pd.DataFrame, categories: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

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