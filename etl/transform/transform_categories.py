import pandas as pd

def clean_categories(categories: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    categories_clean = (
        categories.merge(categories.add_prefix("parent_"), on="parent_category_id", how="left")
                .drop(columns=["parent_category_id", "parent_parent_category_id"])
                .rename(columns={"category_name": "category"})
    )

    return categories_clean