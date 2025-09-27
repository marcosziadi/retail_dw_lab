import pandas as pd

def build_dim_product(clean_products: pd.DataFrame, clean_categories: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    dim_product = (
        clean_products
        .merge(clean_categories, on="category_id", how="left")
        .drop(columns=["category_id"])
    )
    
    dim_product["product_key"] = range(1, len(clean_products) + 1)

    dim_product = (
        dim_product[[
            "product_key",
            "product_id",
            "sku",
            "product_name",
            "brand",
            "unit_price",
            "unit_cost",
            "active_from",
            "active_to",
            "created_at",
            "category",
            "parent_category_name"
        ]]
        .copy()
    )

    return dim_product