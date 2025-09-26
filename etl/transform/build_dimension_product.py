import pandas as pd
from pathlib import Path
from etl.extract import CSVExtractor

STAGING_PATH = Path("staging")

def build_dim_product() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    products_clean = extractor.load_csv(STAGING_PATH, "products_clean")
    categories_clean = extractor.load_csv(STAGING_PATH, "categories_clean")
    
    dim_product = (
        products_clean.merge(categories_clean, on = "category_id", how = "left")
                      .drop(columns = ["category_id"])
    )
    
    dim_product["product_key"] = range(1, len(products_clean) + 1)

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
        ]].copy()
    )

    dim_product.to_csv("warehouse/dim_product.csv", index=False)
    print("dim_product.csv created!")