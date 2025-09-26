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
    
    products_clean["product_key"] = range(1, len(products_clean) + 1)

    dim_products = products_clean.copy()

    dim_products.to_csv("warehouse/dim_product.csv", index=False)
    print("dim_product.csv created!")