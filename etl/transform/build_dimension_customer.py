import pandas as pd
from pathlib import Path
from etl.extract import CSVExtractor

STAGING_PATH = Path("staging")

def build_dim_customer() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    customer_clean = extractor.load_csv(STAGING_PATH, "customers_clean")
    
    customer_clean["customer_key"] = range(1, len(customer_clean) + 1)

    dim_customer = (
        customer_clean[[
            "customer_key",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "gender",
            "birth_date",
            "created_at",
            "marketing_opt_in"
        ]].copy()
    )

    dim_customer.to_csv("warehouse/dim_customer.csv", index=False)
    print("dim_customer.csv created!")