import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    customers_clean = customers.copy()

    customers_clean.to_csv("staging/customers_clean.csv", index=False)
    print("customers_clean.csv created!")