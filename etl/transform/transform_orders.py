import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    orders_clean = orders.copy()

    orders_clean.to_csv("staging/orders_clean.csv", index=False)
    print("orders_clean.csv created!")