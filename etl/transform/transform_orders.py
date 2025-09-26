import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_orders() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    orders = extractor.load_csv(RAW_PATH, "orders")
    
    important_columns = [
        'order_id',
        'customer_id',
        'shipping_address_id',
        'channel_id',
        'order_status',
        'order_date'
    ]
    orders_trimmed = orders[important_columns]

    paid_orders = orders_trimmed["order_status"].isin(["paid","shipped","delivered"])
    orders_clean = orders_trimmed[paid_orders]


    orders_clean.to_csv("staging/orders_clean.csv", index=False)
    print("orders_clean.csv created!")