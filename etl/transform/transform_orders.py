import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_orders(orders: pd.DataFrame, order_marketing: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """
    
    order_marketing_campaign = order_marketing[~ order_marketing["campaign_id"].isnull()]

    order_marketing_campaign_mapping = order_marketing_campaign[["order_id","campaign_id"]]

    orders_paid = orders[orders["order_status"].isin(["paid","shipped","delivered"])]
    
    important_cols = [
        'order_id',
        'customer_id',
        'channel_id',
        'location_id',
        'order_date',
        'order_status',
        'currency',
        'shipping_amount',
        'order_subtotal',
        'order_discount',
        'order_tax',
        'order_total'
    ]

    orders_paid = orders_paid[important_cols].copy()

    orders_paid.to_csv("staging/orders_clean.csv", index=False)
    print("orders_clean.csv created!")

    order_marketing_campaign_mapping.to_csv("staging/mappings/order_marketing_campaign_mapping.csv")