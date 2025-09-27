import pandas as pd

def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    important_columns = [
        'order_id',
        'customer_id',
        'shipping_address_id',
        'channel_id',
        'order_status',
        'order_date'
    ]

    orders_trimmed = orders[important_columns]
    
    paid_status = ["paid","shipped","delivered"]
    paid_orders = (
        orders_trimmed["order_status"]
        .isin(paid_status)
    )
    orders_clean = (
        orders_trimmed[paid_orders]
        .rename(columns={"shipping_address_id": "address_id"})
    )

    orders_clean["order_date"] = (
        pd.to_datetime(orders_clean["order_date"])
        .dt.floor("min")
    )
    
    orders_clean["datetime_id"] = (
        orders_clean["order_date"]
        .dt.strftime("%Y%m%d%H%M")
        .astype(int)
    )
    
    return orders_clean