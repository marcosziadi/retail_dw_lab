import pandas as pd

def clean_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    order_items_clean = (
        order_items
        .drop(columns=["unit_price"])
        .copy()
    )

    return order_items_clean