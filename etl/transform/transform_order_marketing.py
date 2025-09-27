import pandas as pd

def clean_order_marketing(order_marketing: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """
    
    important_columns = [
        "order_id",
        "campaign_id"
    ]
    
    order_marketing_trimmed = order_marketing[important_columns]

    not_null_campaign = (
        order_marketing_trimmed["campaign_id"]
        .notnull()
    )
    order_marketing_clean = (
        order_marketing_trimmed[not_null_campaign]
        .copy()
    )

    return order_marketing_clean