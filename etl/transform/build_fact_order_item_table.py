import pandas as pd

def build_fact_order_item(
        clean_orders: pd.DataFrame,
        clean_order_marketing: pd.DataFrame,
        clean_order_items: pd.DataFrame,
        dim_product: pd.DataFrame,
        dim_channel: pd.DataFrame,
        dim_campaign: pd.DataFrame,
        dim_customer: pd.DataFrame,
        dim_time: pd.DataFrame,
        address_city_mapping: pd.DataFrame,
    ) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    fact_table_raw = (
        clean_order_items
        .merge(clean_orders, on="order_id", how="inner")
        .merge(clean_order_marketing, on="order_id", how="inner")
    )

    fact_table = (
        fact_table_raw
        .merge(address_city_mapping, on = "address_id", how = "left")
        .merge(dim_product[["product_id","product_key"]], on="product_id", how = "left")
        .merge(dim_channel[["channel_id","channel_key"]], on = "channel_id", how = "left")
        .merge(dim_campaign[["campaign_id","campaign_key"]], on = "campaign_id", how = "left")
        .merge(dim_customer[["customer_id","customer_key"]], on = "customer_id", how = "left")
        .merge(dim_time[["datetime_id","time_key"]], on = "datetime_id", how = "left")
        .drop(columns=["order_id","address_id","product_id","channel_id","campaign_id","customer_id","datetime_id"])          
    )
    
    fact_table["order_key"] = range(1, len(fact_table) + 1)

    fact_table = (
        fact_table[[
            "order_key",
            "location_key",
            "product_key",
            "channel_key",
            "campaign_key",
            "customer_key",
            "time_key",
            "line_number",
            "quantity",
            "discount_amount",
            "tax_amount",
        ]]
        .copy()
    )

    return fact_table