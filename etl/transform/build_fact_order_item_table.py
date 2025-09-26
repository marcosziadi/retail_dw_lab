import pandas as pd
from pathlib import Path
from etl.extract import CSVExtractor

STAGING_PATH = Path("staging")
WAREHOUSE_PATH = Path("warehouse")

def build_fact_order_item() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    
    orders_clean = extractor.load_csv(STAGING_PATH, "orders_clean")
    order_marketing_clean = extractor.load_csv(STAGING_PATH, "order_marketing_clean")
    order_items_clean = extractor.load_csv(STAGING_PATH, "order_items_clean")
    address_city_mapping = extractor.load_csv(STAGING_PATH, "address_city_mapping")
    
    dim_product = extractor.load_csv(WAREHOUSE_PATH, "dim_product")
    dim_channel = extractor.load_csv(WAREHOUSE_PATH, "dim_channel")
    dim_campaign = extractor.load_csv(WAREHOUSE_PATH, "dim_campaign")
    dim_customer = extractor.load_csv(WAREHOUSE_PATH, "dim_customer")
    dim_time = extractor.load_csv(WAREHOUSE_PATH, "dim_time")

    fact_table_raw = (
        order_items_clean
        .merge(orders_clean, on="order_id", how="inner")
        .merge(order_marketing_clean, on="order_id", how="inner")
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
    )

    fact_table.to_csv("warehouse/fact_order_item.csv", index=False)
    print("fact_order_item.csv created!")