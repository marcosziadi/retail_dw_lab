import pandas as pd
import datetime as dt



class DimBuilder:
    def __init__(self, config):
        self.config = config

    
    def build_dim_product(self, products_df: pd.DataFrame, categories_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates product dimension by joining products and categories tables.
        """
        
        # Create categories with parent names
        categories_with_parents_df = pd.merge(
            categories_df,
            categories_df.add_prefix("parent_"),
            on = "parent_category_id",
            how = "left",
        )[["category_id","category_name","parent_category_name"]]

        # Create dimension_table
        dim_product = pd.merge(
            products_df,
            categories_with_parents_df,
            on = "category_id",
            how = "left"
        ).drop(columns=["category_id"])

        # NEW SURROGATE KEY
        # dim_product = dim_product.reset_index().rename(columns = {'index':'product_key'})
        # dim_product['product_key'] = dim_product['product_key'] + 1

        print(f"dim_product table was successfully created. Rows: {len(dim_product)}")
        return dim_product


    def build_dim_campaign(self, campaigns_df, channels_df):
        """
        Creates campaign dimension by joining campaigns and channels.
        """

        # JOIN CAMPAIGNS AND CHANNELS
        dim_campaign = pd.merge(
            campaigns_df,
            channels_df,
            on = "channel_id",
            how = "left"
        ).drop(columns=["channel_id"])

        # NEW SURROGATE KEY
        # dim_campaign['campaign_key'] = range(1, len(dim_campaign) + 1)

        print(f"dim_campaign table was successfully created. Rows: {len(dim_campaign)}")
        return dim_campaign

    def build_dim_customer(self, customers_df):
        """
        Creates customer dimension with customers dataframe.
        """

        dim_customer = customers_df.rename(columns = {
            "customer_id": "customer_key"
            })
        print(f"dim_customer table was successfully created. Rows: {len(dim_customer)}")
        return dim_customer

    # def build_dim_date(self, orders_df):
    #     oldest_year = dt.to_datetime(orders_df["order_date"]).min().dt.year
    #     newest_date = dt.to_datetime(orders_df["order_date"]).max().dt.year

        

        