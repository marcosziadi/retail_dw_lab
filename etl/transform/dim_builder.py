import pandas as pd
import datetime as dt



class DimBuilder:
    def __init__(self):
        pass

    def _create_surrogate_key(self, dim_table: pd.DataFrame, key_name: str) -> pd.DataFrame:
        """
        DESCRIPTION
        """

        dim_table[key_name] = range(1, len(dim_table) + 1)

        return dim_table

    def _add_parent_category(self, categories_df: pd.DataFrame) -> pd.DataFrame:
        """
        Replaces the parent_category_id for its name in the categories table
        """
        
        categories_with_parents_df = pd.merge(
            categories_df,
            categories_df.add_prefix("parent_"),
            on="parent_category_id",
            how="left"
        )[["category_id", "category_name", "parent_category_name"]]

        return categories_with_parents_df 


    def build_dim_product(self, dim_product_raw: pd.DataFrame) -> pd.DataFrame:
        """
        DESCRIPTION
        """

        # New surrogate key
        dim_product = self._create_surrogate_key(dim_product_raw, "product_key")

        print(f"dim_product table was successfully created!")

        return dim_product

    def build_dim_campaign(self, dim_campaign_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Creates campaign dimension.
        """

        # New surrogate key
        dim_campaign = self._create_surrogate_key(dim_campaign_raw, "campaign_key")

        print(f"dim_campaign table was successfully created!")

        return dim_campaign

    def build_dim_customer(self, dim_customer_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Creates customer dimension with customers dataframe.
        """

        # New surrogate key
        dim_customer = self._create_surrogate_key(dim_customer_raw, "customer_key")

        print(f"dim_customer table was successfully created!")

        return dim_customer

    def build_dim_channel(self, dim_channel_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Creates channel dimension.
        """

        # New surrogate key
        dim_channel = self._create_surrogate_key(dim_channel_raw, "channel_key")

        print(f"dim_channel table was successfully created!")

        return dim_channel

    def build_dim_location(self, dim_location_raw: pd.DataFrame) -> pd.DataFrame:
        """
        DESCRIPTION
        """
        # ACA HAY QUE SACAR COLUMNA ADDRESS_ID Y REORDENAR DUPLCIADOS
        # New surrogate key
        dim_location = self._create_surrogate_key(dim_location_raw, "location_key")

        print(f"dim_location table was successfully created!")

        return dim_location

    # def build_dim_date(self, orders_df):
    #     oldest_year = dt.to_datetime(orders_df["order_date"]).min().dt.year
    #     newest_date = dt.to_datetime(orders_df["order_date"]).max().dt.year

        

        