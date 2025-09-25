import sys
import os
from pathlib import Path

from etl.extract import CSVExtractor
from etl.transform import DimBuilder
from etl.transform import clean_products
from etl.transform import clean_channels
from etl.transform import clean_campaigns
from etl.transform import clean_customers
from etl.transform import clean_locations
from etl.load import CSVLoader
from utils.config_loader import load_config # SE PODRIA ELIMINAR

import pandas as pd # TEMPORAL SACAR CUANDO SE SAQUEN LOS README
RAW_PATH = Path("raw")
WAREHOUSE_PATH = Path("warehouse")
STAGING_PATH = Path("staging")


def run_etl_pipeline():
    """
    Run the entire ETL pipeline modularly
    """

    print("="*50)
    print("INITIALIZING ETL PIPELINE")
    print("="*50)

    try:
        # EXTRACT & TRANSFORM
        print("\nEXTRACTION: Reading csv files...")
        print("\nSTAGING: Cleaning and preparing tables...")  
        clean_products()
        clean_channels()
        clean_campaigns()
        clean_customers()
        clean_locations()

        print("\nTRANSFORMATION: Creating dimensions...")
        extractor = CSVExtractor(RAW_PATH, STAGING_PATH)
        staging_data = extractor.read_all_staging_csv_files()

        dim_builder = DimBuilder()
        dim_dfs = {}

        dim_product_df = dim_builder.build_dim_product(staging_data["products_clean"])
        dim_dfs["dim_product"] = dim_product_df

        dim_campaign_df = dim_builder.build_dim_campaign(staging_data["campaigns_clean"])
        dim_dfs["dim_campaign"] = dim_campaign_df

        dim_customer_df = dim_builder.build_dim_customer(staging_data["customers_clean"])
        dim_dfs["dim_customer"] = dim_customer_df

        dim_channel_df = dim_builder.build_dim_channel(staging_data["channels_clean"])
        dim_dfs["dim_channel"] = dim_channel_df

        dim_location_df = dim_builder.build_dim_channel(staging_data["locations_clean"])
        dim_dfs["dim_location"] = dim_location_df

        # LOAD
        print("\nLOAD: Saving dimensions...")
        loader = CSVLoader(WAREHOUSE_PATH)
        dims_ready = []
        for dim in dim_dfs:
            success = loader.save_dataframe(dim_dfs[dim], f"{dim}.csv")
            dims_ready.append(success)

        if all(dims_ready):
            print("\n" + "=" * 50)
            print("ETL Pipeline successfully completed!")
            for dim in dim_dfs:
                print(f"{dim} created with {len(dim_dfs[dim])} rows")
            print("=" * 50)
            return True
        else:
            raise Exception("Error saving the file")
    
    except Exception as e:
        print(f"Error in the pipeline: {str(e)}")
        print("=" * 50)
        return False

if __name__ == "__main__":
    run_etl_pipeline()