import sys
import os
from pathlib import Path
from etl.extract import CSVExtractor
from etl.transform import DimBuilder

from etl.transform import clean_products
from etl.transform import clean_channels
from etl.transform import clean_campaigns
from etl.transform import clean_customers
from etl.transform import clean_customer_addresses
from etl.transform import clean_categories
from etl.transform import clean_orders
from etl.transform import clean_order_marketing

from etl.transform import build_dim_product
from etl.transform import build_dim_channel
from etl.transform import build_dim_campaign
from etl.transform import build_dim_customer
from etl.transform import build_dim_location

from etl.load import CSVLoader
from utils.config_loader import load_config # SE PODRIA ELIMINAR

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
        # STAGING
        print("\nSTAGING: Cleaning and preparing tables...")  
        clean_products()
        clean_categories()
        clean_channels()
        clean_campaigns()
        clean_customers()
        clean_customer_addresses()
        clean_orders()
        clean_order_marketing()
        
        # TRANSFORM
        print("\nTRANSFORMATION:\nCreating dimensions...")
        build_dim_product()
        build_dim_channel()
        build_dim_campaign()
        build_dim_customer()
        build_dim_location()

        print("\nCreating fact table...")
        # build_fact_item_sell()

        # LOAD
        print("\nLOAD: Saving dimensions...")
        # loader = CSVLoader(WAREHOUSE_PATH)
        # dims_ready = []
        # for dim in dim_dfs:
        #     success = loader.save_dataframe(dim_dfs[dim], f"{dim}.csv")
        #     dims_ready.append(success)

        # if all(dims_ready):
        #     print("\n" + "=" * 50)
        #     print("ETL Pipeline successfully completed!")
        #     for dim in dim_dfs:
        #         print(f"{dim} created with {len(dim_dfs[dim])} rows")
        #     print("=" * 50)
        #     return True
        # else:
        #     raise Exception("Error saving the file")
    
    except Exception as e:
        print(f"Error in the pipeline: {str(e)}")
        print("=" * 50)
        return False

if __name__ == "__main__":
    run_etl_pipeline()