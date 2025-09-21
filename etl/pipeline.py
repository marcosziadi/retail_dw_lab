import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from etl.extract import CSVExtractor
from etl.transform import DimBuilder
from etl.load import CSVLoader
from utils.config_loader import load_config

def run_etl_pipeline():
    """
    Run the entire ETL pipeline modularly
    """

    print("="*50)
    print("INITIALIZING ETL PIPELINE")
    print("="*50)

    try:
        # LOAD CONFIG
        config = load_config()

        # EXTRACT
        print("\nEXTRACTION: Reading csv files...")
        extractor = CSVExtractor(config["paths"]["raw"])
        raw_data = extractor.read_all_csv_files()
        # Verify required tables written on config
        required_tables = config["dimensions"]["dim_product"]["source_tables"]
        for table in required_tables:
            if table not in raw_data:
                raise ValueError(f"Missing required file: {table}.csv")

        # TRANSFORM
        print("\nTRANSFORMATION: Creating product dimension...")
        dim_builder = DimBuilder(config)
        dim_dfs = {}

        dim_product_df = dim_builder.build_dim_product(
            raw_data["products"],
            raw_data["categories"]
        )
        dim_dfs["dim_product"] = dim_product_df

        dim_campaign_df = dim_builder.build_dim_campaign(
            raw_data["campaigns"],
            raw_data["channels"]
        )
        dim_dfs["dim_campaign"] = dim_campaign_df

        dim_customer_df = dim_builder.build_dim_customer(
            raw_data["customers"]
        )
        dim_dfs["dim_customer"] = dim_customer_df

        # LOAD
        print("\nLOAD: Saving dimensions...")
        loader = CSVLoader(config["paths"]["warehouse"])
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