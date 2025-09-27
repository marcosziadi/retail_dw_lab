from pathlib import Path
from etl.extract import CSVExtractor
from etl import transform as tr
from etl.load import CSVLoader
# from utils.config_loader import load_config # SE PODRIA ELIMINAR

RAW_PATH = Path("raw")
WAREHOUSE_PATH = Path("warehouse")
STAGING_PATH = Path("staging")

def run_etl_pipeline():
    """
    Run the entire ETL pipeline modularly
    """

    print("=" * 50 + "\nINITIALIZING ETL PIPELINE\n" + "=" * 50 + "\n")

    try:
        extract = CSVExtractor()
        staging_loader = CSVLoader(STAGING_PATH)
        
        # ===== EXTRACT =====
        print("- " * 25 + "\nEXTRACTING: Extracting data...\n" + "- " * 25)
        
        raw_data = extract.read_all_csv_files(RAW_PATH)

        print("- " * 25 + "\n")

        # ===== TRANSFORM =====
        print("- " * 25 + "\nSTAGING: Cleaning and preparing tables...\n" + "- " * 25)  

        clean_tables = {
            "products": tr.clean_products(raw_data["products"]),
            "categories": tr.clean_categories(raw_data["categories"]),
            "channels": tr.clean_channels(raw_data["channels"]),
            "campaigns": tr.clean_campaigns(raw_data["campaigns"]),
            "customers": tr.clean_customers(raw_data["customers"]),
            "customer_addresses": tr.clean_customer_addresses(raw_data["customer_addresses"]),
            "orders": tr.clean_orders(raw_data["orders"]),
            "order_marketing": tr.clean_order_marketing(raw_data["order_marketing"]),
            "order_items": tr.clean_order_items(raw_data["order_items"])
        }

        for table in clean_tables:
            staging_loader.save_dataframe(clean_tables[table], f"clean_{table}.csv")    
        
        print("- " * 25 + "\n")

        print("- " * 25 + "\nTRANSFORMATION\n--> Creating dimensions...\n" + "- " * 25)
        
        staging_data = extract.read_all_csv_files(STAGING_PATH)

        dim_tables = {
            "product": tr.build_dim_product(staging_data["clean_products"], staging_data["clean_categories"]),
            "channel": tr.build_dim_channel(staging_data["clean_channels"]),
            "campaign": tr.build_dim_campaign(staging_data["clean_campaigns"]),
            "customer": tr.build_dim_customer(staging_data["clean_customers"]),
            "location": tr.build_dim_location(staging_data["clean_customer_addresses"]),
            "time": tr.build_dim_time(staging_data["clean_orders"])
        }

        dim_location, address_mapping = tr.build_dim_location(staging_data["clean_customer_addresses"])
        dim_tables["location"] = dim_location

        mappings = {
            "address_city_mapping": address_mapping
        }
        staging_loader.save_dataframe(mappings["address_city_mapping"], "address_city_mapping.csv") 

        print("- " * 25 + "\n")

        print("- " * 25 + "\nTRANSFORMATION\n--> Creating fact table...\n" + "- " * 25)

        fact_order_items = (
            tr.build_fact_order_item(
                clean_tables["orders"],
                clean_tables["order_marketing"],
                clean_tables["order_items"],
                dim_tables["product"],
                dim_tables["channel"],
                dim_tables["campaign"],
                dim_tables["customer"],
                dim_tables["time"],
                mappings["address_city_mapping"]
            )
        )

        # ===== LOAD =====
        print("- " * 25 + "\nLOAD: Saving dimensions...\n" + "- " * 25)

        warehouse_loader = CSVLoader(WAREHOUSE_PATH)

        save_status = []
        for table_name, table in dim_tables.items():
            save_status.append(warehouse_loader.save_dataframe(table, f"dim_{table_name}.csv"))

        save_status.append(warehouse_loader.save_dataframe(fact_order_items, "fact_order_items.csv"))
        
        print("- " * 25 + "\n")

        if all(save_status):
            print("=" * 50 + "\nETL Pipeline successfully completed!\n"+ "=" * 50)
            return True
        else:
            raise Exception("Error saving a file")
    
    except Exception as e:
        print(f"Error in the pipeline: {str(e)}")
        print("=" * 50)
        return False

if __name__ == "__main__":
    run_etl_pipeline()