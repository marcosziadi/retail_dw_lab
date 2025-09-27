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
        loader = CSVLoader()
        
        # ===== EXTRACT =====
        print("EXTRACTING DATA")
        
        raw_data = extract.read_all_csv_files(RAW_PATH)
        
        for table in list(raw_data.keys()):
            print(f"\tRead: {table}")
        num_tables = len(raw_data)
        print(f"Extraction completed: {num_tables} tables were read.\n")
        

        # ===== TRANSFORM =====
        print("STAGING")  

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
            loader.save_dataframe(STAGING_PATH, clean_tables[table], f"clean_{table}.csv")    
        
        for table in list(clean_tables.keys()):
            print(f"\tCreated & Saved: clean_{table}")
        num_tables = len(clean_tables)
        print(f"Creation and saving completed: {num_tables} tables were saved.\n")

        print("TRANSFORMATION")

        dim_tables = {
            "product": tr.build_dim_product(clean_tables["products"], clean_tables["categories"]),
            "channel": tr.build_dim_channel(clean_tables["channels"]),
            "campaign": tr.build_dim_campaign(clean_tables["campaigns"]),
            "customer": tr.build_dim_customer(clean_tables["customers"]),
            "time": tr.build_dim_time(clean_tables["orders"])
        }

        dim_location, address_mapping = tr.build_dim_location(clean_tables["customer_addresses"])
        dim_tables["location"] = dim_location
        # AGREGAR IMPRIMIR CREATED AND SAVED DIMENSION Y LUEGO FACT_TABLE
        mappings = {
            "address_city_mapping": address_mapping
        }
        loader.save_dataframe(STAGING_PATH, mappings["address_city_mapping"], "address_city_mapping.csv") 

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

        save_status = []
        for table_name, table in dim_tables.items():
            save_status.append(loader.save_dataframe(WAREHOUSE_PATH, table, f"dim_{table_name}.csv"))
        save_status.append(loader.save_dataframe(WAREHOUSE_PATH, fact_order_items, "fact_order_items.csv"))
        
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