import pandas as pd

def clean_customer_addresses(customer_addresses: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """
   
    important_columns = [
        "address_id",
        "address_type",
        "city",
        "province",
        "country_code"]
        
    customer_addresses_clean = (
        customer_addresses[important_columns]
        .copy()
    )

    return customer_addresses_clean