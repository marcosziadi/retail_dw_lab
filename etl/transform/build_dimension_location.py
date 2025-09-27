import pandas as pd

def build_dim_location(clean_customer_addresses: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """
   
    only_shipping = (
        clean_customer_addresses["address_type"]
        .isin(["shipping"])
    )
    shipping_addresses = clean_customer_addresses[only_shipping]
    
    unique_cities = (
        shipping_addresses[[
            "city",
            "province",
            "country_code"
            ]]
        .drop_duplicates()
    )
    unique_cities["location_key"] = range(1, len(unique_cities) + 1)

    dim_location = (
        unique_cities[[
            "location_key",
            "city",
            "province",
            "country_code"
        ]]
        .copy()
    )

    address_city_mapping = (
        shipping_addresses
        .merge(unique_cities, on=["city", "province", "country_code"], how="left")
        .drop(columns=["city", "province", "country_code", "address_type"])
        )
    
    return dim_location, address_city_mapping
