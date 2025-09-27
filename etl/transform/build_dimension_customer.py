import pandas as pd

def build_dim_customer(clean_customers: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """
 
    clean_customers["customer_key"] = range(1, len(clean_customers) + 1)

    dim_customer = (
        clean_customers[[
            "customer_key",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "gender",
            "birth_date",
            "created_at",
            "marketing_opt_in"
        ]]
        .copy()
    )

    return dim_customer