import yaml
import os
import pandas as pd




with open("../config/settings.yaml") as f:
    config = yaml.safe_load(f)

RAW_PATH = config["paths"]["raw"]



def load_data(csv_name: str) -> pd.DataFrame:
    """Cargar una tabla cruda desde raw"""
    return pd.read_csv(os.path.join(RAW_PATH, f"{csv_name}.csv"))



def check_primary_key(df: pd.DataFrame, pk: str) -> dict:
    """Valida la integridad de la PK: unicidad y no nulos"""
    return {
        "rows": len(df),
        "unique_ids": df[pk].nunique(),
        "null_ids": df[pk].isnull().sum(),
        "pk_valid": (len(df) == df[pk].nunique() and (df[pk].isnull().sum() == 0))
    }



def quality_report(df: pd.DataFrame, pk: str = None) -> pd.DataFrame:
    """Genera un resumen de calidad de datos"""
    report = {
        "rows": df.shape[0],
        "columns": df.shape[1],
        "nulls_total": df.isnull().sum().sum(),
        "duplicates": df.duplicated().sum()
    }
    if pk:
        report.update(check_primary_key(df, pk))
    return pd.DataFrame([report])



