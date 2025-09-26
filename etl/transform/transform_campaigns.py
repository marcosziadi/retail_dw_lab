import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor

RAW_PATH = Path("raw")

def clean_campaigns() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    campaigns = extractor.load_csv(RAW_PATH, "campaigns")
    
    important_columns = ["campaign_id","campaign_name","utm_source","utm_medium","utm_campaign","start_date","end_date","budget_ars"]
    campaigns_clean = campaigns[important_columns]

    campaigns_clean.to_csv("staging/campaigns_clean.csv", index=False)
    print("campaigns_clean.csv created!")