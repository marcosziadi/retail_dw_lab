import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


RAW_PATH = Path("raw")
STAGING_PATH = Path("staging")


def clean_campaigns() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor(RAW_PATH, STAGING_PATH)

    campaigns = extractor.load_csv(RAW_PATH, "campaigns")

    campaigns_clean = campaigns.drop(columns = ["channel_id"])

    campaigns_clean.to_csv("staging/campaigns_clean.csv", index=False)
    print("campaigns_clean.csv created!")