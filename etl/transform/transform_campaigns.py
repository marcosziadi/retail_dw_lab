import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_campaigns(campaigns: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    campaigns_clean = campaigns.drop(columns = ["channel_id"])

    campaigns_clean.to_csv("staging/campaigns_clean.csv", index=False)
    print("campaigns_clean.csv created!")