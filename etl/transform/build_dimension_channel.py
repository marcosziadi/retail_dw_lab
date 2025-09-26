import pandas as pd
from pathlib import Path
from etl.extract import CSVExtractor

STAGING_PATH = Path("staging")

def build_dim_channel() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    channel_clean = extractor.load_csv(STAGING_PATH, "channels_clean")
    
    channel_clean["channel_key"] = range(1, len(channel_clean) + 1)

    dim_channel = channel_clean.copy()

    dim_channel.to_csv("warehouse/dim_channel.csv", index=False)
    print("dim_channel.csv created!")