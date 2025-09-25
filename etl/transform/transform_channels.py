import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


RAW_PATH = Path("raw")
STAGING_PATH = Path("staging")


def clean_channels() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor(RAW_PATH, STAGING_PATH)

    channels = extractor.load_csv(RAW_PATH, "channels")

    channels_clean = channels.copy()

    channels_clean.to_csv("staging/channels_clean.csv", index=False)
    print("channels_clean.csv created!")