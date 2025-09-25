import pandas as pd
from pathlib import Path

from etl.extract import CSVExtractor


def clean_channels(channels: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    channels_clean = channels.copy()

    channels_clean.to_csv("staging/channels_clean.csv", index=False)
    print("channels_clean.csv created!")