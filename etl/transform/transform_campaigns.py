import pandas as pd

def clean_campaigns(campaigns: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    campaigns_clean = (
        campaigns
        .drop(columns=["channel_id"])
    )

    return campaigns_clean