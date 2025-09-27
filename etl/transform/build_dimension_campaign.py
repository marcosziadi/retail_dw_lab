import pandas as pd

def build_dim_campaign(clean_campaigns: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """

    clean_campaigns["campaign_key"] = range(1, len(clean_campaigns) + 1)

    dim_campaign = (
        clean_campaigns[[
            "campaign_key",
            "campaign_id",
            "campaign_name",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "start_date",
            "end_date",
            "budget_ars"
        ]]
        .copy()
    )

    return dim_campaign