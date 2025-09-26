import pandas as pd
from pathlib import Path
from etl.extract import CSVExtractor

STAGING_PATH = Path("staging")

def build_dim_campaign() -> pd.DataFrame:
    """
    DESCRIPTION
    """

    extractor = CSVExtractor()
    campaign_clean = extractor.load_csv(STAGING_PATH, "campaigns_clean")
    
    campaign_clean["campaign_key"] = range(1, len(campaign_clean) + 1)

    dim_campaign = (
        campaign_clean[[
            "campaign_key",
            "campaign_id",
            "campaign_name",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "start_date",
            "end_date",
            "budget_ars"
        ]].copy()
    )

    dim_campaign.to_csv("warehouse/dim_campaign.csv", index=False)
    print("dim_campaign.csv created!")