import pandas as pd

def build_dim_channel(clean_channels: pd.DataFrame) -> pd.DataFrame:
    """
    DESCRIPTION
    """
    
    clean_channels["channel_key"] = range(1, len(clean_channels) + 1)

    dim_channel = (
        clean_channels[[
            "channel_key",
            "channel_id",
            "channel_name",
            "description"
        ]]
        .copy()
    )

    return clean_channels