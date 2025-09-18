import yaml
import os

def load_config(config_file = "./config/settings.yaml"):
    """
    Loads settings from YAML file
    """

    try:
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Setting file not found: {config_file}")
        
        with open(config_file, "r") as file:
            config = yaml.safe_load(file)

        print(f"Settings successfully loaded from {config_file}")
        return config
    
    except FileNotFoundError as e:
        print(f"Error loading settings: {str(e)}")
        raise