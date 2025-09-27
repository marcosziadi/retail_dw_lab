import pandas as pd
from pathlib import Path



class CSVExtractor:
    def __init__(self):
        pass

    def load_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Lee un archivo CSV específico del directorio raw.
        """
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"File does not exist in path: {file_path}")
        except Exception as e:
            raise IOError(f"Error reading file '{file_path.name}': {str(e)}")
        
        return df

    def read_all_csv_files(self, path):
        """
        DESCRIPTION
        """
        dataframes = {}

        for file_path in path.glob("*.csv"):
            table_name = file_path.stem
            df = self.load_csv(file_path)
            dataframes[table_name] = df
                
        return dataframes