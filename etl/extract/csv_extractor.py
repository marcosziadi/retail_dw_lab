import pandas as pd
import os


class CSVExtractor:
    def __init__(self):
        pass

    def load_csv(self, path: str ,file_name: str) -> pd.DataFrame:
        """
        Lee un archivo CSV específico del directorio raw.
        """

        file = f"{file_name}.csv"
        file_path = os.path.join(path, file)

        try:
            df = pd.read_csv(file_path)
            print(f"{file_name} was successfully read.")
        except Exception as e:
            print(f"Error: {file_name} | {str(e)}")
            raise
        
        return df

    def read_all_csv_files(self, path):
        """
        DESCRIPTION
        """
        
        dataframes = {}

        for file in os.listdir(path):
            if file.endswith(".csv"):
                table_name = file.replace(".csv", "")
                df = self.load_csv(path, table_name)
                dataframes[table_name] = df
                
        return dataframes