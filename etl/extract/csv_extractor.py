import pandas as pd
import os


class CSVExtractor:
    def __init__(self, raw_data_path, staging_data_path):
        self.raw_data_path = raw_data_path
        self.staging_data_path = staging_data_path

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

    def read_all_staging_csv_files(self):
        """
        Lee todos los archivos CSV del directorio raw.
        """
        
        dataframes = {}

        for file in os.listdir(self.staging_data_path):
            if file.endswith(".csv"):
                table_name = file.replace(".csv", "")
                df = self.load_csv(self.staging_data_path, table_name)
                dataframes[table_name] = df
                
        return dataframes