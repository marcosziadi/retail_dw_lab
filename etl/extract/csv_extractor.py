import pandas as pd
import os

class CSVExtractor:
    def __init__(self, raw_data_path):
        self.raw_data_path = raw_data_path

    def read_all_csv_files(self):
        """
        Lee todos los archivos CSV del directorio raw
        """
        
        dataframes = {}

        for file in os.listdir(self.raw_data_path):
            if file.endswith(".csv"):
                table_name = file.replace(".csv", "")
                file_path = os.path.join(self.raw_data_path, file)

                try:
                    df = pd.read_csv(file_path)
                    dataframes[table_name] = df
                    print(f"{file} was successfully read.\tRows: {len(df)}")
                except Exception as e:
                    print(f"Error: {file} | {str(e)}")
                    raise
            
        return dataframes