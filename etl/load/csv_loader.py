from pathlib import Path
import pandas as pd

class CSVLoader:
    def __init__(self):
        pass

    def save_dataframe(self, output_path: Path, df: pd.DataFrame, filename: str):
        """
        Saves a dataframe as a csv file
        """
        file_path = output_path / filename

        try:
            output_path.mkdir(parents=True, exist_ok=True)
            df.to_csv(file_path, index = False)
            return True
        except Exception as e:
            raise IOError(f"Error saving file '{filename}' in {file_path}. Details: {str(e)}")
            return False # Quitar returns de funciones, que la validacion de los resultados se hagan en pipeline