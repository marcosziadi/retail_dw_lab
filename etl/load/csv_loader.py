import os

class CSVLoader:
    def __init__(self, output_path):
        self.output_path = output_path
        os.makedirs(output_path, exist_ok = True)

    def save_dataframe(self, df, filename):
        """
        Saves a dataframe as a csv file
        """

        file_path = os.path.join(self.output_path, filename)

        try:
            df.to_csv(file_path, index = False)
            print(f"{filename} was saved in {self.output_path}")
            return True
        except Exception as e:
            print(f"Error saving {filename}: {str(e)}")
            return False