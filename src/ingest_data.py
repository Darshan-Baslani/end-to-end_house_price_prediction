import os
import zipfile
from abc import ABC, abstractmethod

import pandas as pd


# abstract class for data ingestor
class DataIngestor(ABC):
    @abstractmethod
    def ingest(self, file_path:str) -> pd.DataFrame:
        """
        Abstract method to ingest data from a given file
        """
        pass
    
    
class ZipDataIngestor(DataIngestor):
    def ingest(self, file_path:str) -> pd.DataFrame:
        """ 
        Extracts zip file and transfer it into pandas dataframe
        """
        # ensure the file is zip file
        if not file_path.endswith(".zip"):
            raise ValueError("The provided file is not zip file")
        
        # Extract the zip file
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall("extracted_data")
            
        # find the extracted csv file
        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if len(csv_files) == 0:
            raise FileNotFoundError("No CSV file found in extracted data.")
        elif len(csv_files) > 1:
            raise ValueError("Multiple CSV file found.")
        
        # Read csv into a dataframe
        csv_file_path = os.path.join("extracted_data", csv_files)
        df = pd.read_csv(csv_file_path)
        
        return df
        
        
class DataIngestorFactory:
    @staticmethod
    def get_data_ingestor(file_extension:str) -> DataIngestor:
        """ 
        Reuturns teh appropriate DataIngestor based on file extension
        """
        if file_extension == '.zip':
            return ZipDataIngestor()
        else:
            return ValueError(f"No ingestor available for file extension {file_extension}")
        