import os
import pandas as pd
from typing import Optional


class ParquetReader:
    """
    A class to read and process Parquet files from the file system.
    
    This class provides methods to read Parquet files from a specified path,
    with support for reading from subdirectories (partitioned data).
    
    Attributes:
        None
    """
    
    def __init__(self):
        """Initialize the ParquetReader."""
        pass
    
    def process(self, path: str, include_subfolders: bool = False) -> pd.DataFrame:
        """
        Read and process Parquet files from the specified path.
        
        Args:
            path (str): The path to the Parquet file or directory containing Parquet files.
            include_subfolders (bool): If True, reads all Parquet files from subdirectories
                                      (useful for partitioned data). Default is False.
        
        Returns:
            pd.DataFrame: A DataFrame containing the data from the Parquet file(s).
        
        Raises:
            FileNotFoundError: If the specified path does not exist.
            ValueError: If no Parquet files are found in the specified path.
            Exception: If reading the Parquet file(s) fails.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Path does not exist: {path}")
        
        try:
            if include_subfolders:
                # Read all parquet files from subdirectories (partitioned data)
                df = pd.read_parquet(path, engine='pyarrow')
            else:
                # Read a single parquet file or directory
                if os.path.isfile(path):
                    df = pd.read_parquet(path, engine='pyarrow')
                elif os.path.isdir(path):
                    # Read all parquet files in the directory
                    df = pd.read_parquet(path, engine='pyarrow')
                else:
                    raise ValueError(f"Invalid path: {path}")
            
            if df.empty:
                raise ValueError(f"No data found in Parquet files at path: {path}")
            
            return df
        
        except Exception as e:
            raise Exception(f"Failed to read Parquet file(s) from {path}: {e}")
    
    def list_parquet_files(self, path: str) -> list:
        """
        List all Parquet files in the specified directory and subdirectories.
        
        Args:
            path (str): The path to the directory to search for Parquet files.
        
        Returns:
            list: A list of paths to Parquet files found.
        
        Raises:
            FileNotFoundError: If the specified path does not exist.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Path does not exist: {path}")
        
        parquet_files = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.parquet'):
                    parquet_files.append(os.path.join(root, file))
        
        return parquet_files
