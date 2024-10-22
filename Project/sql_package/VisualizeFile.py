from pyspark.sql import SparkSession
import pandas as pd

class ParquetFileHandler:
    def __init__(self):
        pass

    def read_selected_parquet_files(self, parquet_files):
        """
        Reads selected Parquet files into a single Spark DataFrame.
        
        Parameters:
        parquet_files (list): List of paths to selected Parquet files.
        
        Returns:
        tuple: A tuple containing the combined DataFrame and the number of rows, or None if no files were selected.
        """
        if not parquet_files:
            print("No Parquet files to load.")
            return None, 0

        # Initialize Spark session
        spark = SparkSession.builder.appName("parquet_reader").getOrCreate()

        # Load the selected files into a single DataFrame
        df = spark.read.parquet(*parquet_files)

        # Show the first few rows of the combined DataFrame
        df.show()

        spark.stop()
        return df