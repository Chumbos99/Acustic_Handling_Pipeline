from pyspark.sql import SparkSession
import os
import tkinter as tk
from tkinter import filedialog

def choose_output_directory():
    """
    Opens a dialog to let the user choose the output directory where the Parquet files are saved.
    
    Returns:
    str: The chosen output directory path.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main Tkinter window
    output_directory = filedialog.askdirectory(title="Select Output Directory")
    
    if output_directory:
        print(f"Selected output directory: {output_directory}")
        return output_directory
    else:
        print("No directory selected.")
        return None

def choose_parquet_files(output_directory):
    """
    Opens a dialog to let the user select Parquet files from the output directory.
    
    Parameters:
    output_directory (str): The directory containing the Parquet files.
    
    Returns:
    list: A list of paths to the selected Parquet files.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main Tkinter window

    # Prompt the user to select Parquet files
    parquet_files = filedialog.askopenfilenames(
        title="Select Parquet Files",
        initialdir=output_directory,
        filetypes=[("Parquet files", "*.parquet")]
    )

    # Check if any files were selected
    if parquet_files:
        print(f"Selected Parquet files: {parquet_files}")
        return parquet_files
    else:
        print("No files selected.")
        return []

def read_selected_parquet_files(parquet_files):
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