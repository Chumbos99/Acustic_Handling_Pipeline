import pandas as pd
import os
import time
import tkinter as tk
from tkinter import filedialog
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_output_directory(input_directory, output_directory_parent):
    """
    Gets the output directory based on the input directory name.
    
    Args:
        input_directory (str): The directory containing the input CSV files.
        output_directory_parent (str): The parent directory where the output folder should be created.
    
    Returns:
        output_directory (str): The full path to the output directory.
    """
    # Get the name of the input folder
    input_folder_name = os.path.basename(os.path.normpath(input_directory))
    # Create the output directory path by joining the parent directory and input folder name
    output_directory = os.path.join(output_directory_parent, input_folder_name)
    return output_directory

def convert_single_csv_to_parquet(csv_file_path, output_directory):
    """
    Converts a single CSV file to Parquet format.
    
    Args:
        csv_file_path (str): The path of the CSV file to convert.
        output_directory (str): The directory where the Parquet file will be saved.
    """
    # Extract the filename from the CSV file path
    filename = os.path.basename(csv_file_path)
    # Define the path for the new Parquet file
    parquet_file_path = os.path.join(output_directory, filename.replace('.csv', '.parquet'))

    # Check if the Parquet file already exists; if so, skip conversion
    if os.path.exists(parquet_file_path):
        print(f"Skipping {filename}, Parquet file already exists.")
        return

    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path, sep='[,;]', header=None, skiprows=14)
    # Drop the first column (index) from the DataFrame
    df.drop(columns=[0], inplace=True)
    # Rename the columns for clarity
    df.columns = ['Time', 'Voltage']

    # Write the DataFrame to a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow', index=False)
    print(f"Converted {filename} to Parquet and saved as {parquet_file_path}")

def convert_csv_to_parquet(input_directory, output_directory):
    """
    Converts all CSV files in the input directory to Parquet format using multi-threading.
    
    Args:
        input_directory (str): The directory containing the input CSV files.
        output_directory (str): The directory where the Parquet files will be saved.
    """
    # Start measuring execution time
    start_time = time.time()
    # Get a list of all CSV files in the input directory
    csv_files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.endswith('.csv')]

    # Use ThreadPoolExecutor for multi-threading to speed up the conversion process
    with ThreadPoolExecutor() as executor:
        # Create a dictionary of futures to track conversion tasks
        futures = {executor.submit(convert_single_csv_to_parquet, csv_file, output_directory): csv_file for csv_file in csv_files}

        # Process the results as they complete
        for future in as_completed(futures):
            try:
                future.result()  # Wait for the future to complete
            except Exception as e:
                # Handle any exceptions that occur during conversion
                print(f"Error processing file {futures[future]}: {e}")

    # End measuring execution time
    end_time = time.time()
    # Calculate total execution time
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")

def choose_directory():
    """
    Opens a dialog to let the user choose directories for input and output.
    
    Returns:
        str: The full path of the output directory.
    """
    # Create a hidden Tkinter root window
    root = tk.Tk()
    root.withdraw()  # Withdraw the main window to only show dialogs

    # Open a dialog to select the input directory containing CSV files
    input_directory = filedialog.askdirectory(title="Select Input Directory")
    # Open a dialog to select the parent directory for output Parquet files
    output_directory_parent = filedialog.askdirectory(title="Select Output Parent Directory")

    # Check if both directories were selected
    if input_directory and output_directory_parent:
        # Determine the output directory based on the input directory name
        output_directory = get_output_directory(input_directory, output_directory_parent)
        # Create the output directory if it does not already exist
        if not os.path.exists(output_directory):
            os.makedirs(output_directory, exist_ok=True)
            print(f"Created folder: {output_directory}")
        else:
            print(f"Using existing folder: {output_directory}")

        # Convert CSV to Parquet
        convert_csv_to_parquet(input_directory, output_directory)

        # Return the output directory
        return output_directory
    else:
        print("No directory selected.")
        return None
