import os
import tkinter as tk
from tkinter import filedialog




class DirectorySelector:
    @staticmethod
    def choose_input_directory():
        """
        Opens a dialog to let the user choose the input directory for CSV files.
        
        Returns:
        str: The chosen input directory path.
        """
        root = tk.Tk()
        root.withdraw()  # Hide the main Tkinter window
        input_directory = filedialog.askdirectory(title="Select Input Directory")
        
        if input_directory:
            print(f"Selected input directory: {input_directory}")
            return input_directory
        else:
            print("No directory selected.")
            return None

    @staticmethod
    def choose_output_directory():
        """
        Opens a dialog to let the user choose the output directory where the Parquet files are saved.
        
        Returns:
        str: The chosen output directory path.
        """
        root = tk.Tk()
        root.withdraw()  # Hide the main Tkinter window
        output_directory = filedialog.askdirectory(title="Select Output Parent Directory")
        
        if output_directory:
            print(f"Selected output directory: {output_directory}")
            return output_directory
        else:
            print("No directory selected.")
            return None

    @staticmethod
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