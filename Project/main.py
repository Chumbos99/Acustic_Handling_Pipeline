from sql_package import CSVToParquetConverter, ParquetFileHandler, DirectorySelector, ShortTimeFourierTransform
import os

if __name__ == "__main__":
    # Create instances of the classes
    converter = CSVToParquetConverter()
    handler = ParquetFileHandler()

    # Use DirectorySelector to choose input and output directories
    input_directory = DirectorySelector.choose_input_directory()
    output_directory_parent = DirectorySelector.choose_output_directory()
    
    if input_directory and output_directory_parent:
        output_directory = converter.get_output_directory(input_directory, output_directory_parent)
        
        # Ensure the output directory exists
        os.makedirs(output_directory, exist_ok=True)

        # Convert CSV to Parquet
        converter.convert_csv_to_parquet(input_directory, output_directory)

        # Now choose Parquet files for reading
        parquet_files = DirectorySelector.choose_parquet_files(output_directory)

        # Read the selected Parquet files
        handler.read_selected_parquet_files(parquet_files)

        # STFT on parquet files
        transformation = ShortTimeFourierTransform(parquet_files)  # Pass the selected files to the constructor
        transformation.load_parquet()  # Load the data

        # Compare STFT results
        transformation.compare_stft()
