from sql_package import read_selected_parquet_files, choose_parquet_files, choose_directory

if __name__ == "__main__":
    
    # Call the function to convert CSV files to Parquet
    output_directory_path = choose_directory() 

    if output_directory_path:
        # Let the user select specific Parquet files from the chosen directory
        selected_files = choose_parquet_files(output_directory_path)
        
        # Read the selected Parquet files into a Spark DataFrame
        if selected_files:
            df = read_selected_parquet_files(selected_files)