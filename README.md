# ProjectXY

1. Project Overview
CSV to Parquet Converter is a Python-based tool designed to convert CSV files into Parquet format efficiently. Additionally, the project includes a graphical user interface (GUI) for directory selection and file management, providing a user-friendly experience.

2. Features
CSV to Parquet Conversion: Converts CSV files into Parquet format.
Multi-threading Support: Speeds up conversion by processing multiple files in parallel.
GUI: Provides an intuitive interface for selecting directories and files.
Customizable Output Directory: Automatically creates output directories based on user input.
Error Handling: Skips files that have already been converted, ensuring no duplicate conversions.

3. Requirements
Before running the project, make sure you have the following installed:
Python 3.12
Pandas: For CSV handling and Parquet conversion (pip install pandas)
PyArrow: For writing Parquet files (pip install pyarrow)
Tkinter: Built into Python for GUI handling
ThreadPoolExecutor: Part of Python's concurrent.futures for multi-threading
Spark: For reading Parquet files if using the second script (pip install pyspark)
