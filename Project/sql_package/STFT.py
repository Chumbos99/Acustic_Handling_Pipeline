import pandas as pd
import numpy as np
import tensorflow as tf
import torch
import torch.fft as fft
import matplotlib.pyplot as plt

class ShortTimeFourierTransform:
    def __init__(self, file_paths=None):
        self.file_paths = file_paths if file_paths is not None else []  # Accepts a list of file paths
        self.dfs = []  # To store DataFrames of all files

    def load_parquet(self, file_paths=None):
        """
        Loads Parquet files into DataFrames.

        Parameters:
        file_paths (list): List of paths to the Parquet files to load.
        """
        if file_paths is not None:
            self.file_paths = file_paths  # Update file paths if provided

        if not self.file_paths:
            raise ValueError("No file paths provided. Please provide valid file paths.")

        for file_path in self.file_paths:
            # Read the Parquet file into a DataFrame
            df = pd.read_parquet(file_path, engine="pyarrow")
            self.dfs.append(df)

            print(f"\nOriginal DataFrame from {file_path}:\n", df.head())

    def compare_stft(self):
        if not self.dfs:
            print("No DataFrames loaded. Please load Parquet files first.")
            return
        
        for df in self.dfs:
            # Perform TensorFlow STFT
            tf_stft_magnitude = self.stft_with_tensorflow(df)

            # Perform PyTorch STFT
            pt_stft_magnitude = self.stft_with_pytorch(df)

            # Normalize PyTorch STFT result to match TensorFlow's result scale (if needed)
            scale_factor = np.max(tf_stft_magnitude.numpy()) / np.max(pt_stft_magnitude.numpy())
            pt_stft_magnitude_scaled = pt_stft_magnitude * scale_factor

            # Create a figure and axis for both STFTs
            fig, axs = plt.subplots(2, 1, figsize=(10, 8))

            # Plot TensorFlow STFT with rotation
            self.plot_stft(tf_stft_magnitude.numpy().T, "TensorFlow STFT", axs[0], rotate=True)

            # Plot PyTorch STFT
            self.plot_stft(pt_stft_magnitude_scaled.numpy().T, "PyTorch STFT", axs[1])

            # Show both plots in one window
            plt.tight_layout()
            plt.show()

    def stft_with_tensorflow(self, df):
        voltage = df['Voltage'].values
        
        voltage_tensor = tf.convert_to_tensor(voltage, dtype=tf.float32)
        stft_result = tf.signal.stft(voltage_tensor, frame_length=256, frame_step=128)
        magnitude = tf.abs(stft_result)

        # Print out the spectrogram values
        print("\nTensorFlow STFT Magnitude Spectrogram:\n", magnitude.numpy())

        return magnitude

    def stft_with_pytorch(self, df):
        voltage = df['Voltage'].values
        
        voltage_tensor = torch.tensor(voltage, dtype=torch.float32)
        stft_result = torch.stft(voltage_tensor, n_fft=256, hop_length=128, return_complex=True)
        magnitude = torch.abs(stft_result)

        # Print out the spectrogram values
        print("\nPyTorch STFT Magnitude Spectrogram:\n", magnitude.numpy())

        return magnitude

    def plot_stft(self, magnitude, title, ax, rotate=False):
        """
        Plots the STFT magnitude on the given axis.

        Args:
            magnitude (ndarray): STFT magnitude to plot.
            title (str): Title for the plot.
            ax (matplotlib.axes.Axes): The axes to plot on.
            rotate (bool): Whether to rotate the plot.
        """
        if rotate:
            magnitude = np.rot90(magnitude)  # Rotate the matrix by 90 degrees

        im = ax.imshow(magnitude, aspect='auto', origin='lower', cmap='viridis')
        ax.set_title(title)
        ax.set_ylabel("Frequency bins")
        ax.set_xlabel("Time frames")
        plt.colorbar(im, ax=ax)