import pandas as pd
import os
from glob import glob


# Function to cast DataFrame columns to specific data types
def cast_dataframe(df, casting_dict):
    for column, dtype in casting_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
                print(f"Converted {column} -> {dtype}")
            except Exception as e:
                print(f"Error converting {column}: {e}")

    return df


def fix_files(casting_dict):
    directory = os.getcwd()
    parquet_files = glob(os.path.join(directory, "*.parquet"))
    for file in parquet_files:
        print(f"Processing file: {file}")

        # Load file
        df = pd.read_parquet(file)

        # Cast columns
        df = cast_dataframe(df, casting_dict)

        # Save new file
        df.to_parquet(file, engine="pyarrow", index=False)

        print(f"Saved as: {file}\n")

    print("Processing completed!")
