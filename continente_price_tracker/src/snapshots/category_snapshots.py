import os
import pandas as pd

def compile_csv_data_auchan(root_dir, output_path):
    """
    Reads all CSV files from subdirectories of `root_dir` and compiles them into a single DataFrame.
    Uses outer join to handle varying columns across files.

    Parameters:
        root_dir (str): The root directory containing subfolders with CSV files.
        output_path (str): The path where the compiled CSV file should be saved.

    Returns:
        None
    """
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    dataframes = []

    # Walk through all subdirectories and read CSV files
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(subdir, file)
                try:
                    df = pd.read_csv(file_path)
                    print(f"Loaded {file} with shape {df.shape}")
                    dataframes.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    # Merge all DataFrames using outer join
    if dataframes:
        merged_df = pd.concat(dataframes, axis=0, join='outer', ignore_index=True)
        print(f"Merged DataFrame shape: {merged_df.shape}")
    else:
        print("No CSV files found.")
        return

    # Group by different product categories and get descriptive statistics for prices
    df1 = merged_df.groupby(["product_category", "timestamp"])["product_price"].describe().reset_index()
    df2 = merged_df.groupby(["product_category2", "timestamp"])["product_price"].describe().reset_index()
    df2 = df2.rename(columns={"product_category2": "product_category"})
    df3 = merged_df.groupby(["product_category3", "timestamp"])["product_price"].describe().reset_index()
    df3 = df3.rename(columns={"product_category3": "product_category"})

    # Concatenate the grouped DataFrames
    final_df = pd.concat([df1, df2, df3], axis=0, join='outer', ignore_index=True)

    # Save the compiled DataFrame to a CSV file
    final_df.to_csv(output_path, index=False)
    print(f"Data successfully compiled and saved to {output_path}")

def compile_csv_data_continente(root_dir, output_path):
    """
    Reads all CSV files from subdirectories of `root_dir` and compiles them into a single DataFrame.
    Uses outer join to handle varying columns across files.

    Parameters:
        root_dir (str): The root directory containing subfolders with CSV files.
        output_path (str): The path where the compiled CSV file should be saved.

    Returns:
        None
    """
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    dataframes = []

    # Walk through all subdirectories and read CSV files
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(subdir, file)
                try:
                    df = pd.read_csv(file_path)
                    print(f"Loaded {file} with shape {df.shape}")
                    dataframes.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    # Merge all DataFrames using outer join
    if dataframes:
        merged_df = pd.concat(dataframes, axis=0, join='outer', ignore_index=True)
        print(f"Merged DataFrame shape: {merged_df.shape}")
    else:
        print("No CSV files found.")
        return

    # Group by different product categories and get descriptive statistics for prices
    df1 = merged_df.groupby(["Category", "tracking_date"])["Price"].describe().reset_index()
    # df2 = merged_df.groupby(["Category", "tracking_date"])["Price per unit"].describe().reset_index()

    # Save the compiled DataFrame to a CSV file
    df1.to_csv(output_path, index=False)
    print(f"Data successfully compiled and saved to {output_path}")

if __name__ == "__main__":
    root_dir = "data/raw/continente"
    output_path = "data/snapshots/compiled_continente_data.csv"
    compile_csv_data_continente(root_dir, output_path)


    root_dir = "data/raw/auchan"
    output_path = "data/snapshots/compiled_auchan_data.csv"
    compile_csv_data_auchan(root_dir, output_path)
