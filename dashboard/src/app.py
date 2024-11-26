import os
import io
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Read Supabase credentials from environment
SUPABASE_URL = os.getenv("SUPABASE_ENDPOINT")
SUPABASE_KEY = os.getenv("SUPABASE_ACCESS_KEY_ID")
SUPABASE_BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")

def read_csv_files_from_supabase(bucket_name=None, base_path="raw"):
    """
    Reads CSV files from a Supabase storage bucket.
    
    Parameters:
    -----------
    bucket_name : str, optional
        Name of the Supabase storage bucket. If None, uses environment variable.
    base_path : str, optional
        Base path within the bucket to search for CSV files.
    
    Returns:
    --------
    dict
        A dictionary where keys are subfolder names and values are DataFrames 
        resulting from the outer join of all CSV files in that subfolder.
    """
    # Use environment variable if bucket_name is not provided
    bucket_name = bucket_name or SUPABASE_BUCKET_NAME
    
    # Create Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Subfolders to process
    subfolders = ["auchan", "pingo_doce", "continente"]
    dataframes = {}
    
    for folder in subfolders:
        # Construct the full path in the bucket
        folder_path = f"{base_path}/{folder}"
        
        # List all files in the folder
        try:
            files = supabase.storage.from_(bucket_name).list(folder_path)
        except Exception as e:
            print(f"Error listing files in {folder_path}: {e}")
            continue
        
        merged_df = pd.DataFrame()
        
        # Process each CSV file
        for file_info in files:
            # Check if it's a CSV file
            if file_info['name'].endswith('.csv'):
                full_file_path = f"{folder_path}/{file_info['name']}"
                
                try:
                    # Download file content
                    response = supabase.storage.from_(bucket_name).download(full_file_path)
                    
                    # Read CSV from bytes
                    df = pd.read_csv(io.BytesIO(response))
                    
                    # Merge with existing DataFrame using outer join
                    if merged_df.empty:
                        merged_df = df
                    else:
                        merged_df = pd.merge(merged_df, df, how="outer")
                    
                    print(f"Successfully read {full_file_path}")
                
                except Exception as e:
                    print(f"Error reading {full_file_path}: {e}")
        
        # Store the merged DataFrame for the current folder
        dataframes[folder] = merged_df
    
    return dataframes

def main():
    # Get separate DataFrames for each subfolder
    try:
        folder_dataframes = read_csv_files_from_supabase()
        
        # Print each DataFrame
        for folder, df in folder_dataframes.items():
            print(f"\nDataFrame for {folder}:")
            print(df)
            print(f"Shape: {df.shape}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()