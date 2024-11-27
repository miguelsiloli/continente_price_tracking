import os
import pandas as pd
import io
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def load_credentials():
    """
    Load Supabase credentials from .env file.
    """
    return {
        "access_key_id": os.getenv("SUPABASE_ACCESS_KEY_ID"),
        "secret_access_key": os.getenv("SUPABASE_SECRET_ACCESS_KEY"),
        "endpoint_url": os.getenv("SUPABASE_ENDPOINT"),
        "region_name": os.getenv("SUPABASE_REGION"),
        "bucket_name": os.getenv("SUPABASE_BUCKET_NAME"),
    }

def read_and_merge_csv_from_supabase_s3(subfolders, credentials):
    """
    Read CSV files from specified subfolders in a Supabase S3-compatible storage.
    Outer join all CSV files in each subfolder and return a DataFrame per subfolder.

    Parameters:
    -----------
    subfolders : list
        List of subfolders to search for CSV files.
    credentials : dict
        Dictionary containing Supabase S3 credentials.

    Returns:
    --------
    list
        A list of DataFrames, one per subfolder.
    """
    # Create S3 client with custom endpoint
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=credentials["access_key_id"],
        aws_secret_access_key=credentials["secret_access_key"],
        endpoint_url=credentials["endpoint_url"],
        region_name=credentials["region_name"],
        config=boto3.session.Config(signature_version="s3v4"),
    )
    
    dataframes = []
    
    for subfolder in subfolders:
        merged_df = pd.DataFrame()
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=credentials["bucket_name"], Prefix=f"{subfolder}/"):
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    if obj["Key"].lower().endswith(".csv"):
                        try:
                            # Fetch the CSV file
                            response = s3_client.get_object(Bucket=credentials["bucket_name"], Key=obj["Key"])
                            csv_content = response["Body"].read()
                            df = pd.read_csv(io.BytesIO(csv_content))

                            # Outer join with the merged DataFrame
                            if merged_df.empty:
                                merged_df = df
                            else:
                                merged_df = pd.merge(merged_df, df, how="outer")
                        except Exception as e:
                            print(f"Error processing file {obj['Key']}: {e}")
            dataframes.append(merged_df)
        except Exception as e:
            print(f"Error processing subfolder {subfolder}: {e}")
            dataframes.append(pd.DataFrame())  # Add an empty DataFrame in case of error for consistency

    return dataframes


def main():
    # Load credentials from .env
    credentials = load_credentials()
    
    # Define subfolders
    subfolders = ["raw/auchan", "raw/pingo_doce", "raw/continente"]
    
    # Read and merge data
    merged_dataframes = read_and_merge_csv_from_supabase_s3(subfolders, credentials)
    
    # Print summary of each DataFrame
    for subfolder, df in zip(subfolders, merged_dataframes):
        print(f"Subfolder: {subfolder}")
        print(f"DataFrame shape: {df.shape}")
        print(f"First few rows:\n{df.head()}")
        print("---")


if __name__ == "__main__":
    main()
