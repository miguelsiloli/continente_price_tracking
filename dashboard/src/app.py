import boto3
import pandas as pd
import io

def read_and_merge_csv_from_subfolders(
    access_key_id,
    secret_access_key,
    endpoint_url,
    region_name,
    bucket_name,
    subfolders
):
    """
    Reads CSV files from specified subfolders in a Supabase S3-compatible storage.
    Performs an outer join of all CSV files within each subfolder.

    Parameters:
    -----------
    access_key_id : str
        AWS/Supabase S3 Access Key ID.
    secret_access_key : str
        AWS/Supabase S3 Secret Access Key.
    endpoint_url : str
        S3 endpoint URL.
    region_name : str
        AWS/Supabase S3 region.
    bucket_name : str
        Name of the S3 bucket.
    subfolders : list
        List of subfolders to process.

    Returns:
    --------
    dict
        Dictionary where keys are subfolder names and values are merged DataFrames.
    """
    # Create S3 client with custom endpoint
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=boto3.session.Config(signature_version='s3v4')
    )
    
    # Dictionary to store merged DataFrames for each subfolder
    subfolder_dataframes = {}

    # Iterate through each subfolder
    for subfolder in subfolders:
        merged_df = pd.DataFrame()  # Initialize an empty DataFrame for the subfolder
        print(f"Processing subfolder: {subfolder}")

        try:
            # List objects in the subfolder
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=f"{subfolder}/"):
                if 'Contents' not in page:
                    continue

                # Process CSV files in the subfolder
                for obj in page['Contents']:
                    file_key = obj['Key']
                    print(f"  Found file: {file_key}")

                    if file_key.lower().endswith('.csv'):
                        try:
                            # Read the CSV file
                            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                            file_content = response['Body'].read()
                            df = pd.read_csv(io.BytesIO(file_content))

                            # Perform outer join with existing DataFrame
                            if merged_df.empty:
                                merged_df = df
                            else:
                                merged_df = pd.merge(merged_df, df, how='outer')

                        except Exception as e:
                            print(f"  Error processing file {file_key}: {e}")
            
            # Store the merged DataFrame for this subfolder
            subfolder_dataframes[subfolder] = merged_df
            print(f"  Completed merging for subfolder: {subfolder}")
            print(f"  Resulting DataFrame shape: {merged_df.shape}")

        except Exception as e:
            print(f"Error processing subfolder {subfolder}: {e}")

    return subfolder_dataframes


def main():
    # Supabase S3-compatible storage credentials
    SUPABASE_ACCESS_KEY_ID = '73695a84df3764da2ddb50d99ea4831b'
    SUPABASE_SECRET_ACCESS_KEY = '2ae1e36a7600e4ff09cc340f8d25c1b4c1dee583bab76da9cc2afdaddd3eec01'
    SUPABASE_ENDPOINT = 'https://ahluezrirjxhplqwvspy.supabase.co/storage/v1/s3'
    SUPABASE_REGION = 'us-east-1'
    SUPABASE_BUCKET_NAME = 'retail'
    
    # Subfolders to process
    SUBFOLDERS = ["raw/auchan", "raw/pingo_doce", "raw/continente"]
    
    # Read and merge CSV files
    merged_data = read_and_merge_csv_from_subfolders(
        SUPABASE_ACCESS_KEY_ID,
        SUPABASE_SECRET_ACCESS_KEY,
        SUPABASE_ENDPOINT,
        SUPABASE_REGION,
        SUPABASE_BUCKET_NAME,
        SUBFOLDERS
    )
    
    # Print merged DataFrame details for each subfolder
    for subfolder, dataframe in merged_data.items():
        print(f"Subfolder: {subfolder}")
        print(f"DataFrame shape: {dataframe.shape}")
        print("First few rows:")
        print(dataframe.head())
        print("---")


if __name__ == "__main__":
    main()
