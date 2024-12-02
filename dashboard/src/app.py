import os
import pandas as pd
import streamlit as st
import boto3
from dotenv import load_dotenv
import io
import plotly.express as px
import plotly.graph_objs as go

# Load environment variables
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
                            st.error(f"Error processing file {obj['Key']}: {e}")
            dataframes.append(merged_df)
        except Exception as e:
            st.error(f"Error processing subfolder {subfolder}: {e}")
            dataframes.append(pd.DataFrame())  # Add an empty DataFrame in case of error for consistency
    return dataframes

def app():
    st.title("Supabase S3 Data Explorer")

    # Sidebar for configuration
    st.sidebar.header("Data Configuration")
    
    # Load credentials
    try:
        credentials = load_credentials()
    except Exception as e:
        st.error(f"Error loading credentials: {e}")
        return

    # Define subfolders
    subfolders = ["raw/auchan", "raw/pingo_doce", "raw/continente"]
    
    # Load data button
    if st.sidebar.button("Load Data"):
        with st.spinner("Loading data from Supabase S3..."):
            try:
                merged_dataframes = read_and_merge_csv_from_supabase_s3(subfolders, credentials)
                st.session_state.dataframes = merged_dataframes
                st.success("Data loaded successfully!")
            except Exception as e:
                st.error(f"Error loading data: {e}")

    # Check if dataframes are loaded
    if 'dataframes' not in st.session_state:
        st.warning("Please load data first.")
        return

    # DataFrame selection
    selected_df_index = st.sidebar.selectbox(
        "Select DataFrame", 
        range(len(st.session_state.dataframes)), 
        format_func=lambda x: subfolders[x]
    )
    selected_df = st.session_state.dataframes[selected_df_index]

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "DataFrame Info", 
        "Statistical Summary", 
        "Numeric Column Analysis", 
        "Data Visualization"
    ])

    with tab1:
        st.subheader("DataFrame Information")
        st.write(f"DataFrame from {subfolders[selected_df_index]}")
        st.write(f"Shape: {selected_df.shape}")
        st.write("Columns:", list(selected_df.columns))

    with tab2:
        st.subheader("Statistical Summary")
        st.dataframe(selected_df.describe())

    with tab3:
        st.subheader("Numeric Column Analysis")
        numeric_cols = selected_df.select_dtypes(include=['float64', 'int64']).columns
        if len(numeric_cols) > 0:
            selected_numeric_col = st.selectbox("Select Numeric Column", numeric_cols)
            
            # Histogram
            fig_hist = px.histogram(selected_df, x=selected_numeric_col, 
                                    title=f'Distribution of {selected_numeric_col}')
            st.plotly_chart(fig_hist)

            # Box plot
            fig_box = px.box(selected_df, y=selected_numeric_col, 
                             title=f'Box Plot of {selected_numeric_col}')
            st.plotly_chart(fig_box)
        else:
            st.info("No numeric columns found.")

    with tab4:
        st.subheader("Data Visualization")
        # Scatter plot for numeric columns
        numeric_cols = selected_df.select_dtypes(include=['float64', 'int64']).columns
        if len(numeric_cols) >= 2:
            x_col = st.selectbox("X-axis", numeric_cols)
            y_col = st.selectbox("Y-axis", numeric_cols, index=1 if len(numeric_cols) > 1 else 0)
            
            fig_scatter = px.scatter(selected_df, x=x_col, y=y_col, 
                                     title=f'Scatter Plot: {x_col} vs {y_col}')
            st.plotly_chart(fig_scatter)

def main():
    app()

if __name__ == "__main__":
    main()