from auchan.main import preprocess_and_insert_data_auchan
from supabase_interface import ProductDatabaseInterface

# Initialize the database interface
# No need to specify db_name for PostgreSQL connection
db_interface = ProductDatabaseInterface()

# Path to the Parquet file
parquet_file = 'data/raw/auchan_combined.parquet'

# Step 4: Preprocess the data and insert into the database
preprocess_and_insert_data_auchan(parquet_file, db_interface)