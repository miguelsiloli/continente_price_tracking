import psycopg2
from tqdm import tqdm
from psycopg2.extras import execute_batch, execute_values
from auchan.preprocessing import split_price, to_unix_time

class ProductDatabaseInterface:
    """
    A class to interface with the PostgreSQL product database.

    This class provides methods to insert data into the PRODUCT, PRODUCT_PRICING,
    CATEGORY_HIERARCHY, PRODUCT_CATEGORY tables of the database.
    """

    def __init__(self):
        """
        Initialize the ProductDatabaseInterface with PostgreSQL connection.
        Uses environment variables for connection details.
        """
        self.conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.ahluezrirjxhplqwvspy",
            password="9121759591mM!",
            host="aws-0-us-east-1.pooler.supabase.com"
        )
        self.cursor = self.conn.cursor()

    def insert_into_product_table(self, df_product):
        """
        Insert product data into the PRODUCT table and return the product_id_pk.

        Args:
            df_product (pandas.DataFrame): A DataFrame containing product data.
                Expected columns: product_id, product_name, source.
        """
        product_ids_pk = []

        for _, row in tqdm(df_product.iterrows(), total=len(df_product), desc="Inserting into PRODUCT"):
            # Use INSERT ON CONFLICT to handle duplicates
            insert_query = '''
                INSERT INTO PRODUCT (product_id, product_name, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id, source) DO NOTHING
                RETURNING product_id_pk
            '''
            self.cursor.execute(insert_query, (
                row["product_id"], 
                row["product_name"], 
                row["source"]
            ))
            
            # Fetch the product_id_pk
            select_query = '''
                SELECT product_id_pk FROM PRODUCT 
                WHERE product_id = %s AND source = %s
            '''
            self.cursor.execute(select_query, (row["product_id"], row["source"]))
            product_id_pk = self.cursor.fetchone()[0]
            product_ids_pk.append(product_id_pk)

        self.conn.commit()
        return product_ids_pk

    def insert_into_product_pricing_table(self, df_pricing, product_ids_pk):
        """
        Insert pricing data into the PRODUCT_PRICING table with split price.
        Args:
            df_pricing (pandas.DataFrame): Pricing data DataFrame
            product_ids_pk (list): Product IDs corresponding to pricing data
        """
        insert_query = '''
            INSERT INTO PRODUCT_PRICING 
            (product_id_pk, price_integer, price_decimal, price_currency, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id_pk, timestamp) DO NOTHING
        '''

        for i, (_, row) in enumerate(tqdm(df_pricing.iterrows(), total=len(df_pricing), desc="Inserting into PRODUCT_PRICING")):
            price_integer, price_decimal = split_price(row[0])
            unix_timestamp = to_unix_time(row[1])
            
            self.cursor.execute(insert_query, (
                product_ids_pk[i], 
                price_integer, 
                price_decimal, 
                row[2], 
                unix_timestamp
            ))
        
        self.conn.commit()

    def insert_into_category_hierarchy_table(self, df_category):
        """
        Insert category hierarchy data into the CATEGORY_HIERARCHY table and return the category_id.

        Args:
            df_category (pandas.DataFrame): A DataFrame containing category data.
                Expected columns: category_level1, category_level2, category_level3.
        """
        category_ids = []

        for _, row in tqdm(df_category.iterrows(), total=len(df_category), desc="Inserting into CATEGORY_HIERARCHY"):
            # Insert or ignore duplicate categories
            insert_query = '''
                INSERT INTO CATEGORY_HIERARCHY 
                (category_level1, category_level2, category_level3)
                VALUES (%s, %s, %s)
                ON CONFLICT (category_level1, category_level2, category_level3) DO NOTHING
                RETURNING category_id
            '''
            self.cursor.execute(insert_query, (
                row["category_level1"], 
                row["category_level2"], 
                row["category_level3"]
            ))
            
            # Fetch the category_id
            select_query = '''
                SELECT category_id FROM CATEGORY_HIERARCHY
                WHERE (category_level1 = %s OR category_level1 IS NULL)
                AND (category_level2 = %s OR category_level2 IS NULL)
                AND (category_level3 = %s OR category_level3 IS NULL)
                LIMIT 1
            '''
            self.cursor.execute(select_query, (
                row["category_level1"], 
                row["category_level2"], 
                row["category_level3"]
            ))
            category_ids.append(self.cursor.fetchone()[0])

        self.conn.commit()  
        return category_ids

    def insert_into_product_category_table(self, df_product_category, category_ids, product_ids_pk):
        """
        Insert product-category mappings into the PRODUCT_CATEGORY table.

        Args:
            df_product_category (pandas.DataFrame): A DataFrame containing product-category mappings.
                Expected columns: product_id, source, product_category, product_category2, product_category3.
            category_ids (list): List of category_id values corresponding to the categories.
            product_ids_pk (list): List of product_id_pk values for the products.
        """
        insert_query = '''
            INSERT INTO PRODUCT_CATEGORY (product_id_pk, category_id)
            VALUES (%s, %s)
            ON CONFLICT (product_id_pk, category_id) DO NOTHING
        '''

        for idx, row in enumerate(tqdm(df_product_category.itertuples(index=False), total=len(df_product_category), desc="Inserting into PRODUCT_CATEGORY")):
            self.cursor.execute(insert_query, (
                product_ids_pk[idx], 
                category_ids[idx]
            ))

        self.conn.commit()


    def bulk_insert_into_product_table(self, df_product, chunk_size=1000):
        """
        Bulk insert products using execute_values in smaller chunks for efficiency and to avoid timeouts.
        Ensures all product_id_pk are retrieved, even for existing records.
        
        Args:
            df_product (pandas.DataFrame): Product data to insert.
            chunk_size (int): Number of rows to process in each chunk.
        """
        
        product_ids_pk = []
        
        # Prepare data for bulk insert
        data_to_insert = df_product[['product_id', 'product_name', 'source']].values.tolist()
        
        # Insert data in chunks with tqdm progress bar
        for i in tqdm(range(0, len(data_to_insert), chunk_size), desc="Inserting Products"):
            chunk = data_to_insert[i:i + chunk_size]
            
            # First, insert the data
            insert_query = '''
                INSERT INTO PRODUCT (product_id, product_name, source)
                VALUES %s
                ON CONFLICT (product_id, source) DO NOTHING
            '''
            execute_values(self.cursor, insert_query, chunk)
            
            # Then, retrieve all product_id_pk for this chunk
            select_query = '''
                SELECT product_id_pk 
                FROM PRODUCT 
                WHERE (product_id, source) IN (
                    SELECT unnest(%s), unnest(%s)
                )
            '''
            
            # Separate product_id and source for the existing records check
            product_ids = [row[0] for row in chunk]
            sources = [row[2] for row in chunk]
            
            # Execute the select query
            self.cursor.execute(select_query, (product_ids, sources))
            
            # Fetch product_id_pk for all records
            returned_ids = self.cursor.fetchall()
            
            # Extend product_ids_pk with the retrieved IDs
            product_ids_pk.extend([id[0] for id in returned_ids])
        
        self.conn.commit()
        return product_ids_pk

    def bulk_insert_into_product_pricing_table(self, df_pricing, product_ids_pk):
        """
        Bulk insert product pricing using execute_values with tqdm.
        """
        # Prepare data for bulk insert
        data_to_insert = []
        for i, (_, row) in tqdm(enumerate(df_pricing.iterrows()), total=len(df_pricing), desc="Preparing Pricing Data"):
            price_integer, price_decimal = split_price(row[0])
            unix_timestamp = to_unix_time(row[1])
        
            data_to_insert.append((
                product_ids_pk[i],
                price_integer,
                price_decimal,
                row[2],
                unix_timestamp
            ))
    
        # Modify the insert query to use %s placeholders
        insert_query = '''
            INSERT INTO PRODUCT_PRICING
            (product_id_pk, price_integer, price_decimal, price_currency, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id_pk, timestamp) DO NOTHING
        '''
    
        # Use execute_batch with the corrected query and placeholders
        execute_batch(self.cursor, insert_query, data_to_insert, page_size=1000)
        self.conn.commit()

    def bulk_insert_into_category_hierarchy_table(self, df_category):
        """
        Bulk insert category hierarchy using execute_values.
        Ensures category_ids are retrieved for both new and existing categories,
        maintaining the same order and handling duplicates as in the input.
        """
    
        # Prepare data for bulk insert
        data_to_insert = df_category[['category_level1', 'category_level2', 'category_level3']].values.tolist()
    
        category_ids = []
        chunk_size = 1000
    
        # Process in chunks with tqdm progress bar
        for i in tqdm(range(0, len(data_to_insert), chunk_size), desc="Inserting Categories"):
            chunk = data_to_insert[i:i + chunk_size]
        
            # First, insert the data
            insert_query = '''
                INSERT INTO CATEGORY_HIERARCHY
                (category_level1, category_level2, category_level3)
                VALUES %s
                ON CONFLICT (category_level1, category_level2, category_level3) DO NOTHING
            '''
            execute_values(self.cursor, insert_query, chunk)
        
            # Then, create a mapping for each unique category in the chunk
            select_query = '''
                SELECT 
                    category_level1, 
                    category_level2, 
                    category_level3, 
                    category_id
                FROM CATEGORY_HIERARCHY
                WHERE (category_level1, category_level2, category_level3) IN (
                    SELECT unnest(%s), unnest(%s), unnest(%s)
                )
            '''
        
            # Separate category levels for the select query
            level1 = [row[0] for row in chunk]
            level2 = [row[1] for row in chunk]
            level3 = [row[2] for row in chunk]
        
            # Execute the select query
            self.cursor.execute(select_query, (level1, level2, level3))
        
            # Create a mapping of category levels to category_ids
            category_mapping = {(row[0], row[1], row[2]): row[3] for row in self.cursor.fetchall()}
        
            # Retrieve category_ids in the original chunk order, including duplicates
            chunk_ids = [
                category_mapping.get((row[0], row[1], row[2])) 
                for row in chunk
            ]
        
            # Extend category_ids with the retrieved IDs
            category_ids.extend(chunk_ids)
    
        self.conn.commit()
        return category_ids

    def bulk_insert_into_product_category_table(self, df_product_category, product_ids_pk, category_ids):
        """
        Bulk insert product-category relationships using execute_batch.
        
        Args:
            df_product_category (pandas.DataFrame): DataFrame containing product-category information
            product_ids_pk (list): List of product primary keys
            category_ids (list): List of category IDs
        """
        
        # Prepare data for bulk insert
        data_to_insert = []
        for idx in tqdm(range(len(df_product_category)), desc="Preparing Product-Category Relationships"):
            data_to_insert.append((
                product_ids_pk[idx], 
                category_ids[idx]
            ))
        
        insert_query = '''
            INSERT INTO PRODUCT_CATEGORY (product_id_pk, category_id)
            VALUES (%s, %s)
            ON CONFLICT (product_id_pk, category_id) DO NOTHING
        '''
        
        # Use execute_batch for efficient insertion
        execute_batch(self.cursor, insert_query, data_to_insert, page_size=100)
        self.conn.commit()

    def __del__(self):
        """
        Close database connections when object is deleted.
        """
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()