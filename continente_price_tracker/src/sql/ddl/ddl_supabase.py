import psycopg2

def create_product_database():
    # Alternatively, using psycopg2
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres.ahluezrirjxhplqwvspy",
        password="9121759591mM!",
        host="aws-0-us-east-1.pooler.supabase.com"
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        -- Create PRODUCT table
        CREATE TABLE IF NOT EXISTS PRODUCT (
            product_id_pk SERIAL PRIMARY KEY,
            product_id INTEGER,
            product_name VARCHAR(255),
            source VARCHAR(255),
            UNIQUE(product_id, source)
        );

        -- Create PRODUCT_PRICING table
        CREATE TABLE IF NOT EXISTS PRODUCT_PRICING (
            product_id_pk INTEGER,
            price_integer SMALLINT,  -- Integer part of product price
            price_decimal SMALLINT,  -- Discretized decimal part
            price_currency CHAR(3),
            timestamp BIGINT,        -- Unix timestamp
            FOREIGN KEY (product_id_pk) REFERENCES PRODUCT(product_id_pk),
            UNIQUE(product_id_pk, timestamp)
        );

        -- Create CATEGORY_HIERARCHY table
        CREATE TABLE IF NOT EXISTS CATEGORY_HIERARCHY (
            category_id SERIAL PRIMARY KEY,
            category_level1 VARCHAR(255),
            category_level2 VARCHAR(255),
            category_level3 VARCHAR(255),
            UNIQUE (category_level1, category_level2, category_level3)
        );

        -- Create PRODUCT_CATEGORY table
        CREATE TABLE IF NOT EXISTS PRODUCT_CATEGORY (
            product_id_pk INTEGER,
            category_id INTEGER,
            PRIMARY KEY (product_id_pk, category_id),
            FOREIGN KEY (product_id_pk) REFERENCES PRODUCT(product_id_pk),
            FOREIGN KEY (category_id) REFERENCES CATEGORY_HIERARCHY(category_id)
        );
    """)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_product_database()