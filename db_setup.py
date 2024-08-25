import os
import psycopg2
from psycopg2 import sql
from env_loader import load_env_vars

# This function will create the "identify_duplicates" table if it doesn't exist.
def create_table() -> None:
    # Load environment variables (PostgreSQL credentials)
    env_vars = load_env_vars()

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=env_vars['DB_NAME'], 
        user=env_vars['DB_USER'], 
        password=env_vars['DB_PASSWORD'], 
        host=env_vars['DB_HOST'], 
        port=env_vars['DB_PORT']
    )
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    # SQL query to create the table
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS identify_duplicates (
        URL VARCHAR PRIMARY KEY,
        Topics TEXT,
        Visits INT,
        Impressions INT,
        CTR FLOAT,
        Ranking FLOAT
    );
    '''

    # Execute the query
    cursor.execute(create_table_query)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection to free resources
    cursor.close()
    conn.close()

    print("Table created successfully!")

# This allows us to run the script independently or as part of a larger program
if __name__ == "__main__":
    create_table()