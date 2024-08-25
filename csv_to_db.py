import psycopg2
import csv
from env_loader import load_env_vars

# Function to convert CTR from percentage (e.g., '0%') to decimal (e.g., 0.00)
def convert_ctr_to_decimal(ctr: str) -> float:
    """
    This function converts a percentage string (e.g., '0%') into a float (e.g., 0.00).
    """
    # Strip the '%' symbol and divide by 100 to convert percentage to decimal
    try:
        return float(ctr.strip('%')) / 100
    except ValueError:
        # If conversion fails, default to 0.00
        return 0.00

# Function to read data from CSV and insert it into PostgreSQL
def import_csv_to_db(csv_file_path: str) -> None:
    # Load environment variables (PostgreSQL credentials)
    env_vars = load_env_vars()

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=env_vars['DB_NAME'], 
        user=env_vars['DB_USER'], 
        password=env_vars['DB_PASSWORD'], 
        host=env_vars['DB_HOST'], 
        port=env_vars['DB_PORT']
    )
    cursor = conn.cursor()

    # Open the CSV file for reading
    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row

        # Loop through each row in the CSV file
        for row in reader:
            url = row[0]  # The first column is the URL
            visits = int(row[2])  # Third column is Visits
            impressions = int(row[3])  # Fourth column is Impressions
            ctr = convert_ctr_to_decimal(row[4])  # Convert percentage CTR to decimal
            ranking = float(row[5])  # Sixth column is Ranking (float)

            # SQL query to insert data into the table
            insert_query = '''
            INSERT INTO identify_duplicates (URL, Visits, Impressions, CTR, Ranking)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (URL) DO NOTHING;
            '''
            
            # Execute the insert query with the row data
            cursor.execute(insert_query, (url, visits, impressions, ctr, ranking))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

# This allows us to run the script independently
if __name__ == "__main__":
    # Provide the path to your CSV file
    csv_file_path = "url_data.csv"
    import_csv_to_db(csv_file_path)
