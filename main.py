import db_setup
import csv_to_db
import url_processor

def run_project() -> None:
    """
    This function will run the entire project, starting with setting up the database,
    importing CSV data (if necessary), and then processing URLs with ChatGPT.
    """
    # Step 1: Create the table (if it doesn't exist)
    print("Setting up the database...")
    db_setup.create_table()
    
    # Step 2: Import data from CSV into the PostgreSQL table = it will import only the new URLs
    print("Importing data from CSV into the database...")
    csv_file_path = "url_data.csv"  # Make sure CSV is in directory with this script
    csv_to_db.import_csv_to_db(csv_file_path)
    
    # Step 3: Process URLs from the database and analyze with ChatGPT
    print("Processing URLs with ChatGPT and updating the database...")
    url_processor.process_urls()
    
    print("All tasks completed successfully.")

# This allows us to run the entire process when the script is executed
if __name__ == "__main__":
    run_project()
