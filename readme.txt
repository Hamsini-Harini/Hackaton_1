# Hackaton_1 Project
This is an SEO script meant to identify duplicates/dead-weight pages of a target website.
The idea is to find high number URLs (usualy blog posts) addressing the same topic - causing canibalization. 

Here are the steps to take:
1. Export GSC data (in my case the file called "url_data.csv")
2. Set up the PostgreSQL database with corresponding fields by running the db_setup.py 
3. Python will then import the data from CSV into the PostgreSQL database using csv_to_db.py file
4. Now once your data is in PostgreSQL, python will fetch URLs, send them to OpenAI via API and update the database with output. This is perofmed by url_processor.py
5. main.py is the engine starting the whole process. 
6. Once we've enriched our database with ChatGPT data, it's time for SQL querries to filter through data



## Files Overview
Here’s a description of files in the project:

- **db_setup.py**: 
  - Sets up the `identify_duplicates` table in the PostgreSQL database.
  - Table columns: `URL`, `Topics`, `Visits`, `Impressions`, `CTR`, `Ranking`.

- **url_processor.py**: 
  - Contains functions to fetch a batch of URLs from the PostgreSQL database, send them to OpenAI for analysis, extract prominent topics, and update the database with the results.

- **csv_to_db.py**: 
  - Script for importing data from a CSV file (`url_data.csv`) into the PostgreSQL database.

- **main.py**: 
  - Main entry point to run the project. It handles the full process of fetching URLs, analyzing them using OpenAI, and updating the database.

- **env_loader.py**: 
  - Loads environment variables from the `.env` file for use in the project (e.g., OpenAI API key, PostgreSQL credentials).

- **SQL queries to the base.sql**:
  - Contains SQL queries for various database operations, such as selecting data and exporting or importing the database.

- **url_data.csv**: 
  - CSV file with URL data to be processed.

- **.env**: 
  - Stores environment variables (e.g., database credentials, OpenAI API key). This file should **not** be shared publicly.

## Installation and Setup

### 1. Install Dependencies
Ensure you have a virtual environment set up. Install the necessary Python libraries using:

```bash
pip install -r requirements.txt
