import psycopg2
import openai
from env_loader import load_env_vars

# Set the OpenAI API key
env_vars = load_env_vars()
openai.api_key = env_vars['OPENAI_API_KEY']

# Function to fetch a batch of URLs from the database
def fetch_urls(batch_size: int = 10) -> list:
    env_vars = load_env_vars()

    # Connect to the database
    conn = psycopg2.connect(
        dbname=env_vars['DB_NAME'], 
        user=env_vars['DB_USER'], 
        password=env_vars['DB_PASSWORD'], 
        host=env_vars['DB_HOST'], 
        port=env_vars['DB_PORT']
    )
    
    cursor = conn.cursor()

    # SQL query to fetch URLs where Topics is NULL or empty
    fetch_query = f"SELECT URL FROM identify_duplicates WHERE Topics IS NULL OR Topics = '' LIMIT {batch_size};"
    
    cursor.execute(fetch_query)
    
    urls = cursor.fetchall()  # Fetch all URLs

    cursor.close()
    conn.close()

    return [url[0] for url in urls]  # Return a list of URLs


# Function to send a URL to ChatGPT for analysis using the chat API
def analyze_url_with_chatgpt(url: str) -> str:
    # Load the environment variables (including OpenAI API key)
    env_vars = load_env_vars()
    
    openai.api_key = env_vars['OPENAI_API_KEY']

    # Craft the prompt for ChatGPT
    prompt = f"""
    You are an expert in analyzing the content of Hebrew web pages. Go to the URL '{url}' and extract the most prominent topics. 
    Summarize your findings in 5-10 keywords (in English). Consider the keywords in the URL as well.
    If the provided URL is thin in content, return the following output "* No topics found"
    """

    # Use ChatCompletion instead of Completion
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",  # Use a chat-based model
        messages=[
            {"role": "system", "content": "You are an expert in analyzing web pages."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=150
    )

    # Extract the response text (the keywords)
    full_response = response.choices[0].message.content.strip()

    # Extract the list of keywords from the full response
    keywords = []
    for line in full_response.splitlines():
        # Only process lines that look like they have a number and ". " pattern
        if line and line[0].isdigit() and ". " in line:
            try:
                keyword = line.split(". ", 1)[1]
                keywords.append(keyword)
            except IndexError:
                # In case the line doesn't split properly, skip it
                continue

    # Join the keywords as a comma-separated string
    return ", ".join(keywords)


# Function to update the database with the analyzed topics
def update_topics(url: str, topics: str) -> None:
    env_vars = load_env_vars()

    # Connect to the database
    conn = psycopg2.connect(
        dbname=env_vars['DB_NAME'], 
        user=env_vars['DB_USER'], 
        password=env_vars['DB_PASSWORD'], 
        host=env_vars['DB_HOST'], 
        port=env_vars['DB_PORT']
    )
    
    cursor = conn.cursor()

    # Set the topics to NULL if it's an empty string
    if not topics:
        topics = None

    # SQL query to update the Topics column
    update_query = "UPDATE identify_duplicates SET Topics = %s WHERE URL = %s;"
    
    cursor.execute(update_query, (topics, url))

    conn.commit()

    cursor.close()
    conn.close()

# Main function to process URLs one by one
def process_urls(batch_size: int = 10) -> None:
    urls = fetch_urls(batch_size)  # Fetch a batch of URLs

    for url in urls:
        topics = analyze_url_with_chatgpt(url)  # Analyze the URL with ChatGPT
        update_topics(url, topics)  # Update the database with the topics
