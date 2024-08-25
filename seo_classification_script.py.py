import os
import openai
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# Set OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')

def scrape_website_content(url):
    """Scrape the main text content from a website."""
    try:
        response = requests.get(url, timeout=10)  # Adding timeout for robustness
        response.raise_for_status()  # Raise an error for bad responses
        soup = BeautifulSoup(response.text, 'html.parser')
        paragraphs = soup.find_all('p')
        text_content = ' '.join([para.get_text() for para in paragraphs])
        return text_content[:2000]  # Limit to 2000 characters for brevity
    except requests.exceptions.RequestException as e:
        print(f"Failed to scrape {url}: {e}")
        return None

def classify_content(content, retries=3):
    """Use ChatGPT API to classify and tag the content."""
    if content is None or content.strip() == '':
        print("No content available to classify.")
        return None, None
    
    prompt = f"Analyze the following text and provide SEO-related tags and a classification: \n\n{content}\n\nTags: \nClassification:"
    
    delay = 10  # Start with a 10-second delay
    for attempt in range(retries):
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.5
            )
            
            # Extracting tags and classification from the response
            response_text = response['choices'][0]['message']['content'].strip()
            if 'Tags:' in response_text and 'Classification:' in response_text:
                tags, classification = response_text.split('\n')[:2]
                return tags.replace('Tags:', '').strip(), classification.replace('Classification:', '').strip()
            else:
                print(f"Unexpected response format: {response_text}")
                return None, None
        
        except openai.error.RateLimitError as e:
            print(f"Rate limit reached on attempt {attempt + 1}: {e}")
            print(f"Waiting for {delay} seconds before retrying...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff: double the delay each time
        except Exception as e:
            print(f"Error with OpenAI API on attempt {attempt + 1}: {e}")
            time.sleep(delay)  # Wait before retrying for other errors

    print("Max retries reached. Skipping...")
    return None, None

def process_csv(input_csv, output_csv):
    """Read the CSV file, process each URL, and save results."""
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    if 'URL' not in df.columns:
        print("CSV file does not contain 'URL' column.")
        return
    
    tags_list = []
    classification_list = []
    
    for index, row in df.iterrows():
        url = row['URL']
        print(f"Processing {url}...")
        content = scrape_website_content(url)
        tags, classification = classify_content(content)
        tags_list.append(tags)
        classification_list.append(classification)
        
        time.sleep(20)  # Adding a longer delay between requests to avoid hitting rate limits
    
    # Add new columns to the DataFrame
    df['Tags'] = tags_list
    df['Classification'] = classification_list
    
    # Save to a new CSV file
    df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

# Define file paths
input_csv = '/Users/manuel/Desktop/Hackaton_1/Hackaton1_short_test - English - Hackaton1_short_test - Sheet1.csv'
output_csv = '/Users/manuel/Desktop/Hackaton_1/Hackaton1_classified_chatgpt4.csv'

# Run the process
process_csv(input_csv, output_csv)
