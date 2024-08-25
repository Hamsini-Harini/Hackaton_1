import os
import openai
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Set OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')

def scrape_website_content(url):
    """Scrape the main text content from a website."""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        paragraphs = soup.find_all('p')
        text_content = ' '.join([para.get_text() for para in paragraphs])
        return text_content[:2000]  # Limit to 2000 characters for brevity
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return None

def classify_content(content):
    """Use ChatGPT API to classify and tag the content."""
    if content is None:
        return None, None
    
    prompt = f"Analyze the following text and provide SEO-related tags and a classification: \n\n{content}\n\nTags: \nClassification:"
    
    try:
        response = openai.Completion.create(
            engine="text-davinci-003",  # You can use a different model if needed
            prompt=prompt,
            max_tokens=100,
            temperature=0.5
        )
        
        # Extracting tags and classification from the response
        response_text = response.choices[0].text.strip()
        tags, classification = response_text.split('\n')[:2]
        return tags.replace('Tags:', '').strip(), classification.replace('Classification:', '').strip()
    
    except Exception as e:
        print(f"Error with OpenAI API: {e}")
        return None, None

def process_csv(input_csv, output_csv):
    """Read the CSV file, process each URL, and save results."""
    df = pd.read_csv(input_csv)
    
    tags_list = []
    classification_list = []
    
    for index, row in df.iterrows():
        url = row['URL']  # Assuming the CSV has a column named 'URL'
        print(f"Processing {url}...")
        content = scrape_website_content(url)
        tags, classification = classify_content(content)
        tags_list.append(tags)
        classification_list.append(classification)
    
    # Add new columns to the DataFrame
    df['Tags'] = tags_list
    df['Classification'] = classification_list
    
    # Save to a new CSV file
    df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

# Run the process
input_csv = '/mnt/data/Hackaton1_short_test - Sheet1.csv'
output_csv = '/mnt/data/Hackaton1_classified.csv'
process_csv(input_csv, output_csv)
