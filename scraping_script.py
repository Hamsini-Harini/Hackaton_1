import requests
from bs4 import BeautifulSoup
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter

nltk.download('punkt_tab')

# # Load the CSV file
df = pd.read_csv('/Users/manuel/Desktop/Hackaton_1/Hackaton1_short_test - Sheet1.csv')

# Define a function to scrape content from a URL
def scrape_content(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text(separator=' ')
        # print(soup)
        return text
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return ""

# Scrape each URL and extract content
df['Content'] = df['URL'].apply(scrape_content)
# print(df['Content'].head())

# Tokenize the content and remove stopwords
nltk.download('punkt')
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

def extract_keywords(text):
    tokens = word_tokenize(text.lower())
    filtered_words = [word for word in tokens if word.isalnum() and word not in stop_words]
    return filtered_words

df['Keywords'] = df['Content'].apply(extract_keywords)

# Save the scraping results to a new CSV
df[['URL', 'Keywords']].to_csv('/Users/manuel/Desktop/Hackaton_1/Scraping_URL.csv', index=False)

# Count word frequency to identify repeated words
all_keywords = [word for keywords in df['Keywords'] for word in keywords]
# word_count = Counter(all_keywords)

# Function to classify tags
def classify_tags(keywords):
    word_count = Counter(keywords)
    tags = []
    for word in keywords:
        if word_count[word] > 1:  # Adjust this threshold as needed
            tags.append(word)
    return list(set(tags))

# df['Tags'] = df['Keywords'].apply(lambda x: classify_tags(x, classify_tags))
df['Tags'] = df['Keywords'].apply(classify_tags)

# Output the new CSV with tags
df.to_csv('/Users/manuel/Desktop/Hackaton_1/Hackaton1_with_tags_2.csv', index=False)

