import requests
from bs4 import BeautifulSoup
from collections import Counter
import re
import csv
from urllib.parse import urljoin, urlparse

def scrape_real_estate_keywords(url):
    try:
        # Send a GET request to the website
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses

        # Parse the website content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract meta keywords
        meta_keywords = []
        for meta in soup.find_all('meta', attrs={'name': 'keywords'}):
            if 'content' in meta.attrs:
                meta_keywords.extend(meta.attrs['content'].split(','))

        # Extract header tags (h1, h2, h3, etc.)
        header_tags = []
        for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            header_tags.extend([header.get_text(strip=True) for header in soup.find_all(tag)])

        # Extract all visible text from the body
        body_text = soup.get_text(separator=' ', strip=True)
        words = re.findall(r'\b[a-zA-Z]{3,}\b', body_text.lower())

        # Count word frequencies
        word_counts = Counter(words)

        # Filter the top words based on frequency
        common_words = word_counts.most_common(50)  # Adjust the number as needed

        # Combine meta keywords, headers, and common words
        all_keywords = meta_keywords + header_tags + [word for word, _ in common_words]

        # Deduplicate and return the keywords sorted by frequency
        return Counter(all_keywords).most_common()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching the URL: {e}")
        return []

def get_all_links(base_url):
    try:
        # Send a GET request to the base URL
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract all anchor tags with href attributes
        links = set()
        for anchor in soup.find_all('a', href=True):
            link = urljoin(base_url, anchor['href'])
            # Filter out external links
            if urlparse(link).netloc == urlparse(base_url).netloc:
                links.add(link)

        return links

    except requests.exceptions.RequestException as e:
        print(f"Error fetching links from {base_url}: {e}")
        return set()

def save_keywords_to_csv(keywords, filename="keywords.csv"):
    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Keyword", "Frequency"])
            writer.writerows(keywords)
        print(f"Keywords saved to {filename}")
    except Exception as e:
        print(f"Error saving keywords to CSV: {e}")

# Example usage
if __name__ == "__main__":
    base_url = input("Enter a real estate website URL: ")
    all_links = get_all_links(base_url)

    print(f"Found {len(all_links)} internal links.")

    all_keywords = Counter()
    for link in all_links:
        print(f"Scraping keywords from: {link}")
        keywords = scrape_real_estate_keywords(link)
        all_keywords.update(dict(keywords))

    # Save the aggregated keywords to a CSV file
    save_keywords_to_csv(all_keywords.most_common())
