import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import csv

# Set a maximum depth for recursion (avoid crawling indefinitely)
MAX_DEPTH = 2


def is_valid_url(url):
    """Check if URL is valid."""
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_links(url):
    """Download the content of the page and return all links found on this page."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    links = set()
    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        if href.startswith("#"):  # Skip same-page anchors
            continue
        # Convert relative URLs to absolute URLs
        href = urljoin(url, href)
        if is_valid_url(href):
            links.add(href)
    return list(links)


def crawl(url, base_domain, visited, csv_writer, depth):
    """Recursively crawl pages up to MAX_DEPTH and extract internal URLs."""
    if depth > MAX_DEPTH:
        return

    if url in visited:
        return

    print(" " * depth * 2 + f"Crawling: {url}")
    visited.add(url)

    # Write the URL to the CSV file
    csv_writer.writerow([url])

    # Get all links on the current page
    links = get_all_links(url)

    # Filter to only include internal links (i.e. those belonging to the base domain)
    internal_links = []
    for link in links:
        parsed_link = urlparse(link)
        if base_domain in parsed_link.netloc:
            internal_links.append(link)

    # Crawl each of the internal links recursively
    for link in internal_links:
        if link not in visited:
            # Optional delay to be respectful
            time.sleep(1)
            crawl(link, base_domain, visited, csv_writer, depth + 1)


def main():
    start_url = "https://archstonekenya.com/"
    parsed = urlparse(start_url)
    base_domain = parsed.netloc

    # Set to store visited URLs
    visited = set()

    # Open the CSV file for writing
    with open("crawled_urls.csv", "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["URL"])

        # Start crawling
        crawl(start_url, base_domain, visited, csv_writer, depth=0)

    print("\nCrawling complete. URLs have been saved to 'crawled_urls.csv'.")


if __name__ == "__main__":
    main()
