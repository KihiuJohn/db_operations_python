import xml.etree.ElementTree as ET
from datetime import datetime
import csv

# Function to load existing sitemap URLs and images
def load_sitemap(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        if not root.tag.endswith('urlset'):
            print("Error: Incorrect namespace in sitemap. Ensure proper declaration.")
            return None, None, set()
        
        urls = {
            url.find('ns:loc', namespace).text for url in root.findall('ns:url', namespace)
        }
        return root, namespace, urls
    except Exception as e:
        print(f"Error loading sitemap: {e}")
        return None, None, set()

# Function to load crawled URLs and images
def load_crawled_urls(file_path):
    urls = set()
    try:
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip the header
            for row in reader:
                if row:
                    urls.add(row[0])
        return urls
    except Exception as e:
        print(f"Error loading crawled URLs: {e}")
        return set()

# Function to add new URLs and images to sitemap
def update_sitemap(sitemap_root, namespace, existing_urls, new_urls, output_file):
    for new_url in new_urls:
        if new_url not in existing_urls:
            url_element = ET.SubElement(sitemap_root, 'url')

            loc_element = ET.SubElement(url_element, 'loc')
            loc_element.text = new_url

            lastmod_element = ET.SubElement(url_element, 'lastmod')
            lastmod_element.text = datetime.now().strftime('%Y-%m-%d')

            priority_element = ET.SubElement(url_element, 'priority')
            priority_element.text = '0.7'

    try:
        tree = ET.ElementTree(sitemap_root)
        tree.write(output_file, encoding='utf-8', xml_declaration=True)
        print(f"Sitemap updated successfully: {output_file}")
    except Exception as e:
        print(f"Error writing sitemap: {e}")

# Paths for input and output
sitemap_file = "sitemap.xml"
crawled_file = "crawled_urls.csv"
output_file = "updated_sitemap.xml"

# Load existing sitemap and crawled URLs
sitemap_root, namespace, existing_urls = load_sitemap(sitemap_file)
crawled_urls = load_crawled_urls(crawled_file)

if sitemap_root and namespace:
    update_sitemap(sitemap_root, namespace, existing_urls, crawled_urls, output_file)
