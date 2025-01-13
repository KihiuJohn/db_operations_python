import requests
import xml.etree.ElementTree as ET
import sys
import json
import logging
import http.client as http_client

# =======================
# Enable Debug Logs
# =======================

http_client.HTTPConnection.debuglevel = 1
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

# =======================
# Configuration Section
# =======================

HOST = "archstonekenya.com"
KEY = "9f104648591449b2b42c4be68034d714"
KEY_LOCATION = f"http://{HOST}/{KEY}.txt"  # Use HTTP instead of HTTPS temporarily
SITEMAP_URL = "https://www.archstonekenya.com/sitemap.xml"
API_ENDPOINT = "https://api.indexnow.org/indexnow?api-version=1.1"  # Updated version

# =======================
# Fetch Sitemap
# =======================

def fetch_sitemap(sitemap_url):
    try:
        print(f"Fetching sitemap from: {sitemap_url}")
        response = requests.get(sitemap_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching sitemap: {e}")
        sys.exit(1)
    
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"Error parsing sitemap XML: {e}")
        sys.exit(1)
    
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = [url.find('ns:loc', namespace).text for url in root.findall('ns:url', namespace)]
    print(f"Total URLs found: {len(urls)}")
    return urls

# =======================
# Submit to IndexNow
# =======================

def submit_to_indexnow(host, key, urls, batch_size=10000):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i + batch_size]
        payload = {
            "host": host,
            "key": key,
            "urlList": batch  # Removed keyLocation temporarily
        }
        
        print(f"Payload for batch {i//batch_size +1}:")
        print(json.dumps(payload, indent=2))

        try:
            response = requests.post(API_ENDPOINT, json=payload, headers=headers)
            print(response.text)  # Log response
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            print(f"Error submitting batch {i//batch_size +1}: {http_err}")
            print(f"Response Content: {response.text}")
            continue
        except requests.exceptions.RequestException as e:
            print(f"Error submitting batch {i//batch_size +1}: {e}")
            continue

def main():
    print("🔄 Starting IndexNow URL submission process...")
    urls = fetch_sitemap(SITEMAP_URL)
    if not urls:
        print("No URLs found in sitemap. Exiting.")
        sys.exit(1)
    print("🚀 Submitting URLs to IndexNow...")
    submit_to_indexnow(HOST, KEY, urls)
    print("✅ URL submission process completed.")

if __name__ == "__main__":
    main()
