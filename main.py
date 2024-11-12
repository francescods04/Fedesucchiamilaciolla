import pandas as pd
import asyncio
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
import threading
import csv
import os

# Proxy credentials and details
username = ""
password = ""
GEONODE_DNS = "shared-datacenter.geonode.com:9000"

# HTTP Basic Authentication for proxy
proxy = f"http://{username}:{password}@{GEONODE_DNS}"
proxies = {
    "http": proxy,
    "https": proxy
}

# User-Agent header
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/91.0.4472.124 Safari/537.36"
}

# Excel read
df = pd.read_excel("/Users/francescodelsesto/Downloads/f6s (1).xlsx")
urls = df["main href"]

# Limit for concurrent tasks
max_concurrent_tasks = 50

# CSV file path
csv_file = 'f6s_scraping.csv'

# Threading lock for writing to CSV
csv_lock = threading.Lock()

# Function to request a page and parse it with BeautifulSoup
def fetch_page(url, max_retries=10):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            retries += 1
            print(f"Failed to fetch {url} (attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                time.sleep(2)  # Wait for 2 seconds before retrying
            else:
                print(f"Giving up on {url} after {max_retries} attempts.")
                return None

# Function to scrape data from a given URL using BeautifulSoup
def scrape_data(url, max_retries=10):
    retries = 0
    while retries < max_retries:
        try:
            # Fetch the page
            soup = fetch_page(url)
            if not soup:
                retries += 1
                print(f"Failed to fetch page for {url}. Retrying ({retries}/{max_retries})")
                if retries < max_retries:
                    continue
                else:
                    print(f"Giving up on {url} after {max_retries} attempts.")
                    return

            # Parse data using BeautifulSoup selectors
            name_tag = soup.select_one("h1.profile-name")
            name = name_tag.get_text(strip=True) if name_tag else None

            tagline_tag = soup.select_one("p.profile-tagline")
            tagline = tagline_tag.get_text(strip=True) if tagline_tag else None

            about_paragraphs = soup.select("div.profile-description p")
            about = " ".join(p.get_text(strip=True) for p in about_paragraphs) if about_paragraphs else None

            market_tags_list = [a.get_text(strip=True) for a in soup.select("div.markets-list a.market-item")]
            market_tags = ", ".join(market_tags_list) if market_tags_list else None

            data = {
                'url': url,
                'name': name,
                'tagline': tagline,
                'about': about,
                'market_tags': market_tags
            }

            # Check if all required data is present (you can adjust this condition as needed)
            if not name:
                raise ValueError("Missing required data: name")

            # Write data to CSV file
            with csv_lock:
                with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['url', 'name', 'tagline', 'about', 'market_tags'])
                    writer.writerow(data)

            print(f"Scraped data from {url}: {data}")
            # If successful, break the loop
            return

        except Exception as e:
            retries += 1
            print(f"Error processing {url} (attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                time.sleep(2)  # Wait for 2 seconds before retrying
            else:
                print(f"Giving up on {url} after {max_retries} attempts.")
                return

# Async wrapper to run scraper in a thread, with semaphore control
async def async_scraper(url, semaphore):
    async with semaphore:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, scrape_data, url)

# Main async function to run all scrapes concurrently with limited concurrency
async def main():
    # Create the CSV file and write the header if it doesn't exist
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'name', 'tagline', 'about', 'market_tags'])
            writer.writeheader()

    # Start scraping tasks
    semaphore = asyncio.Semaphore(max_concurrent_tasks)  # Limit concurrency
    tasks = [async_scraper(url, semaphore) for url in urls]
    await asyncio.gather(*tasks)

# Check if there's an active event loop, and run accordingly
try:
    asyncio.get_running_loop()
    await main()  # If there's an existing event loop, use await main()
except RuntimeError:
    asyncio.run(main())  # If no event loop is running, use asyncio.run
