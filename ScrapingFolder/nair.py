import cloudscraper
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import time
import random
import json
import os
import re


STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")


blob_service_client = BlobServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
    credential=STORAGE_ACCOUNT_KEY
)


container_client = blob_service_client.get_container_client(CONTAINER_NAME)

blob_service_client = BlobServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
    credential=STORAGE_ACCOUNT_KEY
)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--window-size=1920,1080")


service = Service(executable_path="chromedriver.exe")
driver = webdriver.Chrome(service=service, options=chrome_options)

print("Opening Nairaland front page...")
driver.get("https://www.nairaland.com/")
time.sleep(random.uniform(3, 6))  

links = driver.find_elements(By.XPATH, "//a[contains(@href,'/8')]")
topic_urls = [link.get_attribute("href") for link in links]  # adjust limit
driver.quit()

print(f"Collected {len(topic_urls)} topic URLs")


scraper = cloudscraper.create_scraper()
data_list = []

for url in topic_urls:
    try:
        resp_raw = scraper.get(url, timeout=15)
        if resp_raw.status_code != 200:
            print(f"⚠️ Failed to fetch {url}")
            continue

        resp = HtmlResponse(url=url, body=resp_raw.text, encoding='utf-8')

        name_e = resp.xpath("//div[@class='body']/h2/text()").get(default="").strip()
        title = resp.xpath("//title/text()").get(default="").strip()

        views_match = re.search(r'(\d[\d,]*)\s*Views', resp_raw.text)
        views_count = int(views_match.group(1).replace(',', '')) if views_match else 0

        guests_match = re.search(r'and\s+(\d+)\s+guest', resp_raw.text, re.I)
        guests_count = int(guests_match.group(1)) if guests_match else 0

        poster = resp.xpath("//a[@class='user']/text()").get(default="")

        timestamp_elements = resp.xpath('//span[@class="s"]/text()').getall()
        if timestamp_elements:
            first_timestamp = timestamp_elements[0].strip()
            if 'On' in first_timestamp:
                try:
                    t_str = re.sub(r'On ', '', first_timestamp)
                    t_dt = datetime.strptime(t_str, '%I:%M%p %b %d')
                    timestamp_str = t_dt.strftime('2025-%m-%d %H:%M:%S')
                except ValueError:
                    timestamp_str = ''
            else:
                today = datetime.now().strftime('%Y-%m-%d')
                try:
                    t_dt = datetime.strptime(first_timestamp, '%I:%M%p')
                    timestamp_str = today + ' ' + t_dt.strftime('%H:%M:%S')
                except ValueError:
                    timestamp_str = ''
        else:
            timestamp_str = ''

        scrap_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        nairPg = {
            'NamTit': name_e or title,
            'Views': views_count,
            'guests': guests_count,
            'timestamp': timestamp_str,
            'poster': poster,
            'scrap_time': scrap_time
        }

        data_list.append(nairPg)
        print(f" Scraped: {nairPg['NamTit']}")
        time.sleep(random.uniform(2, 4))  

    except Exception as e:
        print(f" Error on {url}: {e}")
        continue


if data_list:
    current_time = datetime.now()
    folder_path = f"year={current_time.year}/month={current_time.month:02}/day={current_time.day:02}/hour={current_time.hour:02}/"
    filename = f"data_{current_time.strftime('%Y%m%d%H%M%S')}.json"

    

    blob_client = container_client.get_blob_client(f"{folder_path}{filename}")
    blob_client.upload_blob(json.dumps(data_list, indent=4), overwrite=True, blob_type="BlockBlob")

    print(f" Uploaded to Azure Data Lake: {folder_path}{filename}")
else:
    print(" No data scraped. Nothing uploaded.")
