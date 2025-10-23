import scrapy
import re
import json
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime

class NairalandSpider(scrapy.Spider):
    name = 'Scrapedat'
    start_urls = ['https://www.nairaland.com/']

    def __init__(self, *args, **kwargs):
        super(NairalandSpider, self).__init__(*args, **kwargs)
        self.storage_account_name = 'patx'
        self.storage_account_key = 'your adls gen 2 access keys'
        self.container_name = 'naircont'
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{self.storage_account_name}.blob.core.windows.net/",
            credential=self.storage_account_key
        )
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        self.data_list = []

    def parse(self, response):
        links = response.xpath("//a[contains(@href, 'nairaland.com/841')]/@href").getall()
        for link in links:
            yield response.follow(link, self.parse_post)

    def parse_post(self, response):
        name_e = response.xpath("//div[@class='body']/h2/text()").get().strip()

        p_text = response.xpath("//p[@class='nocopy']/text()").get()
        if p_text:
            users_text = re.search(r'Viewing this topic:\s*(.*?)\s*and', p_text)
            if users_text:
                user_list = [user.strip() for user in users_text.group(1).split(',')]
                num_users = len(user_list)
            else:
                num_users = 0
        else:
            num_users = 0

        page_views_text = response.xpath("(//p[@class='bold']/text())[4]").get()
        views_match = re.search(r'\((\d[\d,]*) Views\)', page_views_text)
        views_count = int(views_match.group(1).replace(',', '')) if views_match else 0

        guests_count = response.xpath("normalize-space(substring-before(substring-after(//p[@class='nocopy']/text()[contains(., 'and') and contains(., 'guest(s)')], 'and '), ' guest'))").get()
        guests_count = int(guests_count) if guests_count.isdigit() else 0

        timestamp_elements = response.xpath('//span[@class="s"]')
        if timestamp_elements:
            first_timestamp_element = timestamp_elements[0]
            first_timestamp = first_timestamp_element.xpath('string()').get().strip()
            if 'On' in first_timestamp:
                try:
                    timestamp_str = re.sub(r'On ', '', first_timestamp)
                    timestamp_dt = datetime.strptime(timestamp_str, '%I:%M%p %b %d')
                    timestamp_str = timestamp_dt.strftime('2025-%m-%d %H:%M:%S')
                except ValueError:
                    timestamp_str = ''
            else:
                system_date = datetime.now().strftime('%Y-%m-%d')
                try:
                    timestamp_dt = datetime.strptime(first_timestamp, '%I:%M%p')
                    timestamp_str = system_date + ' ' + timestamp_dt.strftime('%H:%M:%S')
                except ValueError:
                    timestamp_str = ''
        else:
            timestamp_str = ''

        poster = response.xpath("//a[@class='user']/text()").get()

        # Add scrap time
        scrap_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        nairPg = {
            'NamTit': name_e,
            'Views': views_count,
            'guests': guests_count,
            'timestamp': timestamp_str,
            'poster': poster,
            'scrap_time': scrap_time  # New column with system time
        }

        self.data_list.append(nairPg)

    def save_to_datalake(self):
        current_time = datetime.now()
        folder_path = f"year={current_time.year}/month={current_time.month:02}/day={current_time.day:02}/hour={current_time.hour:02}/"
        filename = f"data_{current_time.strftime('%Y%m%d%H%M%S')}.json"
        data_json = json.dumps(self.data_list, indent=4)

        local_path = os.path.join('C:\\Users\\ZBOOK\\Downloads\\Contents', folder_path)
        os.makedirs(local_path, exist_ok=True)
        local_file_path = os.path.join(local_path, filename)
        with open(local_file_path, 'w') as f:
            f.write(data_json)

        blob_client = self.container_client.get_blob_client(blob=f"{folder_path}{filename}")
        blob_client.upload_blob(data_json, blob_type="BlockBlob", overwrite=True)

    def close(self, reason):
        self.save_to_datalake()
        super().close(reason)
