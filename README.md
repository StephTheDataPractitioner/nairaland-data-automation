# Nairaland Data Automation
This project shows how to create an ETL pipeline that scrapes front-page data from Nairaland, retrieves post metrics from each detail page, sends the cleaned data to Azure Data Lake Storage Gen2, and connects it to Power BI for analytics and reporting. The final Power BI report is published,  and scheduled to refresh on a daily basis.

---

It uses:
- **Python** for scraping and data ingestion  
- **Azure Blob Storage SDK** for cloud storage  
- **Power BI** with **Power Query (M)** for data transformation and visualization  
- **Microsoft Entra ID (OAuth2)** for secure, keyless access to the data  

---

## 🧠 Project Overview

The Python script does the following:

1. Launches Chrome using **Selenium** to extract all topic URLs on Nairaland’s front page.  
2. Uses **Cloudscraper** and **Scrapy’s HtmlResponse** to scrape each post’s details such as:
   - Post Title and Forum name (`NamTit`)
   - Number of views and guests
   - Poster name
   - Original timestamp and scrape time
3. Organizes and uploads the scraped data into **Azure Data Lake Storage Gen2**, with date folder patitioning

4. Each JSON file uploaded is uniquely timestamped — this allows Power BI to load only the **latest partition** using M of Power Query.

---

## 🧩 Project Structure
```
ProjFold/
├── Reports/                     # Power BI reports or exported visuals
├── ScrapingFolder/              # Scraping scripts and ChromeDriver
│   ├── chromedriver.exe         # Chrome driver for Selenium
│   ├── nair.py                  # Main scraping Python script
│   └── requirements.txt         # Python dependencies
├── venv/                        # Python virtual environment
├── .gitignore
├── README.md
├── run_scraper.bat              # Batch file to run scraper
```

---

## ⚙️ Environment Setup

### 1️⃣ Create a Virtual Environment

for windows:
```
python -m venv venv
venv\Scripts\activate
```

### 2️⃣ Install Dependencies

Once activated, install all required libraries:
```
pip install -r requirements.txt
```

### 3️⃣ Add System Environment Variables via the Command Line

You can use PowerShell and ensure you run as Administrator:
```
[System.Environment]::SetEnvironmentVariable('AZURE_STORAGE_ACCOUNT_NAME', 'your_storage_account_name', 'Machine')
[System.Environment]::SetEnvironmentVariable('AZURE_STORAGE_ACCOUNT_KEY', 'your_storage_account_key', 'Machine')
[System.Environment]::SetEnvironmentVariable('AZURE_CONTAINER_NAME', 'your_container_name', 'Machine')
```

The script reads them automatically:
```
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
```

✅ This avoids hardcoding sensitive information and keeps credentials secure.

## 🧩💻 Code Explanation
### 🔹 Selenium: Getting Topic URLs

The script starts by launching Chrome, opening Nairaland’s front page, and collecting all topic URLs dynamically.
```
driver.get("https://www.nairaland.com/")
links = driver.find_elements(By.XPATH, "//a[contains(@href,'/8')]")
topic_urls = [link.get_attribute("href") for link in links]
```

This ensures that new front-page posts are always included whenever the script runs.

### 🔹 Cloudscraper + Scrapy: Extracting Post Details

After collecting the URLs, each one is visited to extract post data:
```
scraper = cloudscraper.create_scraper()
resp_raw = scraper.get(url, timeout=15)
resp = HtmlResponse(url=url, body=resp_raw.text, encoding='utf-8')
```

The HtmlResponse object allows Scrapy-style XPath queries to cleanly extract structured content like post titles, poster names, and timestamps.

### 🔹 Regex: Extracting Views and Guests

The script uses regular expressions to extract numerical data directly from the HTML:
```
views_match = re.search(r'(\d[\d,]*)\s*Views', resp_raw.text)
views_count = int(views_match.group(1).replace(',', '')) if views_match else 0

guests_match = re.search(r'and\s+(\d+)\s+guest', resp_raw.text, re.I)
guests_count = int(guests_match.group(1)) if guests_match else 0
```

This captures both views and guest counts, even if the page layout changes slightly.

### 🔹 Timestamp Normalization

Some posts have incomplete timestamps, so the script intelligently formats them with the current date:
```
if 'On' in first_timestamp:
    t_str = re.sub(r'On ', '', first_timestamp)
    t_dt = datetime.strptime(t_str, '%I:%M%p %b %d')
    timestamp_str = t_dt.strftime('2025-%m-%d %H:%M:%S')
```

This ensures all timestamps follow the same consistent pattern before upload.

### 🔹 Uploading to Azure Data Lake

Each successful scrape is stored as a JSON file in a partitioned folder structure:
```
folder_path = f"year={current_time.year}/month={current_time.month:02}/day={current_time.day:02}/hour={current_time.hour:02}/"
blob_client = container_client.get_blob_client(f"{folder_path}{filename}")
blob_client.upload_blob(json.dumps(data_list, indent=4), overwrite=True)
```

Example of upload path:
```
year=2025/month=11/day=01/hour=19/data_20251101193000.json
```



## 📊 Power BI Integration

In Power BI Desktop, use Power Query (M) to connect to your Azure Data Lake container.
The query dynamically filters for the latest hourly JSON partition and expands fields for visualization.

To ensure secure access:

Choose OAuth2 authentication

Set Privacy level = Organizational



## 🧠 Conclusion

This project demonstrates how anyone can build a secure, scalable, ETL pipeline — from web scraping, to cloud storage, and Power BI visualization — using mostly free and open-source tools.

Future updates will include time-series forecasting on post views and visitor trends to predict engagement spikes and dips on Nairaland’s front page.



