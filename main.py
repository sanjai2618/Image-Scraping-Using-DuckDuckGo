import time, json, re
import pandas as pd
import MySQLdb as my
import requests
from pathlib import Path
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from multiprocessing import Pool, cpu_count

# Configuration
BASE_URL = "https://files.xelentcaned.shop/api"
DB_CREDENTIALS = {"host": "localhost", "user": "root", "passwd": "Sanjai@1530", "db": "limg"}
UPLOAD_DELAY = 1

# Your specific list of Excel files
EXCEL_FILES = [
    Path(r"C:\Users\sanja\Videos\To Sanjai. Business Categories in the Businesses section on CanEd website\Canada. CanEd. Jan. 09, 2025\Health & Beauty-1 Canada SIC 7231.xlsx"),
    Path(r"C:\Users\sanja\Videos\To Sanjai. Business Categories in the Businesses section on CanEd website\Canada. CanEd. Jan. 09, 2025\Health & Beauty-2 Canada SIC 2844.xlsx"),
    Path(r"C:\Users\sanja\Videos\To Sanjai. Business Categories in the Businesses section on CanEd website\Canada. CanEd. Jan. 09, 2025\Healthcare Canada SIC 866.xlsx")
]

def get_companies_from_db(cursor):
    cursor.execute("SELECT name FROM limg")
    return set(row[0].upper() for row in cursor.fetchall())

def setup_driver():
    options = Options()
    options.use_chromium = True
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("user-agent=" + UserAgent().random)
    return webdriver.Edge(options=options)

def fetch_images_from_ddg(keyword, driver):
    try:
        driver.get(f"https://duckduckgo.com/?q={keyword}&iax=images&ia=images")
        html = driver.page_source
        vqd_match = re.search(r'vqd=([\'"]?)([\w-]+)\1', html)
        if not vqd_match:
            print("❌ vqd token not found.")
            return []
        vqd = vqd_match.group(2)
        driver.get(f"https://duckduckgo.com/i.js?o=json&q={keyword}&l=us-en&vqd={vqd}&p=1")
        json_text = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "pre"))
        ).text
        data = json.loads(json_text)
        return [img["image"] for img in data.get("results", [])][:1]
    except Exception as e:
        print(f"❌ Error fetching images for '{keyword}': {str(e)}")
        return []

def process_file(excel_path):
    print(f"\n📄 Processing file: {excel_path.name}")
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"❌ Failed to read {excel_path.name}: {e}")
        return

    company_names = sorted(set(str(name).strip() for name in df["COMPANY NAME"] if pd.notna(name)))

    db = my.connect(**DB_CREDENTIALS)
    cur = db.cursor()
    existing_companies = get_companies_from_db(cur)
    driver = setup_driver()

    for idx, company in enumerate(company_names, 1):
        print(f"🔍 [{excel_path.name}] {idx}/{len(company_names)}: {company}")
        if company.upper() in existing_companies:
            print(f"⏭️ Already exists: {company}")
            continue

        search_term = f"{company} in Canada Official logo"
        image_urls = fetch_images_from_ddg(search_term, driver)

        if not image_urls:
            print(f"⚠️ No images found: {company}")
            continue

        padded_urls = image_urls + [None] * (20 - len(image_urls))

        try:
            cur.execute(
                "INSERT INTO limg (name, url) "
                "VALUES (%s, %s);",
                (company, *padded_urls[:1])
            )
            db.commit()
            print(f"✅ Inserted: {company}")
        except Exception as e:
            print(f"❌ DB insert failed for {company}: {str(e)}")

        time.sleep(UPLOAD_DELAY)

    cur.close()
    db.close()
    driver.quit()
    print(f"✅ Finished file: {excel_path.name}")

def main():
    num_workers = min(cpu_count(), len(EXCEL_FILES), 5)
    with Pool(num_workers) as pool:
        pool.map(process_file, EXCEL_FILES)

if __name__ == "__main__":
    main()
