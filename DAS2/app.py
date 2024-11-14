import sys
import csv
import signal
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import os
from datetime import datetime
start_time=datetime.now()
scraped_data = []
scraped_records = set()

def load_from_csv():
    csv_file = "scraped_data.csv"
    if os.path.exists(csv_file):
        try:
            with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    scraped_records.add((row["firm"], row["from_date"], row["to_date"]))
            print(f"Loaded {len(scraped_records)} records from {csv_file}")
        except Exception as e:
            print(f"Error while loading data from CSV: {e}")

def save_to_csv():
    csv_file = "scraped_data.csv"
    fieldnames = ["firm", "from_date", "to_date", "data"]
    try:
        file_exists = os.path.isfile(csv_file)
        with open(csv_file, mode='a' if file_exists else 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for data in scraped_data:
                writer.writerow(data)
        print(f"Scraped data successfully saved to {csv_file}")
    except Exception as e:
        print(f"Error while saving data to CSV: {e}")

def signal_handler(sig, frame):
    print("\nProgram interrupted. Saving data to CSV...")
    save_to_csv()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

url = "https://www.mse.mk/en/stats/symbolhistory/alk"
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=options)
driver.get(url)
wait = WebDriverWait(driver, 10)

dropdown = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Code"]')))
select = Select(dropdown)
valid_firms = [firm.text.strip() for firm in select.options if firm.text.isalpha()]
print(f"Found {len(valid_firms)} valid firms.")

load_from_csv()

def format_macedonian_number(value):
    try:
        num = float(value.replace(",", "").replace(".", "").replace(" ", ""))
        formatted_value = f"{num:,.2f}"
        formatted_value = formatted_value.replace(",", "X").replace(".", ",").replace("X", ".")
        return formatted_value
    except ValueError:
        return value

def scrape_firm(firm, max_retries=1, retry_delay=3):
    global scraped_records
    attempt = 0
    today = datetime.today().strftime("%m/%d/%Y")

    while attempt < max_retries:
        try:
            dropdown = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="Code"]')))
            select = Select(dropdown)
            select.select_by_visible_text(firm)

            latest_to_date = None
            data_found = False

            for i in range(14, 24):
                from_date = f"10/9/20{i}"
                to_date = f"8/9/20{i + 1}"

                if (firm, from_date, to_date) in scraped_records:
                    print(f"Skipping already scraped data for {firm} from {from_date} to {to_date}")
                    continue

                if latest_to_date is None or to_date > latest_to_date:
                    latest_to_date = to_date

                if latest_to_date < today:
                    od_date = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="FromDate"]')))
                    do_date = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="ToDate"]')))

                    od_date.clear()
                    od_date.send_keys(from_date)
                    do_date.clear()
                    do_date.send_keys(to_date)

                    submit_button = wait.until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="report-filter-container"]/ul/li[4]/input'))
                    )
                    submit_button.click()

                    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="resultsTable"]')))
                    result_page = driver.page_source
                    result_soup = BeautifulSoup(result_page, "html.parser")

                    elements = result_soup.select("#resultsTable > tbody > tr")
                    print(f"Found {len(elements)} rows for firm {firm} from {from_date} to {to_date}")

                    for row in elements:
                        columns = row.find_all("td")
                        row_data = [format_macedonian_number(cell.text.strip()) for cell in columns]

                        scraped_data.append({
                            "firm": firm,
                            "from_date": from_date,
                            "to_date": to_date,
                            "data": row_data,
                        })

                    scraped_records.add((firm, from_date, to_date))
                    data_found = True

            if not data_found:
                current_year = datetime.today().year
                for i in range(10):
                    from_date = f"10/9/{current_year - i}"
                    to_date = f"8/9/{current_year - i + 1}"

                    if (firm, from_date, to_date) not in scraped_records:
                        od_date = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="FromDate"]')))
                        do_date = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="ToDate"]')))

                        od_date.clear()
                        od_date.send_keys(from_date)
                        do_date.clear()
                        do_date.send_keys(to_date)

                        submit_button = wait.until(
                            EC.element_to_be_clickable((By.XPATH, '//*[@id="report-filter-container"]/ul/li[4]/input'))
                        )
                        submit_button.click()

                        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="resultsTable"]')))
                        result_page = driver.page_source
                        result_soup = BeautifulSoup(result_page, "html.parser")

                        elements = result_soup.select("#resultsTable > tbody > tr")
                        print(f"Found {len(elements)} rows for firm {firm} from {from_date} to {to_date}")

                        for row in elements:
                            columns = row.find_all("td")
                            row_data = [format_macedonian_number(cell.text.strip()) for cell in columns]

                            scraped_data.append({
                                "firm": firm,
                                "from_date": from_date,
                                "to_date": to_date,
                                "data": row_data,
                            })

                        scraped_records.add((firm, from_date, to_date))

            break

        except Exception as e:
            attempt += 1
            print(f"Error while processing firm '{firm}' (Attempt {attempt}/{max_retries}): {e}")
            if attempt <= max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to scrape firm '{firm}' after {max_retries} attempts.")
                continue

for firm in valid_firms:
    scrape_firm(firm)

save_to_csv()
driver.quit()
end_time=datetime.now()
print(end_time-start_time)