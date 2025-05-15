import os 
from dotenv import load_dotenv
load_dotenv()

from langchain_community.document_loaders import WebBaseLoader

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Define custom class to handle scrolling
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

import pandas as pd
from io import StringIO
import re
from math import ceil



class CustomSeleniumLoader:
    def __init__(self, urls: list[str]):
        self.urls = urls

    def load(self):
        options = Options()
        options.headless = False
        options.add_argument("--window-size=1280,800")
        browser = webdriver.Chrome(options=options)

        all_html = []
        

        for url in self.urls:
            print(f"🌐 Loading URL: {url}")
            browser.get(url)
            time.sleep(5)  # Let it load
            try:
                # ✅ Wait for button to be clickable
                button = WebDriverWait(browser, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/div[3]/div/div/div/button"))
                )
                print("🔵 Button found. Clicking...")
                button.click()
                time.sleep(2)  # ✅ Wait for action to complete after click

            except Exception as e:
                print(f"⚠️ Could not find/click the button: {e}")

            try:
                WebDriverWait(browser, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.table__body"))
                )
                scrollable_container = browser.find_element(By.CSS_SELECTOR, "div.table-overflow.props-table")
                
                count_span = WebDriverWait(browser, 10).until(
                
                EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "span.typography.count")
                    )
                )
    
                # Extract text content, e.g., "228 of 228"
                count_text = count_span.text.strip()
                print(f"🧾 Found count text: {count_text}")  # Debug print

                # Use regex to extract total props
                match = re.search(r"of\s+(\d+)", count_text)
                total_props = int(match.group(1)) if match else 500
                print("TOtal props: ", total_props)
                visible_per_scroll = 21
                scrolls_needed = ceil(total_props / visible_per_scroll)


                # Optional scroll
                for _ in range(scrolls_needed):
                    browser.execute_script("arguments[0].scrollTop += 2000;", scrollable_container)
                    time.sleep(2.1)

                # ✅ Get just the table's HTML
                table_html = scrollable_container.get_attribute("outerHTML")
                all_html.append(StringIO(table_html))

            except Exception as e:
                print(f"❌ Error: {e}")

        browser.quit()

        print(len(all_html))
        # ✅ Return the raw HTML (list if multiple URLs, string if one URL)
        if len(all_html) == 1:
            full_html = f"<table>{table_html}</table>"
            df = pd.read_html(full_html)[0]
            df.to_csv("props.csv", index=False)
            return df
        else:
            print("Multiple tables, need more code to handle this.")



# Custom functions to scrape props data 
def scrape_props_data():
    """
    Scrape props data from the web using custom loader with scrolling.
    """
    loader = CustomSeleniumLoader(
        urls=["https://www.bettingpros.com/nba/picks/prop-bets/"]
    )
    documents = loader.load()
    
    print(documents)
    return {"result": documents.to_string()}


def split_projection(text):
    match = re.match(r"(\d*\.?\d+)([a-zA-Z]+)", str(text))
    if match:
      return match.group(1), match.group(2)
    else:
      return None, None

def transform_props_data():
    """
    Transform the scraped props data into a more usable format.
    """
    # This function would contain logic to transform the data if needed
    df = pd.read_csv("props.csv", skiprows=2, header=None)
    df.columns = ["Player name & Position & Game", "Prop", "Projection", "Proj_BP_Diff", "Rating", "EV", "OPP vs Prop", "L-5", "L-15", "season", "H2H", "Odds", "Analyzer"]
    df["Player name"] = df["Player name & Position & Game"].str.split(" -", expand=True)[0].apply(lambda x: x.rstrip(" ")[:-1])
    df["Position"] = df["Player name & Position & Game"].str.split(" -", expand=True)[0].apply(lambda x: x.rstrip(" ")[-1])
    df["Game"] = df["Player name & Position & Game"].str.split(" -", expand=True)[1]
    df["Prop_line"] = df["Prop"].apply(lambda x: float(re.search(r'\d+\.\d+|\d+', x).group()) if isinstance(x, str) and re.search(r'\d+\.\d+|\d+', x) else None)
    df["Prop_type"] = df["Prop"].apply(lambda x: ' '.join(re.findall(r'[^\d\.\s]+', x)) if isinstance(x, str) else None)
    df["Projection_BP"] = df["Projection"].apply(lambda x: float(re.search(r'\d+\.\d+|\d+', x).group()) if isinstance(x, str) and re.search(r'\d+\.\d+|\d+', x) else None)
    df["Projection_UO_BP"] = df["Projection"].apply(lambda x: ' '.join(re.findall(r'[^\d\.\s]+', x)) if isinstance(x, str) else None)
    df["Odds"]  = df["Odds"].apply(lambda x: x.split("(")[-1].rstrip(")"))
    df.drop(columns=["Player name & Position & Game", "Prop", "Projection","Rating", "Analyzer"], inplace=True, axis = 1)
    df_reordered = df[["Player name", "Position",  "Game", "Prop_line", "Prop_type", "Projection_BP", "Projection_UO_BP" ,"Proj_BP_Diff", "EV", "OPP vs Prop", "L-5", "L-15", "season", "H2H", "Odds"]]
    df_reordered.to_csv("transformed_props.csv", index=False)
    return df_reordered

def load_BQ(transformed_props):
    """
    Load the transformed props data into BigQuery.
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account

    # Set up BigQuery client
    credentials = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Define dataset and table names
    dataset_id = "your_dataset_id"
    table_id = "your_table_id"

    # Load DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        )
    return True


scrape_props_data()
#transform_props_data()


