import os 
from dotenv import load_dotenv
load_dotenv()

from langchain_community.document_loaders import WebBaseLoader

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

import pandas as pd
from io import StringIO
import re
from math import ceil

from airflow.exceptions import AirflowException

from google.cloud import bigquery
from google.oauth2 import service_account

from datetime import datetime
import uuid
import shutil
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

import logging
logger = logging.getLogger(__name__)



class CustomSeleniumLoader:
    def __init__(self, urls: list[str]):
        self.urls = urls

    def load(self):
        options = Options()
        options.headless = True
        options.add_argument("--window-size=1280,800")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        # ✅ Remote WebDriver (connects to standalone-chromium container)
        browser = webdriver.Remote(
            command_executor='http://selenium-chrome:4444/wd/hub',
            options=options
        )

        all_html = []
        screenshot_no = 0

        for url in self.urls:
            logger.info(f"🌐 Loading URL: {url}")
            browser.get(url)
            time.sleep(5)

            try:
                button = WebDriverWait(browser, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/div[3]/div/div/div/button"))
                )
                logger.info("🔵 Button found. Clicking...")
                screenshot_no += 1
                browser.save_screenshot(f"/tmp/screenshot_{screenshot_no}.png")
                button.click()
                screenshot_no += 1
                browser.save_screenshot(f"/tmp/screenshot_{screenshot_no}.png")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"⚠️ Could not find/click the button: {e}")

            try:
                logger.info("🔵 Button found. Clicked")
                WebDriverWait(browser, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.table__body"))
                )
                scrollable_container = browser.find_element(By.CSS_SELECTOR, "div.table-overflow.props-table")
                count_span = WebDriverWait(browser, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.typography.count"))
                )
                count_text = count_span.text.strip()
                logger.info(f"🧾 Found count text: {count_text}")

                match = re.search(r"of\s+(\d+)", count_text)
                total_props = int(match.group(1)) if match else 500
                logger.info(f"Total props: {total_props}")
                visible_per_scroll = 21
                scrolls_needed = ceil(total_props / visible_per_scroll)

                for i in range(scrolls_needed):
                    logger.info(f"🔵 Scrolling{i}")
                    browser.execute_script("arguments[0].scrollTop += 2000;", scrollable_container)
                    logger.info(f"🔽 Scroll #{i+1} of {scrolls_needed}")
                    time.sleep(2.1)

                table_html = scrollable_container.get_attribute("outerHTML")
                all_html.append(StringIO(table_html))
                logger.info("🔵 Finished scrolling")

            except Exception as e:
                logger.error(f"❌ Error during table extraction: {e}")

        browser.quit()
        logger.info("✅ Browser session ended.")

        if len(all_html) == 1:
            full_html = f"<table>{table_html}</table>"
            df = pd.read_html(full_html)[0]
            file_path = f"/opt/airflow/data/props_{str(datetime.now().date())}.csv"  # ✅ updated path
            df.to_csv(file_path, index=False)
            logger.info(f"✅ Data saved to {file_path}")

            if total_props != len(df):
                logger.warning(f"⚠️ Row count mismatch: expected {total_props}, got {len(df)}")
                raise AirflowException("Incorrect number of rows — retrying...")
            return df
        else:
            logger.warning("⚠️ Multiple tables detected — code not handling this yet.")





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
    try:
        logger.info("Trying to read file")
        file_path = f"/opt/airflow/data/props_{str(datetime.now().date())}.csv"
        df = pd.read_csv(file_path, skiprows=2, header=None)
        logger.info("Successfully read file")
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        raise AirflowException("File not found")
    
    logger.info("Passed File Check!!!")
    df.columns = [
        "Player name & Position & Game", "Prop", "Projection", "Proj_BP_Diff",
        "Rating", "EV", "OPP vs Prop", "L-5", "L-15", "season", "H2H", "Odds", "Analyzer"
    ]
    logger.info("Passed columns check!!!")

    df["Player name"] = df["Player name & Position & Game"].str.split("- ", expand=True)[0].apply(lambda x: x.rstrip(" ")[:-1])
    df["Position"] = df["Player name & Position & Game"].str.split("- ", expand=True)[0].apply(lambda x: x.rstrip(" ")[-1])
    df["Game"] = df["Player name & Position & Game"].str.split("- ", expand=True)[1]
    logger.info("Extracted player-related fields")

    df["Prop_line"] = df["Prop"].apply(lambda x: float(re.search(r'\d+\.\d+|\d+', x).group()) if isinstance(x, str) and re.search(r'\d+\.\d+|\d+', x) else None)
    df["Prop_type"] = df["Prop"].apply(lambda x: ' '.join(re.findall(r'[^\d\.\s]+', x)) if isinstance(x, str) else None)

    df["Projection_BP"] = df["Projection"].apply(lambda x: float(re.search(r'\d+\.\d+|\d+', x).group()) if isinstance(x, str) and re.search(r'\d+\.\d+|\d+', x) else None)
    df["Projection_UO_BP"] = df["Projection"].apply(lambda x: ' '.join(re.findall(r'[^\d\.\s]+', x)) if isinstance(x, str) else None)

    df["Odds"] = df["Odds"].apply(lambda x: x.split("(")[-1].rstrip(")"))

    # Drop unnecessary columns
    df.drop(columns=["Player name & Position & Game", "Prop", "Projection", "Rating", "Analyzer"], inplace=True, axis=1)
    logger.info("Dropped unnecessary columns")

    # Explicit data type conversions
    float_columns = ["Prop_line", "Projection_BP", "Proj_BP_Diff", "EV", "L-5", "L-15", "season", "Odds"]
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Player name"] = df["Player name"].astype(str)
    df["Position"] = df["Position"].astype(str)
    df["Game"] = df["Game"].astype(str)
    df["Prop_type"] = df["Prop_type"].astype(str)
    df["Projection_UO_BP"] = df["Projection_UO_BP"].astype(str)
    df["OPP vs Prop"] = df["OPP vs Prop"].astype(str)
    df["H2H"] = df["H2H"].astype(str)

    # Reorder columns
    df_reordered = df[[
        "Player name", "Position", "Game", "Prop_line", "Prop_type",
        "Projection_BP", "Projection_UO_BP", "Proj_BP_Diff", "EV",
        "OPP vs Prop", "L-5", "L-15", "season", "H2H", "Odds"
    ]]

    output_file = f"/opt/airflow/data/transformed_props_{str(datetime.now().date())}.csv"
    df_reordered.to_csv(output_file, index=False)
    logger.info(f"Transformed data saved to {output_file}")
    
    return df_reordered




