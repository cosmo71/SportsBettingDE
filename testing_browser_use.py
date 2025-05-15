from langchain_google_genai import ChatGoogleGenerativeAI
from browser_use import Agent
import asyncio
from dotenv import load_dotenv
load_dotenv()
import os
from datetime import datetime, timedelta
os.environ["BROWSER_TYPE"] = "selenium"

now = datetime.now()
yesterday = now - timedelta(days=1)

async def scroll_table_element(page):
    """Scrolls inside the div with class 'table-overflow props-table' to load all props."""
    table_div = await page.wait_for_selector("div.table-overflow.props-table")
    previous_height = await table_div.evaluate("(el) => el.scrollHeight")
    
    while True:
        await table_div.evaluate("(el) => el.scrollTo(0, el.scrollHeight)")
        await asyncio.sleep(1)  # Small wait for new content to load
        new_height = await table_div.evaluate("(el) => el.scrollHeight")
        if new_height == previous_height:
            break
        previous_height = new_height

async def main():
    agent = Agent(
        task=f"Find all the props for NBA games on {now} by going to https://www.bettingpros.com/nba/picks/prop-bets/. There is a table I want you to scrape the entire table including all columns and all rows, and then save all of that in a CSV file. Keep in mind to scroll down on the table itself to get more props. The total amount of props are given on the right side. Use scrolling tool for whenever you need to scroll the entire page.",
        llm=ChatGoogleGenerativeAI(model="gemini-2.0-flash"),
        tools=[scroll_table_element],  
    )
    await agent.run()

asyncio.run(main())