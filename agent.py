from google.adk.agents import Agent
from google.adk.tools import google_search
from datetime import datetime, timedelta
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types
from google.adk.tools import FunctionTool

import os 
from dotenv import load_dotenv
load_dotenv()




APP_NAME="props_finder_agent"
USER_ID="props1234"
SESSION_ID="0000"

# Define agent
demo_agent = Agent(
    name="props_finder_agent",
    model="gemini-2.0-flash",
    description=(
        "Finds props from prizepicks and provides the best bets for the day. "
    ),
    instruction=(
        "You are an agent that finds props from prizepicks and are specialized in finding all props from different websites for the given game and date." \
        "Scrape the data using scrape_props_data. "
    ),
    tools=[],  
)

# Define parameters (dates, games, etc.)
now = datetime.today()
today_date = datetime.today().strftime("%Y-%m-%d")
tommorow_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
game = "Los Angeles Lakers vs. Menesota Timberwolves"
print(today_date)
print(tommorow_date)

# Session and Runner
session_service = InMemorySessionService()
session = session_service.create_session(app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID)
runner = Runner(agent=demo_agent, app_name=APP_NAME, session_service=session_service)


# Agent Interaction
def call_agent(query):
    """
    Helper function to call the agent with a query.
    """
    content = types.Content(role='user', parts=[types.Part(text=query)])
    events = runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    for event in events:
        if event.is_final_response():
            final_response = event.content.parts[0].text
            print("Agent Response: ", final_response)

call_agent(f"What are all the props for today from prizepicks.")