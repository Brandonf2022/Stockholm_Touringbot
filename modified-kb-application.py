import re
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup as bs
import time
import sqlite3
from urllib.parse import quote_plus
import yaml
from datetime import datetime
import os
import pickle  # Add this import
from KBDownloader import search_swedish_newspapers, fetch_newspaper_data, save_checkpoint, load_checkpoint

# Get today's date
today_date = datetime.today().strftime('%Y-%m-%d')

# Load the YAML configuration file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Assign variables from the YAML configuration
venue_list = config['venue_list']
start_year = config['start_year']
years_to_crawl = config['years_to_crawl']
newspaper = config['newspaper']
db_path = config['db_path']

# Ensure the database file exists
if not os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    conn.close()

# Load checkpoint if it exists
checkpoint = load_checkpoint()
start_year = checkpoint['year'] if checkpoint else start_year
start_half = checkpoint['half'] if checkpoint else 0
start_index = checkpoint['index'] if checkpoint else 0

# Print out all the settings from the YAML configuration file
print("Configuration Settings:")
for key, value in config.items():
    print(f"{key}: {value}")

# Load the venue list
df = pd.read_excel(venue_list)

# Main loop
for year in range(start_year, start_year + years_to_crawl):
    for half in range(2):
        if year == start_year and half < start_half:
            continue
        
        if half == 0:
            from_date = datetime(year, 1, 1)
            to_date = datetime(year, 6, 30)
        else:
            from_date = datetime(year, 7, 1)
            to_date = datetime(year, 12, 31)

        for index, row in df.iloc[start_index:].iterrows():
            query = row['Lokal']
            safe_query = "".join([c if c.isalnum() else "_" for c in query])
            output_dir = f'extracted_data_{safe_query}_{today_date}'
            os.makedirs(output_dir, exist_ok=True)
            
            try:
                result = fetch_newspaper_data(
                    query=query,
                    from_date=from_date.strftime('%Y-%m-%d'),
                    to_date=to_date.strftime('%Y-%m-%d'),
                    newspaper=newspaper,
                    config=config,
                    db_path=db_path
                )
                
                if result.get('success'):
                    print(f"Processed query '{query}' successfully.")
                else:
                    print(f"Failed to process query '{query}': {result.get('message')}")
                
                # Save checkpoint after each query, successful or not
                save_checkpoint(year, half, index + 1)
                
            except Exception as e:
                print(f"Error processing query '{query}': {str(e)}")
                save_checkpoint(year, half, index)
                raise  # Re-raise the exception to stop the script

        print(f"Waiting so KB does not get mad. Currently at {from_date} to {to_date}")
        time.sleep(60)
        start_index = 0  # Reset start_index for the next half-year

    start_half = 0  # Reset start_half for the next year

print("All queries processed for all specified years.")
