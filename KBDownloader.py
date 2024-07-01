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
import pickle

# Function to search Swedish newspapers
def search_swedish_newspapers(to_date, from_date, collection_id, query):
    base_url = 'https://data.kb.se/search'
    encoded_query = quote_plus(query)
    params = {
        'to': to_date,
        'from': from_date,
        'isPartOf.@id': collection_id,
        'q': encoded_query,
        'searchGranularity': 'part'
    }
    headers = {'Accept': 'application/json'}
    response = requests.get(base_url, params=params, headers=headers)
    response.raise_for_status()
    try:
        return response.json()
    except ValueError:
        raise ValueError('Invalid JSON response')

# Function to extract URLs from the result
def extract_urls(result):
    base_url = 'https://data.kb.se'
    details = []
    for hit in result.get('hits', []):
        part_number = hit.get('part')
        page_number = hit.get('page')
        page_id = hit.get('@id')
        package_id = hit.get('hasFilePackage', {}).get('@id', '').split('/')[-1]
        if part_number and page_number and package_id and page_id:
            url = f"{base_url}/{package_id}/part/{part_number}/page/{page_number}"
            details.append({'part_number': part_number, 'page_number': page_number, 'package_id': package_id, 'url': url, 'page_id': page_id})
    return details

# Function to extract XML URLs from API response
def extract_xml_urls(api_response, page_ids):
    xml_urls = {}
    parts_list = api_response.get('hasPart', [])
    for part in parts_list:
        pages = part.get('hasPartList', [])
        for page in pages:
            if page['@id'] in page_ids:
                includes = page.get('includes', [])
                for include in includes:
                    if 'alto.xml' in include['@id']:
                        page_number = int(page['@id'].split('/')[-1].replace('page', ''))
                        xml_urls[page_number] = include['@id']
    return xml_urls

# Function to fetch XML content
def fetch_xml_content(xml_urls, max_retries=5, initial_delay=5):
    xml_content_by_page = {}
    for page_number, url in xml_urls.items():
        retries = 0
        delay = initial_delay
        while retries < max_retries:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    xml_content_by_page[page_number] = response.content
                    break
                else:
                    print(f"Failed to fetch XML content from {url}. Status code: {response.status_code}")
                    if response.status_code == 429:
                        retries += 1
                        print(f"Rate limited. Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= 2
                    else:
                        break
            except requests.exceptions.RequestException as e:
                print(f"Exception occurred while fetching {url}: {e}")
                retries += 1
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
    return xml_content_by_page

# Function to read system message from a file
def read_system_message(filepath, newspaper_date="date not known"):
    try:
        with open(filepath, 'r') as file:
            content = file.read().strip()
        return content.replace('{Newspaper_Date}', newspaper_date)
    except FileNotFoundError:
        return "You are a helpful assistant."

# Function to convert a DataFrame row to JSON and create LLM prompt
def row_to_json(row, config, counter):
    counter += 1
    date = row['Date']
    system_message_content = read_system_message(config['prompt_filepath'], date)
    system_message = {"role": "system", "content": system_message_content}
    
    # Include the composed block content in the user message
    user_content_parts = [str(row['[ComposedBlock Content]'])]
    user_message = {"role": "user", "content": " ".join(user_content_parts)}
    
    custom_id = f"{row['[Package ID]']}-{row['Part']}-{row['Page']}-{counter}"
    return json.dumps({
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": config['llm_model'],  # Use the model from the config
            "messages": [system_message, user_message],
            "max_tokens": config['max_tokens']  # Use the max_tokens from the config
        }
    })

# Function to save DataFrame to SQL database
def save_to_database(df, db_conn, table_name):
    df.to_sql(table_name, db_conn, if_exists='append', index=False)

class Page:
    def __init__(self, xml_path=None, xml_content=None) -> None:
        if xml_path is not None:
            self.load_xml_path(xml_path)
        elif xml_content is not None:
            self.load_xml(xml_content)
        else:
            raise ValueError("No xml path or content provided.")

    def load_xml_path(self, path):
        with open(path, "r", encoding="utf-8") as f:
            xml = f.read()
        self.load_xml(xml)

    def load_xml(self, xml):
        soup = bs(xml, features="xml")
        self.soup = soup

    def extract_date(self):
        file_name_tag = self.soup.find("fileName")
        if file_name_tag:
            file_name = file_name_tag.get_text()
            date_match = re.search(r'_(\d{8})_', file_name)
            if date_match:
                date_str = date_match.group(1)
                formatted_date = f"{date_str[0:4]}.{date_str[4:6]}.{date_str[6:8]}"
                return formatted_date
        return None

    def composed_block_from_keyword(self, keyword):
        words = keyword.split()
        pattern = re.compile('|'.join(re.escape(word) for word in words), re.IGNORECASE)
        token = self.soup.find("String", attrs={"CONTENT": pattern})
        if token:
            yield self.token_to_composed_block(token)
            while True:
                token = token.find_next("String", attrs={"CONTENT": pattern})
                if token is None:
                    break
                yield self.token_to_composed_block(token)

    def token_to_composed_block(self, token):
        composed_block = token.find_parent("ComposedBlock")
        if composed_block:
            text_lines = composed_block.find_all("TextLine")
            content = "\n".join(
                " ".join(string["CONTENT"] for string in text_line.find_all("String"))
                for text_line in text_lines
            )
            return content
        return None

# Checkpoint functions

# Function to save checkpoint
def save_checkpoint(year, half, index):
    checkpoint = {'year': year, 'half': half, 'index': index}
    try:
        with open('checkpoint.pkl', 'wb') as f:
            pickle.dump(checkpoint, f)
        print(f"Checkpoint saved: Year {year}, Half {half}, Index {index}")
    except Exception as e:
        print(f"Failed to save checkpoint: {str(e)}")

# Function to load checkpoint
def load_checkpoint():
    if os.path.exists('checkpoint.pkl'):
        try:
            with open('checkpoint.pkl', 'rb') as f:
                return pickle.load(f)
        except (EOFError, pickle.UnpicklingError):
            print("Checkpoint file is corrupted or empty. Starting from the beginning.")
            os.remove('checkpoint.pkl')  # Remove the corrupted file
        except Exception as e:
            print(f"An error occurred while loading the checkpoint: {str(e)}")
    return None


# Main function
def fetch_newspaper_data(query, from_date, to_date, newspaper, config, db_path):
    counter = 0
    newspaper_dict = {
        'Dagens nyheter': 'https://libris.kb.se/m5z2w4lz3m2zxpk#it',
        'Svenska Dagbladet': 'https://libris.kb.se/2ldhmx8d4mcrlq9#it',
        'Aftonbladet': 'https://libris.kb.se/dwpgqn5q03ft91j#it',
        'Dagligt Allehanda': 'https://libris.kb.se/9tmqzv3m32xfzcz#it'
    }
    collection_id = newspaper_dict.get(newspaper)
    if not collection_id:
        return {"error": "Invalid newspaper name provided"}

    result = search_swedish_newspapers(to_date, from_date, collection_id, query)
    if 'error' in result:
        return {"error": result['error'], "message": result.get('message', 'Unknown error')}

    detailed_info = extract_urls(result)
    page_ids = [info['page_id'] for info in detailed_info]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS newspaper_data (
        Date TEXT,
        [Package ID] TEXT,
        Part TEXT,
        Page INTEGER,
        [ComposedBlock Content] TEXT,
        [Raw API Result] TEXT,
        [Full Prompt] TEXT
    )
    ''')
    total_rows_saved = 0

    for info in detailed_info:
        url = info['url']
        response = requests.get(url, headers={'Accept': 'application/json'})
        if response.status_code == 200:
            api_response = response.json()
            xml_urls = extract_xml_urls(api_response, page_ids)
            xml_content_by_page = fetch_xml_content(xml_urls)
            for page_number, xml_content in xml_content_by_page.items():
                xml_string = xml_content.decode('utf-8')
                page = Page(xml_content=xml_string)
                date = page.extract_date()
                matching_composed_blocks = list(page.composed_block_from_keyword(query))
                if matching_composed_blocks:
                    for block in matching_composed_blocks:
                        counter += 1
                        custom_id = f"{info['package_id']}-{info['part_number']}-{page_number}-{counter}"
                        full_prompt = row_to_json({
                            'Date': date,
                            '[Package ID]': info['package_id'],
                            'Part': info['part_number'],
                            'Page': page_number,
                            '[ComposedBlock Content]': block,
                            '[Raw API Result]': json.dumps(api_response)
                        }, config, counter)

                        cursor.execute('''
                        INSERT INTO newspaper_data 
                        (Date, [Package ID], Part, Page, [ComposedBlock Content], [Raw API Result], [Full Prompt]) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (date, info['package_id'], info['part_number'], page_number, block, 
                            json.dumps(api_response), full_prompt))
                        
                        total_rows_saved += 1

            conn.commit()
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")

    conn.close()

    if total_rows_saved > 0:
        return {"success": True, "message": f"Data processing completed. {total_rows_saved} rows saved to the database."}
    else:
        return {"success": False, "message": "No data to save. This may be because there are no results for this period."}

