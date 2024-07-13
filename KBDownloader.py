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
    date = row['Date']
    system_message_content = read_system_message(config['prompt_filepath'], date)
    system_message = {"role": "system", "content": system_message_content}
    
    # Construct the user message from the ComposedBlock Content
    user_content = str(row['[ComposedBlock Content]'])
    user_message = {"role": "user", "content": user_content}
    
    # Construct a unique custom_id
    custom_id = f"{row['[Package ID]']}-{row['Part']}-{row['Page']}-{counter}"
    
    return json.dumps({
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": config['llm_model'],
            "messages": [system_message, user_message],
            "max_tokens": config['max_tokens']
        }
    })

# Function to save DataFrame to SQL database
def save_to_database(df, db_conn, table_name):
    df.to_sql(table_name, db_conn, if_exists='append', index=False)

class Page:
    def __init__(self, xml_content=None) -> None:
        if xml_content is not None:
            self.load_xml(xml_content)
        else:
            raise ValueError("No xml content provided.")

    def load_xml(self, xml):
        self.soup = bs(xml, features="xml")

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

    def extract_matching_content(self, query):
        matching_content = []
        words = query.split()
        pattern = re.compile('|'.join(re.escape(word) for word in words), re.IGNORECASE)
        
        composed_blocks = self.soup.find_all("ComposedBlock")
        for composed_block in composed_blocks:
            if composed_block.find("String", attrs={"CONTENT": pattern}):
                block_content = self.extract_composed_block_content(composed_block)
                matching_content.append(block_content)
        
        return matching_content

    def extract_composed_block_content(self, composed_block):
        content = []
        text_blocks = composed_block.find_all("TextBlock")
        for text_block in text_blocks:
            text_lines = text_block.find_all("TextLine")
            for text_line in text_lines:
                line_content = " ".join(string["CONTENT"] for string in text_line.find_all("String") if "CONTENT" in string.attrs)
                content.append(line_content)
        return "\n".join(content)


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

import sqlite3
import json

# function to process and save data
def process_and_save_data(xml_content_by_page, info, query, config, db_path):
    # Establish database connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create a dictionary to hold combined results
    combined_results = {}

    # Process each page's content and aggregate results
    for page_number, xml_content in xml_content_by_page.items():
        soup = bs(xml_content, 'xml')
        matching_composed_blocks = extract_matching_content(soup, query)

        # Aggregate matches in a dictionary
        for block in matching_composed_blocks:
            key = f"{info['package_id']}-{info['part_number']}-{page_number}"
            if key not in combined_results:
                combined_results[key] = {
                    'Date': date,
                    'Package ID': info['package_id'],
                    'Part': info['part_number'],
                    'Page': page_number,
                    'ComposedBlock Content': [],
                    'Raw API Result': json.dumps(xml_content_by_page)
                }
            combined_results[key]['ComposedBlock Content'].append(block)

    # Insert aggregated results into the database
    for key, value in combined_results.items():
        full_prompt = row_to_json(value, config)
        cursor.execute('''
            INSERT INTO newspaper_data
            (Date, [Package ID], Part, Page, [ComposedBlock Content], [Raw API Result], [Full Prompt])
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (value['Date'], value['Package ID'], value['Part'], value['Page'], 
              json.dumps(value['ComposedBlock Content']), value['Raw API Result'], full_prompt))

    # Commit changes and close connection
    conn.commit()
    conn.close()

    return {"success": True, "message": f"Data processing completed. {len(combined_results)} rows saved to the database."}

# Function to fetch and process data from URLs
import logging
from contextlib import closing
def fetch_newspaper_data(query, from_date, to_date, newspaper, config, db_path):
    collection_id = newspaper
    total_rows_saved = 0

    logging.info(f"Starting fetch_newspaper_data for query: {query}, dates: {from_date} to {to_date}")

    try:
        search_results = search_swedish_newspapers(to_date, from_date, collection_id, query)
        logging.info(f"Search results received. Hits: {len(search_results.get('hits', []))}")
    except requests.HTTPError as e:
        logging.error(f"Failed to fetch search results: {e}")
        return {"success": False, "message": f"Failed to fetch search results: {e}"}

    urls = extract_urls(search_results)
    logging.info(f"Extracted {len(urls)} URLs from search results")
    
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS newspaper_data (
                Date TEXT,
                [Package ID] TEXT,
                Part TEXT,
                Page INTEGER,
                [ComposedBlock ID] TEXT,
                [ComposedBlock Content] TEXT,
                [Raw API Result] TEXT,
                [Full Prompt] TEXT,
                PRIMARY KEY ([Package ID], Part, Page, [ComposedBlock ID])
            )
        ''')
        conn.commit()
        logging.info("Table 'newspaper_data' created or already exists")
        
        for info in urls:
            url = info['url']
            page_id = info['page_id']

            logging.info(f"Processing URL: {url}")

            try:
                response = requests.get(url)
                response.raise_for_status()
                api_response = response.json()

                xml_urls = extract_xml_urls(api_response, [page_id])
                logging.info(f"Extracted {len(xml_urls)} XML URLs")

                xml_content_by_page = fetch_xml_content(xml_urls)
                logging.info(f"Fetched XML content for {len(xml_content_by_page)} pages")

                for page_number, xml_content in xml_content_by_page.items():
                    page = Page(xml_content)
                    date = page.extract_date()

                    if date is None:
                        logging.warning(f"Could not extract date for page {page_number}. Skipping this page.")
                        continue

                    matching_content = page.extract_matching_content(query)
                    
                    for index, block_content in enumerate(matching_content):
                        composed_block_id = f"{info['package_id']}-{info['part_number']}-{page_number}-{index}"
                        
                        row_data = {
                            'Date': date,
                            '[Package ID]': info['package_id'],
                            'Part': info['part_number'],
                            'Page': page_number,
                            '[ComposedBlock ID]': composed_block_id,
                            '[ComposedBlock Content]': block_content,
                            '[Raw API Result]': json.dumps(api_response)
                        }
                        full_prompt = row_to_json(row_data, config, total_rows_saved)

                        try:
                            cursor.execute('''
                                INSERT OR REPLACE INTO newspaper_data 
                                (Date, [Package ID], Part, Page, [ComposedBlock ID], [ComposedBlock Content], [Raw API Result], [Full Prompt]) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (date, info['package_id'], info['part_number'], page_number, composed_block_id,
                                block_content, json.dumps(api_response), full_prompt))
                            
                            total_rows_saved += 1
                            logging.info(f"Inserted or updated row {total_rows_saved} in database")
                        except sqlite3.Error as e:
                            logging.error(f"Failed to insert or update row in database: {e}")

                conn.commit()
                logging.info(f"Committed changes for URL: {url}")

            except requests.HTTPError as e:
                logging.error(f"Failed to fetch data from {url}. Status code: {e.response.status_code}")
                continue
            except Exception as e:
                logging.error(f"Unexpected error processing URL {url}: {str(e)}")
                continue

    logging.info(f"Data processing completed. Total rows saved: {total_rows_saved}")
    return {"success": True, "message": f"Data processing completed. {total_rows_saved} rows saved to the database."}

def extract_date(soup):
    file_name_tag = soup.find("fileName")
    if file_name_tag:
        file_name = file_name_tag.get_text()
        date_match = re.search(r'_(\d{8})_', file_name)
        if date_match:
            date_str = date_match.group(1)
            return f"{date_str[0:4]}.{date_str[4:6]}.{date_str[6:8]}"
    return None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_date(soup):
    file_name_tag = soup.find("fileName")
    if file_name_tag:
        file_name = file_name_tag.get_text()
        date_match = re.search(r'_(\d{8})_', file_name)
        if date_match:
            date_str = date_match.group(1)
            return f"{date_str[0:4]}.{date_str[4:6]}.{date_str[6:8]}"
    return None

def find_ancestor(element, tag_name, levels):
    for _ in range(levels):
        if element.parent:
            element = element.find_parent(tag_name)
        else:
            return None
    return element

def extract_matching_content(soup, query):
    matching_content = []
    words = query.split()
    pattern = re.compile('|'.join(re.escape(word) for word in words), re.IGNORECASE)
    
    # Find all TextLine elements that match the pattern
    text_lines = soup.find_all("TextLine")
    
    for text_line in text_lines:
        if pattern.search(text_line.get_text()):
            # Traverse up to the printspace (great-great-grandparent)
            printspace = find_ancestor(text_line, "PrintSpace", 3)  # Adjust levels as per the hierarchy
            if printspace:
                block_content = []
                composed_blocks = printspace.find_all("ComposedBlock")
                for composed_block in composed_blocks:
                    text_blocks = composed_block.find_all("TextBlock")
                    for text_block in text_blocks:
                        paragraphs = " ".join(string["CONTENT"] for string in text_block.find_all("String") if "CONTENT" in string.attrs)
                        block_content.append(paragraphs)
                matching_content.append("\n\n".join(block_content))
    
    return matching_content