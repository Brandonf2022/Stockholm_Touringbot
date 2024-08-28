import re
import requests
import json
from bs4 import BeautifulSoup as bs
import time
import sqlite3
from urllib.parse import quote_plus
import os
import pickle
import hashlib
from urllib.parse import urljoin, urlencode
import logging
from contextlib import closing
import time
from sqlite3 import OperationalError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Keep track of the last request time
last_request_time = None

def retry_on_db_lock(func, max_attempts=5, delay=1):
    for attempt in range(max_attempts):
        try:
            return func()
        except OperationalError as e:
            if "database is locked" in str(e) and attempt < max_attempts - 1:
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise
    raise Exception("Max retry attempts reached")


def retry_with_backoff(func, max_attempts=5, initial_wait=1, backoff_factor=2):
    def wrapper(*args, **kwargs):
        attempts = 0
        wait_time = initial_wait
        while attempts < max_attempts:
            try:
                return func(*args, **kwargs)
            except OperationalError as e:
                if "database is locked" in str(e):
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    logging.warning(f"Database locked. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                else:
                    raise
    return wrapper

import sqlite3

@retry_with_backoff
def insert_batch_with_transaction(db_path, data_list):
    with sqlite3.connect(db_path) as conn:
        try:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR IGNORE INTO newspaper_data
                (Date, [Package ID], Part, Page, [ComposedBlock ID], [ComposedBlock Content], [Raw API Result], [Full Prompt])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_list)
            conn.commit()
            return len(data_list)  # Return number of rows inserted
        except sqlite3.Error as e:
            conn.rollback()
            raise

def insert_batch(conn, data_list):
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO newspaper_data
        (Date, [Package ID], Part, Page, [ComposedBlock ID], [ComposedBlock Content], [Raw API Result], [Full Prompt])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', data_list)
    conn.commit()

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


def extract_xml_urls(api_response, page_ids, kb_key=None):
    xml_urls = {}
    base_url = 'https://data.kb.se'
    parts_list = api_response.get('hasPart', [])
    for part in parts_list:
        pages = part.get('hasPartList', [])
        for page in pages:
            if page['@id'] in page_ids:
                includes = page.get('includes', [])
                for include in includes:
                    if 'alto.xml' in include['@id']:
                        page_number = int(page['@id'].split('/')[-1].replace('page', ''))
                        xml_url = urljoin(base_url, include['@id'])
                        if kb_key:
                            query_params = urlencode({'api_key': kb_key})
                            xml_url = f"{xml_url}?{query_params}"
                        xml_urls[page_number] = xml_url
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

    def article_from_keyword(self, keyword, num_blocks=5):
        # Split the keyword into individual words
        keywords = keyword.split()
        # Create a regex pattern that matches any of the words
        pattern = re.compile(r'\b(' + '|'.join(re.escape(word) for word in keywords) + r')\b', re.IGNORECASE)
        
        tokens = self.soup.find_all("String", attrs={"CONTENT": pattern})
        for token in tokens:
            composed_block = token.find_parent("ComposedBlock")
            if composed_block:
                article = self.composed_block_to_text(composed_block)
                # Get previous and next articles
                prev_articles = self.get_sibling_composed_blocks_text(composed_block, direction='previous', count=num_blocks)
                next_articles = self.get_sibling_composed_blocks_text(composed_block, direction='next', count=num_blocks)
                
                # Concatenate the articles
                full_article = (prev_articles + "\n" if prev_articles else "") + article + (("\n" + next_articles) if next_articles else "")
                yield full_article
    
    def get_sibling_composed_blocks_text(self, composed_block, direction='next', count=1):
        siblings = []
        current = composed_block
        for _ in range(count):
            sibling = current.find_next_sibling("ComposedBlock") if direction == 'next' else current.find_previous_sibling("ComposedBlock")
            if sibling:
                siblings.append(sibling)
                current = sibling
            else:
                break
        return "\n".join(self.composed_block_to_text(sib) for sib in siblings)

    def composed_block_to_text(self, composed_block):
        if composed_block is None:
            return None
        text_blocks = composed_block.find_all("TextBlock")
        result = ""
        for text_block in text_blocks:
            paragraph = self.text_block_to_paragraph(text_block)
            result += f"{paragraph}\n\n"
        return result.strip()

    def text_block_to_paragraph(self, text_block):
        if text_block is None:
            return None
        strings = text_block.find_all("String")
        return " ".join(s.get('CONTENT', '') for s in strings)

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
                checkpoint = pickle.load(f)
            if isinstance(checkpoint, dict) and all(key in checkpoint for key in ['year', 'half', 'index']):
                print(f"Checkpoint loaded: Year {checkpoint['year']}, Half {checkpoint['half']}, Index {checkpoint['index']}")
                return checkpoint
            else:
                print("Checkpoint file is invalid. Starting from the beginning.")
                os.remove('checkpoint.pkl')  # Remove the invalid checkpoint file
        except (EOFError, pickle.UnpicklingError):
            print("Checkpoint file is corrupted or empty. Starting from the beginning.")
            os.remove('checkpoint.pkl')  # Remove the corrupted file
        except Exception as e:
            print(f"An error occurred while loading the checkpoint: {str(e)}")
    return None

import sqlite3
import json

def process_and_save_url(url_info, config, db_path, kb_key, rate_limit, num_composed_blocks, max_attempts=5, initial_wait=1, backoff_factor=2):
    attempts = 0
    wait_time = initial_wait

    while attempts < max_attempts:
        try:
            result = fetch_newspaper_data(
                query=url_info['query'],
                from_date=url_info['from_date'],
                to_date=url_info['to_date'],
                newspaper=config['newspaper'],
                config=config,
                db_path=db_path,
                kb_key=kb_key,
                rate_limit=rate_limit,
                num_composed_blocks=num_composed_blocks
            )

            if result.get('success'):
                rows_inserted = result.get('rows_inserted', 0)
                if rows_inserted > 0:
                    logging.info(f"Successfully processed URL for query '{url_info['query']}'. Inserted {rows_inserted} rows.")
                else:
                    logging.info(f"Processed URL for query '{url_info['query']}' but no rows were inserted.")
                return True, rows_inserted
            else:
                logging.warning(f"Failed to process URL for query '{url_info['query']}': {result.get('message')}")
                raise Exception(result.get('message'))

        except Exception as e:
            attempts += 1
            if attempts < max_attempts:
                logging.warning(f"Error processing URL for query '{url_info['query']}'. Retrying in {wait_time} seconds... (Attempt {attempts}/{max_attempts})")
                logging.warning(f"Error details: {str(e)}")
                time.sleep(wait_time)
                wait_time *= backoff_factor
            else:
                logging.error(f"Failed to process URL for query '{url_info['query']}' after {max_attempts} attempts.")
                logging.error(f"Final error: {str(e)}")
                return False, 0

    return False, 0      
#  function to process and save data
def process_and_save_data(xml_content_by_page, info, query, config, db_path, kb_key):
    # Establish database connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create a dictionary to hold combined results
    combined_results = {}

    # Process each page's content and aggregate results
    for page_number, xml_content in xml_content_by_page.items():
        xml_string = xml_content.decode('utf-8')
        page = Page(xml_content=xml_string)
        date = page.extract_date()
        matching_composed_blocks = list(page.article_from_keyword(query))

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
        cursor.execute('''
            INSERT INTO newspaper_data
            (Date, [Package ID], Part, Page, [ComposedBlock Content], [Raw API Result])
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (value['Date'], value['Package ID'], value['Part'], value['Page'],
              json.dumps(value['ComposedBlock Content']), value['Raw API Result']))

    # Commit changes and close connection
    conn.commit()
    conn.close()

    return {"success": True, "message": f"Data processing completed. {len(combined_results)} rows saved to the database."}

# Function to fetch and process data from URLs

import requests
import json
import logging
import time
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
import hashlib

def fetch_newspaper_data(query, from_date, to_date, newspaper, config, db_path, kb_key, rate_limit, num_composed_blocks):
    logging.info(f"Starting fetch_newspaper_data for query: {query}, dates: {from_date} to {to_date}")
    
    total_rows_inserted = 0
    RATE_LIMIT = rate_limit
    last_request_time = None

    try:
        search_results = search_swedish_newspapers(to_date, from_date, newspaper, query)
        logging.info(f"Search results received. Hits: {len(search_results.get('hits', []))}")
    except requests.HTTPError as e:
        logging.error(f"Failed to fetch search results: {e}")
        return {"success": False, "message": f"Failed to fetch search results: {e}", "rows_inserted": 0}

    urls = extract_urls(search_results)
    logging.info(f"Extracted {len(urls)} URLs from search results")

    batch = []
    batch_size = 100

    for info in urls:
        url = info['url']
        page_id = info['page_id']

        # Rate limiting logic
        current_time = time.time()
        if last_request_time is not None:
            elapsed_time = current_time - last_request_time
            if elapsed_time < 1 / RATE_LIMIT:
                time.sleep(1 / RATE_LIMIT - elapsed_time)
        last_request_time = time.time()

        logging.info(f"Processing URL: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            api_response = response.json()

            xml_urls = extract_xml_urls(api_response, [page_id], kb_key)
            logging.info(f"Extracted {len(xml_urls)} XML URLs")

            xml_content_by_page = fetch_xml_content(xml_urls)
            logging.info(f"Fetched XML content for {len(xml_content_by_page)} pages")

            for page_number, xml_content in xml_content_by_page.items():
                xml_string = xml_content.decode('utf-8')
                page = Page(xml_content=xml_string)
                date = page.extract_date()

                articles = list(page.article_from_keyword(query, num_blocks=num_composed_blocks))
                if not articles:
                    logging.info(f"No matching content found for query '{query}' on page {page_number}")
                    continue

                for article in articles:
                    if article:
                        # Generate a unique hash for the article content
                        hash_content = hashlib.md5(article.encode('utf-8')).hexdigest()
                        composed_block_id = f"{info['package_id']}-{info['part_number']}-{page_number}-{hash_content}"

                        batch.append((
                            date,
                            info['package_id'],
                            info['part_number'],
                            page_number,
                            composed_block_id,
                            article,
                            json.dumps(api_response),
                            None  # Placeholder for [Full Prompt] which is no longer needed
                        ))

                        if len(batch) >= batch_size:
                            rows_inserted = insert_batch_with_transaction(db_path, batch)
                            total_rows_inserted += rows_inserted
                            batch = []
                            logging.info(f"Inserted batch of {rows_inserted} rows. Total rows inserted: {total_rows_inserted}")

            logging.info(f"Processed URL: {url}")

        except requests.HTTPError as e:
            logging.error(f"Failed to fetch data from {url}. Status code: {e.response.status_code}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error processing URL {url}: {str(e)}")
            continue

    # Insert any remaining rows in the batch
    if batch:
        rows_inserted = insert_batch_with_transaction(db_path, batch)
        total_rows_inserted += rows_inserted
        logging.info(f"Inserted final batch of {rows_inserted} rows. Total rows inserted: {total_rows_inserted}")

    logging.info(f"Data processing completed. Total rows saved: {total_rows_inserted}")
    return {"success": True, "message": f"Data processing completed. {total_rows_inserted} rows saved to the database.", "rows_inserted": total_rows_inserted}