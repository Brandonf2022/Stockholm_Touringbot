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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Keep track of the last request time
last_request_time = None

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

    def paragraph_from_keyword(self, keyword):
        token = self.soup.find("String", attrs={"CONTENT": keyword})
        yield self.token_to_paragraph(token)
        while (
            token := token.find_next("String", attrs={"CONTENT": keyword})
        ) is not None:
            yield self.token_to_paragraph(token)

    def token_to_paragraph(self, token):
        line_tags = token.parent.parent.find_all("TextLine")
        leading_tokens = (line_tag.find("String") for line_tag in line_tags)
        result = ""
        for leading_token in leading_tokens:
            sentence = self.token_to_sentence(leading_token)
            result += f"{sentence}\n"
        return result.strip()

    def article_from_keyword(self, keyword):
        # Split the keyword into individual words
        keywords = keyword.split()
        # Create a regex pattern that matches any of the words
        pattern = re.compile(r'\b(' + '|'.join(re.escape(word) for word in keywords) + r')\b', re.IGNORECASE)
        
        tokens = self.soup.find_all("String", attrs={"CONTENT": pattern})
        for token in tokens:
            article = self.token_to_article(token)
            if article:
                yield article
    
    def token_to_article(self, token):
        par_tags = token.parent.parent.parent.find_all("TextBlock")
        leading_tokens = (line_tag.find("String") for line_tag in par_tags)
        result = ""
        for leading_token in leading_tokens:
            paragraph = self.token_to_paragraph(leading_token)
            result += f"{paragraph}\n\n"
        return result.strip()

    def token_to_paragraph(self, token):
        if token is None:
            return None
        line_tags = token.find_parent("TextLine").find_next_siblings("TextLine")
        leading_tokens = [line_tag.find("String") for line_tag in line_tags if line_tag.find("String")]
        result = self.token_to_sentence(token) + "\n"
        for leading_token in leading_tokens:
            sentence = self.token_to_sentence(leading_token)
            if sentence:
                result += f"{sentence}\n"
        return result.strip()

    def token_to_sentence(self, token):
        if token is None:
            return None
        return " ".join(s.get('CONTENT', '') for s in token.find_parent("TextLine").find_all("String"))

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

# New function to process and save data
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
        matching_composed_blocks = list(page.composed_block_from_keyword(query))

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
def fetch_newspaper_data(query, from_date, to_date, newspaper, config, db_path, kb_key, rate_limit):
    global last_request_time
    collection_id = newspaper
    total_rows_saved = 0
    RATE_LIMIT = rate_limit
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
                PRIMARY KEY ([ComposedBlock ID])
            )
        ''')
        conn.commit()
        logging.info("Table 'newspaper_data' created or already exists")
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

                    articles = list(page.article_from_keyword(query))
                    if not articles:
                        logging.info(f"No matching content found for query '{query}' on page {page_number}")
                        continue

                    for article in articles:
                        if article:
                            hash_content = hashlib.md5(article.encode('utf-8')).hexdigest()
                            composed_block_id = f"{info['package_id']}-{info['part_number']}-{page_number}-{hash_content}"

                            # Check if the composed_block_id already exists in the database
                            cursor.execute("SELECT COUNT(*) FROM newspaper_data WHERE [ComposedBlock ID] = ?", (composed_block_id,))
                            existing_count = cursor.fetchone()[0]

                            if existing_count == 0:
                                # Insert a new row if the composed_block_id doesn't exist
                                row_data = {
                                    'Date': date,
                                    '[Package ID]': info['package_id'],
                                    'Part': info['part_number'],
                                    'Page': page_number,
                                    '[ComposedBlock ID]': composed_block_id,
                                    '[ComposedBlock Content]': article,
                                    '[Raw API Result]': json.dumps(api_response)
                                }
                                full_prompt = row_to_json(row_data, config, total_rows_saved)

                                try:
                                    cursor.execute('''
                                        INSERT INTO newspaper_data
                                        (Date, [Package ID], Part, Page, [ComposedBlock ID], [ComposedBlock Content], [Raw API Result], [Full Prompt])
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    ''', (date, info['package_id'], info['part_number'], page_number, composed_block_id,
                                          article, json.dumps(api_response), full_prompt))

                                    total_rows_saved += 1
                                    logging.info(f"Inserted row {total_rows_saved} in database")
                                    logging.debug(f"Saved content: {article[:100]}...")  # Debug log, showing first 100 chars
                                except sqlite3.Error as e:
                                    logging.error(f"Failed to insert row in database: {e}")
                            else:
                                logging.info(f"Skipping existing entry with [ComposedBlock ID] '{composed_block_id}'")

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
