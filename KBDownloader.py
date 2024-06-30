import re
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup as bs
import time
import backoff
from urllib.parse import quote_plus

"""
Function to fetch and process data from Swedish newspapers.

Parameters:
- query (str): The search query to be used.
- from_date (str): The start date for the search in 'YYYY-MM-DD' format.
- to_date (str): The end date for the search in 'YYYY-MM-DD' format.
- newspaper (str): The name of the newspaper to search in. Valid options are:
  'Dagens nyheter', 'Svenska Dagbladet', 'Aftonbladet', 'Dagligt Allehanda'.
- prompt_filepath (str): The file path to the LLM prompt configuration file.
- output_filepath (str): The base file path for the output files (without extension).

Returns:
- dict: A dictionary containing the result of the operation with keys:
  'success' (bool): Indicates if the operation was successful.
  'message' (str): Describes the result of the operation or any error encountered.

Example usage:
result = fetch_newspaper_data(
    query='Kungliga musikaliska akademien',
    from_date='1908-10-01',
    to_date='1908-12-31',
    newspaper='Svenska Dagbladet',
    prompt_filepath='oldtimey_touringbot_prompt_for_deployment.txt',
    output_filepath='extracted_data'
)
print(result)
"""


# Function to search Swedish newspapers
def search_swedish_newspapers(to_date, from_date, collection_id, query):
    base_url = 'https://data.kb.se/search'
    
    # Properly encode the query string
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
    response.raise_for_status()  # This will raise an HTTPError for bad responses
    
    try:
        return response.json()
    except ValueError:
        raise ValueError('Invalid JSON response')


# Function to extract URLs from the result
def extract_urls(result):
    base_url = 'https://data.kb.se'
    details = []
    for hit in result['hits']:
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
def fetch_xml_content(xml_urls, max_retries=10, initial_delay=5):
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
                    
                    # If status code indicates rate limiting, wait before retrying
                    if response.status_code == 429:
                        retries += 1
                        print(f"Rate limited. Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                    else:
                        # For other status codes, break the retry loop
                        break
            
            except requests.exceptions.RequestException as e:
                # Handle other possible exceptions such as network errors
                print(f"Exception occurred while fetching {url}: {e}")
                retries += 1
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
    
    return xml_content_by_page

# Function to read system message from a file
def read_system_message(filepath, newspaper_date="date not known"):
    try:
        with open(filepath, 'r') as file:
            content = file.read().strip()
        return content.replace('{Newspaper_Date}', newspaper_date)
    except FileNotFoundError:
        return "You are a helpful assistant."

# Function to convert a DataFrame row to JSON
def row_to_json(row, prompt_filepath, counter):
    counter += 1
    date = row['Date']
    system_message_content = read_system_message(prompt_filepath, date)
    system_message = {"role": "system", "content": system_message_content}
    user_content_parts = [str(row[col]) for col in row.index if col not in ['System Content', 'Package ID', 'Date', 'Part', 'Page']]
    user_message = {"role": "user", "content": " ".join(user_content_parts)}
    custom_id = f"{row['Package ID']}-{row['Part']}-{row['Page']}-{counter}"
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-3.5-turbo-0125",
            "messages": [system_message, user_message],
            "max_tokens": 1000
        }
    }

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
def get_config_value(config, key, default_value):
    value = config.get(key, default_value)
    if isinstance(default_value, str) and not isinstance(value, str):
        # If we expect a string but got something else, use the default
        return default_value
    return value

# Main function
def fetch_newspaper_data(query, from_date, to_date, newspaper, prompt_filepath, output_filepath):
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
    all_data_frames = []
    page_ids = [info['page_id'] for info in detailed_info]

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
                    df = pd.DataFrame(matching_composed_blocks, columns=["ComposedBlock Content"])
                    df['Date'] = date
                    df['Package ID'] = info['package_id']
                    df['Part'] = info['part_number']
                    df['Page'] = page_number
                    all_data_frames.append(df)
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")

    if all_data_frames:
        final_df = pd.concat(all_data_frames, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=["ComposedBlock Content"])
        if not final_df.empty:
            final_df.to_excel(f"{output_filepath}.xlsx", index=False)
            with open(f"{output_filepath}.jsonl", 'w', encoding='utf-8') as jsonl_file:
                for _, row in final_df.iterrows():
                    json_row = row_to_json(row, prompt_filepath, counter)
                    jsonl_file.write(json.dumps(json_row) + '\n')
            return {"success": True, "message": "Data processing and export completed successfully."}
        else:
            return {"success": False, "message": "No data to export after aggregation."}
    else:
        return {"success": False, "message": "No data to export. The list of data frames is empty."}
