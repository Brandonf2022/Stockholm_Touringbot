
"""
LLMDataProcessing.py

This module contains functions for processing JSONL files containing prompts,
interacting with the OpenAI API, and storing results in a SQLite database.
It is designed for use in digital humanities projects, particularly in
musicology research using historical newspaper data.

Main functionalities:
1. Database operations (create tables, fetch and store data)
2. JSONL file processing
3. OpenAI API interactions
4. Event data extraction and storage

Functions:
- create_db_tables(conn): Creates necessary tables in the SQLite database.
- get_checkpoint(conn, file_path): Retrieves the last processed line for a file.
- update_checkpoint(conn, file_path, last_processed_line): Updates the checkpoint for a file.
- extract_and_store_event_data(cursor, custom_id, json_response): Extracts event data from API response and stores it in the database.
- process_jsonl(file_path, db_conn): Processes a single JSONL file, interacting with the OpenAI API and storing results.
- process_all_jsonl_files(directory_path, db_conn): Processes all JSONL files in a directory.
- process_all_prompts(conn): Processes all prompts stored in the newspaper_data table.
- process_prompt(conn, row_id, prompt): Processes a single prompt, interacting with the OpenAI API and storing results.
- fetch_prompts_from_db(conn): Fetches JSON prompts from the newspaper_data table.
- save_results_to_db(conn, results): Saves API results to the Results table in the database.

Usage:
This script is designed to be run as part of a larger digital humanities workflow.
It assumes the existence of a SQLite database with specific tables and an OpenAI API client.

Note: Ensure that the OpenAI API key is properly set up in your environment before running this script.

Dependencies:
- openai
- sqlite3
- json
- logging
- os
- tqdm

Author: Brandon Farnsworth
Date: 01.07.2024

"""


def create_db_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS completions (
            id INTEGER PRIMARY KEY,
            custom_id TEXT UNIQUE,
            content TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS checkpoints (
            id INTEGER PRIMARY KEY,
            file_path TEXT UNIQUE,
            last_processed_line INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            custom_id TEXT UNIQUE,
            konsert_datum TEXT,
            konsert_namn TEXT,
            lokal_namn TEXT,
            konserttyp_namn TEXT,
            producer TEXT
        )
    ''')
    conn.commit()

def get_checkpoint(conn, file_path):
    cursor = conn.cursor()
    cursor.execute('SELECT last_processed_line FROM checkpoints WHERE file_path = ?', (file_path,))
    result = cursor.fetchone()
    return result[0] if result else 0

def update_checkpoint(conn, file_path, last_processed_line):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO checkpoints (file_path, last_processed_line)
        VALUES (?, ?)
    ''', (file_path, last_processed_line))
    conn.commit()

def extract_and_store_event_data(cursor, custom_id, json_response):
    try:
        event_data = json.loads(json_response)
        cursor.execute('''
            INSERT OR REPLACE INTO events 
            (custom_id, konsert_datum, konsert_namn, lokal_namn, konserttyp_namn, producer)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            custom_id,
            event_data.get('konsert_datum', ''),
            event_data.get('konsert_namn', ''),
            event_data.get('lokal_namn', ''),
            event_data.get('konserttyp_namn', ''),
            event_data.get('Producer', '')
        ))
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON for custom_id: {custom_id}")
    except Exception as e:
        logging.error(f"Error storing event data for custom_id {custom_id}: {e}")

def process_jsonl(file_path, db_conn):
    cursor = db_conn.cursor()
    last_processed_line = get_checkpoint(db_conn, file_path)
    
    try:
        with open(file_path, 'r') as file:
            # Skip to the last processed line
            for _ in range(last_processed_line):
                next(file)
            
            # Count remaining lines for progress bar
            remaining_lines = sum(1 for _ in file) - last_processed_line
            file.seek(0, 0)  # Reset file pointer
            for _ in range(last_processed_line):
                next(file)
            
            for current_line, line in tqdm(enumerate(file, start=last_processed_line), total=remaining_lines, desc="Processing lines"):
                try:
                    data = json.loads(line.strip())
                    messages = data['body']['messages']
                    custom_id = data.get('custom_id', f"line_{current_line}")
                    
                    # Check if this custom_id has already been processed
                    cursor.execute('SELECT id FROM completions WHERE custom_id = ?', (custom_id,))
                    if cursor.fetchone():
                        logging.info(f"Skipping already processed custom_id: {custom_id}")
                        continue
                    
                    completion = client.chat.completions.create(
                        model='gpt-3.5-turbo',
                        response_format={"type": "json_object"},
                        messages=messages,
                        max_tokens=data['body']['max_tokens']
                    )
                    
                    json_response = completion.choices[0].message.content
                    
                    # Store the result in the completions table
                    cursor.execute('INSERT INTO completions (custom_id, content) VALUES (?, ?)',
                                   (custom_id, json_response))
                    
                    # Extract and store event data
                    extract_and_store_event_data(cursor, custom_id, json_response)
                    
                    # Update checkpoint every 10 lines
                    if current_line % 10 == 0:
                        update_checkpoint(db_conn, file_path, current_line)
                        db_conn.commit()
                    
                except json.JSONDecodeError:
                    logging.error(f"Error decoding JSON at line {current_line}")
                except Exception as e:
                    logging.error(f"Error during API call at line {current_line}: {e}")
                
            # Final checkpoint update
            update_checkpoint(db_conn, file_path, current_line)
            db_conn.commit()
            
    except IOError as e:
        logging.error(f"Error opening or reading the file: {file_path}. Error: {e}")

def process_all_jsonl_files(directory_path, db_conn):
    jsonl_files = [f for f in os.listdir(directory_path) if f.endswith('.jsonl')]
    for file_name in tqdm(jsonl_files, desc="Processing files"):
        file_path = os.path.join(directory_path, file_name)
        try:
            process_jsonl(file_path, db_conn)
        except Exception as e:
            logging.error(f"Error processing file {file_name}: {e}")

## new SQL functions below ##
def process_all_prompts(conn):
    cursor = conn.cursor()
    try:
        # Fetch all prompts from the newspaper_data table
        cursor.execute('SELECT rowid, [Full Prompt] FROM newspaper_data')
        prompts = cursor.fetchall()
        
        for row_id, prompt in tqdm(prompts, desc="Processing prompts"):
            try:
                process_prompt(conn, row_id, prompt)
            except Exception as e:
                logging.error(f"Error processing prompt with row_id {row_id}: {e}")
        
        logging.info(f"Processed {len(prompts)} prompts from the database.")
    except sqlite3.Error as e:
        logging.error(f"Database error while fetching prompts: {e}")
def process_prompt(conn, row_id, prompt):
    try:
        data = json.loads(prompt)
        messages = data['body']['messages']
        custom_id = data.get('custom_id', f"row_{row_id}")
        
        cursor = conn.cursor()
        
        # Check if this custom_id has already been processed
        cursor.execute('SELECT id FROM completions WHERE custom_id = ?', (custom_id,))
        if cursor.fetchone():
            logging.info(f"Skipping already processed custom_id: {custom_id}")
            return
        
        completion = client.chat.completions.create(
            model='gpt-3.5-turbo',
            response_format={"type": "json_object"},
            messages=messages,
            max_tokens=data['body']['max_tokens']
        )
        
        json_response = completion.choices[0].message.content
        
        # Store the result in the completions table
        cursor.execute('INSERT INTO completions (custom_id, content) VALUES (?, ?)',
                       (custom_id, json_response))
        
        # Extract and store event data
        extract_and_store_event_data(cursor, custom_id, json_response)
        
        conn.commit()
        
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON for row_id {row_id}")
    except Exception as e:
        logging.error(f"Error during API call for row_id {row_id}: {e}")

def extract_and_store_event_data(cursor, custom_id, json_response):
    try:
        event_data = json.loads(json_response)
        cursor.execute('''
            INSERT OR REPLACE INTO events 
            (custom_id, konsert_datum, konsert_namn, lokal_namn, konserttyp_namn, producer)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            custom_id,
            event_data.get('konsert_datum', ''),
            event_data.get('konsert_namn', ''),
            event_data.get('lokal_namn', ''),
            event_data.get('konserttyp_namn', ''),
            event_data.get('Producer', '')
        ))
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON for custom_id: {custom_id}")
    except Exception as e:
        logging.error(f"Error storing event data for custom_id {custom_id}: {e}")



def create_db_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS completions (
            id INTEGER PRIMARY KEY,
            custom_id TEXT UNIQUE,
            content TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            custom_id TEXT UNIQUE,
            konsert_datum TEXT,
            konsert_namn TEXT,
            lokal_namn TEXT,
            konserttyp_namn TEXT,
            producer TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt TEXT NOT NULL,
            result TEXT
        )
    ''')
    conn.commit()

def fetch_prompts_from_db(conn):
    """Fetch JSON prompts from the newspaper_data table."""
    fetch_sql = "SELECT [Full Prompt] FROM newspaper_data"
    try:
        cursor = conn.cursor()
        cursor.execute(fetch_sql)
        prompts = cursor.fetchall()
        logging.info(f"Fetched {len(prompts)} prompts from the database.")
        return [prompt[0] for prompt in prompts]
    except sqlite3.Error as e:
        logging.error(f"Failed to fetch prompts: {e}")
        return []

def save_results_to_db(conn, results):
    """Save results to the Results table in the database."""
    insert_sql = "INSERT INTO Results (prompt, result) VALUES (?, ?)"
    try:
        cursor = conn.cursor()
        cursor.executemany(insert_sql, results)
        conn.commit()
        logging.info(f"Saved {len(results)} results to the database.")
    except sqlite3.Error as e:
        logging.error(f"Failed to save results: {e}")
