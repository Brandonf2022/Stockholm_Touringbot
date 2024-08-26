
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
import sqlite3
import json
import logging
import os
from tqdm import tqdm
from openai import OpenAI

#This one has performers column
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
            custom_id TEXT PRIMARY KEY,
            date TEXT,
            name TEXT,
            venue TEXT,
            organizer TEXT,
            performers TEXT,
            programme TEXT,
            reasoning_steps TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reasoning_steps (
            custom_id TEXT PRIMARY KEY,
            reasoning_steps TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
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
        response_data = json.loads(json_response)
        concerts = response_data.get('Concerts', [])
        reasoning_steps = response_data.get('ReasoningSteps', [])

        # Store each concert
        for concert in concerts:
            cursor.execute('''
                INSERT OR REPLACE INTO events 
                (custom_id, date, name, venue, organizer, performers, programme)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                custom_id,
                concert.get('date', ''),
                concert.get('name', ''),
                concert.get('venue', ''),
                concert.get('organizer', ''),
                ', '.join(concert.get('performers', [])),  # Convert list to string
                concert.get('programme', '')
            ))

        # Store reasoning steps
        for step in reasoning_steps:
            cursor.execute('''
                INSERT INTO reasoning_steps (custom_id, reasoning_steps)
                VALUES (?, ?)
            ''', (custom_id, json.dumps(step)))  # Store each step as a JSON string

        cursor.connection.commit()
        logging.info(f"Stored concert and reasoning data for custom_id: {custom_id}")

    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON for custom_id: {custom_id}")
    except Exception as e:
        logging.error(f"Error storing event data for custom_id {custom_id}: {e}")

def process_all_prompts(conn, client):
    cursor = conn.cursor()
    try:
        # Fetch all prompts from the newspaper_data table
        cursor.execute('SELECT rowid, [Full Prompt] FROM newspaper_data')
        prompts = cursor.fetchall()
        
        for row_id, prompt in tqdm(prompts, desc="Processing prompts"):
            try:
                process_prompt(conn, client, row_id, prompt)
            except Exception as e:
                logging.error(f"Error processing prompt with row_id {row_id}: {e}")
        
        logging.info(f"Processed {len(prompts)} prompts from the database.")
    except sqlite3.Error as e:
        logging.error(f"Database error while fetching prompts: {e}")

import json
import logging

def process_prompt(conn, client, row_id, prompt):
    try:
        logging.info(f"Processing prompt for row_id {row_id}: {prompt}")
        
        # Parse the JSON string
        prompt_data = json.loads(prompt)
        logging.info("Loaded JSON data successfully.")
        logging.info(f"Prompt data type: {type(prompt_data)}")
        logging.info(f"Prompt data content: {json.dumps(prompt_data, indent=2)}")

        # Extract the necessary data from the prompt
        body = prompt_data['body']
        model = body['model']
        messages = body['messages']
        response_format = body['response_format']

        # Ensure messages is a list of dictionaries
        if not isinstance(messages, list) or not all(isinstance(message, dict) for message in messages):
            logging.error(f"Invalid format for messages in prompt for row_id {row_id}")
            return
        
        # Generate a unique custom_id using row_id, Package ID, Part, and Page
        cursor = conn.cursor()
        cursor.execute("SELECT [Package ID], Part, Page FROM newspaper_data WHERE rowid = ?", (row_id,))
        package_id, part, page = cursor.fetchone()
        custom_id = f"{package_id}-{part}-{page}-{row_id}"

        # Prepare the request payload
        request_payload = {
            "model": model,
            "messages": messages,
            "response_format": response_format
        }
        
        logging.info("Prepared request payload.")

        # Log the query being sent to the API
        logging.info("QUERY:")
        logging.info(json.dumps(request_payload, indent=4))

        # Make the API call using the extracted settings
        completion = client.chat.completions.create(**request_payload)

        # Log the full API response for debugging
        json_response = completion.choices[0].message.content
        logging.info("RESPONSE:")
        logging.info(json.dumps(json.loads(json_response), indent=4))

        # Store the result in the completions table
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO completions (custom_id, content)
            VALUES (?, ?)
        ''', (custom_id, json_response))

        conn.commit()

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON for row_id {row_id}: {e}")
        logging.error(f"JSON causing error: {prompt}")
    except Exception as e:
        logging.error(f"Error processing prompt for row_id {row_id}: {e}")
        logging.exception("Full traceback:")

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

def query_events(cursor, custom_id=None):
    if custom_id:
        cursor.execute("SELECT * FROM events WHERE custom_id = ?", (custom_id,))
    else:
        cursor.execute("SELECT * FROM events")

    rows = cursor.fetchall()
    events = []
    for row in rows:
        event = {
            'custom_id': row[0],
            'date': row[1],
            'name': row[2],
            'venue': row[3],
            'organizer': row[4],
            'performers': row[5].split(', ') if row[5] else [],  # Split performers string into a list
            'programme': row[6]
        }
        events.append(event)

    return events
