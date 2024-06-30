
# Initialize SQLite database
db_name = f"{config['database_name']}.db"
conn = create_database(db_name)
cursor = conn.cursor()

# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS venues (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS newspaper_data (
        id INTEGER PRIMARY KEY,
        venue_id INTEGER,
        query_date DATE,
        newspaper TEXT,
        content TEXT,
        FOREIGN KEY (venue_id) REFERENCES venues (id)
    )
''')

# Add the new table creation here
cursor.execute('''
    CREATE TABLE IF NOT EXISTS newspaper_queries (
        id INTEGER PRIMARY KEY,
        query TEXT,
        from_date DATE,
        to_date DATE,
        result TEXT,
        llm_prompt TEXT
    )
''')
# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS venues (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS newspaper_data (
        id INTEGER PRIMARY KEY,
        venue_id INTEGER,
        query_date DATE,
        newspaper TEXT,
        content TEXT,
        FOREIGN KEY (venue_id) REFERENCES venues (id)
    )
''')

# Add the new table creation here
cursor.execute('''
    CREATE TABLE IF NOT EXISTS newspaper_queries (
        id INTEGER PRIMARY KEY,
        query TEXT,
        from_date DATE,
        to_date DATE,
        result TEXT,
        llm_prompt TEXT
    )
''')

def create_database(config):
    db_path = config['database_name']
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create newspaper_queries table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS newspaper_queries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT NOT NULL,
        from_date TEXT NOT NULL,
        to_date TEXT NOT NULL,
        newspaper TEXT NOT NULL,
        result TEXT NOT NULL,
        llm_prompt TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create venue_list table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS venue_list (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Lokal TEXT NOT NULL
    )
    ''')
    
    conn.commit()
    return conn


def initialize_venue_list(conn, config):
    venue_list_path = config['venue_list']
    df = pd.read_excel(venue_list_path)
    
    cursor = conn.cursor()
    
    # Clear existing venue list
    cursor.execute('DELETE FROM venue_list')
    
    # Insert new venue list
    for _, row in df.iterrows():
        cursor.execute('INSERT INTO venue_list (Lokal) VALUES (?)', (row['Lokal'],))
    
    conn.commit()

def get_config_value(config, key, default_value):
    value = config.get(key, default_value)
    if isinstance(default_value, str) and not isinstance(value, str):
        # If we expect a string but got something else, use the default
        return default_value
    return value

# Function to convert a DataFrame row to JSON
def row_to_json(data, config):
    date = data.get('Date', 'Unknown Date')
    system_message_content = read_system_message(config['prompt_filepath'], date)
    system_message = {"role": "system", "content": system_message_content}
    
    user_content_parts = [str(data[col]) for col in data if col not in config.get('excluded_columns', [])]
    user_message = {"role": "user", "content": " ".join(user_content_parts)}
    
    # Generate a unique ID using UUID
    custom_id = f"{data.get('Package ID', 'Unknown')}-{data.get('Part', 'Unknown')}-{data.get('Page', 'Unknown')}-{uuid.uuid4().hex[:8]}"
    
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": config.get('llm_model', "gpt-3.5-turbo"),
            "messages": [system_message, user_message],
            "max_tokens": config.get('max_tokens', 1000)
        }
    }
def fetch_newspaper_data(query, from_date, to_date, newspaper, config, db_connection):
    newspaper_dict = config['newspaper_ids']
    collection_id = newspaper_dict.get(newspaper)
    if not collection_id:
        return {"success": False, "error": "Invalid newspaper name provided"}

    result = search_swedish_newspapers(to_date, from_date, collection_id, query)
    if 'error' in result:
        return {"success": False, "error": result['error'], "message": result.get('message', 'Unknown error')}

    detailed_info = extract_urls(result)
    all_data = []
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
                    for block in matching_composed_blocks:
                        all_data.append({
                            "ComposedBlock Content": block,
                            "Date": date,
                            "Package ID": info['package_id'],
                            "Part": info['part_number'],
                            "Page": page_number
                        })
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")

    if all_data:
        cursor = db_connection.cursor()
        for data_item in all_data:
            llm_prompt = row_to_json(data_item, config)
            cursor.execute('''
                INSERT INTO newspaper_queries (query, from_date, to_date, newspaper, result, llm_prompt)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (query, from_date, to_date, newspaper, json.dumps(data_item), json.dumps(llm_prompt)))
        db_connection.commit()
        return {"success": True, "message": "Data processing completed and stored in database."}
    else:
        return {"success": False, "error": "No data found", "message": "No matching data found for the query."}