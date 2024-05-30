import mysql.connector
import csv
import anthropic
import re
import time
import contextlib
with contextlib.redirect_stdout(None):
    import pygame
import altair as alt
from sqlalchemy import create_engine


def create_connection(host, user, password, database):
    conn = None
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print(f"\033[94mConnected to the MySQL database: {database}")
    except mysql.connector.Error as e:
        print(f"\033[91mError connecting to the database: {e}")
    return conn


def create_table(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY)")
        print(f"\033[94mTable '{table_name}' created successfully.")
    except mysql.connector.Error as e:
        print(f"\033[91mError creating table: {e}")


def insert_data_from_csv(conn, table_name, csv_file):
    try:
        with open(csv_file, 'r') as file:
            csv_data = csv.reader(file)
            headers = next(csv_data)
            
            # Add columns to the table based on CSV headers
            cursor = conn.cursor()
            for header in headers:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {header} VARCHAR(255)")
            
            # Insert data rows into the table
            for row in csv_data:
                placeholders = ','.join(['%s'] * len(row))
                cursor.execute(f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})", row)
            
            conn.commit()
            print(f"\033[94mData inserted from {csv_file} into table '{table_name}' successfully.")
    except mysql.connector.Error as e:
        print(f"\033[91mError inserting data: {e}")


def get_table_metadata(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        metadata = cursor.fetchall()
        return metadata
    except mysql.connector.Error as e:
        print(f"\033[91mError retrieving table metadata: {e}")
        return None


def generate_questions(client, table_name, metadata):
    prompt = f"Table: {table_name}\n\nMetadata:\n"
    for column in metadata:
        prompt += f"{column[0]} ({column[1]})\n"
    prompt += "\nGenerate 3 interesting questions that can be answered using this table with no explanations. ONLY PROVIDE THE QUESTIONS"

    print("Generating questions...")
    questions = []
    with client.messages.stream(
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
        model="claude-3-opus-20240229",
    ) as stream:
        content = ""
        for event in stream:
            if event.type == "content_block_delta":
                content += event.delta.text
                print(event.delta.text, end="", flush=True)
            elif event.type == "content_block_stop":
                content += "\n"
                print()

    questions = re.split(r'\n\d+\.\s', content)
    if questions[0].strip().startswith("Here are 5 interesting questions"):
        questions.pop(0)  

    # Further clean up and adjust formatting if necessary
    questions = [question.strip() for question in questions if question.strip()]

    return questions


def generate_sql_query(client, question, table_name, metadata):
    print()
    prompt = f"Table: {table_name}\n\nMetadata:\n"
    for column in metadata:
        prompt += f"{column[0]} ({column[1]})\n"
    prompt += f"\nQuestion: {question}\n\nGenerate a SQL query to answer this question. Return only the SQL query without any explanations or additional text. Enclose the SQL query within triple backticks (```sql ... ```)."

    sql_query = ""
    with client.messages.stream(
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
        model="claude-3-opus-20240229",
    ) as stream:
        for event in stream:
            if event.type == "content_block_delta":
                sql_query += event.delta.text
                print("\033[93m{}\033[0m".format(event.delta.text), end="", flush=True)
    print() 

    # Extract only the SQL query from the generated response
    sql_query = extract_sql_query(sql_query)

    return sql_query.strip()


def extract_sql_query(text):
    sql_parts = text.split("```sql")
    if len(sql_parts) > 1:
        sql_query = sql_parts[1].split("```")[0].strip()
    else:
        # If the expected format is not found, assume the entire text is the SQL query
        sql_query = text.strip()
    return sql_query


def execute_sql_query(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as e:
        print(f"\033[91mError executing SQL query: {e}")
        return None


def generate_visualization_code(client, prompt, table_name, metadata, host, user, password, database):
    print()
    context = f"Table: {table_name}\n\nMetadata:\n"
    for column in metadata:
        context += f"{column[0]} ({column[1]})\n"
    context += f"\nPrompt: {prompt}\n\nYOUR JOB IS TO ONLY GENERATE ALTAIR VISUALIZATION CODE. Generate the code based on the prompt using the data from the MySQL database. Fetch the necessary data using SQL queries and convert it into a Pandas DataFrame before creating the Altair chart. Return only the code without any explanations or additional text. Enclose the code within triple backticks (```python ... ```). Use the following database connection details:\n\nHost: {host}\nUser: {user}\nPassword: {password}\nDatabase: {database}\n\nUse `chart.serve()` instead of `chart.show()` to display the chart."

    code = ""
    with client.messages.stream(
        max_tokens=1024,
        messages=[{"role": "user", "content": context}],
        model="claude-3-opus-20240229",
    ) as stream:
        for event in stream:
            if event.type == "content_block_delta":
                code += event.delta.text
                print("\033[93m{}\033[0m".format(event.delta.text), end="", flush=True)
    print()

    return code.strip()


def extract_visualization_code(text):
    code_parts = text.split("```python")
    if len(code_parts) > 1:
        code = code_parts[1].split("```")[0].strip()
    else:
        code = text.strip()
    return code


def execute_visualization_code(code, host, user, password, database):
    try:
        # Create a SQLAlchemy engine for the database connection
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
        exec(code, {'alt': alt, 'engine': engine})
    except Exception as e:
        print(f"\033[91mError executing visualization code: {e}\033[0m")  # Red color for error message
        

def conversation_loop(client, table_name, metadata, conn):
    print()
    print("\033[92mWelcome to the conversational interface!")
    print("You can ask questions about the dataset and get answers based on the table metadata.")
    print("Type 'exit' to end the conversation.")
    print("To directly generate an SQL query, start your prompt with '-t'.")
    print("To generate a visualization using Altair, start your prompt with '-v'.\033[0m")

    # Create the table metadata context
    metadata_context = f"Table: {table_name}\n\nMetadata:\n"
    for column in metadata:
        metadata_context += f"{column[0]} ({column[1]})\n"

    while True:
        print()
        user_input = input("\033[96mUser: ")
        if user_input.lower() == 'exit':
            break

        if user_input.startswith('-t'):
            # Generate and execute SQL query directly
            question = user_input[2:].strip()
            sql_query = generate_sql_query(client, question, table_name, metadata)
            results = execute_sql_query(conn, sql_query)
            print("\033[93mResults: {}\033[0m".format(results))
            print()
        elif user_input.startswith('-v'):
            # Generate and execute visualization code
            prompt = user_input[2:].strip()
            code = generate_visualization_code(client, prompt, table_name, metadata, host, user, password, database)
            extracted_code = extract_visualization_code(code)
            execute_visualization_code(extracted_code, host, user, password, database)
        else:
            # Normal conversation loop with table metadata context and streaming
            print()
            print("\033[95mAssistant: \033[0m", end="", flush=True)
            with client.messages.stream(
                model="claude-3-opus-20240229",
                system=f"You are an expert Data Analyst. Your job is to support the user for all of their data related tasks. The user has access to tools that generate SQL queries and Altair visualizations using natural language prompts. THERE IS NO NEED TO PROVIDE THE USER WITH SQL CODE OR VISUALIZATION CODE. IF NEEDED, PROVIDE THEM WITH NATURAL LANGUAGE PROMPTS FOR THE '-t' OR '-v' COMMANDS. Please provide clear and concise responses.\n\n{metadata_context}",
                max_tokens=1024,
                messages=[
                    {"role": "user", "content": user_input}
                ],
            ) as stream:
                for event in stream:
                    if event.type == "content_block_delta":
                        content_delta = event.delta.text
                        print("\033[95m{}\033[0m".format(content_delta), end="", flush=True)
            print()


def animate_ascii_art(art_file, sound_file, delay=0.5):
    pygame.mixer.init()

    sound = pygame.mixer.Sound(sound_file)

    print()

    with open(art_file, "r") as file:
        ascii_art = file.read()

    delay_chars = int(len(ascii_art) * delay)

    # Animate the ASCII art
    for i, char in enumerate(ascii_art):
        print("\033[95m{}\033[0m".format(char), end="", flush=True)
        time.sleep(0.003)  # Adjust the delay as needed

        if i == delay_chars:
            sound.play()

    while pygame.mixer.get_busy():
        pass

    pygame.mixer.quit()
    print("\n" * 2)

# Connection Credentials
host = "your-host"
user = "your-user"
password = "your-password"
database = "your-DB"
csv_file = "your-csv-path"

animate_ascii_art("REMI_BOT.txt", "zelda_secret.mp3", delay=.99)

# Prompt the user for the table name
table_name = input("\033[94mEnter the desired table name: ")

# Anthropic API client
client = anthropic.Anthropic(api_key="your-api-key")

conn = create_connection(host, user, password, database)
if conn:
    create_table(conn, table_name)
    insert_data_from_csv(conn, table_name, csv_file)

    # Retrieve table metadata
    metadata = get_table_metadata(conn, table_name)

    conversation_loop(client, table_name, metadata, conn)  # Pass the conn object

    conn.close()
