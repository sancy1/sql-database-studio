import mysql.connector
from mysql.connector import Error
import pandas as pd
import datetime

import csv
import os
import re


# Function to save connection details to a text file
def save_connection_details(host, user, password):
    with open("connection_details.txt", "w") as file:
        file.write(f"Host: {host}\n")
        file.write(f"User: {user}\n")
        file.write(f"Password: {password}\n")
    return host, user, password

        

# Function to load connection details from a text file
def load_connection_details():
    if os.path.exists("connection_details.txt"):
        with open("connection_details.txt", "r") as file:
            lines = file.readlines()
            host = lines[0].split(": ")[1].strip()
            user = lines[1].split(": ")[1].strip()
            password = lines[2].split(": ")[1].strip()
        return host, user, password
    else:
        return None
    

# Function to get user input for connection details
def get_connection_details():
    host = input("\nEnter host name: e.g 'localhost' :::")
    user = input("Enter User name: e.g 'root' :::")
    password = input("Enter Password :::")
    return host, user, password


# Main function to handle connection options
def handle_connection_options():
    connectOption = input("1. Enter 1 for default connection\n2. Enter 2 for new connection :::")
    
    # Option 1: Default connection
    if connectOption == '1':
        connection_details = load_connection_details()
        if connection_details:
            host, user, password = connection_details
        else:
            print("No default connection details found. Please enter new details.")
            host, user, password = get_connection_details()
            save_connection_details(host, user, password)
            
    
    # Option 2: New connection
    elif connectOption == '2':
        host, user, password = get_connection_details()
        save_connection_details(host, user, password)
    
    return host, user, password



# Prompt User For Database Name: -------------------------------------------------------------
def get_database_name():
    while True:
        database_name = input("Enter Database name :::")
        if not database_name:
            print("Database name cannot be empty. Exiting.")
            exit(1)
        return database_name

    
    
# Get CSV File From The User -------------------------------------------------------------
# def get_file_name():
#     while True:
#         file_name = str(input("\nEnter CSV File Name and Path (Enter 'q' to quit) :::"))
#         if file_name.lower() == 'q':
#             break
#         if not file_name.endswith('.csv'):
#             file_name = file_name + '.csv'
#         return file_name
def get_file_name():
    while True:
        file_name = input("Enter CSV File Name and Path (Enter 'q' to quit) ::: ")
        if file_name.lower() == 'q':
            break
        if not file_name.endswith('.csv'):
            file_name = file_name + '.csv'
        if os.path.isfile(file_name):
            return file_name
        else:
            print("Error: File not found. Please enter a valid CSV file name and path.")



# SAVE CSV FILE --------------------------------------------------------------------
def get_save_csv():
    while True:
        file_name = input("Enter CSV File Name to save SQL data (Enter 'q' or '0' to quit): ")
        if file_name.lower() in ['q', '0']:
            break
        elif not file_name.endswith('.csv'):
            file_name = file_name + '.csv'
        if os.path.exists(file_name):
            print("Error: File already exists. Please enter a different CSV file name.")
        else:
            return file_name



# Get Table Name --------------------------------------------------------------------
def get_table_name():
    table_name = input("Enter table name ::: ")
    return table_name



# Get reference table and column names from the user --------------------------------------
def get_reference_details():
    references = []
    while True:
        reference_table = input("\nEnter reference table name (Enter '0' or 'q' to quit) :::")
        if reference_table.lower() in ['q', '0']:
            break
        reference_column = input("Enter reference column name: ")
        references.append((reference_table, reference_column))  # Append as a tuple
    return references



# Connect to MYSQL SERVER -------------------------------------------------------------
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("\nServer connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


 
# Create Database --------------------------------------------------------------------
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if cursor._connection:
            print("\nDatabase is valid")
        else:
            print("\nDatabase created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        


# Connect to the database ---------------------------------------------------------------
def create_db_connection(host_name, user_name, user_password, db_name):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("Database connection successful")
        return connection
    except Error as err:
        print(f"Error: {err}")
        return None



# Query Execution ---------------------------------------------------------------
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")



# GENERATE COLUNMS FROM CSV HEADERS ---------------------------------------------------------------
def generate_schema(headings, reference_pairs):
    schema = ""
    primary_key_assigned = False
    reference_index = 0  # Track the index of the reference pair
    
    for heading in headings:
        if re.search(r"id|UserId|EmployeeID|CustomerID|ProductID|PostID|AccountID|ArticleID", heading, flags=re.IGNORECASE):
            if not primary_key_assigned:
                schema += f"\t{heading.lower()} INT PRIMARY KEY,\n"
                primary_key_assigned = True
            else:
                schema += f"\t{heading.lower()} INT,\n"
                if reference_pairs and reference_index < len(reference_pairs):
                    reference_table, reference_column = reference_pairs[reference_index]
                    schema += f"\tFOREIGN KEY ({heading.lower()}) REFERENCES {reference_table}({reference_column}),\n"
                    reference_index += 1
                    
        elif '_id'.capitalize() in heading.capitalize():
            if not primary_key_assigned:
                schema += f"\t{heading.lower()} INT PRIMARY KEY,\n"
                primary_key_assigned = True
            else:
                schema += f"\t{heading.lower()} INT,\n"
                if reference_pairs and reference_index < len(reference_pairs):
                    reference_table, reference_column = reference_pairs[reference_index]
                    schema += f"\tFOREIGN KEY ({heading.lower()}) REFERENCES {reference_table}({reference_column}),\n"
                    reference_index += 1
        
        
        # Rating/Reviews
        elif re.search(r"rating|reviews|review", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} DECIMAL(3,2),\n"
        elif 'rating'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} DECIMAL(3,2),\n"
        
        # Varchar() 
        elif re.search(r"name|dept|city|state|province|school|customer|delivery|student|institution|bank|PhoneNumber|phone|course|department|address|website|StreetAddress|title|tags|subject|product|ProductName|PostalCode|zipcode|zip|user|Username|password|PasswordHash|lastName|firstName|country|description|data|message|Content|genre|movies|movie|language", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} VARCHAR(255),\n"
        elif 'name'.capitalize() in heading.capitalize() or 'message'.capitalize() in heading.capitalize() or 'description'.capitalize() in heading.capitalize() or 'product'.capitalize() in heading.capitalize() or 'insti'.upper() in heading.upper() or 'menu'.upper() in heading.upper() or 'count'.upper() in heading.upper() or 'language'.upper() in heading.upper():
            schema += f"\t{heading.lower()} VARCHAR(255),\n"
        
        # Float
        elif re.search(r"salary|amount|price|cost|tax|tip|discount|deposit|withdraw|transfer", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} FLOAT,\n"     
        elif 'price'.capitalize() in heading.capitalize() or 'salary'.capitalize() in heading.capitalize() or 'amount'.capitalize() in heading.capitalize() or 'cost'.capitalize() in heading.capitalize() or 'tax'.capitalize() in heading.capitalize() or 'tip'.capitalize() in heading.capitalize() or 'discount'.capitalize() in heading.capitalize() or 'deposit'.capitalize() in heading.capitalize() or 'withdraw'.capitalize() in heading.capitalize() or 'transfer'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} FLOAT,\n"
        
        # Char(1)
        elif re.search(r"gender|sex", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} CHAR(1),\n"
        elif 'gender'.capitalize() in heading.capitalize() or 'sex'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} CHAR(1),\n"
        
        # Date
        elif re.search(r"date|doj|dod|dob", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} DATE,\n"
        elif 'date'.capitalize() in heading.capitalize() or 'dob'.capitalize() in heading.capitalize() or 'dod'.capitalize() in heading.capitalize() or 'doj'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} DATE,\n"
        
        # Time
        elif re.search(r"time|date_time", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} DATE,\n"
        elif 'time'.capitalize() in heading.capitalize() or 'date_time'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} DATETIME,\n"
        
        # Int
        elif re.search(r"age|year|number|month|day|population|quantity", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} INT,\n"
        elif 'age'.capitalize() in heading.capitalize() or 'year'.capitalize() in heading.capitalize() or 'month'.capitalize() in heading.capitalize() or 'day'.capitalize() in heading.capitalize() or 'population'.capitalize() in heading.capitalize() or 'quantity'.capitalize() in heading.capitalize() or 'number'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} INT,\n"

        # Email
        elif re.search(r"email", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} VARCHAR(150) UNIQUE,\n"
        elif 'email'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} VARCHAR(150) UNIQUE,\n"
        
        # TAX UID
        elif re.search(r"tas_pd", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} INT UNIQUE,\n"
        elif '_pd'.capitalize() in heading.capitalize():
            schema += f"\t{heading.lower()} INT UNIQUE,\n"
            
        # BOOLEAN
        elif re.search(r"is_valid|is_present|is_positive|is_good|is_bad|in_stock|in_school", heading, flags=re.IGNORECASE):
            schema += f"\t{heading.lower()} BOOLEAN,\n"
        elif heading.lower().startswith('is_') or heading.lower().startswith('in_'):
            schema += f"\t{heading.lower()} BOOLEAN,\n"

        # Unknown Types 
        else:
            schema += f"\t{heading.lower()} VARCHAR(225),\n"  # Default for unknown types
    
    return schema[:-2]  # Remove the last comma and newline



# READ CSV FILE ---------------------------------------------------------------
def read_csv_create_table(csv_file, references):
    # Extract table name from CSV filename
    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    # Read the CSV file and extract headers
    with open(csv_file, 'r', newline='') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)

    # Generate SQL CREATE TABLE statement
    sql_query = f"CREATE TABLE {table_name} (\n"
    sql_query += generate_schema(headers, references)
    sql_query += "\n);"

    return sql_query



# MANY TO MANY TABLE RELATIONSHIP TABLE ------------------------------------------------------------
def create_many_to_many_table(connection, references, table_name):
    if references:
        # Assuming we need at least two reference tables for a many-to-many relationship
        if len(references) >= 2:
            first_reference_table, first_reference_column = references[0]
            second_reference_table, second_reference_column = references[1]

            many_to_many_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
              {first_reference_table}_id INT,
              {second_reference_table}_id INT,
              PRIMARY KEY({first_reference_table}_id, {second_reference_table}_id),
              FOREIGN KEY({first_reference_table}_id) REFERENCES {first_reference_table}({first_reference_column}) ON DELETE CASCADE,
              FOREIGN KEY({second_reference_table}_id) REFERENCES {second_reference_table}({second_reference_column}) ON DELETE CASCADE
            );
            """

            return many_to_many_table_query
        else:
            print("Error: At least two reference tables are required for a many-to-many relationship.")
            return None
    else:
        print("Error: No reference details provided.")
        return None



#  ====================================================================================================================================



# INSERT VALUES INTO TABLE ------------------------------------------------------------

# Function to convert date format
def convert_date_format(date_str):
    try:
        # Parse the date string from the CSV file
        dob = datetime.datetime.strptime(date_str, '%d/%m/%Y').date()
        # Format the date into 'YYYY-MM-DD' format
        dob_formatted = dob.strftime('%Y-%m-%d')
        return dob_formatted
    except ValueError:
        return date_str  # Return the original value if it's not a valid date
    
    

# Read CSV file and extract headings and data ------------------------------------------------------------
def read_csv_file(file_path):
    headings = []
    data = []
    with open(file_path, "r") as csv_file:
        reader = csv.reader(csv_file)
        headings = next(reader)  # Extract headings from the first row
        for row in reader:
            formatted_row = []
            for value in row:
                # Attempt to convert the value to date format
                formatted_value = convert_date_format(value)
                formatted_row.append(formatted_value)
            data.append(formatted_row)  # Append formatted data row
    return headings, data




# This function inserts data from a CSV file into an existing table ------------------------------------------------------------
def insert_data_from_csv(connection, table_name, headings, data):
    cursor = connection.cursor()
    try:
        placeholders = ', '.join(['%s'] * len(headings))
        query = f"INSERT INTO {table_name} ({', '.join(headings)}) VALUES ({placeholders})"
        cursor.executemany(query, data)  # Execute the query with data
        connection.commit()
        print("Data inserted successfully\n")
    except Error as err:
        print(f"Error inserting data: {err}")



# QUERY DATABASE / DATA RETRIVING ------------------------------------------------------------
# Read Query
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        return column_names, result
    except Error as err:
        print(f"Error: '{err}'")

# def read_query(connection, query):
#     cursor = connection.cursor()
#     result = None
#     try:
#         cursor.execute(query)
#         if cursor.description is not None:
#             result = cursor.fetchall()
#             column_names = [description[0] for description in cursor.description]
#         else:
#             column_names = None
#         return column_names, result
#     except Error as err:
#         print(f"Error: '{err}'")




# Save to CSV file ------------------------------------------------------------
def save_to_csv(file_name, column_names, results):
    try:
        with open(file_name, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(results)
        print("CSV file saved successfully.\n")
    except Exception as e:
        print(f"Error saving CSV file: {e}")
        
        
        
# Load CSV To Pandas ------------------------------------------------------------
def load_csv_to_pandas(file_name):
    try:
        # Read CSV file
        df = pd.read_csv(file_name)
        
        # Replace 'NULL' values with NaN
        df.replace('NULL', pd.NA, inplace=True)
        
        return df
    except FileNotFoundError:
        print(f"Error: File '{file_name}' not found.")
    except Exception as e:
        print(f"Error loading CSV file into Pandas DataFrame: {e}")




# MINI FUNCTION ==============================================================================

# Clear Window ---------------------------------------------------------------
def clear_window():
    # Check the operating system
    if os.name == 'nt':  # For Windows
        _ = os.system('cls')
    else:  # For Unix-based systems (Linux, macOS)
        _ = os.system('clear')


# Headings / Title Lines
def pretify_headings_line(title):
    print("---" * 12)
    print(title)
    print("---" * 12)
    
    
# Queries Title Lines
def pretify_queries_line(title):
    print("---" * 25)
    print(title)
    print("---" * 25)



# Pandas Success Message ---------------------------------------------------------------
# def pandas_success_message(csv_pandas):
#     if csv_pandas is not None:
#         print(csv_pandas)