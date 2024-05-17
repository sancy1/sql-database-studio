from mysql.connector import Error
import pandas as pd

from db_script import get_save_csv, read_query, load_csv_to_pandas
from db_script import save_to_csv

def print_space():
    print("")


# Define the function outside of any other function
def execute_queries_and_show_data(databases_connection, query_db, table):
    try:
        cursor = databases_connection.cursor()
        cursor.execute(query_db)
        databases_connection.commit()
        update_success_message()
    except Error as err:
        print(f"Error: '{err}'")

    # Save data to CSV file if user chooses to and display with pandas
    save_csv_choice = save_table_choice()
    if save_csv_choice == 'y' or save_csv_choice == 'yes':
        save_file_path = get_save_csv()
        
        column_names, results = read_query(databases_connection, f"SELECT * FROM {table}")
        save_to_csv(save_file_path, column_names, results)
        csv_pandas = load_csv_to_pandas(save_file_path)
        
        if csv_pandas is not None:
            print("Updated data loaded into Pandas DataFrame:\n")
            print(f"Updated '{table}' table\n")
            print(csv_pandas)
    else:
        print("Data not saved to CSV.\n")
        # Fetch the updated data from the database
        try:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            if rows:
                # Get the column names from the cursor description
                columns = [desc[0] for desc in cursor.description]
                # Create a pandas DataFrame from the fetched rows and column names
                df = pd.DataFrame(rows, columns=columns)
                print(f"Updated '{table}' table\n")
                print(df)
            else:
                print("\nNo data retrieved from the query.\n")
        except Error as err:
            print(f"Error: '{err}'")



# Pandas Success Message ---------------------------------------------------------------
def pandas_success_message(csv_pandas):
    if csv_pandas is not None:
        print(csv_pandas)
        
 
# Execute the UPDATE query
def update_success_message():
    print("\nUpdate successful!")
    
    
# Save Option
def save_table_choice():
    save_csv_choice = input("Would you like to save the data to a CSV file? (y/n) :::").lower()
    return save_csv_choice


# Input Type ---------------------------------------------------------------
def get_input_type(value): # Input Type
    """Determine the input type (int or str)"""
    try:
        int(value)
        return 'int'
    except ValueError:
        return 'str'
    
def whereColumn(): # where_column
    where_column = input("Enter the WHERE column name :::")
    return where_column

def whereValue(): # where_value
    where_value = input("Enter the WHERE column value :::")
    return where_value



# SINGLE SELECT CLAUS INPUT ----------------------------------------------------------------
def format_names(names): # FORMAT
    if len(names) == 1:
        return names[0]
    elif len(names) == 2:
        return names[0] + ', ' + names[1]
    else:
        return ', '.join(names[:-1]) + ', ' + names[-1]


def single_select(): # INPUTS
    add_select_to_list = []
    quit_command = '0'
    
    while True:
        select_input = input("Enter a column name for 'SELECT' (Enter '0' to quit) :::")
        if select_input == quit_command:
            if not add_select_to_list:
                return ""  # Return empty string if no columns are selected
            else:
                break
        else:
            add_select_to_list.append(select_input)

    formatted_names = format_names(add_select_to_list)
    return formatted_names



# SINGLE SELECT CLAUS ----------------------------------------------------------------
def execute_query_and_show_data_for_single_select(databases_connection, query_db, table):
    try:
        cursor = databases_connection.cursor()
        cursor.execute(query_db)
        rows = cursor.fetchall()  # Fetch all rows from the result set
        if rows:
            # Get the column names from the cursor description
            columns = [desc[0] for desc in cursor.description]
            # Create a pandas DataFrame from the fetched rows and column names
            df = pd.DataFrame(rows, columns=columns)
            print(f"\nData from table '{table}':\n")
            print(f"{df}\n")
                
            # Save data to CSV file if user chooses to
            save_csv_choice = save_table_choice()
            if save_csv_choice.lower() in ('y', 'yes'):
                save_file_path = get_save_csv()
                save_to_csv(save_file_path, columns, rows)
                print(f"Data saved to '{save_file_path}'.")
            else:
                print("Data not saved to CSV.\n")
        else:
            print(f"\nNo data found in table '{table}'.")
    except Error as err:
        print(f"Error: '{err}'")



# ORDER BY INPUT ----------------------------------------------------------------
def order_by_input():
    order_by_input = input("Enter the column name to order by :::")
    return order_by_input


# Modify the andColumn and andValue functions to keep prompting the user until they enter 0 to quit
def andColumn():
    and_columns = []
    while True:
        column = input("Enter the AND column name (Enter '0' to quit) ::: ")
        if column == '0':
            break
        and_columns.append(column)
    return and_columns

def andValue():
    and_values = []
    while True:
        value = input("Enter the AND column value (Enter '0' to quit) ::: ")
        if value == '0':
            break
        and_values.append(value)
    return and_values


# LIMIT ----------------------------------------------------------------
# def limit_display():
#     limit_display = int(input("Enter LIMIT the number of rows to display '0' to ingnor :::"))
#     return limit_display
def limit_display():
    while True:
        limit_input = input("Enter LIMIT the number of rows to display (enter '0' to ignore) ::: ")
        try:
            limit_display = int(limit_input)
            if limit_display == 0:
                break  # Break the loop if the user enters 0
            elif limit_display < 0:
                print("Please enter a non-negative integer or '0' to ignore.")
            else:
                return limit_display
        except ValueError:
            print("Please enter a valid integer or '0' to ignore.")


def limit_option(): # limit option
    limit_option = str(input("Would you like to use 'LIMIT'? (y/n) :::"))
    return limit_option




# ==========================================================================================


def inner_alias_name():  # Inner Alias Name
    while True:
        alias = input("Enter alias name (enter '0' for no name) :::")
        if alias == '0':
            alias = ''  # Set alias to empty string if user chooses 0
            break
        elif alias.strip():  # Check if alias is not empty after stripping whitespace
            break
        else:
            print("Error: Please provide a valid alias name or enter '0' for no name.")
    return alias



# Function to prompt for date column
def date_column():
    date_column = input("Enter date column (e.g., subscriptionStartDate) :::")
    return date_column


# Function to prompt for DATE_ADD query
def date_add_prompt():
    date_type = input("Enter DATE type (e.g., DATE_ADD) :::")
    interval = input("Enter INTERVAL :::")
    date_columns = input("Enter date column(s) separated by comma (e.g., subscriptionStartDate) :::")
    return date_type, interval, date_columns.split(',')


# Function to prompt for DATEDIFF query
# def datediff_prompt():
#     date_value = input("Enter date value (e.g., '2020-12-20') :::")
#     date_column_name = date_column()
    
#     days = int(input("Enter integer number of days e.g 365 :::"))
#     return date_value, date_column_name, days
def datediff_prompt():
    date_value = input("Enter date value (e.g., '2020-12-20') ::: ")
    date_column_name = date_column()
    
    while True:
        days_input = input("Enter integer number of days (e.g., 365) ::: ")
        try:
            days = int(days_input)
            break  # Break the loop if conversion to int succeeds
        except ValueError:
            print("Please enter a valid integer.")
    return date_value, date_column_name, days



# Function to prompt for DATE_FORMAT query
def interval_column():
    while True:
        interval_str = input("Enter the interval value (in months) :::")
        try:
            interval = int(interval_str)
            break
        except ValueError:
            try:
                interval = int(eval(interval_str))
                break
            except ValueError:
                print("Error: Please enter a valid integer or a string representation of an integer for the interval value.")
    return interval


def format_date_column():
    while True:
        date_column = input("Enter date column (e.g., subscriptionStartDate) :::")
        if date_column.strip():
            return date_column.strip()
        else:
            print("Error: Please enter a valid column name.")


def alias_name_option(): # ALIASN OPTION
    option = input("Would you like to use alias names for the current columns? (y/n) :::")
    return option.lower().strip()

def alias_name(): # ALIAS NAME
    alias_name = input("Enter alias name for the column :::")
    return alias_name.strip()


# SELECT DATE TYPES OPTION ----------------------------------------------------------------------------------
def print_additional_queries_menu():
    print("Choose additional queries:")
    print("1. DATE") # DONE
    print("2. MATH with SELECT clause")
    print("3. ROUND")
    print("4. SUM")
    print("5. AVG")
    print("6. COUNT")
    print("7. MAX")
    print("8. MIN")
    print("9. DISTINCT")
    print("10. CONCAT")
    
# DATE QUERY OPTION
def choose_date_query_type(): # DONE
    date_query_type = input("Choose DATE query type:\n"
                        "1. MAIN DATEDIFF: (SELECT DATEDIFF(end_date_column, start_date_column)\n"
                        "2. DATEDIFF: (SELECT ROUND(DATEDIFF('2024-04-12', date_column) / days e.g 365))\n"
                        "3. INTERVAL DATE_FORMAT: (SELECT DATE_FORMAT(DATE_ADD(date_column, INTERVAL interval_value like numbers MONTH), '%M %e, %Y'))\n"
                        "4. DATE_FORMAT: (SELECT DATE_FORMAT(date_column, '%M %e, %Y'))\n"
                        "5. DATE as Year, Month, Day: (SELECT YEAR(date_column) AS year, MONTH(date_column) AS month, DAY(date_column))\n"
                        "Enter option number :::")
    return date_query_type

# -----------------------------------------------------------------------------------------------------------------------------------------

# Math Coulmn Name   
def math_column_name():
    while True:
        variable_name = input("Enter variable name (e.g., price_list) :::")
        # Check if the input is not empty and is a valid variable name
        if variable_name.strip() and variable_name.isidentifier() and not variable_name.isdigit():
            return variable_name.strip()
        else:
            print("Error: Please enter a valid variable name that is not a pure integer.\n")


def math_column_pure_integer(): # Mathe Pure Integer 
    while True:
        try:
            math_column = int(input("Enter the integer value ::: "))
            return math_column
        except ValueError:
            print("Error: Please enter a valid integer.")



# MATH QUERY OPTION
def choose_math_query_type():
    date_query_type = input("Choose MATH query type:\n"
                        "1. BASIC MATH: (SELECT column1 +, -, *, /, % column2)\n"
                        "2. EXPONENTIATION POWER: (SELECT POWER(column1, column2))\n"
                        "3. SQUARE ROOT: (SELECT SQRT(column1))\n"
                        "4. ABSOLUTE VALUE: (SELECT ABS(column1))\n"
                        "Enter option number: ")
    return date_query_type


def math_symbol(): # Math Symbol
    valid_symbols = ['+', '-', '*', '/', '%', '<', '>', '<>', '!=']
    while True:
        math_symbol_option = input("Enter Math symbol (+, -, *, /, %, <, >, <>, !=) ::: ")
        if math_symbol_option in valid_symbols:
            return math_symbol_option
        else:
            print("Error: Please enter a valid math symbol.")
