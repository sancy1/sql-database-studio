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



# AND INPUTS ----------------------------------------------------------------
def andColumn(): # and_column
    and_column = input("Enter the AND column name :::")
    return and_column

def andValue(): # and_value
    and_value = input("Enter the AND column value :::")
    return and_value

def and_option(): # and option
    while True:
        and_option = input("\nWould you like to use 'AND'? (y/n): ")
        if and_option.lower() == 'y' or and_option.lower() == 'n':
            return and_option.lower()
        else:
            print("Wrong choice! Please enter 'y' or 'n'.")

# def and_option(): # and option
#     and_option = str(input("\nWould you like to use 'AND'? (y/n) :::"))
#     return and_option


def how_many_and_do_you_want(): # How many to add
    while True:
        try:
            how_many_do_want = int(input("How many 'AND' do you want to add? Enter between (1-4): "))
            if 1 <= how_many_do_want <= 4:
                return how_many_do_want
            else:
                print("Wrong choice! Please enter between 1-4.")
        except ValueError:
            print("Wrong choice! Please enter a valid integer between 1-4.")

# def how_many_and_do_you_want(): # How many to add
#     how_many_do_want = int(input("How many 'AND' do you want to add? Enter between (1-4) :::"))
#     print_space()
#     return how_many_do_want



# LIMIT ----------------------------------------------------------------
def limit_display():
    limit_display = int(input("Enter LIMIT the number of rows to display '0' to ingnor :::"))
    return limit_display

def limit_option(): # limit option
    limit_option = str(input("Would you like to use 'LIMIT'? (y/n) :::"))
    return limit_option
