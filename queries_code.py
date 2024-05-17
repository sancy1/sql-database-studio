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
        select_input = input("Enter a column name for 'SELECT' (Enter '0' to skip) :::")
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
# def execute_query_and_show_data_for_single_select(databases_connection, query_db, table):
#     try:
#         cursor = databases_connection.cursor()
#         cursor.execute(query_db)
#         rows = cursor.fetchall()  # Fetch all rows from the result set
#         if rows:
#             # Get the column names from the cursor description
#             columns = [desc[0] for desc in cursor.description]
#             # Create a pandas DataFrame from the fetched rows and column names
#             df = pd.DataFrame(rows, columns=columns)
#             print(f"\nData from table '{table}':\n")
#             print(f"{df}\n")
                
#             # Save data to CSV file if user chooses to
#             save_csv_choice = save_table_choice()
#             if save_csv_choice.lower() in ('y', 'yes'):
#                 save_file_path = get_save_csv()
#                 save_to_csv(save_file_path, columns, rows)
#                 print(f"Data saved to '{save_file_path}'.")
#             else:
#                 print("Data not saved to CSV.\n")
#         else:
#             print(f"\nNo data found in table '{table}'.")
#     except Error as err:
#         print(f"Error: '{err}'")

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
            return df
        else:
            print(f"\nNo data found in table '{table}'.")
            return None
    except Error as err:
        print(f"Error: '{err}'")
        return None
    

# ORDER BY INPUT ----------------------------------------------------------------
def order_by_input():
    order_by_input = input("Enter the column name to order by :::")
    return order_by_input


# LIMIT ----------------------------------------------------------------
# def limit_display():
#     limit_display = int(input("Enter LIMIT the number of rows to display '0' to skip :::"))
#     return limit_display

def limit_display():
    while True:
        limit_input = input("Enter LIMIT the number of rows to display (enter '0' to skip) ::: ")
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


# def limit_display():
#     while True:
#         limit_input = input("Enter LIMIT the number of rows to display (enter '0' to ignore) ::: ")
#         try:
#             limit_display = int(limit_input)
#             if limit_display < 0:
#                 print("Please enter a non-negative integer or '0' to ignore.")
#             else:
#                 return limit_display
#         except ValueError:
#             print("Please enter a valid integer or '0' to ignore.")


def limit_option(): # limit option
    limit_option = str(input("Would you like to use 'LIMIT'? (y/n) :::"))
    return limit_option


def limit_offset(): # Limit offset Integer 
    while True:
        try:
            math_column = int(input("Enter the LIMIT offset number :::"))
            return math_column
        except ValueError:
            print("Error: Please enter a valid integer.")


# ==========================================================================================


def get_additional_query_options():
    while True:
        additional_query_option = input("Enter Query option number (Enter 0 to skip) :::")
        if additional_query_option == '0':
            return []
        else:
            return additional_query_option.split(',')


def round_option(): # ROUND
    option = input("Would you like to use ROUND? (y/n) :::")
    return option.lower().strip()
    
    
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
    date_value = input("Enter date value (e.g., '2020-12-20') :::")
    date_column_name = date_column()
    
    while True:
        days_input = input("Enter integer number of days (e.g., 365) :::")
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


# SELECT TYPES OPTION 
def print_additional_queries_menu():
    print("Choose Query Type:".upper())
    print("------------------")
    
    print("0. SINGLE & MULTIPLE 'SELECT' ONLY")
    print("1. DATE")
    print("2. MATH with SELECT clause") 
    print("3. ROUND") 
    print("4. SUM, AVG, MAX, MIN, COUNT, MONTH, DISTINCT")
    print("5. CONCAT")
    print("6. SELECT ALL '*'")
    print("7. Multiple Aggregate")
    
    
# DATE QUERY OPTION ----------------------------------------------------------------------------------
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
        variable_name = input("Enter variable name :::")
        # Check if the input is not empty and is a valid variable name
        if variable_name.strip() and variable_name.isidentifier() and not variable_name.isdigit():
            return variable_name.strip()
        else:
            print("Error: Please enter a valid variable name that is not a pure integer.\n")


def math_column_pure_integer(): # Mathe Pure Integer 
    while True:
        try:
            math_column = float(input("Enter the integer value :::"))
            return math_column
        except ValueError:
            print("Error: Please enter a valid integer.")



# MATH QUERY OPTION
def choose_math_query_type(): # DONE
    math_query_type = input("Choose MATH query type:\n"
                        "1. BASIC MATH: (SELECT column1 +, -, *, /, % column2)\n"
                        "2. EXPONENTIATION POWER: (SELECT POWER(column1, column2))\n"
                        "3. SQUARE ROOT: (SELECT SQRT(column1))\n"
                        "4. ABSOLUTE VALUE: (SELECT ABS(column1))\n"
                        "Enter option number: ")
    return math_query_type


def math_symbol(): # Math Symbol
    valid_symbols = ['+', '-', '*', '/', '%', '<', '>', '<=', '>=', '<>', '!=', '=']
    while True:
        math_symbol_option = input("Enter Math symbol (+, -, *, /, %, <, >, <=, >=, <>, !=, =) :::")
        if math_symbol_option in valid_symbols:
            return math_symbol_option
        else:
            print("Error: Please enter a valid math symbol.")



# ROUND QUERY OPTION ----------------------------------------------------------------------------------
def choose_round_query_type():  # DONE
    round_query_type = input("Choose DATE query type:\n"
                        "1. DEFAULT ROUND - Round to nearest integer: (ROUND(column))\n"
                        "2. ROUND WITH DECIMAL - Truncate to specified decimal places: (ROUND(column1, decimal_places))\n"
                        "Enter option number :::")
    return round_query_type


# AGGREGATE FUNCTION ----------------------------------------------------------------------------------
 
def primary_column():
    option = input("Do you want to SELECT primary column before others? (y/n) :::")
    return option
 

def aggregate_option(): # AGGREGATE OPTION
    valid_options = {'SUM', 'AVG', 'MAX', 'MIN', 'MONTH'}
    while True:
        option = input("Enter aggregate option (SUM, AVG, MAX, MIN, MONTH), or 0 to exit: ").upper()
        if option == '0':
            return None
        elif option in valid_options:
            return option
        else:
            print("Invalid option. Please enter one of: SUM, AVG, MAX, MIN")
            
            
            
def distinct_column_name(): # DINSTICE
    variable_names = []
    while True:
        variable_name = input("Enter variable name or 0 to quit :::")
        if variable_name.strip() == '0':
            if len(variable_names) == 0:
                print("Error: At least one variable name is required.")
            else:
                break
        elif variable_name.strip() and variable_name.isidentifier() and not variable_name.isdigit():
            variable_names.append(variable_name.strip())
        else:
            print("Error: Please enter a valid variable name that is not a pure integer.")
    return variable_names

    

def choose_aggregate_query_type():  # DONE
    aggregate_query_type = input("Choose AGGREGATE query type:\n"
                        "1. SUM, AVG, MAX, MIN MONTH: (SUM(column)) OR SUM(DISTINCT column)\n"
                        "2. COUNT: (COUNT(*)) OR (COUNT(column))\n"
                        "3. DISTINCT(column) OR DISTINCT(AVG(column)"
                        "Enter option number :::")
    return aggregate_query_type


# CONCAT FUNCTION ----------------------------------------------------------------------------------
 
 
def general_column_name(): # CONCAT
    variable_names = []
    while True:
        variable_name = input("Enter variable name or 0 to quit :::")
        if variable_name.strip() == '0':
            if len(variable_names) == 0:
                print("Error: At least one variable name is required.")
            else:
                break
        elif variable_name.strip() and variable_name.isidentifier() and not variable_name.isdigit():
            variable_names.append(variable_name.strip())
        else:
            print("Error: Please enter a valid variable name that is not a pure integer.")
    return variable_names



def choose_concat_query_type():  # DONE
    concat_query_type = input("Choose CONCAT query type:\n"
                        "1. Concatenate two columns with a space in between: SELECT DISTINCT CONCAT(column1, ' ', column2)\n" # DONE
                        "2. Concatenate three columns with a comma and space in between: SELECT DISTINCT CONCAT(column1, ', ', column2, ', ', column3)\n"
                        "3. Concatenate a column with a constant string: SELECT DISTINCT CONCAT(column1, ', ', 'ConstantString')\n"
                        "4. Concatenate columns and constant strings: SELECT DISTINCT CONCAT(column1, ' ', column2, ' constant ', column3)\n"
                        "5. Concatenate columns with a custom separator: SELECT DISTINCT CONCAT(column1, ' - ', column2, ' - ', column3)\n"
                        "6. Concatenate columns with a custom separator and transform: SELECT DISTINCT CONCAT(column1, '-', UPPER(column2), '-', LOWER(column3))\n"
                        "7. Concatenate columns with a custom separator and cast: SELECT DISTINCT CONCAT(column1, ' - ', CAST(column2 AS VARCHAR), ' - ', column3)\n"
                        "8. Concatenate columns with NULL handling: SELECT DISTINCT CONCAT(IFNULL(column1, ''), ' ', IFNULL(column2, ''), ' ', IFNULL(column3, ''))\n"
                        "9. Concatenate columns with a CASE statement and NULL handling: SELECT DISTINCT CONCAT(CASE WHEN column1 IS NULL THEN 'NULL' ELSE column1 END, ' - ', CASE WHEN column2 IS NULL THEN 'NULL' ELSE column2 END)\n"
                        "10. Concatenate columns with conditions and custom separator: SELECT DISTINCT CONCAT(CASE WHEN condition1 THEN column1 ELSE '' END, '-', CASE WHEN condition2 THEN column2 ELSE '' END, '-', CASE WHEN condition3 THEN column3 ELSE '' END)\n"
                        "11. Concatenate columns with leading zeros: SELECT DISTINCT CONCAT(LPAD(column1, 3, '0'), '-', LPAD(column2, 3, '0'))\n"
                        "12. Concatenate columns with a formatted date: SELECT DISTINCT CONCAT(DATE_FORMAT(column1, '%Y-%m-%d'), ' ', TIME_FORMAT(column2, '%H:%i:%s'))\n"
                        "13. Concatenate columns with HTML tags: SELECT DISTINCT CONCAT('<b>', column1, '</b><br>', column2)\n"
                        "14. Concatenate columns with a delimiter based on a condition: SELECT DISTINCT CONCAT_WS(CASE WHEN condition THEN '-' ELSE '/' END, column1, column2)\n"
                        "Enter option number :::")
    return concat_query_type


# WHERE CLAUSE ===========================================================================================


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


def whereValue(data_type='string'):
    where_value = input("Enter the WHERE column value ::: ")

    if data_type == 'string':
        return where_value
    elif data_type == 'integer':
        try:
            return int(where_value)
        except ValueError:
            return where_value  # Return as string if conversion fails
    elif data_type == 'float':
        try:
            return float(where_value)
        except ValueError:
            return where_value  # Return as string if conversion fails
    elif data_type == 'date':
        # Check if the input represents a date in the format 'YYYY-MM-DD'
        if len(where_value) == 10 and where_value[4] == '-' and where_value[7] == '-':
            try:
                year, month, day = map(int, where_value.split('-'))
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return where_value
            except ValueError:
                pass

        # If the input doesn't meet the date format, return as string
        return where_value
    else:
        # If an invalid data type is specified, return as string
        return where_value




# Modify the andColumn and andValue functions to keep prompting the user until they enter 0 to quit
def andColumn(): # AND Column
    and_columns = []
    while True:
        column = input("Enter the AND column name (Enter '0' to quit) ::: ")
        if column == '0':
            break
        and_columns.append(column)
    return and_columns

def andValue(): # AND Value
    and_values = []
    while True:
        value = input("Enter the AND column value (Enter '0' to quit) ::: ")
        if value == '0':
            break
        and_values.append(value)
    return and_values


def choose_where_query_type():
    where_query_type = input(
        "Choose WHERE query type:\n"
        "1. WHERE column_name = column_value\n" # DONE
        "2. WHERE column_name > column_value\n"
        "3. WHERE column_name * column_value > 100 or column_name2\n\n"
        "-. WHERE store_id != 1\n"
        "-. WHERE NOT column_name = value\n"
        "-. WHERE column_name IS NULL\n"
        "-. WHERE column_name IS NOT NULL\n"
        "-. WHERE column_name IN (value1, value2, value3)\n"
        "-. WHERE order_date IN ('2024-01-01', '2024-02-01', '2024-03-01')\n\n"
        "-. WHERE column_name LIKE 'pattern'\n"
        "-. WHERE column_name LIKE '%pattern'\n"
        "-. WHERE column_name LIKE 'pattern%'\n"
        "-. WHERE column_name LIKE '%pattern%'\n\n"
        "-. WHERE column_name REGEXP 'pattern'\n"
        "-. WHERE product_name REGEXP '^A.*'\n"
        "-. WHERE column_name REGEXP '[0-9]'\n"
        "-. WHERE order_description REGEXP '^[0-9]{3}-[0-9]{3}-[0-9]{4}$'\n"
        "-. WHERE email REGEXP 'gmail\.com$'\n\n"
        "-. WHERE grade = 'A' OR grade = 'B'\n"
        "-. WHERE age < 25 OR total_purchase_amount > 1000\n"
        "-. WHERE category = 'Electronics' OR price < 100\n"
        "-. WHERE salary > 80000 OR department = 'HR'\n"
        "-. WHERE order_date < '2023-01-01' OR total_amount > 500\n"
        "-. WHERE country = 'United States' OR membership_status = 'Premium'\n"
        "4. WHERE column1 = value1 AND column2 = value2\n"
        "5. WHERE make = 'Toyota' AND model = 'Corolla' AND year >= 2018 AND year <= 2022 AND price < 20000\n"
        "6. WHERE (column1 = value1 AND column2 = value2) OR (column3 = value3)\n"
        "7. WHERE (department = 'Sales' AND salary > 50000) OR department = 'Marketing'\n\n"
        "8. WHERE column_name BETWEEN value1 AND value2\n"
        "9. WHERE (department = 'Sales' AND salary > 60000)  OR (department = 'Marketing' AND salary > 50000)\n"
        "10. WHERE make = 'Toyota' AND model = 'Corolla' AND year BETWEEN 2018 AND 2022 AND price < 20000\n"
        "11. WHERE column_name BETWEEN value1 AND value2\n"
        "12. WHERE column_name BETWEEN value1 AND value2 AND another_column = 'some_value'\n"
        "13. WHERE date_column BETWEEN 'start_date' AND 'end_date' AND another_column = 'some_value'\n"
        "-. WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'\n"
        "-. WHERE date_column BETWEEN 'start_date' AND 'end_date'\n"
        "-. WHERE price BETWEEN 100 AND 200\n"
        "-. WHERE numeric_column BETWEEN min_value AND max_value\n\n"
        "14. WHERE phone IS NOT NULL AND (city LIKE '%ach%' OR city LIKE '%och%')\n"
        "15. WHERE phone IS NOT NULL AND (city LIKE '%ach%' OR city LIKE '%och%') OR last_name = 'William'\n"
        "-. RE MONTH(order_date) = 2 AND YEAR(order_date) = 2017\n"
        "-. WHERE (product_name LIKE '%Frame%' OR product_name LIKE '%Frameset%')\n"
        "-. WHERE (product_name LIKE '%Frame%' OR product_name LIKE '%Frameset%') OR product_name LIKE '%Women''s%'\n"
        "-. WHERE product_name REGEXP '^[A-H]' AND list_price <= 299.99\n"
        "-. WHERE (product_name LIKE 'Trek%' OR product_name LIKE 'surly%') AND model_year <> 2016\n"
        "-. WHERE dob BETWEEN 1500 AND 1900\n"
        "-. WHERE artyear BETWEEN 1800 AND 1900\n"
        "-. WHERE (product_name LIKE '%Frame%' OR product_name LIKE '%Frameset%') AND list_price <= 299.99\n"
        "-. WHERE period in ('Modern', 'Baroque', 'Impressionism')\n"
        "Enter option number :::"
    )
    return where_query_type



# ORDER BY CLAUSE ===========================================================================================


# def general_colum_name():
#     while True:
#             variable_name = input("Enter column name :::")
#             if variable_name.strip() and variable_name.isidentifier() and not variable_name.isdigit():
#                 return variable_name.strip()
#             else:
#                 print("Error: Please enter a valid variable name that is not a pure integer.\n")


def order_by_operators():
    order_operators = ['ASC', 'DESC', 'IS NULL', 'IS NOT NULL', 'IS NULL DESC', 'IS NULL ASC', 'IS NOT NULL DESC', 'IS NOT NULL ASC']
    while True:
        
        order_operators_options = input(
            "Choose ORDER BY operator: \n"
            "-. ASC, DESC\n"
            "-. IS NULL\n"
            "-. IS NOT NULL\n"
            "-. IS NULL DESC\n"
            "-. IS NULL ASC\n"
            "-. IS NOT NULL DESC\n"
            "-. IS NOT NULL ASC\n"
            "Enter option number :::"
            )
        for x in order_operators:
            if order_operators_options == x:
                return x
        else:
            print("Error: Please enter a valid operator for ORDER BY clause.")
    
    
def choose_orderby_type():
    orderby_query_type = input(
        "Choose ORDER BY query type:\n" 
        "1. ORDER BY column_name\n"
        "2. ORDER BY column_name DESC, ASC, NULL etc.\n"
        "3. ORDER BY column1 ASC, column2 DESC\n"
        "4. ORDER BY column1 * 2 or column2\n"
        "5. ORDER BY column1 DESC, column2\n"
        "Enter option number :::"
    )
    return orderby_query_type



# GROUP BY CLAUSE ===========================================================================================

def multi_gen_column_name():
    columns = []
    while True:
        variable_name = input("Enter column name (enter 0 to quit): ")
        if variable_name == '0':
            break
        elif variable_name.strip() and variable_name.isidentifier() and not variable_name.isdigit():
            columns.append(variable_name.strip())
        else:
            print("Error: Please enter a valid variable name that is not a pure integer.")
    return columns


def choose_groupby_type():
    groupby_query_type = input(
        "Choose GROUP BY query type:\n" 
        "1. GROUP BY column_name\n" 
        "2. GROUP BY column_name1, column_name2 WITH ROLLUP\n" 
        "3. GROUP BY MONTH(date)\n"
        "Enter option number :::"
    )
    return groupby_query_type


def direct():
    instruction = "Enter the second Column"
    return instruction
    

# HAVING CLAUSE ===========================================================================================

def choose_having_type():
    having_query_type = input(
        "Choose HAVING query type:\n" 
        "1. HAVING SUM(sales_amount) > 1000\n"
        "2. HAVING AVG(score) > 80\n"
        "3. HAVING HAVING COUNT(order_id) > 5\n"
        "4. HAVING HAVING AVG(temperature) > 25\n"
        "5. HAVING SUM(revenue) > 5000\n"
        "6. HAVING COUNT(*) > 5\n"
        "7. HAVING COUNT(*) > 5 AND SUM(salary) > 500000\n"
        "8. HAVING (SUM(salary) / COUNT(*)) > 45000\n"
        "9. HAVING UPPER(category) LIKE '%ELECTRONICS%'\n"
        "Enter option number :::"
    )
    return having_query_type








# Multiple aggregate
# SELECT first_name, last_name, MONTH(dob), MAX(dob), SUM(dob)
# SELECT first_name, AVG(YEAR(NOW()) - YEAR(dob)) AS avg_age











