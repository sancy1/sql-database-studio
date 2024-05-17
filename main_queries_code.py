from mysql.connector import Error
import pandas as pd
import os
import datetime


from db_script import get_database_name, create_server_connection, get_save_csv, read_query, load_csv_to_pandas, pretify_queries_line
from db_script import create_database, create_db_connection, clear_window, get_table_name, save_to_csv
from query_option import queriesOptions
from queries_code import execute_queries_and_show_data, save_table_choice, update_success_message, get_input_type, whereColumn, whereValue, single_select
from queries_code import execute_query_and_show_data_for_single_select, order_by_input, limit_display, limit_option, print_additional_queries_menu, math_symbol
from queries_code import andColumn, andValue, print_space, interval_column, format_date_column, choose_date_query_type, choose_math_query_type, math_column_name
from queries_code import date_column, date_add_prompt, datediff_prompt, alias_name_option, alias_name, math_column_pure_integer, inner_alias_name, round_option
from queries_code import choose_round_query_type, get_additional_query_options, choose_aggregate_query_type, aggregate_option, primary_column, limit_offset
from queries_code import choose_concat_query_type, general_column_name, choose_where_query_type, choose_orderby_type, order_by_operators
from queries_code import choose_groupby_type, multi_gen_column_name, direct


# ALL Database Queries ---------------------------------------------------------------        
def query_database_codes(host, user, password):
    
    connection = create_server_connection(host, user, password) # Connect to server (MYSQL Workbench)
    database_name = get_database_name() # Get database name from the user
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}" 
    create_database(connection, create_database_query) # Create database
    databases_connection = create_db_connection(host, user, password, database_name)
    
    queriesInput = queriesOptions()

    # 1. Display/Show All Data in a Database ---------------------------------------------------------------
    if queriesInput == '1':
        clear_window()
        print()
        title = "1. Display/Show all data from the database\n   select * from table_name;"
        pretify_queries_line(title)
        
        table = get_table_name()
        # save_file_path = get_save_csv()
        
        query_db = f"""
        SELECT *
        FROM {table};
        """
        
        # Execute the READ query
        def execute_queries_and_show_data_2():
            column_names, results = read_query(databases_connection, query_db)
            
            # Save data to CSV file if user chooses to and display with pandas
            save_csv_choice = save_table_choice()
            if save_csv_choice == 'y' or save_csv_choice == 'yes':
                save_file_path = get_save_csv()
                
                column_names, results = read_query(databases_connection, f"SELECT * FROM {table}")
                save_to_csv(save_file_path, column_names, results)
                csv_pandas = load_csv_to_pandas(save_file_path)
                
                if csv_pandas is not None:
                    print(f"Data retrived from '{table}' table:\n")
                    print(csv_pandas)
            else:
                print("\nData not saved to CSV.")
                # Fetch the updated data from the database
                try:
                    cursor = databases_connection.cursor()
                    cursor.execute(query_db)
                    rows = cursor.fetchall()  # Fetch all rows explicitly
                    databases_connection.commit()
                    update_success_message()
                    
                    if rows:
                        # Get the column names from the cursor description
                        columns = [desc[0] for desc in cursor.description]
                        # Create a pandas DataFrame from the fetched rows and column names
                        df = pd.DataFrame(rows, columns=columns)
                        print(f"Data retrived from '{table}' table:\n")
                        print(df)
                    else:
                        print("\nNo data retrieved from the query.\n")
                except Error as err:
                    print(f"Error: '{err}'")
            
        execute_queries_and_show_data_2()
    
    
    # 2. Show All Databases ---------------------------------------------------------------
    elif queriesInput == '2': 
        clear_window()
        print()
        title = "2. Show All Databases || SHOW DATABASES"
        pretify_queries_line(title)
        
        # Execute the SHOW DATABASES query
        query_db = "SHOW DATABASES;"
        try:
            cursor = databases_connection.cursor()
            cursor.execute(query_db)
            databases = cursor.fetchall()
            if databases:
                pass #print("Databases:")
                for database in databases:
                    print(database[0])
            else:
                print("No databases found.")
        except Error as err:
            print(f"Error: '{err}'")
            
        
    # 3. Show All Tables in the Database ---------------------------------------------------------------
    elif queriesInput == '3': 
        clear_window()
        print()
        title = "3. Show Tables in the Database || SHOW TABLES;"
        pretify_queries_line(title)
        
        # Execute the SHOW TABLES query
        query_db = "SHOW TABLES;"
        try:
            cursor = databases_connection.cursor()
            cursor.execute(query_db)
            tables = cursor.fetchall()
            if tables:
                pass #print("Tables in the database:")
                for table in tables:
                    print(table[0])
            else:
                print("No tables found in the database.")
        except Error as err:
            print(f"Error: '{err}'")


    # 4. UPDATE ---------------------------------------------------------------  
    elif queriesInput == '4': 
        clear_window()
        print()
        title = "4. Update Table In The Database\n   UPDATE {table_name} SET {set_column} = {set_value} WHERE {where_column} = {where_value}"
        pretify_queries_line(title)
        
        table = get_table_name()
        
        # Prompt user for SET column name, value, and type
        set_column = input("Enter the SET column name :::")
        set_value = input("Enter the SET column value :::")
        set_type = get_input_type(set_value)
        print("")
        
        # Prompt user for WHERE column name, value, and type
        where_column = whereColumn()
        where_value = whereValue()
        where_type = get_input_type(where_value)
        
        # Adjust the query based on data types
        if set_type == 'str':
            set_value = f"'{set_value}'"
        if where_type == 'str':
            where_value = f"'{where_value}'"
        
        query_db = f"""
        UPDATE {table}
        SET {set_column} = {set_value}
        WHERE {where_column} = {where_value};
        """
        execute_queries_and_show_data(databases_connection, query_db, table)


    # 5. DELETE ---------------------------------------------------------------  
    elif queriesInput == '5': 
        clear_window()
        print()
        title = "5. Delete Data from Table\n   DELETE FROM {table_name} WHERE {where_column} = {where_value}"
        pretify_queries_line(title)
        
        table = get_table_name()
        
        # Prompt user for WHERE column name, value, and type
        where_column = whereColumn()
        where_value = whereValue()
        where_type = get_input_type(where_value)
        
        # Adjust the query based on data types
        if where_type == 'str':
            where_value = f"'{where_value}'"
        
        query_db = f"""
        DELETE FROM {table}
        WHERE {where_column} = {where_value};
        """
        execute_queries_and_show_data(databases_connection, query_db, table)
    

    # 6. DELETE AN ITEM FROM A TABLE --------------------------------------------------------------- 
    elif queriesInput == '6':
        clear_window()
        print()
        title = "6. Delete ALL Data from a Table\n   DELETE FROM {table_name}"
        pretify_queries_line(title)
        
        table = get_table_name()
        
        query_db = f"""
        DELETE FROM {table};
        """
        execute_queries_and_show_data(databases_connection, query_db, table)
    

    # 7. DROP TABLE --------------------------------------------------------------- 
    elif queriesInput == '7':
        clear_window()
        print()
        title = "7. Drop Table\n   DROP TABLE {table_name}"
        pretify_queries_line(title)
        
        table = get_table_name()
        
        query_db = f"""
        DROP TABLE {table};
        """
        execute_queries_and_show_data(databases_connection, query_db, table)
    

    # 8. DROP DATABASE --------------------------------------------------------------- 
    elif queriesInput == '8':
        clear_window()
        print()
        title = "8. Drop Database\n   DROP DATABASE {database_name}"
        pretify_queries_line(title)
        
        database_name = get_database_name()
        
        query_db = f"""
        DROP DATABASE {database_name};
        """
        execute_queries_and_show_data(databases_connection, query_db, database_name)
    

    # 9. DUPLICATE TABLE --------------------------------------------------------------- 
    elif queriesInput == '9':
        clear_window()
        print()
        title = "9. Duplicate Table\n   CREATE TABLE {table_backup} AS SELECT * FROM {original_table};"
        pretify_queries_line(title)

        tables = get_table_name(), get_table_name()  # Get both table names and create a tuple
        table_backup, original_table = tables

        query_db = f"""
        CREATE TABLE {table_backup}
        AS SELECT *
        FROM {original_table};
        """
        execute_queries_and_show_data(databases_connection, query_db, tables)

    
    elif queriesInput == '10':
        clear_window()
        print()
        title = "Single Table Queries"
        pretify_queries_line(title)

        table = get_table_name()
        queries = []  # To store all queries performed
        
        while True:
            print_space()
            #print("Choose additional queries:")
            print_additional_queries_menu()
            additional_query_option = get_additional_query_options()
            additional_query_option = [int(option.strip()) for option in additional_query_option]
            
            print_space()
            select_columns = single_select()  # Existing function to prompt for column names
            
            #Prompt for alias names
            print_space()
            alias_option = input("Do you want to specify column aliases? (y/n): ")
            if alias_option.lower() == 'y':
                alias_names = []
                for column in select_columns.split(','):
                    alias_names.append(input(f"Enter alias for column '{column}': "))
                select_columns = ', '.join([f"{column} AS `{alias}`" for column, alias in zip(select_columns.split(','), alias_names)])
            
            if 1 in additional_query_option:
                # Date option ---------------------------------------------------------
                print_space()
                date_query_type = choose_date_query_type()
                print_space()
                
                if date_query_type == '1':
                    end_date_column = format_date_column()
                    start_date_column = format_date_column()
                    date_query = f"DATEDIFF({end_date_column}, {start_date_column})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{date_query} AS `{alias}`"
                    else:
                        select_columns += f", {date_query} AS `{alias}`"
                
                elif date_query_type == '2':
                    date_value, date_column, days = datediff_prompt()
                    date_query = f"ROUND(DATEDIFF('{date_value}', {date_column}) / {days})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{date_query} AS `{alias}`"
                    else:
                        select_columns += f", {date_query} AS `{alias}`"

                elif date_query_type == '3':
                    date_column = format_date_column() 
                    interval_value = interval_column()
                    date_query = f"(DATE_FORMAT(DATE_ADD(`{date_column}`, INTERVAL {interval_value} MONTH), '%M %e, %Y'))"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{date_query} AS `{alias}`"
                    else:
                        select_columns += f", {date_query} AS `{alias}`"
                
                elif date_query_type == '4':
                    date_column = format_date_column() 
                    date_query = f"(DATE_FORMAT(`{date_column}`, '%M %e, %Y'))"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{date_query} AS `{alias}`"
                    else:
                        select_columns += f", {date_query} AS `{alias}`"
                
                elif date_query_type == '5':
                    date_column = format_date_column()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"YEAR({date_column}) AS year, MONTH({date_column}) AS month, DAY({date_column}) AS day"
                    else:
                        select_columns += f", YEAR({date_column}) AS year, MONTH({date_column}) AS month, DAY({date_column}) AS day"

            elif 2 in additional_query_option:
                # Math option ---------------------------------------------------------
                print_space()
                math_query_type = choose_math_query_type()
                print_space()
                
                if math_query_type == '1': 
                    math_column1 = math_column_name()
                    math_symbol_option = math_symbol()
                    
                    print_space()
                    math_column2_option = input("Provide column2 or enter an integer (C/I): ")
                    if math_column2_option.upper() == 'C':
                        math_column2 = math_column_name()
                    elif math_column2_option.upper() == 'I':
                        math_column2 = math_column_pure_integer()
                    
                    print_space()
                    round_option_value = round_option() # ROUND OPTION
                    if round_option_value == 'y':
                        round_integer = math_column_pure_integer()
                        mathes_query = f"ROUND({math_column1} {math_symbol_option} {math_column2}, {round_integer})"
                    else:
                        mathes_query = f"({math_column1} {math_symbol_option} {math_column2})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{mathes_query} AS `{alias}`"
                    else:
                        select_columns += f", {mathes_query} AS `{alias}`"
                
                elif math_query_type == '2':
                    math_column1 = math_column_name()
                    print_space()
                    
                    math_column2_option = input("Provide column2 or enter an integer (C/I): ")
                    if math_column2_option.upper() == 'C':
                        math_column2 = math_column_name()
                    elif math_column2_option.upper() == 'I':
                        math_column2 = math_column_pure_integer()
                    
                    print_space()
                    round_option_value = round_option() # ROUND OPTION
                    if round_option_value == 'y':
                        round_integer = math_column_pure_integer()
                        mathes_query = f"ROUND(POWER({math_column1}, {math_column2}, {round_integer}))"
                    else:
                        mathes_query = f"POWER({math_column1}, {math_column2})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{mathes_query} AS `{alias}`"
                    else:
                        select_columns += f", {mathes_query} AS `{alias}`"
                
                elif math_query_type == '3':
                    math_column1 = math_column_name()
                    
                    round_option_value = round_option() # ROUND OPTION
                    if round_option_value == 'y':
                        round_integer = math_column_pure_integer()
                        mathes_query = f"ROUND(SQRT({math_column1}, {round_integer}))"
                    else:
                        mathes_query = f"SQRT({math_column1})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{mathes_query} AS `{alias}`"
                    else:
                        select_columns += f", {mathes_query} AS `{alias}`"
                
                elif math_query_type == '4':
                    math_column1 = math_column_name()
                    
                    print_space()
                    round_option_value = round_option() # ROUND OPTION
                    if round_option_value == 'y':
                        round_integer = math_column_pure_integer()
                        mathes_query = f"ROUND(ABS({math_column1}, {round_integer}))"
                    else:
                        mathes_query = f"ABS({math_column1})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{mathes_query} AS `{alias}`"
                    else:
                        select_columns += f", {mathes_query} AS `{alias}`"
                    
                    
            elif 3 in additional_query_option:
                # ROUND option ---------------------------------------------------------
                print_space()
                round_query_type = choose_round_query_type()
                print_space()   
                
                if round_query_type == '1':  
                    math_column1 = math_column_name() 
                    
                    round_query = f"ROUND({math_column1})"  
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{round_query} AS `{alias}`"
                    else:
                        select_columns += f", {round_query} AS `{alias}`"
                    
                elif round_query_type == '2':
                    math_column1 = math_column_name()
                    round_integer = math_column_pure_integer()
                    
                    round_query = f"ROUND({math_column1}, {round_integer})"  
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{round_query} AS `{alias}`"
                    else:
                        select_columns += f", {round_query} AS `{alias}`"
                    
                    
            elif 4 in additional_query_option:
                # Aggregate option ---------------------------------------------------------       
                print_space()
                aggregate_query_type = choose_aggregate_query_type()
                print_space()
                
                if aggregate_query_type == '1': # SUM, AVG, MAX, MIN MONTH
                    aggregate_option_value = aggregate_option()  # function returning aggregate function like COUNT, SUM, etc.
                    aggregate_column_name = math_column_name()  # function returning the column name
                    
                    print_space()
                    distinct_with_aggregate = input("Would you like to use DISTINCT? (y/n): ").lower()
                    if distinct_with_aggregate == 'y':
                        aggregate_query = f"{aggregate_option_value}(DISTINCT {aggregate_column_name})"
                    else:
                        aggregate_query = f"{aggregate_option_value}({aggregate_column_name})"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{aggregate_query} AS `{alias}`"
                    else:    
                        select_columns += f", {aggregate_query} AS `{alias}`"
                
                
                elif aggregate_query_type == '2': # COUNT
                    count_option = input("Would you like to use a column? (y/n) :::")
                    if count_option.lower() == 'y':
                        aggregate_column_name = math_column_name()
                        aggregate_query = f"COUNT({aggregate_column_name})"
                    else:
                        aggregate_query = "COUNT(*)"
                    alias = inner_alias_name()
                    
                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{aggregate_query} AS `{alias}`"
                    else:    
                        select_columns += f", {aggregate_query} AS `{alias}`"
                    
                    
                if aggregate_query_type == '3':  # DISTINCT
                    distinct_option = input("Would you like to use an aggregate function? (y/n): ").lower()
                    if distinct_option == 'y':
                        aggregate_option_value = aggregate_option()
                        aggregate_column_names = general_column_name()
                        aggregate_query = f"{aggregate_option_value}(DISTINCT {', '.join(aggregate_column_names)})"
                    else:
                        aggregate_column_names = general_column_name()
                        aggregate_query = f"DISTINCT {', '.join(aggregate_column_names)}"

                    alias = inner_alias_name()

                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{aggregate_query} AS `{alias}`"
                    else:    
                        select_columns += f", {aggregate_query} AS `{alias}`"


            elif 5 in additional_query_option:
                # Concat option ---------------------------------------------------------  
                print_space()
                concat_query_type = choose_concat_query_type()
                print_space() 
                
                if concat_query_type == '1':
                    concat_column_names = general_column_name()

                    # Construct individual COALESCE expressions for each column
                    coalesce_expressions = [f"COALESCE({col}, '')" for col in concat_column_names]

                    # Construct the CONCAT expression
                    concat_query = f"CONCAT_WS(' ', {', '.join(coalesce_expressions)})"

                    # Prompt the user to provide an alias name for the concatenated column
                    alias = input("Enter an alias name for the concatenated column: ")

                    #primary_column_option = primary_column()
                    if select_columns == "":
                        select_columns += f"{concat_query} AS `{alias}`"
                    else:
                        select_columns += f", {concat_query} AS `{alias}`"
                        
                        
            elif 6 in additional_query_option:        
                # SELECT ALL '*' CLAUSE ======================================================================
                print_space()
                all = "*"
                if select_columns == "":
                    select_columns += f" {all}"
                else:
                    break
            

            
            # Define sub-functions for each query type
            # def date_query_operations(date_query_type):
            #     alias = inner_alias_name()  # Generate an alias name

            #     if date_query_type == '1':
            #         end_date_column = format_date_column()
            #         start_date_column = format_date_column()
            #         date_query = f"DATEDIFF({end_date_column}, {start_date_column}) AS `{alias}`"

            #     elif date_query_type == '2':
            #         date_value = input("Enter date value (e.g., '2024-04-12'): ")
            #         date_column = format_date_column()
            #         days = int(input("Enter number of days (e.g., 365): "))
            #         date_query = f"ROUND(DATEDIFF('{date_value}', {date_column}) / {days}) AS `{alias}`"

            #     elif date_query_type == '3':
            #         date_column = format_date_column()
            #         interval_value = int(input("Enter interval value in months: "))
            #         date_query = f"(DATE_FORMAT(DATE_ADD({date_column}, INTERVAL {interval_value} MONTH), '%M %e, %Y')) AS `{alias}`"

            #     elif date_query_type == '4':
            #         date_column = format_date_column()
            #         date_query = f"(DATE_FORMAT({date_column}, '%M %e, %Y')) AS `{alias}`"

            #     elif date_query_type == '5':
            #         date_column = format_date_column()
            #         date_query = f"YEAR({date_column}) AS year, MONTH({date_column}) AS month, DAY({date_column}) AS day"

            #     return add_commas_to_query(date_query)



            # def add_commas_to_query(query):
            #     # Add commas to the query if needed
            #     query = query.strip()  # Remove leading and trailing whitespace
            #     if not query.endswith(","):
            #         query += ","  # Add a comma at the end if it's missing
            #     return query


            # def math_query_operations(math_query_type):
            #     if math_query_type == '1':
            #         math_column1 = math_column_name()
            #         math_symbol_option = math_symbol()
            #         math_column2_option = input("Provide column2 or enter an integer (C/I): ")
            #         if math_column2_option.upper() == 'C':
            #             math_column2 = math_column_name()
            #         elif math_column2_option.upper() == 'I':
            #             math_column2 = math_column_pure_integer()
            #         math_query = f"(({math_column1} {math_symbol_option} {math_column2}))"
            #     elif math_query_type == '2':
            #         math_column1 = math_column_name()
            #         math_column2 = math_column_name()
            #         math_query = f"(POWER({math_column1}, {math_column2}))"
            #     elif math_query_type == '3':
            #         math_column1 = math_column_name()
            #         math_query = f"(SQRT({math_column1}))"
            #     elif math_query_type == '4':
            #         math_column1 = math_column_name()
            #         math_query = f"(ABS({math_column1}))"
                
            #     # Prompt for alias name
            #     alias_name_prompt = input("Enter an alias name or 0 to skip: ")
            #     if alias_name_prompt != '0':
            #         math_query += f" AS `{alias_name_prompt}`"
                
            #     return math_query

           
            # def aggregate_option():
            #     valid_options = {'SUM', 'AVG', 'MAX', 'MIN', 'MONTH'}
            #     while True:
            #         option = input("Enter aggregate option (SUM, AVG, MAX, MIN, MONTH), or 0 to exit: ").upper()
            #         if option == '0':
            #             return None
            #         elif option in valid_options:
            #             return option
            #         else:
            #             print("Invalid option. Please enter one of: SUM, AVG, MAX, MIN")

            
            # def choose_aggregate_query_type():
            #     aggregate_query_type = input("Choose AGGREGATE query type:\n"
            #                         "1. SUM, AVG, MAX, MIN MONTH: (SUM(column)) OR SUM(DISTINCT column)\n"
            #                         "2. COUNT: (COUNT(*)) OR (COUNT(column))\n"
            #                         "3. DISTINCT(column) OR DISTINCT(AVG(column))\n"
            #                         "Enter option number ::: ")
            #     return aggregate_query_type
            

            # def handle_aggregate_query(select_columns, aggregate_query_type, math_symbol_option=None):
            #     if aggregate_query_type == '1':  # SUM, AVG, MAX, MIN MONTH
            #         aggregate_option_value = aggregate_option()
            #         aggregate_column_name = math_column_name()

            #         distinct_with_aggregate = input("Would you like to use DISTINCT? (y/n): ").lower()
            #         if distinct_with_aggregate == 'y':
            #             aggregate_query = f"{aggregate_option_value}(DISTINCT {aggregate_column_name})"
            #         else:
            #             aggregate_query = f"{aggregate_option_value}({aggregate_column_name})"
            #         alias = inner_alias_name()

            #         if select_columns == "":
            #             select_columns += f"{aggregate_query} AS `{alias}`"
            #         else:
            #             select_columns += f", {aggregate_query} AS `{alias}`"

            #     elif aggregate_query_type == '2':  # COUNT
            #         count_option = input("Would you like to use a column? (y/n) :::")
            #         if count_option.lower() == 'y':
            #             aggregate_column_name = math_column_name()
            #             aggregate_query = f"COUNT({aggregate_column_name})"
            #         else:
            #             aggregate_query = "COUNT(*)"
            #         alias = inner_alias_name()

            #         if select_columns == "":
            #             select_columns += f"{aggregate_query} AS `{alias}`"
            #         else:
            #             select_columns += f", {aggregate_query} AS `{alias}`"

            #     elif aggregate_query_type == '3':  # DISTINCT
            #         distinct_option = input("Would you like to use an aggregate function? (y/n): ").lower()
            #         if distinct_option == 'y':
            #             aggregate_option_value = aggregate_option()
            #             aggregate_column_names = general_column_name()
            #             aggregate_query = f"{aggregate_option_value}(DISTINCT {', '.join(aggregate_column_names)})"
            #         else:
            #             aggregate_column_names = general_column_name()
            #             aggregate_query = f"DISTINCT {', '.join(aggregate_column_names)}"

            #         alias = inner_alias_name()

            #         if select_columns == "":
            #             select_columns += f"{aggregate_query} AS `{alias}`"
            #         else:
            #             select_columns += f", {aggregate_query} AS `{alias}`"
            #     return select_columns


            # # Define a list to store queries
            # clear_window()
            # pending_queries = []

            # # Main loop to allow users to select options continuously
            # while True:
            #     # Present menu options to the user
            #     print_space()
            #     date_query_type = choose_date_query_type()
            #     print_space()
            #     math_query_type = choose_math_query_type()
            #     print_space()
            #     aggregate_query_type = choose_aggregate_query_type()
            #     print_space()

            #     select_option = input(
            #         "Choose from the list of queries:\n"
            #         f"1. {date_query_type}\n"
            #         f"2. {math_query_type}\n"
            #         f"3. {aggregate_query_type}\n"
            #         "Enter your option (0 to quit): "
            #     )
            #     print_space()

            #     # Exit loop if the user chooses to quit
            #     if select_option == '0':
            #         break

            #     # Store the pending query
            #     pending_query = ""
            #     select_option = int(select_option)

            #     if select_option == 1:
            #         # Perform date query operation
            #         print('-' * 45)
            #         print("Performing date query operation:".upper())
            #         print('-' * 45)
            #         date_query_type = choose_date_query_type()
            #         pending_query = date_query_operations(date_query_type)

            #     elif select_option == 2:
            #         # Perform math query operation
            #         print('-' * 45)
            #         print("Performing math query operation:".upper())
            #         print('-' * 45)
            #         math_query_type = choose_math_query_type()
            #         pending_query = math_query_operations(math_query_type)

            #         # Prompt user for math symbol option
            #         math_symbol_prompt = input("Would you like to use a math symbol? (y/n): ")
            #         if math_symbol_prompt.lower() == 'y':
            #             math_symbol_option = math_symbol()
            #             pending_query += math_symbol_option

            #     elif select_option == 3:
            #         # Handle aggregate query operation
            #         print('-' * 45)
            #         print("Performing aggregate query operation:".upper())
            #         print('-' * 45)
            #         aggregate_query_type = choose_aggregate_query_type()
            #         pending_query = handle_aggregate_query(select_columns, aggregate_query_type)

            #     # Add commas to the pending query for options 1 and 3
            #     if select_option in [1, 3]:
            #         # Remove any trailing commas before adding a new one
            #         if pending_query.endswith(','):
            #             pending_query = pending_query[:-1]
            #         pending_query += ','

            #     # Append the pending query to the list
            #     pending_queries.append(pending_query)

            # # Integrate the pending queries into the main SQL query
            # if pending_queries:
            #     select_columns = ' '.join(pending_queries)  
            #     if select_columns.endswith(','):
            #         select_columns = select_columns[:-1]  # Remove the last comma
                
            #     query_with_placeholder = "SELECT {} FROM {}"  
            #     final_query = query_with_placeholder.format(select_columns, table)  # Replace placeholder with actual values
            #     print(final_query) 
            # else:
            #     # No queries selected, use the existing query
            #     print(query_with_placeholder.format('*', table))  # Use placeholder with '*' for select columns

            


            # WHERE CLAUSE ======================================================================
            print_space()
            where_clause_query_choice = input("Would you like to add WHERE query clause? (y/n) :::")
            print_space()
            
            if where_clause_query_choice.lower() == 'y':
                where_query_option = choose_where_query_type()
                print_space()
                
                if where_query_option == '1': 
                    where_column = whereColumn()
                    math_symbol_option = math_symbol()
                    where_value = whereValue()
                    
                    # where_query = f"({where_column} {math_symbol_option} {where_value})"
                    where_query = ""
                    if isinstance(where_value, str):
                        where_query = f"({where_column} {math_symbol_option} '{where_value}')"
                    
                    elif isinstance(where_value, datetime.date):
                        formatted_date = where_value.strftime("%Y-%m-%d")
                        where_query = f"({where_column} {math_symbol_option} '{formatted_date}')"
                    else:
                        where_query = f"({where_column} {math_symbol_option} {where_value})"
                    select_query = f" WHERE {where_query}"
        
            else:
                select_query = ""
            


            # GROUP BY CLAUSE ======================================================================
            print_space()
            groupby_clause_query_choice = input("Would you like to add GROUP BY query clause? (y/n): ")
            print_space()

            groupby_select_query = ""
            if groupby_clause_query_choice.lower() == 'y':
                groupby_option = choose_groupby_type()
                
                if groupby_option == '1':
                    groupby_columns = multi_gen_column_name()
                    groupby_select_query = " GROUP BY " + ", ".join(groupby_columns)
                    
                elif groupby_option == '2':
                    groupby_columns1 = multi_gen_column_name()
                    groupby_select_query = f" GROUP BY {', '.join(groupby_columns1)} WITH ROLLUP"
                
                elif groupby_option == '3':
                    groupby_columns = multi_gen_column_name()
                    user_aggregate = input("Would you like to use an aggregate function? (y/n): ")
                    if user_aggregate.lower() == 'y' and groupby_columns:
                        groupby_option_value = aggregate_option()  # function returning aggregate function like COUNT, SUM, etc.
                        groupby_column_name = math_column_name()  # function returning the column name
                        if groupby_columns:
                            groupby_select_query = f" GROUP BY {', '.join(groupby_columns)}, {groupby_option_value}({groupby_column_name})"
                        else:
                            groupby_select_query = f" GROUP BY {groupby_option_value}({groupby_column_name})"
                    elif user_aggregate.lower() == 'y':
                        groupby_option_value = aggregate_option()  # function returning aggregate function like COUNT, SUM, etc.
                        groupby_column_name = math_column_name()  # function returning the column name
                        groupby_select_query = f" GROUP BY {groupby_option_value}({groupby_column_name})"
                    elif groupby_columns:
                        groupby_select_query = f" GROUP BY {', '.join(groupby_columns)}"
                    else:
                        groupby_select_query = ""
            else:
                groupby_select_query = ""
                


            # ORDERBY CLAUSE ======================================================================
            print_space()
            order_by_clause_query_choice = input("Would you like to add ORDER BY query clause? (y/n) :::")
            print_space()
            
            if order_by_clause_query_choice.lower() == 'y':
                order_by_query_option = choose_orderby_type()
                print_space()
   
                if order_by_query_option == '1':
                    order_by_column = multi_gen_column_name()
                    order_by_select_query = f" ORDER BY " + ", ".join(order_by_column)

                elif order_by_query_option == '2':
                    order_by_column = multi_gen_column_name()
                    order_operators = order_by_operators()
                    order_by_select_query = f" ORDER BY {', '.join(order_by_column)} {order_operators}"

                elif order_by_query_option == '3':
                    order_by_column1 = multi_gen_column_name()
                    order_operators1 = order_by_operators()
                    direct()
                    print_space()
                    order_by_column2 = multi_gen_column_name()
                    order_operators2 = order_by_operators()
                    order_by_select_query = f" ORDER BY {', '.join(order_by_column1)} {order_operators1}, {', '.join(order_by_column2)} {order_operators2}"   
                    
                elif order_by_query_option == '4':
                    order_by_column1 = multi_gen_column_name()
                    order_by_symbol = math_symbol()
                    
                    order_by_column_option = input("Provide column2 or enter an integer (C/I): ")
                    if order_by_column_option.upper() == 'C':
                        order_by_column2 = math_column_name()
                    elif order_by_column_option.upper() == 'I':
                        order_by_column2 = math_column_pure_integer()
                    order_by_select_query = f" ORDER BY {', '.join(order_by_column1)} {order_by_symbol} {order_by_column2}"
                
                elif order_by_query_option == '5':
                    order_by_column1 = multi_gen_column_name()
                    order_operators = order_by_operators()
                    order_by_column2 = multi_gen_column_name()
                    order_by_select_query = f" ORDER BY {', '.join(order_by_column1)} {order_operators}, {', '.join(order_by_column2)}" 
            else:
                order_by_select_query = ""
            


            # LIMIT CLAUSE ======================================================================
            print_space()
            limit_clause_query_choice = limit_option()
            
            if limit_clause_query_choice.lower() == 'y':
                show_limit = limit_display() 
                limit_offset_option = input("Would you like to use OFFSET? (y/n) :::") 
                
                if limit_offset_option.lower() == 'y':
                    offset_value = limit_offset()
                    limit_select_query = f" LIMIT {offset_value}, {show_limit}"
                else: 
                    limit_select_query = f" LIMIT {show_limit}"
            else:
                limit_select_query = ""
            
            
            
            
            

            # THE SQL QUERY -----------------------------------------------------------------
            query = f"SELECT {select_columns} FROM {table}{select_query}{groupby_select_query}{order_by_select_query}{limit_select_query}"
            queries.append(query)
            
            print_space()
            additional_option = input("Would you like to add more queries? Enter 'yes' to continue or any other key to skip: ")
            print_space()
            if additional_option.lower() != 'yes':
                break
            
        
        # Execute all queries and store results ---------------------------------------
        query_results = []
        for query in queries:
            clear_window()
            print_space()
            print("Query:", query)
            print_space()
            result = execute_query_and_show_data_for_single_select(databases_connection, query, table)
            query_results.append(result)

        # Display all query results
        # clear_window()
        combined_result = None  # Initialize combined_result
        for i, result in enumerate(query_results):
            print(f"Query {i+1}:")
            if result is not None:
                print(result)
                if combined_result is None:
                    combined_result = result
                else:
                    combined_result = pd.merge(combined_result, result, left_index=True, right_index=True)
            else:
                print("None.")

        # Display combined table
        if combined_result is not None:
            print("\n------------------------------------------------------------------------------------")
            print(f"Data from table '{table}':")
            print("------------------------------------------------------------------------------------")
            print(combined_result)
            print_space()
        else:
            print("No combined table generated due to missing query results.")
            

    # SAVE TO CSV FILE ----------------------------------------------------------------
    # List to store all query results
    query_results = []

    # Loop through all queries and execute them
    for query in queries:
        result_df = execute_query_and_show_data_for_single_select(databases_connection, query, table)
        if result_df is not None:
            query_results.append(result_df)

    # Check if there are any query results
    if query_results:
        # Combine all DataFrames horizontally
        combined_df = pd.concat(query_results, axis=1)

        # Check if user wants to save the data to CSV
        save_csv_choice = save_table_choice()
        if save_csv_choice.lower() in ('y', 'yes'):
            save_file_path = get_save_csv()
            combined_df.to_csv(save_file_path, index=False)  # Save DataFrame to CSV without index
            print(f"Data saved to '{save_file_path}'.")
        else:
            print("Data not saved to CSV.\n")
    else:
        print("No data to save. None of the queries returned any results.")


