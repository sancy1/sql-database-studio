from mysql.connector import Error
import pandas as pd

from db_script import get_database_name, create_server_connection, get_save_csv, read_query, load_csv_to_pandas, pretify_queries_line
from db_script import create_database, create_db_connection, clear_window, get_table_name, save_to_csv
from query_option import queriesOptions
from queries_code import execute_queries_and_show_data, save_table_choice, update_success_message, get_input_type, whereColumn, whereValue, single_select
from queries_code import execute_query_and_show_data_for_single_select, order_by_input, limit_display, limit_option, print_additional_queries_menu, math_symbol
from queries_code import andColumn, andValue, print_space, interval_column, format_date_column, choose_date_query_type, choose_math_query_type, math_column_name
from queries_code import date_column, date_add_prompt, datediff_prompt, alias_name_option, alias_name, math_column_pure_integer, inner_alias_name


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

        

    # 10. SINGLE SELECT CLAUS ---------------------------------------------------------------
    # elif queriesInput == '10':
    #     clear_window()
    #     print()
    #     title = "10. Show multiple columns data"
    #     pretify_queries_line(title)

    #     table = get_table_name()
    #     select_columns = single_select()
        
    #     print("")
    #     option = input("1. Enter 1 for ORDER BY\n2. Enter 2 for WHERE\nPress enter for default :::")
    #     print("")
        
    #     if option == '1': # ORDER BY
    #         order_by = order_by_input()
    #         limit_choice = limit_option()
            
    #         if limit_choice.lower() == 'y':
    #             show_limit = limit_display()
                
    #             if select_columns:
    #                 query_db = f"""
    #                 SELECT {select_columns}
    #                 FROM {table}
    #                 ORDER BY {order_by}
    #                 LIMIT {show_limit};
    #                 """
    #                 execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
    #             else:
    #                 print("No columns selected.")
    #         else:
    #             if select_columns:
    #                 query_db = f"""
    #                 SELECT {select_columns}
    #                 FROM {table}
    #                 ORDER BY {order_by};
    #                 """
    #                 execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
    #             else:
    #                 print("No columns selected.")
        
    #     elif option == '2': # WHERE
    #         where_column = whereColumn()
    #         where_value = whereValue()
    #         where_type = get_input_type(where_value)
            
    #         # Format the value if it's a string type
    #         if where_type == 'str':
    #             where_value = f"'{where_value}'"
            
    #         # Prompt for AND conditions
    #         print_space()
    #         and_columns = andColumn()
    #         print_space()
    #         and_values = andValue()
    #         where_conditions = [f"{where_column} = {where_value}"]
    #         where_conditions.extend([f"{column} = '{value}'" for column, value in zip(and_columns, and_values)])
    #         where_clause = " AND ".join(where_conditions)

    #         if select_columns:
    #             query_db = f"""
    #             SELECT {select_columns}
    #             FROM {table}
    #             WHERE {where_clause};
    #             """
    #             execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
    #         else:
    #             print("No columns selected.")
        
    #     elif not option:  # Checking if option is empty (user pressed Enter)
    #         if select_columns:
    #             query_db = f"""
    #             SELECT {select_columns}
    #             FROM {table};
    #             """
    #             execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
    #     else:
    #         print("Invalid option entered. Default selection will be displayed.")

    
    elif queriesInput == '10':
        clear_window()
        print()
        title = "10. Single Table Queries"
        pretify_queries_line(title)

        table = get_table_name()
        
        while True:
            additional_option = input("Would you like to add more queries? Enter 0 to quit :::")
            if additional_option == '0':
                break
            else:
                # Prompt the user to choose additional queries
                print_space()
                print_additional_queries_menu()
                additional_query_option = input("Enter option number(s) separated by comma (e.g., '1,2') :::").split(',')
                additional_query_option = [int(option.strip()) for option in additional_query_option]
                print_space()
                
                # Initialize variables to store query components
                select_columns = single_select()  # Existing function to prompt for column names
                
                # Prompt for alias names
                print_space()
                alias_option = input("Do you want to specify column aliases? (y/n): ")
                if alias_option.lower() == 'y':
                    alias_names = []
                    for column in select_columns.split(','):
                        alias_names.append(input(f"Enter alias for column '{column}': "))
                    select_columns = ', '.join([f"{column} AS `{alias}`" for column, alias in zip(select_columns.split(','), alias_names)])
                        
                # Inside the elif block for option 1 (DATE)
                if 1 in additional_query_option:
                    # Date option ---------------------------------------------------------
                    print_space()
                    date_query_type = choose_date_query_type()
                    print_space()
                    
                    if date_query_type == '1':
                        # MAIN DATEDIFF 
                        end_date_column = format_date_column()
                        start_date_column = format_date_column()
                        date_query = f"DATEDIFF({end_date_column}, {start_date_column})"
                        alias = inner_alias_name()
                        select_columns += f", {date_query} AS `{alias}`"
                    
                    elif date_query_type == '2':
                        # DATEDIFF option
                        date_value, date_column, days = datediff_prompt()
                        date_query = f"ROUND(DATEDIFF('{date_value}', {date_column}) / {days})"
                        alias = inner_alias_name()  # Prompt for an alias
                        select_columns += f", {date_query} AS `{alias}`"

                    elif date_query_type == '3':
                        # DATE_FORMAT option
                        date_column = format_date_column() 
                        interval_value = interval_column()
                        date_query = f"(DATE_FORMAT(DATE_ADD(`{date_column}`, INTERVAL {interval_value} MONTH), '%M %e, %Y'))"
                        alias = inner_alias_name()  # Prompt for an alias
                        select_columns += f", {date_query} AS `{alias}`"
                    
                    elif date_query_type == '4':
                        # DATE_FORMAT option
                        date_column = format_date_column() 
                        date_query = f"(DATE_FORMAT(`{date_column}`, '%M %e, %Y'))"
                        alias = inner_alias_name()  # Prompt for an alias
                        select_columns += f", {date_query} AS `{alias}`"
                    
                    elif date_query_type == '5':
                        # Extract year, month, and day from date column
                        date_column = format_date_column()
                        select_columns += f", YEAR({date_column}) AS year, MONTH({date_column}) AS month, DAY({date_column}) AS day"


                elif 2 in additional_query_option:
                    # Math option ---------------------------------------------------------
                    print_space()
                    math_query_type = choose_math_query_type()
                    print_space()
                    
                    if math_query_type == '1': 
                        # Basic Math
                        math_column1 = math_column_name()
                        math_symbol_option = math_symbol()
                        
                        print_space()
                        math_column2_option = input("Provide column2 or enter an integer (C/I) :::")
                        if math_column2_option.upper() == 'C':
                            math_column2 = math_column_name()
                        elif math_column2_option.upper() == 'I':
                            math_column2 = math_column_pure_integer()
                        
                        date_query = f"({math_column1} {math_symbol_option} {math_column2})"
                        alias = inner_alias_name()
                        select_columns += f", {date_query} AS `{alias}`"
                    
                    elif math_query_type == '2':
                        # EXPONENTIATION POWER:
                        math_column1 = math_column_name()
                        print_space()
                        
                        math_column2_option = input("Provide column2 or enter an integer (C/I) :::")
                        if math_column2_option.upper() == 'C':
                            math_column2 = math_column_name()
                        elif math_column2_option.upper() == 'I':
                            math_column2 = math_column_pure_integer()
                        
                        date_query = f"POWER({math_column1}, {math_column2})"
                        alias = inner_alias_name()
                        select_columns += f", {date_query} AS `{alias}`"
                    
                    elif math_query_type == '3':
                        # SQUARE ROOT
                        math_column1 = math_column_name()
                        
                        date_query = f"SQRT({math_column1})"
                        alias = inner_alias_name()
                        select_columns += f", {date_query} AS `{alias}`"
                    
                    elif math_query_type == '4':
                        # ABSOLUTE VALUE
                        math_column1 = math_column_name()
                        
                        date_query = f"ABS({math_column1})"
                        alias = inner_alias_name()
                        select_columns += f", {date_query} AS `{alias}`"
                        
                    
                    
                # Finalize the SELECT query
                select_query = f"SELECT {select_columns} FROM {table}"
                # Execute the query and display the results
                execute_query_and_show_data_for_single_select(databases_connection, select_query, table)
               
               
               
        # DISPLAY SIMPLE SELECT FROM TABLE WITHOUT WHERE CLAUSE       
        select_columns = single_select()
        if select_columns:
                query_db = f"""
                SELECT {select_columns}
                FROM {table};
                """
                execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
        else:
            print("Invalid option entered. Default selection will be displayed.")
    