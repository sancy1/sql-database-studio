from mysql.connector import Error
import pandas as pd

from db_script import get_database_name, create_server_connection, get_save_csv, read_query, load_csv_to_pandas, pretify_queries_line
from db_script import create_database, create_db_connection, clear_window, get_table_name, save_to_csv
from query_option import queriesOptions
from queries_code import execute_queries_and_show_data, save_table_choice, update_success_message, get_input_type, whereColumn, whereValue, single_select
from queries_code import execute_query_and_show_data_for_single_select, order_by_input, limit_display, limit_option, and_option, how_many_and_do_you_want
from queries_code import andColumn, andValue, print_space


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
        
        

    # 6. SINGLE SELECT CLAUS ---------------------------------------------------------------
    elif queriesInput == '6':
        clear_window()
        print()
        title = "6. Show multiple columns data"
        pretify_queries_line(title)

        table = get_table_name()
        select_columns = single_select()
        
        print("")
        option = input("1. Enter 1 for ORDER BY\n2. Enter 2 for WHERE\nPress enter for default :::")
        print("")
        
        if option == '1': # ORDER BY
            order_by = order_by_input()
            limit_choice = limit_option()
            
            if limit_choice.lower() == 'y':
                show_limit = limit_display()
                
                if select_columns:
                    query_db = f"""
                    SELECT {select_columns}
                    FROM {table}
                    ORDER BY {order_by}
                    LIMIT {show_limit};
                    """
                    execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                else:
                    print("No columns selected.")
            else:
                if select_columns:
                    query_db = f"""
                    SELECT {select_columns}
                    FROM {table}
                    ORDER BY {order_by};
                    """
                    execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                else:
                    print("No columns selected.")
        
        elif option == '2': # WHERE
            where_column = whereColumn()
            where_value = whereValue()
            where_type = get_input_type(where_value)
            
            # Format the value if it's a string type
            if where_type == 'str':
                where_value = f"'{where_value}'"
            
            # Ask the user if they want to add additional AND conditions
            and_choice = and_option() 
            if and_choice.lower() == 'y':
                how_many_and = how_many_and_do_you_want()
                print_space()
                
                # Handle the case of one AND condition
                if how_many_and == 1:
                    and_column = andColumn()
                    and_value = andValue()
                    and_type = get_input_type(and_value)
                    
                    # Format the value if it's a string type
                    if and_type == 'str':
                        and_value = f"'{and_value}'"

                    # Construct and execute the SQL query
                    if select_columns:
                        query_db = f"""
                        SELECT {select_columns}
                        FROM {table}
                        WHERE {where_column} = {where_value} AND {and_column} = {and_value};
                        """
                        execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                    else:
                        print("No columns selected.")
                        
                # Handle the case of two AND conditions
                elif how_many_and == 2:
                    and_column1 = andColumn()
                    and_value1 = andValue()
                    and_type1 = get_input_type(and_value1)
                    
                    and_column2 = andColumn()
                    and_value2 = andValue()
                    and_type2 = get_input_type(and_value2)
                    
                    # Format the values if they're string types
                    if and_type1 == 'str':
                        and_value1 = f"'{and_value1}'"
                    if and_type2 == 'str':
                        and_value2 = f"'{and_value2}'"

                    # Construct and execute the SQL query
                    if select_columns:
                        query_db = f"""
                        SELECT {select_columns}
                        FROM {table}
                        WHERE {where_column} = {where_value} AND {and_column1} = {and_value1} AND {and_column2} = {and_value2};
                        """
                        execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                    else:
                        print("No columns selected.")
                        
                # Handle the case of three AND conditions
                elif how_many_and == 3:
                    and_column2 = andColumn()
                    and_value2 = andValue()
                    and_type2 = get_input_type(and_value2)
                    
                    and_column3 = andColumn()
                    and_value3 = andValue()
                    and_type3 = get_input_type(and_value3)
                    
                    # Format the values if they're string types
                    if and_type2 == 'str':
                        and_value2 = f"'{and_value2}'"
                    if and_type3 == 'str':
                        and_value3 = f"'{and_value3}'"

                    # Construct and execute the SQL query
                    if select_columns:
                        query_db = f"""
                        SELECT {select_columns}
                        FROM {table}
                        WHERE {where_column} = {where_value} AND {and_column2} = {and_value3} AND {and_column3} = {and_value3};
                        """
                        execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                    else:
                        print("No columns selected.")
                        
                # Handle the case of four AND conditions
                elif how_many_and == 4:
                    and_column4 = andColumn()
                    and_value4 = andValue()
                    and_type4 = get_input_type(and_value4)
                    
                    and_column5 = andColumn()
                    and_value5 = andValue()
                    and_type5 = get_input_type(and_value5)
                    
                    # Format the values if they're string types
                    if and_type4 == 'str':
                        and_value4 = f"'{and_value4}'"
                    if and_type5 == 'str':
                        and_value5 = f"'{and_value5}'"

                    # Construct and execute the SQL query
                    if select_columns:
                        query_db = f"""
                        SELECT {select_columns}
                        FROM {table}
                        WHERE {where_column} = {where_value} AND {and_column4} = {and_value4} AND {and_column5} = {and_value5};
                        """
                        execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                    else:
                        print("No columns selected.")

                                
                else:
                    if select_columns:
                        query_db = f"""
                        SELECT {select_columns}
                        FROM {table}
                        WHERE {where_column} = {where_value};
                        """
                        execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
                    else:
                        print("No columns selected.")
                
                
                
        elif not option:  # Checking if option is empty (user pressed Enter)
            if select_columns:
                query_db = f"""
                SELECT {select_columns}
                FROM {table};
                """
                execute_query_and_show_data_for_single_select(databases_connection, query_db, table)
        else:
            print("Invalid option entered. Default selection will be displayed.")
            

    