from mysql.connector import Error

from db_script import get_database_name, get_file_name, get_reference_details, create_server_connection
from db_script import create_database, create_db_connection, execute_query, read_csv_create_table, clear_window
from db_script import create_many_to_many_table, read_csv_file, insert_data_from_csv, get_save_csv, get_table_name
from db_script import convert_date_format, read_csv_file, insert_data_from_csv, read_query, save_to_csv, load_csv_to_pandas
from db_script import pretify_headings_line, pretify_queries_line, handle_connection_options
from main_queries_code import query_database_codes


def main():
    while True:
        clear_window()
        print("\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
        print("|| SQL Database Studio || Alexander Cyril ".upper())  
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

        print("")
        title = "CONNECT TO MYSQL SERVER"
        pretify_headings_line(title)
        host, user, password = handle_connection_options()
                
        # MAIN MNEU ---------------------------------------------------------------
        clear_window()
        print("")
        title = "APP MENU"
        pretify_headings_line(title)
            
        menu = input("1. Enter 1 to to perfrom the following:\nA. Create database\nB. Create table and relationship\nC. Insert values into table\n\n2. Enter 2 to write queries or retrive data from database\n3. Enter 3 to view data with pandas\n\nEnter your choice here :::")
        
        if menu == '1':
            # Create and Connect to Database ---------------------------------------------------------------
            connection = create_server_connection(host, user, password) # Connect to server (MYSQL Workbench)
            database_name = get_database_name() # Get database name from the user
            
            create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}" 
            create_database(connection, create_database_query) # Create database
            databases_connection = create_db_connection(host, user, password, database_name) # connect to datatbase


            # Create Tables ---------------------------------------------------------------
            clear_window()
            print("")
            title = "CREATE TABLE AND INSERT VALUES"
            pretify_headings_line(title)
            table_option = input("1. Enter 1 to create table\n2. Enter 2 to create many_to_many relationship table\n3. Enter 3 to Insert Values into an existing Table :::")
            
                # Create Table From CSV File ---------------------------------------------------------------
            if table_option == '1':
                clear_window()
                print("")
                title = "CREATE TABLE"
                pretify_headings_line(title)
                
                csv_file_path = get_file_name()
                references = get_reference_details()
                
                processed_headers = read_csv_create_table(csv_file_path, references)
                print(f"\n{processed_headers}\n")
                create_db_table(processed_headers, host, user, password, database_name)


                # Create Many to Many Relationship Table ---------------------------------------------------
            elif table_option == '2':
                clear_window()
                print("")
                title = "MANY-TO-MANY RELATIONSHIP"
                pretify_headings_line(title)
                
                table_name = input("Enter the name for the many-to-many relationship table. '0' to quit\nHINT use the name of the tables (table name) to create many to many table name. e.g participant_course.\nThese are two tables names that formed many to many relation therfore must be combined as one name :::")
                references = get_reference_details()
                
                many_to_many_table_query = create_many_to_many_table(connection, references, table_name)
                if many_to_many_table_query:
                    print(f"\n{many_to_many_table_query}\n")
                else:
                    print("Error: Failed to create many-to-many relationship table.")
                many_to_many_table(many_to_many_table_query, host, user, password, database_name)


            # Insert Values into the Table ---------------------------------------------------
            elif table_option == '3':
                clear_window()
                print("")
                title = "INSERT VALUES INTO A TABLE"
                pretify_headings_line(title)
                
                file_path = get_file_name()  # Get the file path from user input
                headings, data = read_csv_file(file_path)  # Extract headings and data from CSV file
                table_name = get_table_name()  # Get the existing table name from user input
                insert_data_from_csv(databases_connection, table_name, headings, data)  # Insert data into the existing table
                
                print(headings)
                # print("Data:")
                for row in data:
                    print(row)
                            
            elif table_option == '0':
                print("You quit the App...\n")
                exit()
                
                
        # 3. View Data with pandas ------------------------------------------------------------
        elif menu == '3':
            clear_window()
            print()
            title = "3. View Data with Pandas"
            pretify_queries_line(title)
            
            csv_file_path = get_file_name()
            csv_pandas = load_csv_to_pandas(csv_file_path)
            pandas_success_message(f"\n{csv_pandas}")
            
            
        # Query Database ------------------------------------------------------------------
        elif menu == '2':
            query_database_codes(host, user, password)
            
   
        # REPEAT THE PROGRAM
        if input("\nPerforme Another SQL Query? (Y/N) :::").strip().upper() != 'Y':
            print("=============== END OF PROGRAM. GOODBYE! ===============\n")
            clear_window()
            break
        
        



# Create Table in Database ---------------------------------------------------------------
def create_db_table(processed_headers, host, user, password, database_name):
    connection = create_db_connection(host, user, password, database_name) 
    try:
        if connection and processed_headers:
            execute_query(connection, processed_headers) # Execute defined query
            print("Table Created successfully\n")
    except Error as err:
            print(f"Error Creating Table: {err}")
            


# Create Many to Many Relationship Table in Database ---------------------------------------------------------------
def many_to_many_table(many_to_many_table_query, host, user, password, database_name):
    connection = create_db_connection(host, user, password, database_name) 
    try:
        if connection and many_to_many_table_query:
            execute_query(connection, many_to_many_table_query) # Execute defined query
            print("Table Created successfully\n")
    except Error as err:
            print(f"Error Creating Table: {err}")

            

# Pandas Success Message ---------------------------------------------------------------
def pandas_success_message(csv_pandas):
    if csv_pandas is not None:
        print(csv_pandas)
        
 

if __name__ == "__main__":
    main()
