from db_script import clear_window, pretify_queries_line


def queriesOptions():
    clear_window()
    print()
    title = "Query Menu"
    pretify_queries_line(title)
    
    # QUERY MENU ---------------------------------------------------------------------
    query_menu = input("""
            
        (1). Display/Show all data from the database
        (2). Show All Databases
        (3). Show All Tables in the Database
        (4). Update Table
        
        (5). Delete An Item from a Table 
        (6). Delete All Items in a Table
        (7). Drop Table
        (8). Drop Database
        
        (9). Duplicate Table                
        (10). Single Table Queries
        
        11. Show data from a specific table and column
        12. Show data from a specific table and column with condition 
                            
        Enter your choice here! :::""")

    return query_menu

