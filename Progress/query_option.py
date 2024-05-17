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
        (5). Delete Item from Table
            
        (5). Delete All Items in a Table
        (6). Drop Table
        (7). Drop Database
                               
        8. Show data from a specific table
        9. Show data from a specific table and column
        10. Show data from a specific table and column with condition 
                            
        Enter your choice here! :::""")

    return query_menu

