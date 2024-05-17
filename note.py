# Define sub-functions for each query type
            def date_query_operations(date_query_type):
                if date_query_type == '1':
                    end_date_column = format_date_column()
                    start_date_column = format_date_column()
                    date_query = f"DATEDIFF({end_date_column}, {start_date_column})"
                elif date_query_type == '2':
                    date_value = input("Enter date value (e.g., '2024-04-12'): ")
                    date_column = format_date_column()
                    days = int(input("Enter number of days (e.g., 365): "))
                    date_query = f"ROUND(DATEDIFF('{date_value}', {date_column}) / {days})"
                elif date_query_type == '3':
                    date_column = format_date_column()
                    interval_value = int(input("Enter interval value in months: "))
                    date_query = f"(DATE_FORMAT(DATE_ADD({date_column}, INTERVAL {interval_value} MONTH), '%M %e, %Y'))"
                elif date_query_type == '4':
                    date_column = format_date_column()
                    date_query = f"(DATE_FORMAT({date_column}, '%M %e, %Y'))"
                elif date_query_type == '5':
                    date_column = format_date_column()
                    date_query = f"YEAR({date_column}) AS year, MONTH({date_column}) AS month, DAY({date_column}) AS day"
                return date_query

            def math_query_operations(math_query_type):
                if math_query_type == '1':
                    math_column1 = math_column_name()
                    math_symbol_option = math_symbol()
                    math_column2_option = input("Provide column2 or enter an integer (C/I): ")
                    if math_column2_option.upper() == 'C':
                        math_column2 = math_column_name()
                    elif math_column2_option.upper() == 'I':
                        math_column2 = math_column_pure_integer()
                    math_query = f"({math_column1} {math_symbol_option} {math_column2})"
                elif math_query_type == '2':
                    math_column1 = math_column_name()
                    math_column2 = math_column_name()
                    math_query = f"POWER({math_column1}, {math_column2})"
                elif math_query_type == '3':
                    math_column1 = math_column_name()
                    math_query = f"SQRT({math_column1})"
                elif math_query_type == '4':
                    math_column1 = math_column_name()
                    math_query = f"ABS({math_column1})"
                return math_query

           
            def aggregate_option():
                valid_options = {'SUM', 'AVG', 'MAX', 'MIN', 'MONTH'}
                while True:
                    option = input("Enter aggregate option (SUM, AVG, MAX, MIN, MONTH), or 0 to exit: ").upper()
                    if option == '0':
                        return None
                    elif option in valid_options:
                        return option
                    else:
                        print("Invalid option. Please enter one of: SUM, AVG, MAX, MIN")

            
            def choose_aggregate_query_type():
                aggregate_query_type = input("Choose AGGREGATE query type:\n"
                                    "1. SUM, AVG, MAX, MIN MONTH: (SUM(column)) OR SUM(DISTINCT column)\n"
                                    "2. COUNT: (COUNT(*)) OR (COUNT(column))\n"
                                    "3. DISTINCT(column) OR DISTINCT(AVG(column))\n"
                                    "Enter option number ::: ")
                return aggregate_query_type
            


            def handle_aggregate_query(select_columns, aggregate_query_type, math_symbol_option=None):
                if aggregate_query_type == '1':  # SUM, AVG, MAX, MIN MONTH
                    aggregate_option_value = aggregate_option()
                    aggregate_column_name = math_column_name()

                    distinct_with_aggregate = input("Would you like to use DISTINCT? (y/n): ").lower()
                    if distinct_with_aggregate == 'y':
                        aggregate_query = f"{aggregate_option_value}(DISTINCT {aggregate_column_name})"
                    else:
                        aggregate_query = f"{aggregate_option_value}({aggregate_column_name})"
                    alias = inner_alias_name()

                    if select_columns == "":
                        select_columns += f"{aggregate_query} AS `{alias}`"
                    else:
                        select_columns += f", {aggregate_query} AS `{alias}`"

                elif aggregate_query_type == '2':  # COUNT
                    count_option = input("Would you like to use a column? (y/n) :::")
                    if count_option.lower() == 'y':
                        aggregate_column_name = math_column_name()
                        aggregate_query = f"COUNT({aggregate_column_name})"
                    else:
                        aggregate_query = "COUNT(*)"
                    alias = inner_alias_name()

                    if select_columns == "":
                        select_columns += f"{aggregate_query} AS `{alias}`"
                    else:
                        select_columns += f", {aggregate_query} AS `{alias}`"

                elif aggregate_query_type == '3':  # DISTINCT
                    distinct_option = input("Would you like to use an aggregate function? (y/n): ").lower()
                    if distinct_option == 'y':
                        aggregate_option_value = aggregate_option()
                        aggregate_column_names = general_column_name()
                        aggregate_query = f"{aggregate_option_value}(DISTINCT {', '.join(aggregate_column_names)})"
                    else:
                        aggregate_column_names = general_column_name()
                        aggregate_query = f"DISTINCT {', '.join(aggregate_column_names)}"

                    alias = inner_alias_name()

                    if select_columns == "":
                        select_columns += f"{aggregate_query} AS `{alias}`"
                    else:
                        select_columns += f", {aggregate_query} AS `{alias}`"

                if math_symbol_option:
                    select_columns += f" {math_symbol_option}"

                return select_columns




            
            
            # Define a list to store queries
            pending_queries = []

            # Main loop to allow users to select options continuously
            while True:
                # Present menu options to the user
                print_space()
                date_query_type = choose_date_query_type()
                math_query_type = choose_math_query_type()
                aggregate_query_type = choose_aggregate_query_type()
                print_space()

                select_option = input(
                    "Choose from the list of queries:\n"
                    f"1. {date_query_type}\n"
                    f"2. {math_query_type}\n"
                    f"3. {aggregate_query_type}\n"
                    "Enter your option (0 to quit): "
                )

                # Exit loop if the user chooses to quit
                if select_option == '0':
                    break

                # Store the pending query
                pending_query = ""

                select_option = int(select_option)

                if select_option == 1:
                    # Perform date query operation
                    print("Performing date query operation:")
                    date_query_type = choose_date_query_type()
                    pending_query = date_query_operations(date_query_type)

                elif select_option == 2:
                    # Perform math query operation
                    print("Performing math query operation:")
                    math_query_type = choose_math_query_type()
                    pending_query = math_query_operations(math_query_type)

                    # Prompt user for math symbol option
                    math_symbol_prompt = input("Would you like to use a math symbol? (y/n): ")
                    if math_symbol_prompt.lower() == 'y':
                        math_symbol_option = math_symbol()
                        pending_query += math_symbol_option

                elif select_option == 3:
                    # Handle aggregate query operation
                    print("Performing aggregate query operation:")
                    aggregate_query_type = choose_aggregate_query_type()
                    math_symbol_prompt = input("Would you like to use Math symbol? (y/n): ")
                    if math_symbol_prompt.lower() == 'y':
                        math_symbol_option = math_symbol()
                        pending_query = handle_aggregate_query(select_columns, aggregate_query_type, math_symbol_option)
                    else:
                        pending_query = handle_aggregate_query(select_columns, aggregate_query_type)


                # Append the pending query to the list
                pending_queries.append(pending_query)

            # Integrate the pending queries into the main SQL query
            select_columns = ' '.join(pending_queries)  # Modified here to include spaces between expressions
