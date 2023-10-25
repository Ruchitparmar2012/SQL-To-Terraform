import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import os
import snowflake.connector

# Replace these with your Snowflake account credentials and connection details
account = ''  # Replace with your Snowflake account URL
warehouse = ''
database = ''
schema = ''
username = ''  # Replace with your Snowflake username
password = ''  # Replace with your Snowflake password

# Create the SQL_Files and Function folders
sql_files_dir = "SQL_Files"
function_dir = os.path.join(sql_files_dir, "Function")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(function_dir):
    os.mkdir(function_dir)

# Establish a connection to Snowflake
conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role='SYSADMIN'
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

# Query Snowflake to get a list of functions in the specified database and schema
function_query = f'''
    SELECT function_name, ARGUMENT_SIGNATURE
    FROM information_schema.functions
    WHERE function_schema = '{schema}' AND function_catalog = '{database}'
'''

# Execute the query to get the list of functions
cursor.execute(function_query)

# Fetch the results
functions = cursor.fetchall()

# Close the cursor and connection when done with the function query
cursor.close()

def extract_type(ip_string):
    ip_string_trimmed = ip_string.replace('(','').replace(')','')
    elements = [element.strip() for element in ip_string_trimmed.split(',')]
    dtypes = [element.split()[-1] for element in elements]
    result = ','.join(dtypes)
    return result

file_cnt = {}

if functions:
    # Iterate through the functions and retrieve DDL statements
    for function_info in functions:
        function_name = function_info[0]
        if '()' not in function_info[1]:
            function_details = extract_type(function_info[1])
            ddl_query = f'''SELECT GET_DDL('FUNCTION', '{database}.{schema}.{function_name}({function_details})',true)'''
        else:
            ddl_query = f'''SELECT GET_DDL('FUNCTION', '{database}.{schema}.{function_name}()',true)'''

        # Construct the fully qualified function name
        fully_qualified_function_name = f'{database}.{schema}.{function_name}'

        # Create a new cursor for the DDL query
        cursor = conn.cursor()

        # Execute the DDL query
        cursor.execute(ddl_query)

        # Fetch the DDL statement
        ddl_statement = cursor.fetchone()[0]

        # Modify the DDL statement to include the database and schema names
        
        #modified_ddl_statement = ddl_statement.replace(f'create or replace FUNCTION "{function_name}"',
                                                      #f'CREATE OR REPLACE FUNCTION {fully_qualified_function_name}')

        # Remove line breaks and extra spaces
        modified_ddl_statement =  ddl_statement.replace('\r\n', '\n')

        # Step 2: Create a .sql file and write the DDL statement to it
        sql_file_name = f"{function_dir}/{fully_qualified_function_name}.sql"
        #print(sql_file_name)
        
        if sql_file_name in file_cnt:
            file_cnt[sql_file_name] += 1
        else:
            file_cnt[sql_file_name] = 1

        new_name = sql_file_name
        if file_cnt[sql_file_name] > 1:
            base_name, ext = os.path.splitext(sql_file_name)
            new_name = f"{base_name}_{file_cnt[sql_file_name]}{ext}"
            
        with open(new_name, 'w') as sql_file:     
            sql_file.write(modified_ddl_statement)
            
        #Step 3: Print the file name (database.schema.function name)
        print(f"Generated SQL file: {new_name}")

        # Close the cursor for the DDL query
        cursor.close()
else:
    print(f"No functions found in the specified schema '{schema}' in database '{database}'.")

# Close the connection
conn.close()
