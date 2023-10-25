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

#  Create the SQL_Files and Table folders
sql_files_dir = "SQL_Files"
table_dir = os.path.join(sql_files_dir, "Table")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(table_dir):
    os.mkdir(table_dir)

# Establish a connection to Snowflake
conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

# Query Snowflake to get a list of tables in the specified database and schema
table_query = f'''
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema}' AND table_catalog = '{database}'
'''

# Execute the query to get the list of tables
cursor.execute(table_query)

# Fetch the results
tables = cursor.fetchall()

# Close the cursor and connection when done with the table query
cursor.close()

if tables:
    # Iterate through the tables and retrieve DDL statements
    for table_info in tables:
        table_name = table_info[0]

        # Construct the fully qualified table name
        fully_qualified_table_name = f'{database}.{schema}.{table_name}'

        # Query to retrieve the DDL statement for the table
        ddl_query = f'''
            SELECT GET_DDL('TABLE', '{schema}.{table_name}')
        '''

        # Create a new cursor for the DDL query
        cursor = conn.cursor()

        # Execute the DDL query
        cursor.execute(ddl_query)

        # Fetch the DDL statement
        ddl_statement = cursor.fetchone()[0]

        # Modify the DDL statement to include the database and schema names
        modified_ddl_statement = ddl_statement.replace(f'create or replace TABLE {table_name}',
                                                      f'CREATE OR REPLACE TABLE {fully_qualified_table_name}')

        # Step 2: Create a .sql file and write the DDL statement to it
        sql_file_name = f"{table_dir}/{fully_qualified_table_name}.sql"
        with open(sql_file_name, 'w') as sql_file:
            sql_file.write(modified_ddl_statement)

        # Step 3: Print the file name (database.schema.table name)
        print(f"Generated SQL file: {sql_file_name}")

        # Close the cursor for the DDL query
        cursor.close()
else:
    print(f"No tables found in the specified schema '{schema}' in database '{database}'.")

# Close the connection
conn.close()