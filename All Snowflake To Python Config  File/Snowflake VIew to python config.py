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

# Create the SQL_Files and View folders
sql_files_dir = "SQL_Files"
view_dir = os.path.join(sql_files_dir, "View")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(view_dir):
    os.mkdir(view_dir)

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

# Query Snowflake to get a list of views in the specified database and schema
view_query = f'''
    SELECT table_name
    FROM information_schema.views
    WHERE table_schema = '{schema}' AND table_catalog = '{database}'
'''

# Execute the query to get the list of views
cursor.execute(view_query)

# Fetch the results
views = cursor.fetchall()

# Close the cursor and connection when done with the view query
cursor.close()

if views:
    # Iterate through the views and retrieve DDL statements
    for view_info in views:
        view_name = view_info[0]

        # Construct the fully qualified view name
        fully_qualified_view_name = f'{database}.{schema}.{view_name}'

        # Query to retrieve the DDL statement for the view
        ddl_query = f'''
            SELECT GET_DDL('VIEW', '{schema}.{view_name}', true)
        '''

        # Create a new cursor for the DDL query
        cursor = conn.cursor()

        # Execute the DDL query
        cursor.execute(ddl_query)

        # Fetch the DDL statement
        ddl_statement = cursor.fetchone()[0]


        # Modify the DDL statement to include the database and schema names
        modified_ddl_statement = ddl_statement.replace(f'create or replace view {view_name}',
                                                       f'create or replace view {fully_qualified_view_name}')

        # Remove line breaks and extra spaces
        modified_ddl_statement = ddl_statement.replace('\r\n', '\n')

        # Step 2: Create a .sql file and write the DDL statement to it
        sql_file_name = f"{view_dir}/{fully_qualified_view_name}.sql"
        with open(sql_file_name, 'w') as sql_file:
            sql_file.write(modified_ddl_statement)

        # Step 3: Print the file name (database.schema.view name)
        print(f"Generated SQL file: {fully_qualified_view_name}")

        # Close the cursor for the DDL query
        cursor.close()
else:
    print(f"No views found in the specified schema '{schema}' in database '{database}'.")

# Close the connection
conn.close()