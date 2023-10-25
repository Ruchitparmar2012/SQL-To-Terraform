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

# Create the SQL_Files and Sequence folders
sql_files_dir = "SQL_Files"
sequence_dir = os.path.join(sql_files_dir, "Sequence")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(sequence_dir):
    os.mkdir(sequence_dir)

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

# Query Snowflake to get a list of sequences in the specified database and schema
sequence_query = f'''
    SELECT sequence_name
    FROM information_schema.sequences
    WHERE sequence_schema = '{schema}' AND sequence_catalog = '{database}'
'''

# Execute the query to get the list of sequences
cursor.execute(sequence_query)

# Fetch the results
sequences = cursor.fetchall()

# Close the cursor and connection when done with the sequence query
cursor.close()

if sequences:
    # Iterate through the sequences and retrieve DDL statements
    for sequence_info in sequences:
        sequence_name = sequence_info[0]

        # Construct the fully qualified sequence name
        fully_qualified_sequence_name = f'{database}.{schema}.{sequence_name}'

        # Query to retrieve the DDL statement for the sequence
        ddl_query = f'''
            SELECT GET_DDL('SEQUENCE', '{schema}.{sequence_name}', true)
        '''

        # Create a new cursor for the DDL query
        cursor = conn.cursor()

        # Execute the DDL query
        cursor.execute(ddl_query)

        # Fetch the DDL statement
        ddl_statement = cursor.fetchone()[0]

        # Modify the DDL statement to include the database and schema names
        modified_ddl_statement = ddl_statement.replace(f'create or replace sequence {sequence_name}',
                                                       f'create or replace sequence {fully_qualified_sequence_name}')

        # Remove line breaks and extra spaces
        modified_ddl_statement = ddl_statement.replace('\r\n', '\n')

        # Create a .sql file and write the DDL statement to it
        sql_file_name = f"{sequence_dir}/{fully_qualified_sequence_name}.sql"
        with open(sql_file_name, 'w') as sql_file:
            sql_file.write(modified_ddl_statement)

        # Print the file name (database.schema.sequence name)
        print(f"Generated SQL file: {fully_qualified_sequence_name}")

        # Close the cursor for the DDL query
        cursor.close()
else:
    print(f"No sequences found in the specified schema '{schema}' in database '{database}'.")

# Close the connection
conn.close()