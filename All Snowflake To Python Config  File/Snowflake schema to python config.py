import warnings
import os
import snowflake.connector

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Replace these with your Snowflake account credentials and connection details
account = ''  # Replace with your Snowflake account URL
warehouse = 'DEMO_WH'
database = ''
username = ''  # Replace with your Snowflake username
password = ''  # Replace with your Snowflake password


# Create the SQL_Files and Schema folders
sql_files_dir = "SQL_Files"
schema_dir = os.path.join(sql_files_dir, "Schema")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(schema_dir):
    os.mkdir(schema_dir)

# Establish a connection to Snowflake
conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

# Query Snowflake to get a list of schemas and their details
schema_query = f'''
    SELECT  CATALOG_NAME , SCHEMA_NAME, IS_TRANSIENT, IS_MANAGED_ACCESS, RETENTION_TIME,COMMENT
    FROM {database}.information_schema.SCHEMATA
'''

# Execute the query to get the list of schemas and their details
cursor.execute(schema_query)

# Fetch the results
schemas = cursor.fetchall()

# Close the cursor
cursor.close()

# Iterate through the schemas and retrieve schema details
for schema_info in schemas:
    schema_name = schema_info[1]
    # Construct the fully qualified schema name
    fully_qualified_schema_name = f'{database}.{schema_name}'
    
#     Create a .sql file and write the schema details to it
    sql_file_name = f"{schema_dir}/{fully_qualified_schema_name}.sql"
    with open(sql_file_name, 'w') as sql_file:
        sql_file.write(f"DATABASE_NAME = {database}\n")  # Add the database name
        sql_file.write(f"SCHEMA_NAME = {schema_name}\n")
        sql_file.write(f"IS_TRANSIENT = {schema_info[2]}\n")
        sql_file.write(f"IS_MANAGED_ACCESS = {schema_info[3]}\n")
        sql_file.write(f"RETENTION_TIME = {schema_info[4]}\n")
        sql_file.write(f"COMMENT = {schema_info[5]}\n")
        

#     Print the file name (database.schema name)
    print(f"Generated SQL file: {sql_file_name}")

# Close the connection
conn.close()