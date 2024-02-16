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
schema_dir = os.path.join(sql_files_dir, "Database")

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
    SELECT DISTINCT DATABASE_NAME ,DATABASE_OWNER,IS_TRANSIENT,RETENTION_TIME,COMMENT FROM {database}.INFORMATION_SCHEMA."DATABASES" 
'''

# Execute the query to get the list of schemas and their details
cursor.execute(schema_query)

# Fetch the results
schemas = cursor.fetchall()

# Close the cursor
cursor.close()

# Iterate through the schemas and retrieve schema details
for schema_info in schemas:
        
    
    # Construct the fully qualified schema name
    fully_qualified_schema_name = f'{schema_info[0]}'
    
#     Create a .sql file and write the schema details to it
    sql_file_name = f"{schema_dir}/{fully_qualified_schema_name}.sql"
    with open(sql_file_name, 'w') as sql_file:
        sql_file.write(f"DATABASE_NAME = {schema_info[0]}\n")
        sql_file.write(f"IS_TRANSIENT = {schema_info[2]}\n")
        sql_file.write(f"RETENTION_TIME = {schema_info[3]}\n")
        sql_file.write(f"COMMENT = {schema_info[4]}\n")
        

#     Print the file name (database.schema name)
    print(f"Generated SQL file: {sql_file_name}")

# Close the connection
conn.close()

