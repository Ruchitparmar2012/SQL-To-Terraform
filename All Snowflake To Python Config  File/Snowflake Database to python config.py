
import warnings
import os
import snowflake.connector

# Suppress DeprecationWarnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Replace these with your Snowflake account credentials and connection details
account = ''  # Replace with your Snowflake account URL
warehouse = ''
database = ''
schema = ''
username = ''  # Replace with your Snowflake username
password = ''  # Replace with your Snowflake password

#  Create the SQL_Files and Database folders
sql_files_dir = "SQL_Files"
database_dir = os.path.join(sql_files_dir, "Database")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(database_dir):
    os.mkdir(database_dir)

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

# Step 2: Fetch Snowflake database DDL
ddl_query = f'''
    SELECT GET_DDL('DATABASE', '{database}')
'''
# Execute the DDL query to get database DDL
cursor.execute(ddl_query)
database_ddl = cursor.fetchone()[0]

# Write the database DDL to a .sql file
database_sql_file = f"{database_dir}/{database}.sql"
with open(database_sql_file, 'w') as sql_file:
    sql_file.write(database_ddl)

print(f"Generated SQL file for database '{database}': {database_sql_file}")

# Step 3: Query information_schema to get database information
database_info_query = f'''
    SELECT *
    FROM information_schema.databases
    WHERE database_name = '{database}'
'''

cursor.execute(database_info_query)
database_info = cursor.fetchone()

# Print database information
print("\nDatabase Information:")
for column, value in zip(cursor.description, database_info):
    print(f"{column[0]}: {value}")

# Close the cursor
cursor.close()

# Close the connection
conn.close()
