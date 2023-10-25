import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import snowflake.connector
import os

# Replace these with your Snowflake account credentials and connection details
account = ''  # Replace with your Snowflake account URL
warehouse = ''
database = ''
schema = ''
username = ''  # Replace with your Snowflake username
password = ''  # Replace with your Snowflake password

# Create the SQL_Files and FileFormat folders
sql_files_dir = "SQL_Files"
file_format_dir = os.path.join(sql_files_dir, "File_Format")

if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(file_format_dir):
    os.mkdir(file_format_dir)

# Snowflake connection
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

# Query Snowflake to get a list of procedures in the specified database and schema
query = """select file_format_schema, file_format_name from information_schema.file_formats
             where file_format_schema = '{0}'
            """.format(schema)
            
# Execute the query to get the list of procedures
cursor.execute(query)

# Fetch the results
FORMAT_NAME = cursor.fetchall()

# Close the cursor and connection when done with the procedure query
cursor.close()

file_formats_to_extract = []
for i in FORMAT_NAME:
    schema_name = i[0]
    file_format_name = i[1]
    full_name = f"{schema_name}.{file_format_name}"
    file_formats_to_extract.append(full_name)

# Function to extract and save DDL statements to files
def extract_and_save_ddl(object_type, object_name):
    cursor = conn.cursor()

    # Create a query to extract DDL statements using GET_DDL
    query = f"SELECT GET_DDL('{object_type}', '{object_name}',true);"

    try:
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            ddl_statement = result[0]

            # Update the output directory to use "fileformat"
            output_directory = os.path.join('SQL_Files', 'File_Format')

            # Ensure the output directory exists, or create it
            os.makedirs(output_directory, exist_ok=True)

            # Update the output filename to use the new directory path
            output_filename = os.path.join(output_directory, f'{object_name}.sql')

            # Replace comments and print statements for consistency
            print(f'{object_type} {object_name} DDL saved to {output_filename}')

              # Save the DDL statement to the output file
            with open(output_filename, 'w') as file:
                file.write(ddl_statement)
        else:
            print(f'{object_type} {object_name} not found.')
            

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error extracting {object_type} {object_name}: {str(e)}")
    finally:
        cursor.close()

# Iterate over the file formats and extract DDL
for file_format_name in file_formats_to_extract:
    extract_and_save_ddl('FILE_FORMAT', file_format_name)

# Close the Snowflake connection
conn.close()