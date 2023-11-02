# this is my code 
import os
import re
import time  # Import the time module

# Get the current time in microseconds before starting execution
start_time = time.perf_counter()

# Get the current time in microseconds after finishing execution
end_time = time.perf_counter()

# Calculate the elapsed time in microseconds
elapsed_time_microseconds = (end_time - start_time) * 1e6  # Convert to microseconds

# Print the elapsed time in microseconds
print(f"Elapsed time: {elapsed_time_microseconds:.2f} microseconds")

# Get the current working directory
current_directory = os.getcwd()

# Specify the relative folder path containing .sql files
relative_folder_path = 'SQL_Files/Database'

# Combine the current working directory with the relative folder path
folder_path = os.path.join(current_directory, relative_folder_path)

sql_contents_list = []

try:
    # Get a list of all files in the folder
    files = os.listdir(folder_path)

    # Filter out only the .sql files
    sql_files = [file for file in files if file.endswith('.sql')]

    # Read the contents of each .sql file and store them in a list
    for sql_file in sql_files:
        file_path = os.path.join(folder_path, sql_file)
        with open(file_path, 'r') as file:
            sql_contents = file.read()
            sql_contents_list.append(sql_contents)

except FileNotFoundError:
    print(f"Folder not found: {folder_path}")

except Exception as e:
    print(f"An error occurred: {e}")
    

# This code removes double quotes outside of DDL, including Database, schema, table name
def remove_outer_quotes(sql):
    ls1 = sql.split("(")[0].replace('"', '')
    ls2 = ["(" + i for i in sql.split("(")[1:]]
    ls2.insert(0, ls1)
    sql = "".join(ls2)

    return sql

def check_table_comment(sql):
    comment_match = re.search(r"comment\s*=\s*'([^']*)'", sql, re.IGNORECASE)

    if comment_match:
        comment = comment_match.group(1)
        return comment
    else:
        return None


resource_table_name_list = []

# Main Python code
def python_terraform(sql, comment):
    comment = check_table_comment(sql)
    
    if comment:
       
        code = ""
        ddl = sql.split(';')
    
        for command in ddl:
            command = command.strip().upper()
            create_commands = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+TABLE(.*?)\(", command, re.DOTALL)
    
            # Get the database name, schema name, table name
            for create_command in create_commands:
                create_command = create_command.strip()
                database_info = create_command.split()[0].split('.')
                database_name = database_info[0].replace('"', '')
                schema_name = database_info[1].replace('"', '')
                table_name = database_info[2].replace('"', '')
                data_retention_time_in_days_schema = 1
    
                # Set the dynamic database name / remove dev, prod name
                dynamic_db = ''
                dynamic__main_db = ''
                if database_name.endswith("_DEV"):
                    dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT}")
                    dynamic__main_db += database_name.replace("_DEV", "")
                elif database_name.endswith("_PROD"):
                    dynamic_db += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT}")
                    dynamic__main_db += database_name.replace("_PROD", "")
    
                
    
                # Create Database
                resource_table_name = f"resource \"snowflake_database\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
                code += f"{resource_table_name} {{\n"
                code += f"\tdatabase = \"{dynamic_db}\"\n"
                
                resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
                resource_table_name_list.append(resource_table_name_demo)
                
                code += f"\tschema = \"{schema_name}\"\n"
                code += f"\tname = \"{table_name}\"\n"
                code += f"\tdata_retention_days = {data_retention_time_in_days_schema}\n"
                code += f"\tcomment = \"{comment}\"\n"
    
    
                code += "}\n\n"
    

        return code,resource_table_name_demo
    else:
        return None,None
# Create the output folder
output_folder = os.path.join(current_directory, 'Terraform_Files', 'Database')

try:
    os.makedirs(output_folder, exist_ok=True)
except Exception as e:
    print(f"An error occurred while creating the output folder: {e}")

# Process each SQL content and generate Terraform code
for i, sql_contents in enumerate(sql_contents_list): # read the file data 
    sql_without_quotes = remove_outer_quotes(sql_contents) # remove remove_outer_quotes
    check_table_comment_value = check_table_comment(sql_without_quotes)
    
    main,resource_table_name_demo = python_terraform(sql_without_quotes, check_table_comment_value) # main sql code 
    if main is not None:
        try:
            output_filename = os.path.join(output_folder, f"{resource_table_name_demo}.tf")
            
            with open(output_filename, 'w') as tf_file:
                tf_file.write(main)
        except Exception as e:
            print(f"An error occurred while writing the output file: {e}")
