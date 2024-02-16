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
    



resource_File_Format_name_list = []

# Main Python code
def python_terraform(sql):

    code = ""

    database_match = re.search( r"DATABASE_NAME\s*=\s*(\w+)", sql , re.IGNORECASE | re.DOTALL)

    # Extract the DATABASE_NAME value from the match object
    if database_match:
        database_value = database_match.group(1)
#         print(f"DATABASE_NAME: {database_value}")
    else:
        print("DATABASE_NAME not found")

        
    RETENTION_TIME_match = re.search(r"RETENTION_TIME\s*=\s*(\d+)",  sql , re.IGNORECASE | re.DOTALL)

    # Extract the DATABASE_NAME value from the match object
    if RETENTION_TIME_match:
        RETENTION_TIME_value  = RETENTION_TIME_match.group(1)
    else:
        print("RETENTION_TIME_value not found")
        
    COMMENT_match = re.search(r"COMMENT\s*=\s*(.*)",  sql , re.IGNORECASE | re.DOTALL)

    # Extract the DATABASE_NAME value from the match object
    if COMMENT_match:
        comment_value = COMMENT_match.group(1).strip()
    else :
        pass

#             data_retention_time_in_days_schema = 1

#             # Set the dynamic database name / remove dev, prod name
    dynamic_db = ''
    dynamic__main_db = ''
    if database_value.endswith("_DEV"):
        dynamic_db += database_value.replace("_DEV", "_${var.SF_ENVIRONMENT}")
        dynamic__main_db += database_value.replace("_DEV", "")
    elif database_value.endswith("_PROD"):
        dynamic_db += database_value.replace("_PROD", "_${var.SF_ENVIRONMENT}")
        dynamic__main_db += database_value.replace("_PROD", "")


#     print(dynamic__main_db)
#             # Create Schema
    if dynamic__main_db == '':
        resource_table_name = f"resource \"snowflake_database\" \"{database_value}_DB\""

    else :
        resource_table_name = f"resource \"snowflake_database\" \"{dynamic__main_db}_DB\""

        
    code += f"{resource_table_name} {{\n"
    if dynamic_db=='':
        code += f"\tname = \"{database_value}\"\n"
    else:
        code += f"\tname = \"{dynamic_db}\"\n"
        
    resource_table_name_demo = f'{database_value}'
    resource_File_Format_name_list.append(resource_table_name_demo)

    
    if comment_value:
#         comment_value = COMMENT_match.group(1).strip()
        code += f"\tcomment = \"{comment_value}\"\n"
    else:
        pass
    
    code += f"\tdata_retention_time_in_days   = {RETENTION_TIME_value}\n"


    code += "}\n\n"


    return code

for i, sql_contents in enumerate(sql_contents_list):
    main = python_terraform(sql_contents)
#     print(main)   
    
    database_match = re.search( r"DATABASE_NAME\s*=\s*(\w+)", sql_contents , re.IGNORECASE | re.DOTALL)

    # Extract the DATABASE_NAME value from the match object
    if database_match:
        database_value = database_match.group(1)
        
        output_folder = os.path.join(current_directory, 'Terraform_Files','Database')
        try:
            os.makedirs(output_folder, exist_ok=True)
        except Exception as e:
            print(f"An error occurred while creating the output folder: {e}")
        
        dynamic_db = ''
        dynamic__main_db = ''
        if database_value.endswith("_DEV"):
            dynamic_db += database_value.replace("_DEV", "_${var.SF_ENVIRONMENT}")
            dynamic__main_db += database_value.replace("_DEV", "")
        elif database_value.endswith("_PROD"):
            dynamic_db += database_value.replace("_PROD", "_${var.SF_ENVIRONMENT}")
            dynamic__main_db += database_value.replace("_PROD", "")

        for i, sql_contents in enumerate(sql_contents_list):
            main = python_terraform(sql_contents)

#             for i in resource_File_Format_name_list:
#                 resource_name = i 
        if dynamic__main_db=='':   
            output_filename = os.path.join(output_folder, f"{database_value}_DB.tf")

            try:
                with open(output_filename, 'w') as tf_file:
                    tf_file.write(main)
            except Exception as e:
                print(f"An error occurred while writing the output file: {e}")
        else:
   
            output_filename = os.path.join(output_folder, f"{dynamic__main_db}_DB.tf")

            try:
                with open(output_filename, 'w') as tf_file:
                    tf_file.write(main)
            except Exception as e:
                print(f"An error occurred while writing the output file: {e}")
