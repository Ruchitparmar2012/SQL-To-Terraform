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
relative_folder_path = 'SQL_Files/sequence'

# Combine the current working directory with the relative folder path
folder_path = os.path.join(current_directory, relative_folder_path)

try:
    # Get a list of all files in the folder
    files = os.listdir(folder_path)

    # Filter out only the .sql files
    sql_files = [file for file in files if file.endswith('.sql')]

    # Read the contents of each .sql file and store them in a list
    sql_contents_list = []
    for sql_file in sql_files:
        file_path = os.path.join(folder_path, sql_file)
        with open(file_path, 'r') as file:
            sql_contents = file.read()
            sql_contents_list.append(sql_contents)
            
except FileNotFoundError:
    print(f"Folder not found: {folder_path}")

except Exception as e:
    print(f"An error occurred: {e}")


# this code remove double quotes outside form DDL / Including Database, schema, table name 
def remove_outer_quotes(sql):
    ls1 = sql.split("(")[0].replace('"','')
    ls2 = ["("+i for i in sql.split("(")[1:]] 
    ls2.insert(0,ls1)
    sql = "".join(ls2)  
    
    return sql


resource_table_name_list=  []
def python_terraform(sql):
    code = ""
    
    ddl = sql.split(';')
    # create a main loop for find the all data  
    for command in ddl: 
        command = command.strip().upper()
        

        if command:
            create_commands = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+SEQUENCE(.*?)(?:\s*\n|$)", command, re.DOTALL)

            for create_command in create_commands:
                create_command = create_command.strip()
                
                # get the database name , schema name , tabel name
                extract_schema_database_table = re.search(r'\b(\w+)\.(\w+)\.(\w+)', create_command)
                database_name, schema_name, table_name = extract_schema_database_table.groups()

                
                data_retention_time_in_days_schema = 1
    
                # set the dynamic database name  / remove dev , prod name
                dynamic_db = ''
                dynamic__main_db =''
                if database_name.endswith("_DEV"): 
                        dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT}")
                        dynamic__main_db += database_name.replace("_DEV", "")
    
                elif database_name.endswith("_PROD"):
                        dynamic_db  += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT}")
                        dynamic__main_db += database_name.replace("_PROD", "")

                # -------------------------------------------------------
                # Search for  start_with_match the pattern in the SQL code
                start_with_match = re.search(r'start with\s+(-?\d+)', sql, re.IGNORECASE)
                
                # Check if a match is found
                if start_with_match:
                    # Extract and print the value of 'start with'
                    start_with_value = start_with_match.group(1)
                else:
                    print("Start with value not found.")


                # Search for the pattern in the SQL code
                order_match = re.search(r'order\s*;*\s*$', sql, re.IGNORECASE | re.MULTILINE)
                

                    
                # Search for increment_by_match the pattern in the SQL code
                increment_by_match = re.search(r'increment by\s+(-?\d+)', sql, re.IGNORECASE)
                
                # Check if a match is found
                if increment_by_match:
                    # Extract and print the value of 'start with'
                    increment_by_value = increment_by_match.group(1)
                else:
                    print("increment by value not found.")
    
                # Create Sequence
                resource_table_name = f"resource \"snowflake_sequence\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
                code += f"{resource_table_name} {{\n"
                code += f"\tname =\"{table_name}\"\n"
                code += f"\tdatabase = \"{dynamic_db}\"\n"
                code += f"\tschema = \"{schema_name}\"\n"
                resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
                resource_table_name_list.append(resource_table_name_demo)

                code += f"\tstart_with = {start_with_value}\n"
                code += f"\tincrement  = {increment_by_value}\n"

                # Check if the pattern is found

                if order_match:
                    code += f"\torder  = true\n"
                else:
                    code += f"\torder  = false\n"
                    
                
           
                code += "}\n\n"
                     
    return code

    
        
# Process each SQL content and generate Terraform code
for sql_contents in sql_contents_list:
    sql_without_quotes = remove_outer_quotes(sql_contents)
    main = python_terraform(sql_without_quotes)
    # Extract database name and schema name from the SQL content

    
    create_commands = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+SEQUENCE(.*?)(?:\s*\n|$)", sql_without_quotes, re.DOTALL|re.IGNORECASE)

    for create_command in create_commands:
        create_command = create_command.strip()

        # get the database name , schema name , tabel name
        extract_schema_database_table = re.search(r'\b(\w+)\.(\w+)\.(\w+)', create_command,re.DOTALL|re.IGNORECASE)
        database_name, schema_name, table_name = extract_schema_database_table.groups()

                
        output_folder = os.path.join(current_directory, 'Terraform_Files', database_name, schema_name, 'Sequence')

        try:
            os.makedirs(output_folder, exist_ok=True)
        except Exception as e:
            print(f"An error occurred while creating the output folder: {e}")

        # Write Terraform code to the appropriate output file
        try:
            dynamic_db = ''
            dynamic__main_db = ''
            if database_name.endswith("_DEV"):
                dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT}")
                dynamic__main_db += database_name.replace("_DEV", "")
            elif database_name.endswith("_PROD"):
                dynamic_db += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT}")
                dynamic__main_db += database_name.replace("_PROD", "")

            resource_table_name = f"{dynamic__main_db}_{schema_name}_{table_name}"
            output_filename = os.path.join(output_folder, f"{resource_table_name}.tf")
            with open(output_filename, 'w') as tf_file:
                tf_file.write(main)
        except Exception as e:
            print(f"An error occurred while writing the output file: {e}")
