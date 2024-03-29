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
relative_folder_path = 'SQL_Files/Function'

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

import re
# this code remove double quotes outside form DDL / Including Database, schema, table name 
def remove_outer_quotes(sql):
    ls1 = sql.split("(")[0].replace('"','')
    ls2 = ["("+i for i in sql.split("(")[1:]] 
    ls2.insert(0,ls1)
    sql = "".join(ls2)  
    
    return sql


import re
resource_table_name_list=  []
def python_terraform(sql):
    code = ""
    
    ddl = sql.split(';')
    # create a main loop for find the all data  
    
    for command in ddl: 
        command = command.strip().upper()
            
        # Updated regex for SQL functions
        create_commands = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION(.*?)\(", command, re.DOTALL)
        
        # get the database name, schema name, function name
        for create_command in create_commands:
            create_command = create_command.strip()
            # get the database name, schema name, function name
            extract_schema_database_function = re.search(r'\b(\w+)\.(\w+)\.(\w+)', create_command)
            database_name, schema_name, table_name = extract_schema_database_function.groups()
  
            
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
            
            
            #----------------------------------------------------------------------------------


            #------------------------------------------------------------------------------------------------
            # All regex Pattern

            return_pattern = r'RETURNS\s+(.*?)\n\s*'

            return_type_values = re.findall(return_pattern, sql, re.DOTALL | re.IGNORECASE)

            return_type_value = return_type_values[0].strip() if return_type_values else None

            arguments_match = re.findall(r'"([^"]+)"\s+([A-Z][A-Z0-9()]*(?:\([0-9, ]+\))?)(?:,\s*|\))', sql)
            arguments = [{"name": arg[0], "type": arg[1]} for arg in arguments_match]

            # regex for language
            language_match = re.search(r'LANGUAGE\s+["\']?(\w+)["\']?', sql, re.IGNORECASE)

            if language_match:
                language_value = language_match.group(1)

            #regex for null_input_behavior
            null_input_behavior = re.search(r'NULL_INPUT_BEHAVIOR\s+([^\n]+)', sql ,re.DOTALL | re.IGNORECASE)

            if null_input_behavior:
                null_input_behavior=null_input_behavior.group(1).strip()

            # regex for return_behavior
            return_behavior_item = re.search(r'return_behavior\s+([^\n]+)', sql, re.DOTALL | re.IGNORECASE)

            if return_behavior_item:
                return_behavior_value = return_behavior_item.group(1).strip()
  

            # regex for comemnt
            comment_match = re.search(r'COMMENT\s+["\'](.+?)["\']', sql, re.DOTALL | re.IGNORECASE)
            if comment_match:
                    comment_got = comment_match.group(1).strip()

            # regex for HANDLER
            HANDLER_match = re.search(r'HANDLER\s*=\s*["\']([^"\']+)[\'"]', command, re.DOTALL | re.IGNORECASE)

            if HANDLER_match:
                HANDLER_value = HANDLER_match.group(1).strip()
            else :
                pass

            # find the RUNTIME VERSION
            RUNTIME_VERSION_match = re.search(r'RUNTIME_VERSION\s*=\s*["\']([^"\']+)[\'"]', sql, re.DOTALL | re.IGNORECASE)

            if RUNTIME_VERSION_match:
                RUNTIME_VERSION_value = RUNTIME_VERSION_match.group(1).strip()
            else :
                pass

            # Create a regex pattern for PACKAGES
            packages_pattern = re.compile(r"PACKAGES\s*=\s*\((.*?)\)",  re.DOTALL | re.IGNORECASE)
            
            # Find the value inside the round brackets for PACKAGES
            matches = packages_pattern.search(sql)
            
            if matches:
                packages_value = matches.group(1)
                # Replace single quotes with double quotes
                packages_value = packages_value.replace("'", "\"")
                
                
            # Use re.search() to find the first occurrence of the pattern in the SQL code
            statement_match = re.search(r"AS\s+'(.*?)';", sql, re.DOTALL)

            # Check if there's a match before accessing the group attribute
            if statement_match:
                statement_matches = statement_match.group(1)
                extracted_code_single_quotes = statement_matches.replace("''", "'")
                extracted_code_replaced = re.sub(r'(_PROD|_DEV)', r'_${var.SF_ENVIRONMENT}', extracted_code_single_quotes)
                # print(extracted_code_replaced)
            else:
                print('none')
                
            sql = sql.upper()

            # this is for sql langauge 
            if language_value=='SQL':
                
                # main code 
                resource_table_name = f"resource \"snowflake_function\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
                code += f"{resource_table_name} {{\n"
                code += f"\tname = \"{table_name}\"\n"
                code += f"\tdatabase = \"{dynamic_db}\"\n"
                code += f"\tschema = \"{schema_name}\"\n"
                resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
                resource_table_name_list.append(resource_table_name_demo)
                code += f"\tlanguage  = \"{language_value}\"\n"
    
                argument_name = []
                argument_type = []
                for argument in arguments:
    
                        argument_name.append(argument["name"])
                        argument_type.append(argument["type"])
    
                for i in zip(argument_name,argument_type):
                    code += f"\n\targuments {{\n"
                    code += f"\t\tname = \"{i[0]}\"\n"
                    code += f"\t\ttype = \"{i[1]}\"\n"
                    code += "}\t\n"
    
                if "COMMENT" in sql:
                    try:
                            code += f"\tcomment = \"{comment_got}\"\n"
                    except AttributeError:
                        pass
                elif "COMMENT" not in sql:
                    pass
                    
                if "RETURNS" in sql:
                    try:
                        return_type_value = return_type_value.replace('"','')
                        code += f"\treturn_type = \"{return_type_value}\"\n"
                        
                    except AttributeError:
                        pass
                elif "RETURNS" not in sql:
                    pass  
    
                if "NULL_INPUT_BEHAVIOR" in sql:
                    try:
                        null_input_behavior = null_input_behavior.replace('"','')
                        code += f"\tnull_input_behavior  = \"{null_input_behavior}\"\n"
                    except AttributeError:
                        pass
                elif "NULL_INPUT_BEHAVIOR" not in sql:
                    pass
                    
                if "RETURN_BEHAVIOR" in sql:
                    try:
                        return_behavior_value = return_behavior_value.replace('"','')
                        code += f"\treturn_behavior = \"{return_behavior_value}\"\n"
                    except AttributeError:
                        pass
                elif "RETURN_BEHAVIOR" not in sql:
                    pass
    
                # code += f"\tstatement =  \"{extracted_code_replaced}\"\n"
                code += f"\tstatement = <<-EOT\n{extracted_code_replaced}\n EOT\n"
    
                code += "}\n\n"
            # this is for JAVASCRIPT langauge   
            elif language_value=='JAVASCRIPT':

                # main code 
                resource_table_name = f"resource \"snowflake_function\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
                code += f"{resource_table_name} {{\n"
                code += f"\tname = \"{table_name}\"\n"
                code += f"\tdatabase = \"{dynamic_db}\"\n"
                code += f"\tschema = \"{schema_name}\"\n"
                resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
                resource_table_name_list.append(resource_table_name_demo)
                code += f"\tlanguage  = \"{language_value}\"\n"
    
                argument_name = []
                argument_type = []
                for argument in arguments:
    
                        argument_name.append(argument["name"])
                        argument_type.append(argument["type"])
    
                for i in zip(argument_name,argument_type):
                    code += f"\n\targuments {{\n"
                    code += f"\t\tname = \"{i[0]}\"\n"
                    code += f"\t\ttype = \"{i[1]}\"\n"
                    code += "}\t\n"
    
                if "COMMENT" in sql:
                    try:
                            code += f"\tcomment = \"{comment_got}\"\n"
                    except AttributeError:
                        pass
                elif "COMMENT" not in sql:
                    pass
                    
                if "RETURNS" in sql:
                    try:
                        return_type_value = return_type_value.replace('"','')
                        code += f"\treturn_type = \"{return_type_value}\"\n"
                        
                    except AttributeError:
                        pass
                elif "RETURNS" not in sql:
                    pass  
    
                if "NULL_INPUT_BEHAVIOR" in sql:
                    try:
                        null_input_behavior = null_input_behavior.replace('"','')
                        code += f"\tnull_input_behavior  = \"{null_input_behavior}\"\n"
                    except AttributeError:
                        pass
                elif "NULL_INPUT_BEHAVIOR" not in sql:
                    pass
                    
                if "RETURN_BEHAVIOR" in sql:
                    try:
                        return_behavior_value = return_behavior_value.replace('"','')
                        code += f"\treturn_behavior = \"{return_behavior_value}\"\n"
                    except AttributeError:
                        pass
                elif "RETURN_BEHAVIOR" not in sql:
                    pass

                if "HANDLER" in sql:
                    try:
                        code += f"\thandler= \"{HANDLER_value}\"\n"
                    except AttributeError:
                        pass
                elif "HANDLER" not in sql:
                    pass
                
                
                # code += f"\tstatement =  <<-EOT\n{extracted_code_replaced}\"\n"
                code += f"\tstatement = <<-EOT\n{extracted_code_replaced}\n EOT\n"
    
                code += "}\n\n"

            # this is for python language 
            else:

                # main code 
                resource_table_name = f"resource \"snowflake_function\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
                code += f"{resource_table_name} {{\n"
                code += f"\tname = \"{table_name}\"\n"
                code += f"\tdatabase = \"{dynamic_db}\"\n"
                code += f"\tschema = \"{schema_name}\"\n"
                resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
                resource_table_name_list.append(resource_table_name_demo)
                code += f"\tlanguage  = \"{language_value}\"\n"
    
                argument_name = []
                argument_type = []
                for argument in arguments:
    
                        argument_name.append(argument["name"])
                        argument_type.append(argument["type"])
    
                for i in zip(argument_name,argument_type):
                    code += f"\n\targuments {{\n"
                    code += f"\t\tname = \"{i[0]}\"\n"
                    code += f"\t\ttype = \"{i[1]}\"\n"
                    code += "}\t\n"
    
                if "COMMENT" in sql:
                    try:
                            code += f"\tcomment = \"{comment_got}\"\n"
                    except AttributeError:
                        pass
                elif "COMMENT" not in sql:
                    pass
                    
                if "RETURNS" in sql:
                    try:
                        return_type_value = return_type_value.replace('"','')
                        code += f"\treturn_type = \"{return_type_value}\"\n"
                        
                    except AttributeError:
                        pass
                elif "RETURNS" not in sql:
                    pass  
    
                if "NULL_INPUT_BEHAVIOR" in sql:
                    try:
                        null_input_behavior = null_input_behavior.replace('"','')
                        code += f"\tnull_input_behavior  = \"{null_input_behavior}\"\n"
                    except AttributeError:
                        pass
                elif "NULL_INPUT_BEHAVIOR" not in sql:
                    pass
                    
                if "RETURN_BEHAVIOR" in sql:
                    try:
                        return_behavior_value = return_behavior_value.replace('"','')
                        code += f"\treturn_behavior = \"{return_behavior_value}\"\n"
                    except AttributeError:
                        pass
                elif "RETURN_BEHAVIOR" not in sql:
                    pass

                if "RUNTIME_VERSION" in sql:
                    try:
                        code += f"\truntime_version = \"{RUNTIME_VERSION_value}\"\n"
                    except AttributeError:
                        pass 
                elif "RUNTIME_VERSION" not in sql:
                    pass

                if "PACKAGES" in sql:
                    try:
                        code += f"\tpackages = [{packages_value}]\n"
                    except AttributeError:
                        pass
                elif "PACKAGES" not in sql:
                    pass
                
                if "HANDLER" in sql:
                    try:
                        code += f"\thandler= \"{HANDLER_value}\"\n"
                    except AttributeError:
                        pass
                elif "HANDLER" not in sql:
                    pass
                    
                # code += f"\tstatement =  \"{extracted_code_replaced}\"\n"
                code += f"\tstatement = <<-EOT\n{extracted_code_replaced}\n EOT\n"
                code += "}\n\n"
                    
 
    return code


# Process each SQL content and generate Terraform code
for sql_contents in sql_contents_list:
    sql_without_quotes = remove_outer_quotes(sql_contents)
    main = python_terraform(sql_without_quotes)
    # Extract database name and schema name from the SQL content

    
    create_commands = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION(.*?)\(", sql_without_quotes, re.DOTALL | re.IGNORECASE)

    # get the database name, schema name, function name
    for create_command in create_commands:
        create_command = create_command.strip()
        # get the database name, schema name, function name
        extract_schema_database_function = re.search(r'\b(\w+)\.(\w+)\.(\w+)', create_command ,re.DOTALL | re.IGNORECASE)
        database_name, schema_name, table_name = extract_schema_database_function.groups()

        output_folder = os.path.join(current_directory, 'Terraform_Files', database_name, schema_name, 'Function')

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
