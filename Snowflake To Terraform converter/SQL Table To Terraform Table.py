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
relative_folder_path = 'SQL_Files/Table'

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
                # data_retention_time_in_days_schema = 1
    
                # Set the dynamic database name / remove dev, prod name
                dynamic_db = ''
                dynamic__main_db = ''
                if database_name.endswith("_DEV"):
                    dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT}")
                    dynamic__main_db += database_name.replace("_DEV", "")
                elif database_name.endswith("_PROD"):
                    dynamic_db += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT}")
                    dynamic__main_db += database_name.replace("_PROD", "")
    
                
    
                # Create table
                resource_table_name = f"resource \"snowflake_table\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
                code += f"{resource_table_name} {{\n"
                code += f"\tdatabase = \"{dynamic_db}\"\n"
                
                resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
                resource_table_name_list.append(resource_table_name_demo)
                
                code += f"\tschema = \"{schema_name}\"\n"
                code += f"\tname = \"{table_name}\"\n"
                # code += f"\tdata_retention_days = {data_retention_time_in_days_schema}\n"
                code += f"\tchange_tracking = false\n"
                code += f"\tcomment = \"{comment}\"\n"
    
                # Find the column names
                column_matches = re.findall(
                    r'"([^"]+)"|([a-zA-Z_][\w\s/\-*^]*)\s+(?:AS\s+)?(?:(?:VARCHAR|CHAR|NUMBER|BINARY|STRING|TIMESTAMP_NTZ|TIMESTAMP_TZ|TIMESTAMP|TIME|ARRAY|VARIANT|OBJECT|TIMESTAMP_LTZ|GEOGRAPHY|GEOMETRY|BLOB|CLOB|TINYINT|DECIMAL)\([\d,]+\)|ARRAY|VARCHAR|TIMESTAMP_NTZ|TIMESTAMP_TZ|TIMESTAMP|BOOLEAN|TEXT|DATE|INT|INTEGER|FLOAT|VARIANT|OBJECT|TIMESTAMP_LTZ|GEOGRAPHY|GEOMETRY|BLOB|CLOB|TINYINT|DECIMAL)',
                    sql.replace('\n', ' '))
                column_names = [column[0] or column[1] for column in column_matches if column[0] or column[1]]
                column_names = [column.strip() for column in column_names if column.strip() != 'AS']
    
                # Find the Not Null column names
                not_null_pattern = r'(?:"(.*?)".*?NOT NULL|(\w+)\s+.*?NOT NULL)'
                matches = re.findall(not_null_pattern, sql)
                not_null_columns = [match[0] or match[1] for match in matches]
    
                # Find the Comment column names
                Comment_pattern = r'(?:"(.*?)".*?COMMENT\s+\'(.*?)\'|(\w+)\s+.*?COMMENT\s+\'(.*?)\')'
                matches = re.findall(Comment_pattern, sql)
                comment_columns = [match[0] or match[2] for match in matches]
                comment_values = [match[1] or match[3] for match in matches]
    
                # Find generated_always_as in columns
                generated_pattern = r'(?:\"(.*?)\"|\b(\w+)\b).*?(?<=AS\s)\((.*?)\)\s*(?:(?=COMMENT)|(?=NOT\s+NULL)|(?=,|$))'
                generated_always_as = re.findall(generated_pattern, sql, re.MULTILINE)
    
                # Find DEFAULT values in columns
                DEFAULT_pattern = r'(?:\"(.*?)\"|\b(\w+)\b).*?(?<=DEFAULT\s)([^,\n]*?)(?=\s+(?:COMMENT|NOT\s+NULL|,\s*$))'
                DEFAULT_matches = re.findall(DEFAULT_pattern, sql, re.MULTILINE | re.IGNORECASE)
    
                # Find Check constraints in columns
                pattern_check = r'(?:\"(.*?)\"|\b(\w+)\b)\s+.*?CHECK\s+\((.*?)\)\s*(?:(?:COMMENT|NOT\s+NULL|,\s*$))'
                check_matches = re.findall(pattern_check, sql)
    
                # Find the data types
                data_type_matches = re.findall(
                    r'("([^"]+)"|([\w\s/*&^?!-]+))\s+((?:VARCHAR|CHAR|NUMBER|BINARY|STRING|TIMESTAMP_NTZ|TIMESTAMP_TZ|TIMESTAMP|TIME|BOOLEAN|TEXT|DATE|INT|INTEGER|FLOAT|VARIANT|ARRAY|OBJECT|TIMESTAMP_LTZ|GEOGRAPHY|GEOMETRY|BLOB|CLOB|TINYINT|DECIMAL)(?:\([\d,]+\))?)',
                    sql.replace('\n', ' '))
    
                column_name_list = []
                data_types_list = []
                for match in data_type_matches:
                    full_match, quoted_column_name, unquoted_column_name, data_type = match
                    column_name = quoted_column_name or unquoted_column_name
                    column_name = column_name.strip()
                    data_type = data_type.strip()
                    column_name_list.append(column_name)
                    data_types_list.append(data_type)
    
                # Generate Terraform code for each column
                for col, j in zip(column_names, data_types_list):
                    code += f"\ncolumn {{\n"
                    code += f"\tname = \"{col}\"\n"
                    code += f"\ttype = \"{j}\"\n"
    
                    # Handle generated_always_as column
                    for quoted_column, unquoted_column, expression in generated_always_as:
                        generated_column_name = quoted_column.strip() if quoted_column else unquoted_column.strip()
                        expression_cleaned = expression.strip()
    
                        if col == generated_column_name:
                            code += f"\tgenerated_always_as  = \"{expression_cleaned}\"\n"
    
                    # Handle Default column
                    for match_def in DEFAULT_matches:
                        DEFAULT_column_name = match_def[0] if match_def[0] else match_def[1]
                        DEFAULT_default_value = match_def[2]
                        DEFAULT_column_name = DEFAULT_column_name.replace('"', '').strip()
                        DEFAULT_default_value = DEFAULT_default_value.replace('"', '').replace("'", '').strip()
    
                        if col == DEFAULT_column_name:
                            code += f"\tdefault = \"{DEFAULT_default_value}\"\n"
    
                    # Handle Check column
                    for check_match in check_matches:
                        check_column_name = check_match[0] or check_match[1]
                        check_condition = check_match[2]
                        if col == check_column_name:
                            code += f"\tcheck {{\n"
                            code += f"\tcondition = \"{check_condition}\"\n"
                            code += "}\t\n"
    
                    # Handle Not Null column
                    if col in not_null_columns:
                        code += f"\tnullable = false\n"
                    else:
                        code += f"\tnullable = true\n"
    
                    # Handle Comment column
                    for comm_col, comm_values in zip(comment_columns, comment_values):
                        if comm_col == col:
                            code += f"\tcomment = \"{comm_values}\"\n"
    
                    code += "}\n\n"
    
                code += "}\n\n"
        return code,resource_table_name_demo
    else:
        return None,None
# Create the output folder
output_folder = os.path.join(current_directory, 'Terraform_Files', 'Table')

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
