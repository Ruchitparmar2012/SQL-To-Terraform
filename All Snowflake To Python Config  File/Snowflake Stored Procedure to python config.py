import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import os
import snowflake.connector

# Replace these with your Snowflake account credentials and connection details
account = ''  # Replace with your Snowflake account URL
warehouse = 'DEMO_WH'
database = ''
schema = ''
username = ''  # Replace with your Snowflake username
password = ''  # Replace with your Snowflake password

#  Create the SQL_Files and Procedure folders
sql_files_dir = "SQL_Files"
procedure_dir = os.path.join(sql_files_dir, "stored procedure")
#procedure_dir = "stored procedure"
if not os.path.exists(sql_files_dir):
    os.mkdir(sql_files_dir)

if not os.path.exists(procedure_dir):
    os.mkdir(procedure_dir)

# Establish a connection to Snowflake
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
# pass your scehma here all 
schemas = ['ACTIVE_DIRECTORY','ADMINPAYROLL','ALAYACARE','AMERIHEALTH','ASR','AXXESS','BI_REPOSITORY','CCSI','CENTENE',
'CLEARCARE','COSTALSYNCDATA','DATAFLEXSYNCDATA','DATA_CLEANSING','DATA_VAULT','DEVERO','DOCEBO','EMPEONEDISON','EMPEONPREFERRED','EMPLOYEESETUP',
'GENERATIONSALLIANCE','GPSYNCDATA','HAHUSERS','HAH_APP','HAH_REPORTING','HHAEXCHANGEEDISON','HHAEXCHANGEOPENSYSTEMS','HHAEXCHANGEOSHAH',
'HHAEXCHANGEPREFERRED','JAZZHR','LANDING_8X8','MATRIXCARE','MATRIXCARE_FULLLOAD','MOLINA','PARADOX','PAYLOCITY',
'PAYOR_CONTRACT_UI','PUBLIC','QUALTRICS_SURVEYS','RISKONNECT','SANDATAEXCHANGE','SANDATAIMPORT','TRUSTPOINTDATA','TRUSTPOINTDATA_HIST',
'VIVENTIUM','WORKDAY','ZENDESK']


# Query Snowflake to get a list of procedures in the specified database and schema

procedures = []
for schema in schemas:
# Execute the query to get the list of procedures
    procedure_query = f'''
    SELECT (procedure_catalog || '.' || procedure_schema || '.' || procedure_name) procedure_name, ARGUMENT_SIGNATURE
    FROM information_schema.procedures
    WHERE procedure_schema = '{schema}' AND procedure_catalog = '{database}'
    '''
    cursor.execute(procedure_query)
    procedure = cursor.fetchall()
    procedures.extend(procedure)
    

# Close the cursor and connection when done with the procedure query
cursor.close()

#
def extract_type(ip_string):
    ip_string_trimmed = ip_string.replace('(','').replace(')','')
    elements = [element.strip() for element in ip_string_trimmed.split(',')]
    dtypes = [element.split()[-1] for element in elements]
    result = ','.join(dtypes)
    return result

if procedures:
    # Iterate through the procedures and retrieve DDL statements
    for procedure_info in procedures:
        procedure_name = procedure_info[0]
        if '()' not in procedure_info[1]:
            procedure_details = extract_type(procedure_info[1])
            #print(procedure_details, '\n')
            ddl_query = f'''SELECT GET_DDL('PROCEDURE', '{procedure_name}({procedure_details})',true)'''
            #print(ddl_query, '\n')
        else:
            ddl_query = f'''SELECT GET_DDL('PROCEDURE', '{procedure_name}()',true)'''
            #print(ddl_query, '\n')

        # Construct the fully qualified procedure name
        fully_qualified_procedure_name = f'{procedure_name}'
        try:
            # Query to retrieve the DDL statement for the procedure

            # Create a new cursor for the DDL query
            cursor = conn.cursor()

            # Execute the DDL query
            cursor.execute(ddl_query)

            # Fetch the DDL statement
            ddl_statement = cursor.fetchone()[0]

            # Modify the DDL statement to include the database and schema names
            modified_ddl_statement = ddl_statement.replace(f'CREATE OR REPLACE PROCEDURE "{procedure_name}"',
                                                          f'CREATE OR REPLACE PROCEDURE {fully_qualified_procedure_name}')

            # Remove line breaks and extra spaces
            modified_ddl_statement = modified_ddl_statement.replace('\r\n', '\n').replace('\xa0', ' ').replace('\u2003', ' ').replace("''", "'")
        
            # Step 2: Create a .sql file and write the DDL statement to it
            sql_file_name = f"{procedure_dir}/{fully_qualified_procedure_name}.sql"
            with open(sql_file_name, 'w') as sql_file:
                sql_file.write(modified_ddl_statement.encode("utf-8").decode("utf-8"))

            # Step 3: Print the file name (database.schema.procedure name)
            print(f"Generated SQL file: ", sql_file_name)

            # Close the cursor for the DDL query
            cursor.close()
        except snowflake.connector.errors.ProgrammingError as e:
            print('Procedure does not exist or not authorized ', fully_qualified_procedure_name)
else:
    print(f"No procedures found in the specified schema '{schema}' in database '{database}'.")

# Close the connection
conn.close()
