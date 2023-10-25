CREATE OR REPLACE PROCEDURE DW_DEV.INTEGRATION.GET_EMPLOYEE_KEYRING_DATA_FOR_CONFIG_TENANT("INPUT_DATA" VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas','simplejson')
HANDLER = 'check_values'
EXECUTE AS OWNER
AS '
def check_values(snowpark_session, input_data: dict):
  import pandas
  lst = []
  results_df = snowpark_session.sql(''''''select column_name from dw_dev.information_schema.columns where table_name = ''EMPLOYEE_CROSS_WALK'' and table_schema= ''INTEGRATION''
'''''')
  columns_list = results_df.to_pandas()[''COLUMN_NAME''].values.tolist()
  
  AMS_KEYS = [''AMS_SOURCE_SYSTEM_ID'',''AMS_SYSTEM_CODE'',''AMS_SOURCE_SYSTEM'',''AMS_SYSTEM'']
  PAYROLL_KEYS = [''PAYROLL_SOURCE_SYSTEM_ID'',''PAYROLL_SYSTEM_CODE'',''PAYROLL_SOURCE_SYSTEM'',''PAYROLL_SYSTEM'']

  columns_list += AMS_KEYS
  columns_list += PAYROLL_KEYS
  lst = []


  # Check if any value from the input keys is not present in both AMS_KEYS and PAYROLL_KEYS
  if (not any(key.upper() in AMS_KEYS or key.upper() in PAYROLL_KEYS for key in input_data.keys())) and "AMS_EMPLOYEE_KEY" not in input_data and "PAYROLL_EMPLOYEE_KEY" not in input_data and "EMPLOYEE_ENTERPRISE_ID" not in input_data:
    return("AMS_SOURCE_SYSTEM or PAYROLL_SOURCE_SYSTEM is mandatory.")

  elif (any(key.upper() in AMS_KEYS or key.upper() in PAYROLL_KEYS for key in input_data) and len(input_data)==1) or (all(key.upper() in AMS_KEYS or key.upper() in PAYROLL_KEYS for key in input_data) and len(input_data)==2):
    return("Please provide another input along with AMS_SOURCE_SYSTEM or PAYROLL_SOURCE_SYSTEM.")


  elif any(key not in PAYROLL_KEYS and (key in AMS_KEYS and (input_data[key].upper() == ''NULL'' or input_data[key].strip() == "" )) for key in input_data):
    return("Please provide a valid value for AMS_SOURCE_SYSTEM.")
  
  elif any(key not in AMS_KEYS and (key in PAYROLL_KEYS and (input_data[key].upper() == ''NULL'' or input_data[key].strip() == "")) for key in input_data):
    return("Please provide a valid value for PAYROLL_SOURCE_SYSTEM.")


  elif (any(key in AMS_KEYS or key in PAYROLL_KEYS for key in input_data) and any(input_data[key].upper() != ''NULL'' and input_data[key].strip() != "" for key in input_data) and len(input_data) > 1) or ("AMS_EMPLOYEE_KEY" in input_data or "PAYROLL_EMPLOYEE_KEY" in input_data or "EMPLOYEE_ENTERPRISE_ID" in input_data):
    for key,value in input_data.items():
      if key in columns_list:
        if value.strip() != '''' and  value.upper() != ''NULL'' :
          if key.upper().startswith(("AMS_SOURCE","AMS_SYSTEM")):
            if value.upper().startswith(("SANDATA")):
              lst.append(f"(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''4'')")
            elif value.upper().startswith(("DATAFLEX")):
              lst.append(f"(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''3'')")
            elif value.upper().startswith(("CLEAR")):
              lst.append(f"(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''16'')")
            elif value.upper().startswith(("ALLIANCE","GENERATIONS")):
              lst.append(f"(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''19'')")
            else:
              lst.append(f"(UPPER(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR) = UPPER(''{value}'') or UPPER(CW.AMS_SYSTEM_CODE) = UPPER(''{value}'' ))")
          elif key.upper().startswith(("PAYROLL_SOURCE","PAYROLL_SYSTEM")):
            if value.upper().startswith(("UKG","TRUSTPOINT","KRONOS")):
              lst.append(f"(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR = ''6'')")
            elif value.upper().startswith(("GREATPLAINS","GP","GREAT")):
              lst.append(f"(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR = ''5'')")
            elif value.upper().startswith(("PAYLOCITY","ADAPTIVE")):
              lst.append(f"(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR = ''10'')")
            else:
              lst.append(f"(UPPER(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR) = UPPER(''{value}'') or UPPER(CW.PAYROLL_SYSTEM_CODE) = UPPER(''{value}'') )")
          else:
            lst.append(f"UPPER(CW.{key.upper()}) = UPPER(''{value}'')")
      else:
        return (f"Please check spelling of ''{key}'' ")
    line = " and ".join(lst)
    return get_ams_employee_id(snowpark_session,line)
def get_ams_employee_id(snowpark_session,condition_statement : str ):
  import pandas
  import json
  try:
      results_df = snowpark_session.sql(f''''''SELECT  object_agg(EMPLOYEE_ENTERPRISE_ID,  object_construct_keep_null(
                            ''INACTIVE'',CW.INACTIVE,
                            ''EMPLOYEE_FIRST_NAME'',CW.FIRST_NAME,
                            ''EMPLOYEE_LAST_NAME'',CW.LAST_NAME,
                            ''EMPLOYEE_ENTERPRISE_ID'',CW.EMPLOYEE_ENTERPRISE_ID,
                            ''AMS_EMPLOYEE_ID'',CW.AMS_EMPLOYEE_ID,
                            ''PAYROLL_EMPLOYEE_ID'',CW.PAYROLL_EMPLOYEE_ID,
                            ''PAYROLL_ID'',CW.PAYROLL_EMPLOYEE_ID ,
                            ''GATOR_EMPLOYEE_ACCOUNT_ID'',CW.GATOR_EMPLOYEE_ACCOUNT_ID,
                            ''COACHUPCARE_ID'',CW.COACHUPCARE_ID,
                            ''CARIBOU_ID_SENT'',CW.CARIBOU_ID_SENT,
                            ''SERVICENOW_ID'',CW.SERVICENOW_ID,
                            ''DERIVED_WORKDAY_ID'',CW.DERIVED_WORKDAY_ID,
                            ''WORKDAY_USERNAME'',CW.WORKDAY_USERNAME,
                            ''WORKDAY_INTERNAL_ID'',CW.WORKDAY_INTERNAL_ID,
                            ''BROADSPIRE_ID'',CW.BROADSPIRE_ID,
                            ''OKTA_ID'',CW.OKTA_ID,
                            ''OKTA_USERNAME'',CW.OKTA_USERNAME,
                            ''ACTIVE_DIRECTORY_SID'',CW.ACTIVE_DIRECTORY_SID,
                            ''EMPLOYEE_RISKONNECT_ID'',CW.EMPLOYEE_RISKONNECT_ID)) AS obj
                    FROM DW_DEV.INTEGRATION.EMPLOYEE_CROSS_WALK_14APR23_CONFIG_TENANT CW
                     WHERE   {condition_statement}  AND CW.INACTIVE <> TRUE AND CW.DUP_FLAG <> TRUE 
					  '''''')
      x = results_df.collect()[0][0].replace(''\\n'', '''').strip()
      res = json.loads(x)
      
      if len(res) == 0:
         res = json.loads(''''''{ "Message": "No data found"}'''''')
         return res
      else:
         return res
  except Exception as ex:
      return ("An exception occurred:",str(ex))
   	    
  
';