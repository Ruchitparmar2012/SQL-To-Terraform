resource "snowflake_procedure" "DW_INTEGRATION_GET_KEYRING_DATA_BY_JAVA_TEST" {
	name ="GET_KEYRING_DATA_BY_JAVA_TEST"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "INTEGRATION"
	language  = "JAVASCRIPT"

	arguments {
		name = "INPUT_DATA"
		type = "VARIANT"
}	
	return_type = "VARIANT"
	execute_as = "OWNER"
	statement = <<-EOT 

async function check_values(input_data) {
  const snowflakeSession = snowflake.createStatement({ sqlText: "SELECT COLUMN_NAME FROM DW_${var.SF_ENVIRONMENT}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ''EMPLOYEE_CROSS_WALK'' AND TABLE_SCHEMA = ''INTEGRATION''" });
  const result = await snowflakeSession.execute();
  const columns_list = result.getColumnValueMap("COLUMN_NAME");
  
  const AMS_KEYS = [''AMS_SOURCE_SYSTEM_ID'', ''AMS_SYSTEM_CODE'', ''AMS_SOURCE_SYSTEM'', ''AMS_SYSTEM''];
  const PAYROLL_KEYS = [''PAYROLL_SOURCE_SYSTEM_ID'', ''PAYROLL_SYSTEM_CODE'', ''PAYROLL_SOURCE_SYSTEM'', ''PAYROLL_SYSTEM''];

  columns_list.push(...AMS_KEYS);
  columns_list.push(...PAYROLL_KEYS);
  
  const lst = [];

  // Check if any value from the input keys is not present in both AMS_KEYS and PAYROLL_KEYS
  if (
    !Object.keys(input_data).some(key => AMS_KEYS.includes(key.toUpperCase()) || PAYROLL_KEYS.includes(key.toUpperCase())) &&
    !input_data.AMS_EMPLOYEE_KEY &&
    !input_data.PAYROLL_EMPLOYEE_KEY &&
    !input_data.EMPLOYEE_ENTERPRISE_ID
  ) {
    return ''AMS_SOURCE_SYSTEM or PAYROLL_SOURCE_SYSTEM is mandatory.'';
  } else if (
    (Object.keys(input_data).some(key => AMS_KEYS.includes(key.toUpperCase()) || PAYROLL_KEYS.includes(key.toUpperCase())) && Object.keys(input_data).length === 1) ||
    (Object.keys(input_data).every(key => AMS_KEYS.includes(key.toUpperCase()) || PAYROLL_KEYS.includes(key.toUpperCase())) && Object.keys(input_data).length === 2)
  ) {
    return ''Please provide another input along with AMS_SOURCE_SYSTEM or PAYROLL_SOURCE_SYSTEM.'';
  } else if (
    Object.keys(input_data).some(key => !(PAYROLL_KEYS.includes(key) && (input_data[key].toUpperCase() === ''NULL'' || input_data[key].trim() === ''''))) &&
    Object.keys(input_data).every(key => !AMS_KEYS.includes(key) || (input_data[key].toUpperCase() === ''NULL'' || input_data[key].trim() === ''''))
  ) {
    return ''Please provide a valid value for AMS_SOURCE_SYSTEM.'';
  } else if (
    Object.keys(input_data).some(key => !(AMS_KEYS.includes(key) && (input_data[key].toUpperCase() === ''NULL'' || input_data[key].trim() === ''''))) &&
    Object.keys(input_data).every(key => !PAYROLL_KEYS.includes(key) || (input_data[key].toUpperCase() === ''NULL'' || input_data[key].trim() === ''''))
  ) {
    return ''Please provide a valid value for PAYROLL_SOURCE_SYSTEM.'';
  } else if (
    (
      Object.keys(input_data).some(key => AMS_KEYS.includes(key) || PAYROLL_KEYS.includes(key)) &&
      Object.keys(input_data).some(key => input_data[key].toUpperCase() !== ''NULL'' && input_data[key].trim() !== '''') &&
      Object.keys(input_data).length > 1
    ) ||
    input_data.AMS_EMPLOYEE_KEY ||
    input_data.PAYROLL_EMPLOYEE_KEY ||
    input_data.EMPLOYEE_ENTERPRISE_ID
  ) {
    for (const [key, value] of Object.entries(input_data)) {
      if (columns_list.includes(key)) {
        if (value.trim() !== '''' && value.toUpperCase() !== ''NULL'') {
          if (key.toUpperCase().startsWith(''AMS_SOURCE'')) {
            if (value.toUpperCase().startsWith(''SANDATA'')) {
              lst.push("(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''4'')");
            } else if (value.toUpperCase().startsWith(''DATAFLEX'')) {
              lst.push("(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''3'')");
            } else if (value.toUpperCase().startsWith(''CLEAR'')) {
              lst.push("(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''16'')");
            } else if (value.toUpperCase().startsWith(''ALLIANCE'') || value.toUpperCase().startsWith(''GENERATIONS'')) {
              lst.push("(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR = ''19'')");
            } else {
              lst.push(`(UPPER(CW.AMS_SOURCE_SYSTEM_ID::VARCHAR) = UPPER(''NULL'') OR UPPER(CW.AMS_SYSTEM_CODE) = UPPER(''NULL''))`);
            }
          } else if (key.toUpperCase().startsWith(''PAYROLL_SOURCE'')) {
            if (value.toUpperCase().startsWith(''UKG'') || value.toUpperCase().startsWith(''TRUSTPOINT'') || value.toUpperCase().startsWith(''KRONOS'')) {
              lst.push("(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR = ''6'')");
            } else if (value.toUpperCase().startsWith(''GREATPLAINS'') || value.toUpperCase().startsWith(''GP'') || value.toUpperCase().startsWith(''GREAT'')) {
              lst.push("(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR = ''5'')");
            } else if (value.toUpperCase().startsWith(''PAYLOCITY'') || value.toUpperCase().startsWith(''ADAPTIVE'')) {
              lst.push("(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR = ''10'')");
            } else {
              lst.push(`(UPPER(CW.PAYROLL_SOURCE_SYSTEM_ID::VARCHAR) = UPPER(''NULL'') OR UPPER(CW.PAYROLL_SYSTEM_CODE) = UPPER(''NULL''))`);
            }
          } else {
            lst.push(`UPPER(CW.${key.toUpperCase()}) = UPPER(''NULL'')`);
          }
        }
      } else {
        return `Please check spelling of ''NULL''`;
      }
    }
    const line = lst.join('' AND '');
    return get_ams_employee_id(line);
  }
}

async function get_ams_employee_id(condition_statement) {
  const query = `SELECT OBJECT_AGG(EMPLOYEE_ENTERPRISE_ID, OBJECT_CONSTRUCT_KEEP_NULL(
    ''EMPLOYEE_FIRST_NAME'', CW.FIRST_NAME,
    ''EMPLOYEE_LAST_NAME'', CW.LAST_NAME,
    ''EMPLOYEE_ENTERPRISE_ID'', CW.EMPLOYEE_ENTERPRISE_ID,
    ''AMS_EMPLOYEE_ID'', CW.AMS_EMPLOYEE_ID,
    ''PAYROLL_EMPLOYEE_ID'', CW.PAYROLL_EMPLOYEE_ID,
    ''PAYROLL_ID'', CW.PAYROLL_EMPLOYEE_ID,
    ''GATOR_EMPLOYEE_ACCOUNT_ID'', CW.GATOR_EMPLOYEE_ACCOUNT_ID,
    ''COACHUPCARE_ID'', CW.COACHUPCARE_ID,
    ''CARIBOU_ID_SENT'', CW.CARIBOU_ID_SENT,
    ''SERVICENOW_ID'', CW.SERVICENOW_ID,
    ''DERIVED_WORKDAY_ID'', CW.DERIVED_WORKDAY_ID,
    ''WORKDAY_USERNAME'', CW.WORKDAY_USERNAME,
    ''WORKDAY_INTERNAL_ID'', CW.WORKDAY_INTERNAL_ID,
    ''BROADSPIRE_ID'', CW.BROADSPIRE_ID,
    ''OKTA_ID'', CW.OKTA_ID,
    ''OKTA_USERNAME'', CW.OKTA_USERNAME,
    ''ACTIVE_DIRECTORY_SID'', CW.ACTIVE_DIRECTORY_SID,
    ''EMPLOYEE_RISKONNECT_ID'', CW.EMPLOYEE_RISKONNECT_ID
  )) AS obj
  FROM DW_${var.SF_ENVIRONMENT}.INTEGRATION.EMPLOYEE_CROSS_WALK_E2E_TENANT CW
  WHERE NULL AND CW.DUP_FLAG <> TRUE`;
  
  const snowflakeSession = snowflake.createStatement({ sqlText: query });
  const result = await snowflakeSession.execute();
  const x = result.getColumnValue(1).replace(/\\\\n/g, '''').trim();
  const res = JSON.parse(x);
  
  if (Object.keys(res).length === 0) {
    return { ''Message'': ''No data found'' };
  } else {
    return res;
  }
}

return check_values(INPUT_DATA);

 EOT
}

