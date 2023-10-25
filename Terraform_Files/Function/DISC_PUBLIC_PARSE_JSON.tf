resource "snowflake_procedure" "DISC_PUBLIC_PARSE_JSON" {
	name = "PARSE_JSON"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	language  = "PYTHON"

	arguments {
		name = "JSON_STRING"
		type = "VARCHAR(16777216)"
}	
	return_type = "VARIANT"
	runtime_version = "3.8"
	handler= "VALIDATE_JSON"
	statement =  "
import json
def validate_json(json_string):
    try:
        return json.loads(json_string)
    except ValueError as err:
        return err
"
}

