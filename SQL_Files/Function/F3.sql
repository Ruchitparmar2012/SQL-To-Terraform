
CREATE OR REPLACE FUNCTION DISC_DEV.PUBLIC.PARSE_JSON("JSON_STRING" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'validate_json'
AS '
import json
def validate_json(json_string):
    try:
        return json.loads(json_string)
    except ValueError as err:
        return err
';
