resource "snowflake_procedure" "DISC_PUBLIC_UDF_TEST" {
	name = "UDF_TEST"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	language  = "PYTHON"
	return_type = "VARIANT"
	runtime_version = "3.8"
	packges = "["numpy","pandas","xgboost==1.5.0"]"
	handler= "UDF"
	statement =  "
import numpy as np
import json
import pandas as pd
import xgboost as xgb
def udf():
  return [np.__version__, pd.__version__, xgb.__version__, json.__version__]
"
}

