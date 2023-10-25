
CREATE OR REPLACE FUNCTION DISC_DEV.PUBLIC.UDF_TEST()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('numpy','pandas','xgboost==1.5.0')
HANDLER = 'udf'
AS '
import numpy as np
import json
import pandas as pd
import xgboost as xgb
def udf():
  return [np.__version__, pd.__version__, xgb.__version__, json.__version__]
';
