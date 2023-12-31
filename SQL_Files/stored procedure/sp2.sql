CREATE OR REPLACE PROCEDURE DW_DEV.HAH.DELETE_STAGE_DEMO_DIM_EMPLOYEE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
  sqlCmd = `DELETE FROM DW_DEV.HAH.DIM_EMPLOYEE WHERE SOURCE_SYSTEM_ID = 900 AND EMPLOYEE_KEY \\
  NOT IN (SELECT EMPLOYEE_KEY FROM DW_DEV.STAGE.DEMO_DIM_EMPLOYEE WHERE SOURCE_SYSTEM_ID = 900)`;
  sqlStmt = snowflake.createStatement( {sqlText: sqlCmd} );
  rs = sqlStmt.execute();
  return ''Done'';
';
