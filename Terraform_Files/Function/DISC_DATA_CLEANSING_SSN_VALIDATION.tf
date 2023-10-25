resource "snowflake_procedure" "DISC_DATA_CLEANSING_SSN_VALIDATION" {
	name = "SSN_VALIDATION"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "DATA_CLEANSING"
	language  = "SQL"

	arguments {
		name = "SSN"
		type = "VARCHAR(16777216)"
}	
	return_type = "VARCHAR(11)"
	statement =  "
     SELECT 
     CASE 
    WHEN TRIM(SSN) = '' OR SSN IS NULL THEN  NULL
    WHEN LENGTH(REGEXP_REPLACE(TRIM(SSN),'[^[:digit:]$]'))<>9 THEN NULL
        WHEN NOT (NULLIF(TRIM(REGEXP_REPLACE(SSN ,'\\-|\\\\\\\\s|\\\\\\\\\\\\\\\\|[A-Z]')),'') LIKE ANY ('666%','000%','9%','%0000','___00%'))
        THEN (CASE WHEN
     CONTAINS(TRIM(SSN), '-') = FALSE  THEN CONCAT(SUBSTRING(SSN,1,3)||'-'||SUBSTRING(SSN,4,2)||'-'||SUBSTRING(SSN,6,4))
     ELSE TRIM(SSN)
     END)
        ELSE  NULL 
    END
"
}

