resource "snowflake_table" "DISC_DEMO_BILL_RATE1" {
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "DEMO"
	name = "BILL_RATE1"
	data_retention_days = 1
	change_tracking = false
	comment = "Demo comemnt"

column {
	name = "TIME *&^ TAKEN_IN_MINUTES"
	type = "CLOB"
	generated_always_as  = "CAST(CAST(DATE_DIFFTIMESTAMPINSECONDS(START_TIME, NVL(END_TIME, '2023-06-21 09:45:02.420000000Z')) AS VARCHAR(16777216)) AS TIME(9))"
	nullable = false
	comment = "2023-06-21 09:45:02.420000000Z"
}


column {
	name = "code-RATE_ID"
	type = "VARCHAR(16777216)"
	generated_always_as  = "demo(code(DATE_DIFFTIMESTAMPINSECONDS(START_TIME, NVL(END_TIME, '2023-06-21 09:45:02.420000000Z')) AS VARCHAR(16777216)) AS TIME(9))"
	nullable = true
	comment = "Demo code"
}


column {
	name = "BILL *11 RATE_ID_deno"
	type = "TIME(9)"
	default = "(1 + 2)"
	nullable = true
	comment = "PRO"
}


column {
	name = "demoRATE_ID_deno"
	type = "GEOGRAPHY"
	default = "CURRENT_TIMESTAMP"
	nullable = false
	comment = "LAST"
}


column {
	name = "BILL_RATE_GUID"
	type = "VARCHAR(16777216)"
	default = "2023-08-04 00:00:00"
	nullable = false
}


column {
	name = "BILL_CODE_ID"
	type = "TIME(9)"
	nullable = false
}


column {
	name = "RATE_RATE_ID"
	type = "NUMBER(38,0)"
	generated_always_as  = "pro(pro(DATE_DIFFTIMESTAMPINSECONDS(START_TIME, NVL(END_TIME, '2023-06-21 09:45:02.420000000Z')) AS VARCHAR(16777216)) AS TIME(9))"
	nullable = false
}


column {
	name = "CAT"
	type = "NUMBER(38,0)"
	default = "(1 + 2)"
	nullable = false
	comment = "PRO as as 2122"
}


column {
	name = "START_DATE"
	type = "NUMBER(38,0)"
	nullable = true
}


column {
	name = "DEFAULT_RATE_ID"
	type = "OBJECT"
	nullable = true
}


column {
	name = "EXTERNAL_ID"
	type = "TIME(9)"
	nullable = true
}


column {
	name = "TO_DATE_TMP"
	type = "VARCHAR(16777216)"
	nullable = true
}


column {
	name = "TO_DATE"
	type = "TIME(9)"
	nullable = true
}

}

