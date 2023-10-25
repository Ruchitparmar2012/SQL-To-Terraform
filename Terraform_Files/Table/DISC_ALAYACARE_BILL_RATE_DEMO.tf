resource "snowflake_table" "DISC_ALAYACARE_BILL_RATE_DEMO" {
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "ALAYACARE"
	name = "BILL_RATE_DEMO"
	data_retention_days = 1
	change_tracking = false
	comment = "demo comment for BILL_RATE_demo"

column {
	name = "BILL - RATE_id"
	type = "BLOB"
	default = "CURRENT_TIMESTAMP"
	nullable = false
	comment = "LAST"
}


column {
	name = "BILL_RATE_GUID"
	type = "NUMBER(38,0)"
	default = "CURRENT_TIMESTAMP"
	nullable = true
	comment = "LAST"
}


column {
	name = "BILL_CODE_ID"
	type = "NUMBER(38,0)"
	nullable = false
	comment = "demo J**IJ"
}


column {
	name = "RATE"
	type = "NUMBER(13,2)"
	nullable = true
}


column {
	name = "UNITS"
	type = "GEOMETRY"
	nullable = true
}


column {
	name = "START_DATE"
	type = "TIMESTAMP_NTZ(9)"
	nullable = true
}


column {
	name = "DEFAULT_RATE_ID"
	type = "NUMBER(38,0)"
	nullable = true
}


column {
	name = "EXTERNAL_ID"
	type = "VARCHAR(16777216)"
	nullable = true
}


column {
	name = "TO_DATE_TMP"
	type = "TIMESTAMP_NTZ(9)"
	nullable = true
}


column {
	name = "TO_DATE"
	type = "TIMESTAMP_NTZ(9)"
	nullable = false
}

}

