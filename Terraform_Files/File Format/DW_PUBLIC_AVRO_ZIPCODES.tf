resource "snowflake_file_format" "DW_PUBLIC_AVRO_ZIPCODES" {
	name = "AVRO_ZIPCODES"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	comment_value = "demo 3"
	format_type = "AVRO"
	compression = "SNAPPY"
	trim_space = false
	replace_invalid_characters= false
	null_if = "\\n"
}

