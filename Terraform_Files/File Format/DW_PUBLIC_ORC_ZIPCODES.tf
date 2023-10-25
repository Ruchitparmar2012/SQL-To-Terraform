resource "snowflake_file_format" "DW_PUBLIC_ORC_ZIPCODES" {
	name = "ORC_ZIPCODES"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	comment_value = "demo 2"
	format_type = "ORC"
	trim_space = false
	replace_invalid_characters= false
	null_if = "\\n"
}

