resource "snowflake_file_format" "DW_PUBLIC_JSON_ZIPCODES" {
	name = "JSON_ZIPCODES"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	comment_value = "demo 5"
	format_type = "JSON"
	compression = "NONE"
	date_format = "AUTO"
	date_format = "AUTO"
	timestamp_format = "AUTO"
	binary_format = "HEX"
	trim_space = false
	null_if = "\\n"
	file_extension = "NONE"
	enable_octal = false
	allow_deplicate = false
	strip_outer_array = false
	strip_null = false
	replace_invalid_characters= false
	ignore_utf8_errors = false
	skip_byte_order_mark = true
}

