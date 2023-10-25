resource "snowflake_file_format" "DW_PUBLIC_TSV_ZIPCODES" {
	name = "TSV_ZIPCODES"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	comment_value = "demo 1"
	format_type = "TSV"
	compression = "NONE"
	record_delimiter = "NONE"
	field_delimiter = "\t"
	file_extension = "NONE"
	parse_header =  false
	skip_header = "15555"
	skip_blank_lines = false
	date_format = "AUTO"
	date_format = "AUTO"
	timestamp_format = "AUTO"
	binary_format = "HEX"
	escape = "NONE"
	escape_unenclosed_field = "\\"
	trim_space = false
	field_optionally_enclosed_by = "NONE"
	null_if = "\\n"
	error_on_column_count_mismatch_ = true
	replace_invalid_characters = false
	empty_field_as_null = true
	skip_byte_order_mark = true
	encoding = "UTF-8"
}

