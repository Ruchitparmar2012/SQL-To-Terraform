resource "snowflake_file_format" "DW_PUBLIC_XML_ZIPCODES" {
	name = "XML_ZIPCODES"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	comment_value = "demo 9"
	format_type = "XML"
	compression = "NONE"
	ignore_utf8_errors = false
	preserve_space = false
	strip_outer_element = false
	disable_snowflake_data = false
	disable_auto_convert = false
	replace_invalid_characters = false
	skip_byte_order_mark = true
}

