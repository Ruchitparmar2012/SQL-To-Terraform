resource "snowflake_file_format" "DW_PUBLIC_PARQUET_ZIPCODES" {
	name = "PARQUET_ZIPCODES"
	database = "DW_${var.SF_ENVIRONMENT}"
	schema = "PUBLIC"
	comment_value = "demo 8"
	format_type = "PARQUET"
	compression = "SNAPPY"
	snappy_compression = false
	binary_as_text = false
	use_logical_type = false
	trim_space = false
	replace_invalid_characters= false
	null_if = "\\n"
}

