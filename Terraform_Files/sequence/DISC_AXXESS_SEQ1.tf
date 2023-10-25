resource "snowflake_procedure" "DISC_AXXESS_SEQ1" {
	name ="SEQ1"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "AXXESS"
	start_with = 1
	increment  = 1
	order  = true
}

