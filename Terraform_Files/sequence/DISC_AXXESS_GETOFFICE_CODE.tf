resource "snowflake_procedure" "DISC_AXXESS_GETOFFICE_CODE" {
	name ="GETOFFICE_CODE"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "AXXESS"
	start_with = -8
	increment  = -1
	order  = true
}

