resource "snowflake_procedure" "DISC_AXXESS_TESTDG_1" {
	name ="TESTDG_1"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "AXXESS"
	start_with = -8
	increment  = -1
	order  = true
}

