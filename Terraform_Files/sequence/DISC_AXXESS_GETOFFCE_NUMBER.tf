resource "snowflake_procedure" "DISC_AXXESS_GETOFFCE_NUMBER" {
	name ="GETOFFCE_NUMBER"
	database = "DISC_${var.SF_ENVIRONMENT}"
	schema = "AXXESS"
	start_with = 8659
	increment  = 1
	order  = true
}

