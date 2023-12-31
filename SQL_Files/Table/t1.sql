create or replace TABLE DISC_DEV."DEMO"."BILL_RATE_TEST" (
	"BILL - RATE_id" BLOB DEFAULT 'CURRENT_TIMESTAMP' NOT NULL COMMENT 'LAST' ,
	BILL_RATE_GUID NUMBER(38,0) DEFAULT CURRENT_TIMESTAMP COMMENT 'LAST',
	BILL_CODE_ID NUMBER(38,0) NOT NULL COMMENT 'demo J**IJ',
	RATE NUMBER(13,2),
	UNITS GEOMETRY,
	START_DATE TIMESTAMP_NTZ(9),
	DEFAULT_RATE_ID NUMBER(38,0),
	EXTERNAL_ID VARCHAR(16777216),
	TO_DATE_TMP TIMESTAMP_NTZ(9),
	TO_DATE TIMESTAMP_NTZ(9) NOT NULL,
	COMMENT = 'demo comment for BILL_RATE_TEST'
);
