CREATE OR REPLACE PROCEDURE AWS_LANDING_INGEST_DB_DEV.DEMO.TRUNCATE_TABLES()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
--- Truncate AWS Landing Area Tables
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.QUALIFICATIONS_CERTIFICATION_ACHIEVEMENT;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.RELATED_PERSON_ADDRESS_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_ADDITIONAL_INFORMATION;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_ADDRESS_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_COMPENSATION;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_LEAVE_OF_ABSENCE;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_ORGANIZATION_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_SUMMARY;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_SUPERVISOR;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_EMAIL_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_PHONE_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.AUDIT;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.RELATED_PERSON;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.RELATED_PERSON_EMAIL_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.RELATED_PERSON_NAME_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.RELATED_PERSON_PHONE_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_IDENTIFICATION_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_PERSONAL;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_POSITION_DATA;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_STATUS;
TRUNCATE TABLE AWS_LANDING_INGEST_DB_DEV.DEMO.WORKER_NAME_DATA;


    return 'Success';
END;
$$
