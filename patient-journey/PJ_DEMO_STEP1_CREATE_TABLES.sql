USE DATABASE PATIENT_DB;
USE SCHEMA PATIENT_DB.PUBLIC;

-- ENCOUNTER CODES
SELECT COUNT(*) AS CNT, CODE, DESCRIPTION
FROM ENCOUNTERS
GROUP BY CODE, DESCRIPTION
ORDER BY CNT DESC; 

-- ENCOUNTER REASON CODES
SELECT COUNT(*) AS CNT, REASONCODE, REASONDESCRIPTION
FROM ENCOUNTERS
GROUP BY REASONCODE, REASONDESCRIPTION
ORDER BY CNT DESC;

-- CREATE ENCOUNTER REASON TABLE
CREATE OR REPLACE TABLE ENCOUNTER_REASONS (
	REASONCODE NUMBER,
	REASONDESCRIPTION VARCHAR
) AS
SELECT REASONCODE, MAX(REASONDESCRIPTION) AS REASONDESCRIPTION
FROM ENCOUNTERS
WHERE REASONCODE IS NOT NULL
GROUP BY REASONCODE;
SELECT * FROM ENCOUNTER_REASONS;
