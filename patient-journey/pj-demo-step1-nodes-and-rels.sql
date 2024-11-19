USE DATABASE PATIENT_DB;
USE SCHEMA PATIENT_DB.PUBLIC;


-- ///////////////////////////////////////////////////////////
-- 1) CREATE NODES
-- ///////////////////////////////////////////////////////////
-- We create two node types: Patients and Procedures
-- This requires creating a mapping table for each type then union-ing them into one view of node ids
-- To focus the analysis will filter to one type of encounter.
    -- Otherwise the analysis will explode to many different health care areas (dental, cancer, renal, etc.)
    -- We will focus just on Pregnancy related encounters =>
    -- encounter_reason_code = 72892002

-- node id sequence necessary for mapping
CREATE OR REPLACE SEQUENCE node_id START = 0 INCREMENT = 1;

-- create procedure node mapping (one node per each unique procedure)
CREATE OR REPLACE TABLE PROC_TYPE_NODE_MAPPING (NODEID, CODE) AS
SELECT node_id.nextval, CODE
FROM (
         SELECT DISTINCT CODE
         FROM PROCEDURES
         WHERE PROCEDURES.REASONCODE=72892002
     );
SELECT * FROM PROC_TYPE_NODE_MAPPING;

-- create patient node mapping (one node per each unique patient)
CREATE OR REPLACE TABLE PATIENT_NODE_MAPPING (NODEID, ID) AS
SELECT node_id.nextval AS NODEID, PATIENT
FROM (
         SELECT DISTINCT PATIENT
         FROM PROCEDURES
         WHERE PROCEDURES.REASONCODE=72892002
     );
SELECT * FROM PATIENT_NODE_MAPPING ORDER BY ID;

-- final node view to use with Neo4j (union all nodeids)
CREATE OR REPLACE VIEW NODES(nodeId) AS
SELECT NODEID FROM PATIENT_NODE_MAPPING
UNION
SELECT NODEID FROM PROC_TYPE_NODE_MAPPING;
SELECT * FROM NODES;

-- ///////////////////////////////////////////////////////////
-- 2) CREATE RELATIONSHIPS
-- ///////////////////////////////////////////////////////////
-- We only have one relationship type: procedure encounters that represent patients receiving procedures
-- We will create a view for relationships with:
    -- sourceNodeIds representing patients
    -- targetNodeIds representing unique procedures
CREATE OR REPLACE VIEW RELATIONSHIPS(sourceNodeId, targetNodeId) AS
SELECT
    PATIENT_NODE_MAPPING.NODEID AS sourceNodeId,
    PROC_TYPE_NODE_MAPPING.NODEID AS targetNodeId
FROM PROCEDURES
         INNER JOIN PATIENT_NODE_MAPPING
                    ON PROCEDURES.PATIENT = PATIENT_NODE_MAPPING.ID
         INNER JOIN PROC_TYPE_NODE_MAPPING
                    ON PROCEDURES.CODE = PROC_TYPE_NODE_MAPPING.CODE
WHERE REASONCODE=72892002;
SELECT * FROM RELATIONSHIPS;

-- count verify - these should be equal
SELECT COUNT(*) FROM RELATIONSHIPS;
SELECT COUNT(*) FROM PROCEDURES WHERE REASONCODE=72892002