-- ///////////////////////////////////////////////////////////
-- 1) SETUP
-- ///////////////////////////////////////////////////////////
-- Create a consumer role for users of the GDS application
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_user TO ROLE gds_role;
-- Create a consumer role for administrators of the GDS application
CREATE ROLE IF NOT EXISTS gds_admin_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_admin TO ROLE gds_admin_role;

-- Grant access to consumer data
-- The application reads consumer data to build a graph object, and it also writes results into new tables.
-- We therefore need to grant the right permissions to give the application access.
GRANT USAGE ON DATABASE PATIENT_DB TO APPLICATION neo4j_graph_analytics;
GRANT USAGE ON SCHEMA PATIENT_DB.PUBLIC TO APPLICATION neo4j_graph_analytics;

-- required to read view data into a graph
GRANT SELECT ON ALL VIEWS IN SCHEMA PATIENT_DB.PUBLIC TO APPLICATION neo4j_graph_analytics;
GRANT SELECT ON ALL TABLES IN SCHEMA PATIENT_DB.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- required to write computation results into a table
GRANT CREATE TABLE ON SCHEMA PATIENT_DB.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- optional, ensuring the consumer role has access to tables created by the application
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA PATIENT_DB.PUBLIC TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA PATIENT_DB.PUBLIC TO ROLE accountadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PATIENT_DB.PUBLIC TO ROLE accountadmin;

-- Spin up Neo4j
CALL neo4j_graph_analytics.gds.create_session('CPU_X64_M');

-- show the various functions available
-- SHOW USER FUNCTIONS IN APPLICATION neo4j_graph_data_science;

-- drop graph if already exists.
SELECT neo4j_graph_analytics.gds.graph_drop('enc_graph', { 'failIfMissing': false });

-- ///////////////////////////////////////////////////////////
-- 2) PATIENT SIMILARITY
-- ///////////////////////////////////////////////////////////
USE DATABASE PATIENT_DB;
USE SCHEMA PATIENT_DB.PUBLIC;

-- create graph projection
SELECT neo4j_graph_analytics.gds.graph_project('enc_graph', {
    'nodeTable': 'patient_db.public.nodes',
    'relationshipTable': 'patient_db.public.relationships',
    'readConcurrency': 28
});

-- run node similarity to find similar patients by encounter procedures
SELECT neo4j_graph_analytics.gds.node_similarity('enc_graph', {
    'mutateRelationshipType': 'SIMILAR_PROCEDURES_TO',
    'mutateProperty': 'similarity',
    'concurrency': 28
});

-- (OPTIONAL) write similarity relationships back to a table and view
SELECT neo4j_graph_analytics.gds.write_relationships('enc_graph', {
'relationshipType': 'SIMILAR_PROCEDURES_TO',
'relationshipProperty': 'similarity',
'table': 'patient_db.public.SIMILAR_PROCEDURES_TO'
});
SELECT * FROM SIMILAR_PROCEDURES_TO;


-- ///////////////////////////////////////////////////////////
-- 3) PATIENT COHORTS (LOUVAIN COMMUNITIES)
-- ///////////////////////////////////////////////////////////

-- run louvain community detection
SELECT neo4j_graph_analytics.gds.louvain('enc_graph', {
    'relationshipTypes': ['SIMILAR_PROCEDURES_TO'],
    'relationshipWeightProperty': 'similarity',
    'mutateProperty': 'cohort'
});

-- write community ids, a.k.a. "cohorts" to a table & view
SELECT neo4j_graph_analytics.gds.write_nodeproperties('enc_graph',
           {'nodeProperties': ['cohort'], 'table': 'patient_db.public.cohorts'});
SELECT * FROM COHORTS;

-- combine cohorts with  patient data in patient_cohorts view
CREATE OR REPLACE VIEW PATIENT_COHORTS AS
SELECT  COHORTS.COHORT, PATIENTS.*
FROM PATIENT_NODE_MAPPING
         JOIN COHORTS ON PATIENT_NODE_MAPPING.NODEID = COHORTS.NODEID
         JOIN PATIENTS ON PATIENT_NODE_MAPPING.ID = PATIENTS.ID
ORDER BY COHORT DESC;
SELECT * FROM PATIENT_COHORTS;

-- view cohort patient counts
SELECT COHORT, COUNT(*) AS CNT FROM PATIENT_COHORTS GROUP BY COHORT ORDER BY CNT DESC;

-- ///////////////////////////////////////////////////////////
-- 4) CLEANUP NEO4J
-- ///////////////////////////////////////////////////////////
-- turn off Neo4j and spins down compute pool
CALL neo4j_graph_analytics.gds.stop_session();

-- ///////////////////////////////////////////////////////////
-- 5) VIEWS FOR ANALYSIS
-- ///////////////////////////////////////////////////////////
-- below we create some more helpful views for further analysis
    -- mainly a view for creating sankey charts to visualize patient journeys by cohort
-- We will filter out some ubiquitous procedures for easier readability. Namely:
    -- CODE=274804006 (Evaluation of uterine fundal height)
    -- CODE=225158009 (Auscultation of the fetal heart)
-- The below commented out query was used to identify these  ubiquitous procedures
/*SELECT COHORT, CODE, DESCRIPTION, CNT, PERCENT_CNT
FROM (
    SELECT COHORT, CODE, DESCRIPTION, COUNT(*) AS CNT, RATIO_TO_REPORT(CNT) OVER (PARTITION BY COHORT) AS PERCENT_CNT
    FROM PROCEDURE_COHORTS
    GROUP BY COHORT, CODE, DESCRIPTION
)
WHERE percent_cnt > 0.1
ORDER BY COHORT, CNT DESC;*/

-- create procedure cohort view filtering out ubiquitous procedures
CREATE OR REPLACE VIEW PROCEDURE_COHORTS AS
SELECT  PATIENT_COHORTS.COHORT, PROCEDURES.*
FROM PROCEDURES
         JOIN PATIENT_COHORTS ON PROCEDURES.PATIENT = PATIENT_COHORTS.ID
WHERE PROCEDURES.REASONCODE=72892002 //Pregnancy Encounters;
AND PROCEDURES.CODE NOT IN (274804006, 225158009); // Evaluation of uterine fundal height & Auscultation of the fetal heart
SELECT * FROM PROCEDURE_COHORTS;

-- create sankey view (TODO: The below can likely be combined into one query)
CREATE OR REPLACE VIEW PROCEDURE_COHORT_PATHS AS
SELECT COHORT, PATIENT, ENCOUNTER, START_TIME, CODE,
    FIRST_VALUE(CODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME, ENCOUNTER ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) SOURCE_PROC,
    LAST_VALUE(CODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME, ENCOUNTER  ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) TARGET_PROC
FROM PROCEDURE_COHORTS
ORDER BY COHORT, PATIENT, START_TIME, ENCOUNTER;
SELECT * FROM PROCEDURE_COHORT_PATHS;

CREATE OR REPLACE VIEW PROCEDURE_COHORT_SANKEY AS
SELECT COHORT, SOURCE_PROC, TARGET_PROC, COUNT(*) AS FREQ, RATIO_TO_REPORT(FREQ) OVER (PARTITION BY COHORT) AS PERCENT_FREQ
FROM PROCEDURE_COHORT_PATHS
GROUP BY COHORT, SOURCE_PROC, TARGET_PROC
ORDER BY COHORT, FREQ DESC;
SELECT * FROM PROCEDURE_COHORT_SANKEY;