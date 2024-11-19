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

CALL neo4j_graph_analytics.gds.create_session('CPU_X64_M');

SELECT neo4j_graph_analytics.gds.graph_drop('enc_reason_graph', { 'failIfMissing': false });


-- ///////////////////////////////////////////////////////////
-- 2) PATIENT SIMILARITY
-- ///////////////////////////////////////////////////////////
USE DATABASE PATIENT_DB;
USE SCHEMA PATIENT_DB.PUBLIC;

SELECT neo4j_graph_analytics.gds.graph_project('enc_reason_graph', {
    'nodeTable':         'patient_db.public.nodes',
    'relationshipTable': 'patient_db.public.relationships',
    'readConcurrency':   28
});


SELECT neo4j_graph_analytics.gds.node_similarity('enc_reason_graph', {
    'mutateRelationshipType': 'SIMILAR_ENC_REASONS_TO',
    'mutateProperty':         'similarity',
    'concurrency':            28
});

SELECT neo4j_graph_analytics.gds.write_relationships('enc_reason_graph', {
    'relationshipType':     'SIMILAR_ENC_REASONS_TO',
    'relationshipProperty': 'similarity',
    'table':                'patient_db.public.SIMILAR_ENC_REASONS_TO'
});
SELECT * FROM SIMILAR_ENC_REASONS_TO;


SELECT neo4j_graph_data_science.gds.degree_centrality('enc_reason_graph', {
    'mutateProperty': 'degree'
});
SELECT neo4j_graph_data_science.gds.write_nodeproperties('enc_reason_graph',
 {'nodeProperties': ['degree'], 'table': 'patient_db.public.degree'});

 
-- ///////////////////////////////////////////////////////////
-- 3) PATIENT COHORTS (LEIDEN COMMUNITIES)
-- ///////////////////////////////////////////////////////////
SHOW USER FUNCTIONS IN APPLICATION neo4j_graph_data_science;

SELECT neo4j_graph_analytics.gds.louvain('enc_reason_graph', {
    'relationshipTypes': ['SIMILAR_ENC_REASONS_TO'],
    'relationshipWeightProperty': 'similarity',
    'mutateProperty': 'cohort'
});

SELECT neo4j_graph_analytics.gds.write_nodeproperties('enc_reason_graph',
 {'nodeProperties': ['cohort'], 'table': 'patient_db.public.cohorts'});
SELECT * FROM COHORTS;

-- create patient cohort view
CREATE OR REPLACE VIEW PATIENT_COHORTS AS
    SELECT  COHORTS.COHORT, PATIENTS.*
    FROM PATIENT_NODE_MAPPING
    JOIN COHORTS ON PATIENT_NODE_MAPPING.NODEID = COHORTS.NODEID 
    JOIN PATIENTS ON PATIENT_NODE_MAPPING.ID = PATIENTS.ID
    ORDER BY COHORT DESC;
SELECT * FROM PATIENT_COHORTS;

CREATE OR REPLACE VIEW ENCOUNTER_COHORTS AS
    SELECT  PATIENT_COHORTS.COHORT, ENCOUNTERS.*
    FROM ENCOUNTERS
    JOIN PATIENT_COHORTS ON ENCOUNTERS.PATIENT = PATIENT_COHORTS.ID;
SELECT * FROM ENCOUNTER_COHORTS;

CREATE OR REPLACE VIEW ENCOUNTER_COHORT_PATHS AS
SELECT COHORT, ID, START_TIME, PATIENT, REASONCODE,
FIRST_VALUE(REASONCODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) SOURCE_ENC,
LAST_VALUE(REASONCODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) TARGET_ENC
FROM ENCOUNTER_COHORTS
ORDER BY COHORT, PATIENT, START_TIME;
SELECT * FROM ENCOUNTER_COHORT_PATHS;

CREATE OR REPLACE VIEW ENCOUNTER_COHORT_SANKY AS
SELECT COHORT, SOURCE_ENC, TARGET_ENC, COUNT(*) AS FREQ
FROM ENCOUNTER_COHORT_PATHS
WHERE (SOURCE_ENC IS NOT NULL) AND (TARGET_ENC IS NOT NULL)
GROUP BY COHORT, SOURCE_ENC, TARGET_ENC
ORDER BY COHORT, FREQ DESC;
SELECT * FROM ENCOUNTER_COHORT_SANKY;

-- ///////////////////////////////////////////////////////////
-- 4) CLEANUP
-- ///////////////////////////////////////////////////////////
CALL neo4j_graph_analytics.gds.stop_session();

-- ///////////////////////////////////////////////////////////
-- 5) VIEWS FOR ANALYSIS
-- ///////////////////////////////////////////////////////////

CREATE OR REPLACE VIEW ENCOUNTER_PATHS AS
SELECT ID, START_TIME, PATIENT, REASONCODE,
FIRST_VALUE(REASONCODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) SOURCE_ENC,
LAST_VALUE(REASONCODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) TARGET_ENC
FROM ENCOUNTERS
ORDER BY PATIENT, START_TIME;
SELECT * FROM ENCOUNTER_PATHS;

CREATE OR REPLACE VIEW ENCOUNTER_SANKY AS
SELECT SOURCE_ENC, TARGET_ENC, COUNT(*) AS FREQ
FROM ENCOUNTER_PATHS
WHERE (SOURCE_ENC IS NOT NULL) AND (TARGET_ENC IS NOT NULL)
GROUP BY SOURCE_ENC, TARGET_ENC
ORDER BY FREQ DESC;
SELECT * FROM ENCOUNTER_SANKY;

/* --- this one shot isn't working
SELECT COUNT(*) AS FREQ,
FIRST_VALUE(REASONCODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) SOURCE_ENC,
LAST_VALUE(REASONCODE) OVER (PARTITION BY PATIENT ORDER BY START_TIME ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) TARGET_ENC
FROM ENCOUNTERS
WHERE REASONCODE IS NOT NULL
GROUP BY SOURCE_ENC, TARGET_ENC
ORDER BY FREQ DESC;*/
