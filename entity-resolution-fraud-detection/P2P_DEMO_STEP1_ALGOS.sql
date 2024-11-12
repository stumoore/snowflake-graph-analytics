-- ///////////////////////////////////////////////////////////
-- 1) SETUP
-- ///////////////////////////////////////////////////////////
-- Create a consumer role for users of the GDS application
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE neo4j_graph_data_science.app_user TO ROLE gds_role;
-- Create a consumer role for administrators of the GDS application
CREATE ROLE IF NOT EXISTS gds_admin_role;
GRANT APPLICATION ROLE neo4j_graph_data_science.app_admin TO ROLE gds_admin_role;

-- Grant access to consumer data
-- The application reads consumer data to build a graph object, and it also writes results into new tables.
-- We therefore need to grant the right permissions to give the application access.
GRANT USAGE ON DATABASE p2p_demo TO APPLICATION neo4j_graph_data_science;
GRANT USAGE ON SCHEMA p2p_demo.public TO APPLICATION neo4j_graph_data_science;

-- required to read tabular data into a graph
GRANT SELECT ON ALL TABLES IN SCHEMA p2p_demo.public TO APPLICATION neo4j_graph_data_science;
-- required to write computation results into a table
GRANT CREATE TABLE ON SCHEMA p2p_demo.public TO APPLICATION neo4j_graph_data_science;
-- optional, ensuring the consumer role has access to tables created by the application
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA p2p_demo.public TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA p2p_demo.public TO ROLE accountadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA p2p_demo.public TO ROLE accountadmin;

CALL neo4j_graph_data_science.gds.create_session('CPU_X64_M');

SELECT neo4j_graph_data_science.gds.graph_drop('transaction_graph', { 'failIfMissing': false });
USE DATABASE p2p_demo;

-- ///////////////////////////////////////////////////////////
-- 2) LOUVIAN & PAGERANK
-- ///////////////////////////////////////////////////////////
-- user table (nodes)
USE DATABASE p2p_demo;
USE SCHEMA public;
SELECT * FROM p2p_users;


-- transaction table relationships
SELECT * FROM p2p_agg_transactions;


-- Construct a graph projection from node and relationship tables
SELECT neo4j_graph_data_science.gds.graph_project('transaction_graph', {
    'nodeTable': 'p2p_demo.public.p2p_users',
                                                  'relationshipTable': 'p2p_demo.public.p2p_agg_transactions'});

--calculate louvain communities
SELECT neo4j_graph_data_science.gds.louvain('transaction_graph', {'mutateProperty': 'community_id'});

-- Write  to table
SELECT neo4j_graph_data_science.gds.write_nodeproperties('transaction_graph',
           {'nodeProperties': ['community_id'], 'table': 'p2p_demo.public.p2p_louvain'});

-- Query result from table
SELECT * FROM p2p_louvain ORDER BY community_id;


--calculate transaction pagerank
SELECT neo4j_graph_data_science.gds.page_rank('transaction_graph', {'mutateProperty': 'score'});
-- Write  to table
SELECT neo4j_graph_data_science.gds.write_nodeproperties('transaction_graph',
           {'nodeProperties': ['score'], 'table': 'p2p_demo.public.p2p_transaction_pagerank'});
-- Query result from table
SELECT * FROM p2p_transaction_pagerank ORDER BY score DESC;


-- ///////////////////////////////////////////////////////////
-- 3) WCC Entity Resolution
-- ///////////////////////////////////////////////////////////
SELECT * FROM p2p_trans_w_shared_card;

-- Construct a graph projection from node and relationship tables
SELECT neo4j_graph_data_science.gds.graph_drop('entity_linking_graph', { 'failIfMissing': false });

-- Similar to calling the functions with simple or qualified names, we have to reference the tables wither with qualified names or with simple names while USE ing the database or schema.
SELECT neo4j_graph_data_science.gds.graph_project(
               'entity_linking_graph',
           { 'nodeTable': 'p2p_demo.public.p2p_users',
               'relationshipTable': 'p2p_demo.public.p2p_trans_w_shared_card'});

-- calculate wcc
SELECT neo4j_graph_data_science.gds.wcc('entity_linking_graph', {'mutateProperty': 'wcc_id'});

-- Write  to table
SELECT neo4j_graph_data_science.gds.write_nodeproperties('entity_linking_graph',
           {'nodeProperties': ['wcc_id'], 'table': 'p2p_demo.public.P2P_COMPONENTS'}
);
SELECT * FROM P2P_COMPONENTS ORDER BY wcc_id;

-- create a resolved entity view based on WCC
CREATE OR REPLACE VIEW resolved_p2p_users AS
SELECT p2p_components.wcc_id,
       count(*) AS user_count,
       TO_NUMBER(SUM(p2p_users.fraud_transfer_flag)>0) AS has_fraud_flag,
       ARRAY_AGG(p2p_users.nodeId) AS user_ids
FROM p2p_users JOIN p2p_components ON p2p_users.nodeId = p2p_components.nodeId
GROUP BY wcc_id ORDER BY user_count DESC;
SELECT * FROM resolved_p2p_users;

-- ///////////////////////////////////////////////////////////
-- 4) CLEANUP
-- ///////////////////////////////////////////////////////////
CALL neo4j_graph_data_science.gds.stop_session();