-- CREATE WAREHOUSE zach_gds_demo;
/*
CREATE OR REPLACE TABLE p2p_demo.public.P2P_AGG_TRANSACTIONS (
	SOURCENODEID NUMBER(38,0),
	TARGETNODEID NUMBER(38,0),
	TOTAL_AMOUNT FLOAT,
	TRANSACTION_COUNT FLOAT
) AS
SELECT sourceNodeId, targetNodeId, SUM(transaction_amount) AS total_amount, COUNT(*) AS transaction_count
FROM p2p_demo.public.P2P_TRANSACTIONS
GROUP BY sourceNodeId, targetNodeId;
SELECT * FROM p2p_demo.public.P2P_AGG_TRANSACTIONS;
*/

CREATE OR REPLACE TABLE p2p_demo.public.P2P_AGG_TRANSACTIONS (
	SOURCENODEID NUMBER(38,0),
	TARGETNODEID NUMBER(38,0),
	TOTAL_AMOUNT FLOAT
) AS
SELECT sourceNodeId, targetNodeId, SUM(transaction_amount) AS total_amount
FROM p2p_demo.public.P2P_TRANSACTIONS
GROUP BY sourceNodeId, targetNodeId;
SELECT * FROM p2p_demo.public.P2P_AGG_TRANSACTIONS;