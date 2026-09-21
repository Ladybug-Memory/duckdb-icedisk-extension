-- Generates the icebug-disk v1 fixture directory used by the sqllogictests.
-- Mirrors the logic of the `icebug-format` CLI (see icebug_format.cli):
-- nodes are stored ORDER BY pk (row order == dense CSR id), indices hold
-- (target UBIGINT [, props]) sorted by (source, target), indptr is N+1
-- UBIGINT row pointers with a leading zero.
-- Run: duckdb -f test/data/gen_fixtures.sql   (from the repo root)

-- ---------------------------------------------------------------- source DB
CREATE TABLE nodes_user(id BIGINT, name VARCHAR);
INSERT INTO nodes_user VALUES (100, 'Adam'), (101, 'Bob'), (102, 'Cara'), (103, 'Dee');

CREATE TABLE nodes_city(id BIGINT, name VARCHAR);
INSERT INTO nodes_city VALUES (10, 'Springfield'), (11, 'Shelbyville');

CREATE TABLE edges_follows(source BIGINT, target BIGINT, since INTEGER);
INSERT INTO edges_follows VALUES (100, 101, 2018), (100, 102, 2019), (101, 103, 2020), (102, 100, 2021);

CREATE TABLE edges_livesin(source BIGINT, target BIGINT);
INSERT INTO edges_livesin VALUES (100, 10), (101, 10), (102, 11), (103, 11);

-- ---------------------------------------------------------------- CSR nodes
CREATE TABLE csr_nodes_user AS SELECT * FROM nodes_user ORDER BY id;
CREATE TABLE csr_nodes_city AS SELECT * FROM nodes_city ORDER BY id;

-- ---------------------------------------------------------------- follows (User -> User, homogeneous)
CREATE TABLE relations_follows AS
WITH src_map AS (SELECT row_number() OVER (ORDER BY id) - 1 AS csr_index, id AS oid FROM csr_nodes_user),
     dst_map AS (SELECT row_number() OVER (ORDER BY id) - 1 AS csr_index, id AS oid FROM csr_nodes_user)
SELECT m1.csr_index AS csr_source, m2.csr_index AS csr_target, e.since
FROM edges_follows e
JOIN src_map m1 ON e.source = m1.oid
JOIN dst_map m2 ON e.target = m2.oid;

CREATE TABLE indptr_follows AS
WITH node_range AS (SELECT unnest(range(0, 4)) AS node_id),
     degrees AS (SELECT csr_source AS src, COUNT(*) AS deg FROM relations_follows GROUP BY csr_source),
     cumulative AS (
         SELECT node_range.node_id,
                COALESCE(SUM(degrees.deg) OVER (ORDER BY node_range.node_id ROWS UNBOUNDED PRECEDING), 0) AS ptr
         FROM node_range LEFT JOIN degrees ON node_range.node_id = degrees.src
     )
SELECT ptr FROM cumulative ORDER BY node_id;
CREATE OR REPLACE TABLE indptr_follows AS
SELECT 0::UBIGINT AS ptr UNION ALL SELECT ptr::UBIGINT FROM indptr_follows ORDER BY ptr;

CREATE TABLE indices_follows AS
SELECT csr_target::UBIGINT AS target, since FROM relations_follows ORDER BY csr_source, csr_target;

-- ---------------------------------------------------------------- livesin (User -> City, heterogeneous)
CREATE TABLE relations_livesin AS
WITH src_map AS (SELECT row_number() OVER (ORDER BY id) - 1 AS csr_index, id AS oid FROM csr_nodes_user),
     dst_map AS (SELECT row_number() OVER (ORDER BY id) - 1 AS csr_index, id AS oid FROM csr_nodes_city)
SELECT m1.csr_index AS csr_source, m2.csr_index AS csr_target
FROM edges_livesin e
JOIN src_map m1 ON e.source = m1.oid
JOIN dst_map m2 ON e.target = m2.oid;

CREATE TABLE indptr_livesin AS
WITH node_range AS (SELECT unnest(range(0, 4)) AS node_id),
     degrees AS (SELECT csr_source AS src, COUNT(*) AS deg FROM relations_livesin GROUP BY csr_source),
     cumulative AS (
         SELECT node_range.node_id,
                COALESCE(SUM(degrees.deg) OVER (ORDER BY node_range.node_id ROWS UNBOUNDED PRECEDING), 0) AS ptr
         FROM node_range LEFT JOIN degrees ON node_range.node_id = degrees.src
     )
SELECT ptr FROM cumulative ORDER BY node_id;
CREATE OR REPLACE TABLE indptr_livesin AS
SELECT 0::UBIGINT AS ptr UNION ALL SELECT ptr::UBIGINT FROM indptr_livesin ORDER BY ptr;

CREATE TABLE indices_livesin AS
SELECT csr_target::UBIGINT AS target FROM relations_livesin ORDER BY csr_source, csr_target;

-- ---------------------------------------------------------------- export
COPY csr_nodes_user TO 'test/data/icebug_csr/nodes_user.parquet'
  (FORMAT PARQUET, KV_METADATA { icebug_disk_version: 'v1' });
COPY csr_nodes_city TO 'test/data/icebug_csr/nodes_city.parquet'
  (FORMAT PARQUET, KV_METADATA { icebug_disk_version: 'v1' });
COPY indices_follows TO 'test/data/icebug_csr/indices_follows.parquet'
  (FORMAT PARQUET, KV_METADATA { icebug_disk_version: 'v1' });
COPY indptr_follows TO 'test/data/icebug_csr/indptr_follows.parquet'
  (FORMAT PARQUET, KV_METADATA { icebug_disk_version: 'v1' });
COPY indices_livesin TO 'test/data/icebug_csr/indices_livesin.parquet'
  (FORMAT PARQUET, KV_METADATA { icebug_disk_version: 'v1' });
COPY indptr_livesin TO 'test/data/icebug_csr/indptr_livesin.parquet'
  (FORMAT PARQUET, KV_METADATA { icebug_disk_version: 'v1' });

-- schema.cypher lives next to the parquet files as test/data/icebug_csr/schema.cypher
-- (committed; handwritten in exactly the format the CLI generates).
SELECT 'follows' AS edge, * FROM indices_follows;
SELECT 'follows_ptr' AS edge, * FROM indptr_follows;
SELECT 'livesin' AS edge, * FROM indices_livesin;
SELECT 'livesin_ptr' AS edge, * FROM indptr_livesin;
