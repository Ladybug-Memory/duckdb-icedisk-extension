# icedisk — query icebug on-disk graphs as DuckDB tables

```sql
LOAD icedisk;
SELECT icedisk_attach('my_graph_csr');
-- mounted 4 views from 'my_graph_csr': edges_follows, edges_livesin, nodes_city, nodes_user
-- (or: PRAGMA icedisk_attach('...'))

SELECT * FROM edges_follows;   -- (source, target, since) with original IDs
SELECT * FROM nodes_user;      -- pass-through parquet scan
```

`icedisk` is a DuckDB extension that mounts **[icebug-disk v1][icebug-format]**
directories — CSR graphs stored as Parquet plus a Cypher schema — as native
DuckDB node / edge tables. It consumes the `indices` / `indptr` Parquet files
directly, so after attaching, `SELECT * FROM edges_follows` behaves as if you
were querying the original DuckDB node / edge tables the CSR was built from.

This repo was cloned from the DuckDB extension template; the template's demo
`waddle` functions were replaced by the icedisk implementation in
`src/icedisk_extension.cpp`.

## icebug-disk v1 layout (what this reads)

Produced by the [`icebug-format` CLI][icebug-format] from a DuckDB source
database whose tables are named `nodes_*` / `edges_*`:

| File | Content |
| --- | --- |
| `nodes_<name>.parquet` | Original node table, pass-through. Row order defines the dense CSR id (`0..N-1`); the first column is the primary key. |
| `indices_<name>.parquet` | CSR `indices`: one row per edge sorted by (source, target). Column `target` holds the dense target id; extra columns are edge properties. |
| `indptr_<name>.parquet` | CSR `indptr`: `N+1` row pointers (column `ptr`). |
| `schema.cypher` | `CREATE NODE TABLE …` / `CREATE REL TABLE … (FROM a TO b …)` statements used to resolve edge endpoints. |

Each Parquet file carries an `icebug_disk_version` key (`v1`); files stamped
with any other version are rejected, files without the key are accepted
(hand-built CSR works too).

## Functions

```sql
LOAD icedisk;

-- Mount a whole CSR directory as views (scalar form returns a summary,
-- PRAGMA form returns Success; both are idempotent):
SELECT icedisk_attach('path/to/my_graph_csr');
-- mounted 4 views from 'path/to/my_graph_csr': edges_follows, edges_livesin, nodes_city, nodes_user
PRAGMA icedisk_attach('path/to/my_graph_csr');

-- Created views:
--   nodes_<name>  -> SELECT * FROM read_parquet('.../nodes_<name>.parquet')
--   edges_<name>  -> SELECT * FROM icedisk_edge_scan('.../indices_*.parquet', '.../indptr_*.parquet',
--                                                   '.../src_nodes_*.parquet', '.../dst_nodes_*.parquet')

-- Expand one CSR pair directly (dense CSR ids as UBIGINT):
SELECT * FROM icedisk_edge_scan('indices_follows.parquet', 'indptr_follows.parquet');

-- ... or mapped back to the original primary keys through the nodes tables:
SELECT * FROM icedisk_edge_scan('indices_follows.parquet', 'indptr_follows.parquet',
                                'nodes_user.parquet', 'nodes_user.parquet');

-- Heterogeneous edges: source ids from one node table, targets from another
-- (attach picks these automatically from schema.cypher FROM/TO):
SELECT * FROM icedisk_edge_scan('indices_livesin.parquet', 'indptr_livesin.parquet',
                                'nodes_user.parquet', 'nodes_city.parquet');

SELECT icedisk_version();  -- icedisk 0.1.0 (icebug-disk v1)
```

After `icedisk_attach`, ordinary relational queries — including joins between
node and edge tables — just work:

```sql
SELECT u.name, c.name
FROM edges_livesin e
JOIN nodes_user u ON u.id = e.source
JOIN nodes_city c ON c.id = e.target
ORDER BY u.name;
```

The sibling [`icebug-format`][icebug-format] checkout (if cloned next to this
repo) can be attached the same way, e.g.
`SELECT icedisk_attach('../icebug-format/examples/karate/duckdb/karate_csr')`.

## Notes & limitations

- `icedisk_edge_scan` materialises the CSR arrays at bind time (via DuckDB's
  own Parquet reader) and then streams expanded edges; the scan itself is
  thread-safe and parallel. Very large graphs pay a bind-time memory cost
  proportional to `E` — keep an eye on this for billion-edge directories.
- Node id mapping follows file row order (no `ORDER BY`), matching the
  reference `test_csr_duckdb.py` from `icebug-format`. The CLI writes node
  tables `ORDER BY pk`, so dense id == sorted position.
- `indptr` accepts a `ptr` or `row_ptr` column; `indices` accepts a `target`
  (else `dst`/`destination`/`to`, else first) column. Types are cast to
  `UBIGINT` on read.
- If `schema.cypher` is absent, or an edge's endpoint tables are missing,
  edges attach in dense-id mode (or via the first node table found).
- `CREATE OR REPLACE VIEW` fails if a *table* (not a view) already holds the
  target name — attach into a fresh database or schema to avoid clashes.

## Building & testing

Requires the `duckdb` and `extension-ci-tools` submodules (already wired):

```sh
git submodule update --init
make release        # builds DuckDB + the extension + the unittest runner
make test_release   # runs test/sql/*.test (needs no extra setup)
```

Tests use the committed fixture directory `test/data/icebug_csr`
(heterogeneous user/city graph); regenerate it with:

```sh
duckdb -f test/data/gen_fixtures.sql   # + schema.cypher, see the file header
```

Manual smoke test with the built shell:

```sh
./build/release/duckdb :memory: "LOAD icedisk; SELECT icedisk_attach('test/data/icebug_csr'); SELECT * FROM edges_follows;"
```

[icebug-format]: https://github.com/Ladybug-Memory/icebug-format
