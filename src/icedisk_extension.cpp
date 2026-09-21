//===----------------------------------------------------------------------===//
//                         Icedisk
//
// icedisk_extension.cpp
//
// DuckDB extension that mounts icebug-disk v1 directories (CSR graphs stored
// as Parquet) as native DuckDB node / edge tables.
//
// icebug-disk v1 layout (produced by the `icebug-format` CLI from a DuckDB
// source database whose tables are named nodes_* / edges_*):
//
//   nodes_<name>.parquet    original node table, pass-through. Row order
//                           defines the dense CSR id (0..N-1); the first
//                           column is the primary key.
//   indices_<name>.parquet  CSR `indices`: one row per edge, sorted by
//                           (source, target). Column `target` holds the dense
//                           target id; any extra columns are edge properties.
//   indptr_<name>.parquet   CSR `indptr`: N+1 row pointers (column `ptr`).
//   schema.cypher           Cypher schema with CREATE NODE TABLE /
//                           CREATE REL TABLE .. (FROM a TO b ..) statements.
//
// Provided functions:
//
//   icedisk_edge_scan(indices_path, indptr_path)
//   icedisk_edge_scan(indices_path, indptr_path, src_nodes_path, dst_nodes_path)
//       Table function that expands a CSR pair back into a relational edge
//       table (source, target [, props...]). Without node paths the ids are
//       the dense CSR ids (UBIGINT); with node paths they are mapped back to
//       the original primary keys through the nodes parquet row order.
//
//   SELECT icedisk_attach('path/to/csr_dir')  -- or: PRAGMA icedisk_attach('...')
//       Discovers every nodes_*/indices_*/indptr_*.parquet pair in the
//       directory, uses schema.cypher (when present) to resolve
//       FROM/TO node types, and creates native-feeling views:
//         nodes_<name>  -> SELECT * FROM read_parquet('...')
//         edges_<name>  -> SELECT * FROM icedisk_edge_scan(...)
//       After the call, `SELECT * FROM edges_follows` etc. just work.
//
//   icedisk_version() -> VARCHAR
//
//===----------------------------------------------------------------------===//

#define DUCKDB_EXTENSION_MAIN

#include "icedisk_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <algorithm>
#include <atomic>
#include <fstream>
#include <map>
#include <regex>
#include <sstream>

namespace duckdb {

static constexpr const char *ICEDISK_DISK_VERSION = "v1";

//===----------------------------------------------------------------------===//
// Small helpers
//===----------------------------------------------------------------------===//

static string EscapeQuote(const string &s) {
	string out;
	out.reserve(s.size());
	for (char c : s) {
		if (c == '\'') {
			out += "''";
		} else {
			out += c;
		}
	}
	return out;
}

static string QuoteIdent(const string &s) {
	string out = "\"";
	for (char c : s) {
		if (c == '"') {
			out += "\"\"";
		} else {
			out += c;
		}
	}
	out += "\"";
	return out;
}

static string ToLowerCopy(string s) {
	for (auto &c : s) {
		c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
	}
	return s;
}

// Run SQL on a helper connection sharing the same database instance.
// Table-function bind / pragma bodies must not run nested queries on the
// calling ClientContext (its query is being planned / executed), so every
// helper query goes through a fresh Connection instead.
static unique_ptr<MaterializedQueryResult> HelperQuery(ClientContext &context, const string &sql) {
	Connection con(*context.db);
	auto result = con.Query(sql);
	if (!result) {
		throw IOException("icedisk: helper query returned no result for: " + sql);
	}
	if (result->HasError()) {
		throw IOException("icedisk: helper query failed: " + result->GetError() + "\nQuery: " + sql);
	}
	return result;
}

static void HelperExecute(ClientContext &context, const string &sql) {
	HelperQuery(context, sql);
}

struct ParquetSchema {
	vector<string> names;
	vector<LogicalType> types;
};

static ParquetSchema ReadParquetSchema(ClientContext &context, const string &path) {
	auto result = HelperQuery(context, "SELECT * FROM read_parquet('" + EscapeQuote(path) + "') LIMIT 0");
	ParquetSchema schema;
	schema.names = result->names;
	schema.types = result->types;
	return schema;
}

static vector<Value> FetchSingleColumn(ClientContext &context, const string &sql) {
	auto result = HelperQuery(context, sql);
	vector<Value> values;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			values.push_back(chunk->GetValue(0, r));
		}
	}
	return values;
}

// Verify the icebug_disk_version key when present. Files written by the
// icebug-format CLI carry icebug_disk_version='v1'. Hand-built CSR parquet
// without the key is accepted for forward compatibility. The check itself is
// best-effort: when the parquet extension is not loaded, the subsequent data
// reads surface the actionable error instead.
static void CheckDiskVersion(ClientContext &context, const string &path) {
	unique_ptr<MaterializedQueryResult> result;
	try {
		Connection con(*context.db);
		result = con.Query("SELECT key, value FROM parquet_kv_metadata('" + EscapeQuote(path) + "')");
	} catch (...) {
		return;
	}
	if (!result || result->HasError()) {
		return;
	}
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			auto key = chunk->GetValue(0, r).ToString();
			if (ToLowerCopy(key) == "icebug_disk_version") {
				auto value = ToLowerCopy(chunk->GetValue(1, r).ToString());
				if (value != ICEDISK_DISK_VERSION) {
					throw IOException("icedisk: unsupported icebug_disk_version '" + value + "' in file '" + path +
					                  "' (this extension reads v1)");
				}
				return;
			}
		}
	}
}

//===----------------------------------------------------------------------===//
// icedisk_edge_scan table function
//===----------------------------------------------------------------------===//

struct EdgeScanBindData : public TableFunctionData {
	string indices_path;
	string indptr_path;
	string src_nodes_path;
	string dst_nodes_path;
	bool has_mapping = false;

	vector<uint64_t> indptr;     // N+1 row pointers
	vector<uint64_t> targets;    // E dense target ids
	vector<Value> src_ids;       // N_src original ids (empty when dense)
	vector<Value> dst_ids;       // N_dst original ids (empty when dense)
	vector<vector<Value>> props; // [prop_idx][edge_idx]
	vector<string> prop_names;

	idx_t node_count = 0;
	idx_t edge_count = 0;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<EdgeScanBindData>(*this);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<EdgeScanBindData>();
		return indices_path == o.indices_path && indptr_path == o.indptr_path && src_nodes_path == o.src_nodes_path &&
		       dst_nodes_path == o.dst_nodes_path;
	}
};

struct EdgeScanGlobalState : public GlobalTableFunctionState {
	std::atomic<idx_t> next_offset {0};
};

static void ResolveCSRColumn(const ParquetSchema &schema, const vector<string> &candidates, string &result) {
	for (auto &cand : candidates) {
		for (auto &name : schema.names) {
			if (ToLowerCopy(name) == cand) {
				result = name;
				return;
			}
		}
	}
	if (schema.names.empty()) {
		throw IOException("icedisk: parquet file has no columns");
	}
	result = schema.names[0];
}

static unique_ptr<FunctionData> EdgeScanBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<EdgeScanBindData>();
	bind_data->indices_path = input.inputs[0].GetValue<string>();
	bind_data->indptr_path = input.inputs[1].GetValue<string>();
	if (input.inputs.size() > 2) {
		bind_data->src_nodes_path = input.inputs[2].GetValue<string>();
		bind_data->dst_nodes_path = input.inputs[3].GetValue<string>();
		bind_data->has_mapping = true;
	}

	for (auto *p : {&bind_data->indices_path, &bind_data->indptr_path}) {
		if (!FileSystem::GetFileSystem(context).FileExists(*p)) {
			throw IOException("icedisk_edge_scan: file not found: '" + *p + "'");
		}
	}
	if (bind_data->has_mapping) {
		for (auto *p : {&bind_data->src_nodes_path, &bind_data->dst_nodes_path}) {
			if (!FileSystem::GetFileSystem(context).FileExists(*p)) {
				throw IOException("icedisk_edge_scan: nodes file not found: '" + *p + "'");
			}
		}
	}

	CheckDiskVersion(context, bind_data->indices_path);
	CheckDiskVersion(context, bind_data->indptr_path);

	// --- indptr ---
	auto indptr_schema = ReadParquetSchema(context, bind_data->indptr_path);
	string indptr_col;
	ResolveCSRColumn(indptr_schema, {"ptr", "row_ptr"}, indptr_col);
	auto indptr_values =
	    FetchSingleColumn(context, "SELECT " + QuoteIdent(indptr_col) + "::UBIGINT FROM read_parquet('" +
	                                   EscapeQuote(bind_data->indptr_path) + "')");
	bind_data->indptr.reserve(indptr_values.size());
	for (auto &v : indptr_values) {
		if (v.IsNull()) {
			throw IOException("icedisk_edge_scan: NULL value in indptr file '" + bind_data->indptr_path + "'");
		}
		bind_data->indptr.push_back(v.GetValue<uint64_t>());
	}

	// --- indices + edge properties ---
	auto indices_schema = ReadParquetSchema(context, bind_data->indices_path);
	string target_col;
	ResolveCSRColumn(indices_schema, {"target", "dst", "destination", "to"}, target_col);
	vector<string> prop_cols;
	for (auto &name : indices_schema.names) {
		if (name != target_col) {
			prop_cols.push_back(name);
		}
	}
	bind_data->prop_names = prop_cols;

	auto target_values =
	    FetchSingleColumn(context, "SELECT " + QuoteIdent(target_col) + "::UBIGINT FROM read_parquet('" +
	                                   EscapeQuote(bind_data->indices_path) + "')");
	bind_data->targets.reserve(target_values.size());
	for (auto &v : target_values) {
		if (v.IsNull()) {
			throw IOException("icedisk_edge_scan: NULL value in indices target column of '" + bind_data->indices_path +
			                  "'");
		}
		bind_data->targets.push_back(v.GetValue<uint64_t>());
	}
	bind_data->edge_count = bind_data->targets.size();

	bind_data->props.resize(prop_cols.size());
	for (idx_t p = 0; p < prop_cols.size(); p++) {
		bind_data->props[p] = FetchSingleColumn(context, "SELECT " + QuoteIdent(prop_cols[p]) + " FROM read_parquet('" +
		                                                     EscapeQuote(bind_data->indices_path) + "')");
		if (bind_data->props[p].size() != bind_data->edge_count) {
			throw IOException("icedisk_edge_scan: property column '" + prop_cols[p] + "' has " +
			                  to_string(bind_data->props[p].size()) + " rows, expected " +
			                  to_string(bind_data->edge_count));
		}
	}

	// --- node id mapping (dense CSR id -> original primary key) ---
	LogicalType src_type = LogicalType::UBIGINT;
	LogicalType dst_type = LogicalType::UBIGINT;
	if (bind_data->has_mapping) {
		CheckDiskVersion(context, bind_data->src_nodes_path);
		if (bind_data->dst_nodes_path != bind_data->src_nodes_path) {
			CheckDiskVersion(context, bind_data->dst_nodes_path);
		}
		auto src_schema = ReadParquetSchema(context, bind_data->src_nodes_path);
		auto dst_schema = (bind_data->dst_nodes_path == bind_data->src_nodes_path)
		                      ? src_schema
		                      : ReadParquetSchema(context, bind_data->dst_nodes_path);
		if (src_schema.names.empty() || dst_schema.names.empty()) {
			throw IOException("icedisk_edge_scan: nodes parquet file has no columns");
		}
		// First column is the primary key; file order defines the dense CSR id.
		src_type = src_schema.types[0];
		dst_type = dst_schema.types[0];
		string src_pk = src_schema.names[0];
		string dst_pk = dst_schema.names[0];
		bind_data->src_ids = FetchSingleColumn(context, "SELECT " + QuoteIdent(src_pk) + " FROM read_parquet('" +
		                                                    EscapeQuote(bind_data->src_nodes_path) + "')");
		if (bind_data->dst_nodes_path == bind_data->src_nodes_path) {
			bind_data->dst_ids = bind_data->src_ids;
		} else {
			bind_data->dst_ids = FetchSingleColumn(context, "SELECT " + QuoteIdent(dst_pk) + " FROM read_parquet('" +
			                                                    EscapeQuote(bind_data->dst_nodes_path) + "')");
		}
	}

	// --- validation ---
	if (bind_data->indptr.empty()) {
		throw IOException("icedisk_edge_scan: indptr file '" + bind_data->indptr_path + "' is empty");
	}
	if (bind_data->indptr[0] != 0) {
		throw IOException("icedisk_edge_scan: indptr must start at 0 in '" + bind_data->indptr_path + "'");
	}
	for (idx_t i = 1; i < bind_data->indptr.size(); i++) {
		if (bind_data->indptr[i] < bind_data->indptr[i - 1]) {
			throw IOException("icedisk_edge_scan: indptr must be non-decreasing in '" + bind_data->indptr_path + "'");
		}
	}
	if (bind_data->indptr.back() != bind_data->edge_count) {
		throw IOException("icedisk_edge_scan: indptr last entry (" + to_string(bind_data->indptr.back()) +
		                  ") does not match indices row count (" + to_string(bind_data->edge_count) + ")");
	}
	bind_data->node_count = bind_data->indptr.size() - 1;
	if (bind_data->has_mapping) {
		if (bind_data->src_ids.size() != bind_data->node_count) {
			throw IOException("icedisk_edge_scan: src nodes file has " + to_string(bind_data->src_ids.size()) +
			                  " rows but indptr describes " + to_string(bind_data->node_count) + " nodes");
		}
		uint64_t dst_n = bind_data->dst_ids.size();
		for (idx_t e = 0; e < bind_data->edge_count; e++) {
			if (bind_data->targets[e] >= dst_n) {
				throw IOException("icedisk_edge_scan: target id " + to_string(bind_data->targets[e]) +
				                  " out of range (" + to_string(dst_n) + " dst nodes)");
			}
		}
	}

	// --- output schema: (source, target [, props...]) like a native edge table ---
	names.push_back("source");
	return_types.push_back(src_type);
	names.push_back("target");
	return_types.push_back(dst_type);
	for (auto &prop : prop_cols) {
		bool found = false;
		for (idx_t c = 0; c < indices_schema.names.size(); c++) {
			if (indices_schema.names[c] == prop) {
				names.push_back(prop);
				return_types.push_back(indices_schema.types[c]);
				found = true;
				break;
			}
		}
		if (!found) {
			throw InternalException("icedisk_edge_scan: lost track of property column '" + prop + "'");
		}
	}

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> EdgeScanInit(ClientContext &context, TableFunctionInitInput &input) {
	(void)context;
	(void)input;
	return make_uniq<EdgeScanGlobalState>();
}

static void EdgeScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	(void)context;
	auto &bind_data = data.bind_data->Cast<EdgeScanBindData>();
	auto &state = data.global_state->Cast<EdgeScanGlobalState>();

	if (bind_data.edge_count == 0) {
		output.SetCardinality(0);
		return;
	}
	idx_t start = state.next_offset.fetch_add(STANDARD_VECTOR_SIZE);
	if (start >= bind_data.edge_count) {
		output.SetCardinality(0);
		return;
	}
	idx_t count = std::min<idx_t>(STANDARD_VECTOR_SIZE, bind_data.edge_count - start);

	// CSR row (source node) for an edge position via binary search on indptr.
	// Thread-safe: indptr/targets/ids are read-only after bind.
	auto &indptr = bind_data.indptr;
	for (idx_t r = 0; r < count; r++) {
		idx_t pos = start + r;
		idx_t src = static_cast<idx_t>(std::upper_bound(indptr.begin(), indptr.end(), pos) - indptr.begin()) - 1;
		uint64_t tgt = bind_data.targets[pos];
		if (bind_data.has_mapping) {
			output.SetValue(0, r, bind_data.src_ids[src]);
			output.SetValue(1, r, bind_data.dst_ids[tgt]);
		} else {
			output.SetValue(0, r, Value::UBIGINT(src));
			output.SetValue(1, r, Value::UBIGINT(tgt));
		}
		for (idx_t p = 0; p < bind_data.props.size(); p++) {
			output.SetValue(2 + p, r, bind_data.props[p][pos]);
		}
	}
	output.SetCardinality(count);
}

//===----------------------------------------------------------------------===//
// icedisk_attach pragma: mount a whole icebug-disk directory as views
//===----------------------------------------------------------------------===//

struct AttachRelInfo {
	string edge_key;  // lowercase, e.g. "follows"
	string from_node; // lowercase node key, e.g. "user"
	string to_node;   // lowercase node key, e.g. "user"
};

static vector<AttachRelInfo> ParseSchemaCypher(const string &text) {
	vector<AttachRelInfo> rels;
	// CREATE REL TABLE Follows(FROM User TO User, ...); backticks optional.
	std::regex rel_re(R"(CREATE\s+REL\s+TABLE\s+`?(\w+)`?\s*\(\s*FROM\s+`?(\w+)`?\s+TO\s+`?(\w+)`?)",
	                  std::regex::icase);
	auto begin = std::sregex_iterator(text.begin(), text.end(), rel_re);
	auto end = std::sregex_iterator();
	for (auto it = begin; it != end; ++it) {
		AttachRelInfo info;
		info.edge_key = ToLowerCopy((*it)[1].str());
		info.from_node = ToLowerCopy((*it)[2].str());
		info.to_node = ToLowerCopy((*it)[3].str());
		rels.push_back(info);
	}
	return rels;
}

static string AttachDirectory(ClientContext &context, const string &dir) {
	FileSystem &fsys = FileSystem::GetFileSystem(context);
	if (!fsys.DirectoryExists(dir)) {
		throw IOException("icedisk_attach: not a directory: '" + dir + "'");
	}
	string abs_dir = dir;
	if (!fsys.IsPathAbsolute(abs_dir)) {
		abs_dir = fsys.JoinPath(FileSystem::GetWorkingDirectory(), abs_dir);
	}

	// --- discover parquet files ---
	// node key -> (view name, path); edge key -> (view suffix, indices path, indptr path)
	struct NodeEntry {
		string suffix;
		string path;
	};
	std::map<string, NodeEntry> nodes; // keyed by lowercase node key
	struct EdgeEntry {
		string suffix;
		string indices_path;
		string indptr_path;
		bool has_indices = false;
		bool has_indptr = false;
	};
	std::map<string, EdgeEntry> edges; // keyed by lowercase edge key

	for (auto &entry : fsys.Glob(fsys.JoinPath(abs_dir, "*.parquet"))) {
		string path = entry.path;
		string fname = fsys.ExtractName(path);
		string lower = ToLowerCopy(fname);
		auto strip = [&](const string &prefix, const string &suffix) -> string {
			return fname.substr(prefix.size(), fname.size() - prefix.size() - suffix.size());
		};
		if (lower.rfind("nodes_", 0) == 0 && lower.size() > 14 && lower.compare(lower.size() - 8, 8, ".parquet") == 0) {
			string suffix = strip("nodes_", ".parquet");
			nodes[ToLowerCopy(suffix)] = {suffix, path};
		} else if (lower.rfind("indices_", 0) == 0 && lower.size() > 16 &&
		           lower.compare(lower.size() - 8, 8, ".parquet") == 0) {
			string suffix = strip("indices_", ".parquet");
			auto &e = edges[ToLowerCopy(suffix)];
			e.suffix = suffix;
			e.indices_path = path;
			e.has_indices = true;
		} else if (lower.rfind("indptr_", 0) == 0 && lower.size() > 15 &&
		           lower.compare(lower.size() - 8, 8, ".parquet") == 0) {
			string suffix = strip("indptr_", ".parquet");
			auto &e = edges[ToLowerCopy(suffix)];
			e.suffix = suffix;
			e.indptr_path = path;
			e.has_indptr = true;
		}
	}

	if (nodes.empty() && edges.empty()) {
		throw IOException("icedisk_attach: no nodes_*.parquet / indices_*.parquet / indptr_*.parquet files found in '" +
		                  abs_dir + "'");
	}

	// --- schema.cypher for FROM/TO resolution ---
	std::map<string, AttachRelInfo> rel_by_edge;
	string schema_path = fsys.JoinPath(abs_dir, "schema.cypher");
	if (fsys.FileExists(schema_path)) {
		std::ifstream in(schema_path);
		std::stringstream buf;
		buf << in.rdbuf();
		for (auto &rel : ParseSchemaCypher(buf.str())) {
			rel_by_edge[rel.edge_key] = rel;
		}
	}

	idx_t views_created = 0;
	vector<string> created_views;

	// --- node views: pass-through parquet scans ---
	for (auto &kv : nodes) {
		auto &node = kv.second;
		string view = "nodes_" + node.suffix;
		string sql = "CREATE OR REPLACE VIEW " + QuoteIdent(view) + " AS SELECT * FROM read_parquet('" +
		             EscapeQuote(node.path) + "')";
		HelperExecute(context, sql);
		views_created++;
		created_views.push_back(view);
	}

	// --- edge views: CSR expansion, mapped through node tables when known ---
	string first_node_path;
	if (!nodes.empty()) {
		first_node_path = nodes.begin()->second.path;
	}
	for (auto &kv : edges) {
		auto &edge = kv.second;
		if (!edge.has_indices || !edge.has_indptr) {
			throw IOException("icedisk_attach: edge '" + edge.suffix +
			                  "' is missing its indices/indptr parquet pair in '" + abs_dir + "'");
		}
		string src_nodes_path;
		string dst_nodes_path;
		auto rel_it = rel_by_edge.find(kv.first);
		if (rel_it != rel_by_edge.end()) {
			auto src_it = nodes.find(rel_it->second.from_node);
			auto dst_it = nodes.find(rel_it->second.to_node);
			if (src_it != nodes.end()) {
				src_nodes_path = src_it->second.path;
			}
			if (dst_it != nodes.end()) {
				dst_nodes_path = dst_it->second.path;
			}
		}
		// Fallback: homogeneous mapping through the first node table so
		// structure-only directories still yield readable ids.
		if (src_nodes_path.empty() && !nodes.empty()) {
			src_nodes_path = first_node_path;
		}
		if (dst_nodes_path.empty() && !nodes.empty()) {
			dst_nodes_path = first_node_path;
		}

		string call;
		if (!src_nodes_path.empty() && !dst_nodes_path.empty()) {
			call = "icedisk_edge_scan('" + EscapeQuote(edge.indices_path) + "', '" + EscapeQuote(edge.indptr_path) +
			       "', '" + EscapeQuote(src_nodes_path) + "', '" + EscapeQuote(dst_nodes_path) + "')";
		} else {
			call =
			    "icedisk_edge_scan('" + EscapeQuote(edge.indices_path) + "', '" + EscapeQuote(edge.indptr_path) + "')";
		}
		string view = "edges_" + edge.suffix;
		HelperExecute(context, "CREATE OR REPLACE VIEW " + QuoteIdent(view) + " AS SELECT * FROM " + call);
		views_created++;
		created_views.push_back(view);
	}

	if (views_created == 0) {
		throw IOException("icedisk_attach: nothing to attach in '" + abs_dir + "'");
	}

	std::sort(created_views.begin(), created_views.end());
	string summary = "mounted " + to_string(views_created) + " views from '" + dir + "': ";
	for (idx_t i = 0; i < created_views.size(); i++) {
		if (i > 0) {
			summary += ", ";
		}
		summary += created_views[i];
	}
	return summary;
}

static void IcediskAttachPragma(ClientContext &context, const FunctionParameters &parameters) {
	AttachDirectory(context, parameters.values[0].GetValue<string>());
}

static void IcediskAttachScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	ClientContext &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t dir) {
		return StringVector::AddString(result, AttachDirectory(context, dir.GetString()));
	});
}

//===----------------------------------------------------------------------===//
// icedisk_version scalar function
//===----------------------------------------------------------------------===//

static void IcediskVersionFun(DataChunk &args, ExpressionState &state, Vector &result) {
	(void)args;
	(void)state;
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value("icedisk 0.1.0 (icebug-disk v1)"));
}

//===----------------------------------------------------------------------===//
// Extension entry point
//===----------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	TableFunctionSet edge_scan_set("icedisk_edge_scan");
	TableFunction edge_scan_dense({LogicalType::VARCHAR, LogicalType::VARCHAR}, EdgeScanFunction, EdgeScanBind,
	                              EdgeScanInit);
	TableFunction edge_scan_mapped(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, EdgeScanFunction,
	    EdgeScanBind, EdgeScanInit);
	edge_scan_set.AddFunction(edge_scan_dense);
	edge_scan_set.AddFunction(edge_scan_mapped);
	loader.RegisterFunction(edge_scan_set);

	loader.RegisterFunction(PragmaFunction::PragmaCall("icedisk_attach", IcediskAttachPragma, {LogicalType::VARCHAR}));

	auto icedisk_attach_function =
	    ScalarFunction("icedisk_attach", {LogicalType::VARCHAR}, LogicalType::VARCHAR, IcediskAttachScalar);
	loader.RegisterFunction(icedisk_attach_function);

	auto icedisk_version_function = ScalarFunction("icedisk_version", {}, LogicalType::VARCHAR, IcediskVersionFun);
	loader.RegisterFunction(icedisk_version_function);
}

void IcediskExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string IcediskExtension::Name() {
	return "icedisk";
}

std::string IcediskExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(icedisk, loader) {
	duckdb::LoadInternal(loader);
}
}
