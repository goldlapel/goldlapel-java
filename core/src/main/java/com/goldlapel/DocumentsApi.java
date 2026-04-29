package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Documents namespace API — accessible as {@code gl.documents}.
 *
 * <p>Wraps the doc-store methods in a sub-API instance held on the parent
 * {@link GoldLapel} client. The instance shares all state (license, dashboard
 * token, http session, conn, DDL pattern cache) by reference back to the
 * parent — no duplication.
 *
 * <p>The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call here:
 *
 * <ol>
 *   <li>Calls {@code POST /api/ddl/doc_store/create} (idempotent) to
 *       materialize the canonical {@code _goldlapel.doc_<name>} table and
 *       pull its query patterns.
 *   <li>Caches {@code (tables, query_patterns)} on the parent
 *       {@link GoldLapel} instance for the session's lifetime — one HTTP
 *       round-trip per (family, name) per session.
 *   <li>Hands the patterns off to the existing {@code Utils.docX} functions
 *       so they execute against the canonical table name instead of
 *       hand-writing their own DDL.
 * </ol>
 *
 * <p>Sub-API class shape mirrors {@link StreamsApi} — this is the canonical
 * pattern for the wrapper rollout. Other namespaces (search, cache, queues,
 * counters, hashes, zsets, geo, …) stay flat for now; they migrate to nested
 * form one-at-a-time as their own schema-to-core phase fires.
 */
public final class DocumentsApi {
    private final GoldLapel gl;

    DocumentsApi(GoldLapel gl) {
        // Hold a back-reference to the parent client. Never copy lifecycle
        // state (token, port, conn) onto this instance — always read through
        // `gl` so a config change on the parent (e.g. proxy restart with a
        // new dashboard token) is reflected immediately on the next call.
        this.gl = gl;
    }

    /**
     * Fetch (and cache per-instance) canonical doc-store DDL + query patterns
     * from the proxy. Cache lives on the parent {@link GoldLapel} instance.
     *
     * <p>{@code unlogged} is a creation-time option; passed only on the first
     * call for a given (family, name) since the proxy's
     * {@code CREATE TABLE IF NOT EXISTS} makes subsequent calls no-op
     * DDL-wise. If a caller flips {@code unlogged} across calls in the same
     * session, the table's storage type is whatever it was on first create —
     * wrappers don't migrate it.
     */
    public Map<String, Object> patterns(String collection) {
        return patterns(collection, false);
    }

    public Map<String, Object> patterns(String collection, boolean unlogged) {
        Utils.validateIdentifier(collection);
        String token = gl.dashboardToken() != null ? gl.dashboardToken() : Ddl.tokenFromEnvOrFile();
        Map<String, Object> options = null;
        if (unlogged) {
            options = new LinkedHashMap<>();
            options.put("unlogged", Boolean.TRUE);
        }
        return Ddl.fetchPatterns(gl.ddlCache(), "doc_store", collection,
            gl.dashboardPort(), token, options);
    }

    // -- Collection lifecycle ------------------------------------------------

    /**
     * Eagerly materialize the doc-store table. Other methods will also
     * materialize on first use, so calling this is optional — provided for
     * callers that want explicit setup at startup time.
     */
    public void createCollection(String collection) {
        patterns(collection);
    }

    public void createCollection(String collection, boolean unlogged) {
        patterns(collection, unlogged);
    }

    // -- CRUD ----------------------------------------------------------------

    public Map<String, Object> insert(String collection, String documentJson) throws SQLException {
        return Utils.docInsert(gl.resolveConn(), collection, documentJson, patterns(collection));
    }

    public Map<String, Object> insert(String collection, String documentJson, Connection conn) throws SQLException {
        return Utils.docInsert(conn, collection, documentJson, patterns(collection));
    }

    public List<Map<String, Object>> insertMany(String collection, List<String> documents) throws SQLException {
        return Utils.docInsertMany(gl.resolveConn(), collection, documents, patterns(collection));
    }

    public List<Map<String, Object>> insertMany(String collection, List<String> documents, Connection conn) throws SQLException {
        return Utils.docInsertMany(conn, collection, documents, patterns(collection));
    }

    public List<Map<String, Object>> find(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip) throws SQLException {
        return Utils.docFind(gl.resolveConn(), collection, filterJson, sortJson, limit, skip, patterns(collection));
    }

    public List<Map<String, Object>> find(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, Connection conn) throws SQLException {
        return Utils.docFind(conn, collection, filterJson, sortJson, limit, skip, patterns(collection));
    }

    public Map<String, Object> findOne(String collection, String filterJson) throws SQLException {
        return Utils.docFindOne(gl.resolveConn(), collection, filterJson, patterns(collection));
    }

    public Map<String, Object> findOne(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docFindOne(conn, collection, filterJson, patterns(collection));
    }

    public Iterator<Map<String, Object>> findCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize) throws SQLException {
        return Utils.docFindCursor(gl.resolveConn(), collection, filterJson, sortJson, limit, skip, batchSize, patterns(collection));
    }

    public Iterator<Map<String, Object>> findCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize, Connection conn) throws SQLException {
        return Utils.docFindCursor(conn, collection, filterJson, sortJson, limit, skip, batchSize, patterns(collection));
    }

    public int update(String collection, String filterJson, String updateJson) throws SQLException {
        return Utils.docUpdate(gl.resolveConn(), collection, filterJson, updateJson, patterns(collection));
    }

    public int update(String collection, String filterJson, String updateJson, Connection conn) throws SQLException {
        return Utils.docUpdate(conn, collection, filterJson, updateJson, patterns(collection));
    }

    public int updateOne(String collection, String filterJson, String updateJson) throws SQLException {
        return Utils.docUpdateOne(gl.resolveConn(), collection, filterJson, updateJson, patterns(collection));
    }

    public int updateOne(String collection, String filterJson, String updateJson, Connection conn) throws SQLException {
        return Utils.docUpdateOne(conn, collection, filterJson, updateJson, patterns(collection));
    }

    public int delete(String collection, String filterJson) throws SQLException {
        return Utils.docDelete(gl.resolveConn(), collection, filterJson, patterns(collection));
    }

    public int delete(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docDelete(conn, collection, filterJson, patterns(collection));
    }

    public int deleteOne(String collection, String filterJson) throws SQLException {
        return Utils.docDeleteOne(gl.resolveConn(), collection, filterJson, patterns(collection));
    }

    public int deleteOne(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docDeleteOne(conn, collection, filterJson, patterns(collection));
    }

    public Map<String, Object> findOneAndUpdate(String collection, String filterJson,
            String updateJson) throws SQLException {
        return Utils.docFindOneAndUpdate(gl.resolveConn(), collection, filterJson, updateJson, patterns(collection));
    }

    public Map<String, Object> findOneAndUpdate(String collection, String filterJson,
            String updateJson, Connection conn) throws SQLException {
        return Utils.docFindOneAndUpdate(conn, collection, filterJson, updateJson, patterns(collection));
    }

    public Map<String, Object> findOneAndDelete(String collection, String filterJson) throws SQLException {
        return Utils.docFindOneAndDelete(gl.resolveConn(), collection, filterJson, patterns(collection));
    }

    public Map<String, Object> findOneAndDelete(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docFindOneAndDelete(conn, collection, filterJson, patterns(collection));
    }

    public List<String> distinct(String collection, String field, String filterJson) throws SQLException {
        return Utils.docDistinct(gl.resolveConn(), collection, field, filterJson, patterns(collection));
    }

    public List<String> distinct(String collection, String field, String filterJson, Connection conn) throws SQLException {
        return Utils.docDistinct(conn, collection, field, filterJson, patterns(collection));
    }

    public long count(String collection, String filterJson) throws SQLException {
        return Utils.docCount(gl.resolveConn(), collection, filterJson, patterns(collection));
    }

    public long count(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docCount(conn, collection, filterJson, patterns(collection));
    }

    public void createIndex(String collection, List<String> keys) throws SQLException {
        Utils.docCreateIndex(gl.resolveConn(), collection, keys, patterns(collection));
    }

    public void createIndex(String collection, List<String> keys, Connection conn) throws SQLException {
        Utils.docCreateIndex(conn, collection, keys, patterns(collection));
    }

    /**
     * Run a Mongo-style aggregation pipeline.
     *
     * <p>{@code $lookup.from} references in the pipeline are resolved to
     * their canonical proxy tables ({@code _goldlapel.doc_<name>}) — each
     * unique {@code from} collection triggers an idempotent describe/create
     * against the proxy and is cached for the session.
     */
    public List<Map<String, Object>> aggregate(String collection, String pipelineJson) throws SQLException {
        return aggregate(collection, pipelineJson, gl.resolveConn());
    }

    public List<Map<String, Object>> aggregate(String collection, String pipelineJson, Connection conn) throws SQLException {
        Map<String, Object> srcPatterns = patterns(collection);
        Map<String, String> lookupTables = Utils.resolveLookupTables(this, pipelineJson);
        return Utils.docAggregate(conn, collection, pipelineJson, srcPatterns, lookupTables);
    }

    // -- Watch / TTL / capped ------------------------------------------------

    public Thread watch(String collection, BiConsumer<String, String> callback) throws SQLException {
        return Utils.docWatch(gl.resolveConn(), collection, callback, patterns(collection));
    }

    public Thread watch(String collection, BiConsumer<String, String> callback, Connection conn) throws SQLException {
        return Utils.docWatch(conn, collection, callback, patterns(collection));
    }

    public void unwatch(String collection) throws SQLException {
        Utils.docUnwatch(gl.resolveConn(), collection, patterns(collection));
    }

    public void unwatch(String collection, Connection conn) throws SQLException {
        Utils.docUnwatch(conn, collection, patterns(collection));
    }

    public void createTtlIndex(String collection, int expireAfterSeconds) throws SQLException {
        Utils.docCreateTtlIndex(gl.resolveConn(), collection, expireAfterSeconds, patterns(collection));
    }

    public void createTtlIndex(String collection, int expireAfterSeconds, Connection conn) throws SQLException {
        Utils.docCreateTtlIndex(conn, collection, expireAfterSeconds, patterns(collection));
    }

    public void createTtlIndex(String collection, int expireAfterSeconds, String field) throws SQLException {
        Utils.docCreateTtlIndex(gl.resolveConn(), collection, expireAfterSeconds, field, patterns(collection));
    }

    public void createTtlIndex(String collection, int expireAfterSeconds, String field, Connection conn) throws SQLException {
        Utils.docCreateTtlIndex(conn, collection, expireAfterSeconds, field, patterns(collection));
    }

    public void removeTtlIndex(String collection) throws SQLException {
        Utils.docRemoveTtlIndex(gl.resolveConn(), collection, patterns(collection));
    }

    public void removeTtlIndex(String collection, Connection conn) throws SQLException {
        Utils.docRemoveTtlIndex(conn, collection, patterns(collection));
    }

    public void createCapped(String collection, int maxDocuments) throws SQLException {
        Utils.docCreateCapped(gl.resolveConn(), collection, maxDocuments, patterns(collection));
    }

    public void createCapped(String collection, int maxDocuments, Connection conn) throws SQLException {
        Utils.docCreateCapped(conn, collection, maxDocuments, patterns(collection));
    }

    public void removeCap(String collection) throws SQLException {
        Utils.docRemoveCap(gl.resolveConn(), collection, patterns(collection));
    }

    public void removeCap(String collection, Connection conn) throws SQLException {
        Utils.docRemoveCap(conn, collection, patterns(collection));
    }
}
