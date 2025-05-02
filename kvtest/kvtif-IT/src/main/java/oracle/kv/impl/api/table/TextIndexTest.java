/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import oracle.kv.MetadataNotFoundException;
import oracle.kv.TestBase;
import oracle.kv.Version;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.tif.ElasticsearchHandler;
import oracle.kv.impl.tif.TextIndexFeeder;
import oracle.kv.impl.tif.esclient.esRequest.GetIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexDocumentRequest;
import oracle.kv.impl.tif.esclient.esRequest.PutMappingRequest;
import oracle.kv.impl.tif.esclient.esRequest.SearchRequest;
import oracle.kv.impl.tif.esclient.esResponse.IndexDocumentResponse;
import oracle.kv.impl.tif.esclient.esResponse.PutMappingResponse;
import oracle.kv.impl.tif.esclient.esResponse.SearchResponse;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;
import oracle.kv.impl.tif.esclient.restClient.ESAdminClient;
import oracle.kv.impl.tif.esclient.restClient.ESDMLClient;
import oracle.kv.impl.tif.esclient.restClient.ESRestClient;
import oracle.kv.impl.util.SecurityConfigTestBase;
import oracle.kv.impl.util.TestPasswordReader;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.xregion.XRegionTestBase.ReqRespThread;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.Index;
import oracle.kv.table.Index.IndexType;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import com.fasterxml.jackson.core.JsonGenerator;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextIndexTest extends TableTestBase {

    private volatile int esHttpPort;
    private static final String esClusterName = "tif-test-es-cluster";
    private ElasticsearchHandler esHandler;
    private ESRestClient restClient;
    /* This is defined here for access by static methods */
    private static final String namespace = "es";


    private final String table = "Joke";
    private final String childTable = table + ".JokeRemarks";
    private final String textIndex = "JokeIndex";
    private final String textIndexOnChildTable = "JokeRemarkIndex";
    private final String wildcard = "*";

    private final boolean traceOnConsole = false;

    @Override
    protected String getNamespace() {
        return namespace;
    }

    @BeforeClass
    public static void staticSetUp() throws Exception {
        Assume.assumeFalse(
            "FTS is not currently compatible with security",
            SECURITY_ENABLE);
        Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
        Assume.assumeTrue("Skipping test suite due to missing http port",
            System.getProperty("es.http.port") != null);
        /* Need a larger heap size for these tests */
        snMemoryMB = (int) (SN_MEMORY_MB_DEFAULT * 1.5);
        TableTestBase.staticSetUp();
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        snMemoryMB = SN_MEMORY_MB_DEFAULT;
        TableTestBase.staticTearDown();
    }

    @Override
    public void setUp() throws Exception {

        long start = System.currentTimeMillis();


        /* Elasticsearch logging is noisy by default on stdout; just disable it
         * unless debugging.
         */
        org.apache.log4j.Logger log4jRoot =
            org.apache.log4j.Logger.getRootLogger();
        log4jRoot.removeAllAppenders();
        log4jRoot.addAppender
            (org.apache.log4j.varia.NullAppender.getNullAppender());

        esHttpPort = Integer.parseInt(System.getProperty("es.http.port"));

        restartStore();

        Thread.sleep(1000);
        String esMembers = "localhost:" + esHttpPort;

        esHandler =
            ElasticsearchHandler.newInstance(esClusterName, esMembers, false,
                                             null, 5000, logger);

        restClient = esHandler.getEsRestClient();

        if (reqRespThread != null) {
            reqRespThread.stopResResp();
        }
        if (TestBase.mrTableMode) {
            /**
             * start a mock service so "creat table" DDLs for MRTables
             * can finish*/
            reqRespThread =
                new ReqRespThread(store, null, false);
            reqRespThread.start();
        }

        trace(getTime() + ": finish setup in " +
              (System.currentTimeMillis() - start) / 1000 + " secs");

    }

    @Override
    public void tearDown() throws Exception {
        long start = System.currentTimeMillis();
        super.tearDown();
        if (esHandler != null) {
            esHandler.close();
        }
        esHandler = null;
        restClient = null;
        trace(getTime() + ": finish tear down in " +
              (System.currentTimeMillis() - start) / 1000 + " secs");
    }

    /* Test creating text indices on table */
    @Test
    public void testCreateTextIndex () throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*Not secure*/ false,
             /* force clear to ensure a clean start */
             true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        TableAPI tableAPI = store.getTableAPI();

        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl
            ("CREATE TABLE " + table + " " +
             "(id INTEGER, category STRING, txt STRING, PRIMARY KEY (id))",
             true, true/*noMRTableMode*/);

        /* Ensure that the previous create has completed.  See [#25187] */
        tableAPI.getTable(getNamespace(), table);
        /* Add a child table, to verify indexing works on it. */
        executeDdl
            ("CREATE TABLE " + childTable + " " +
             "(remarkId INTEGER, remark STRING, PRIMARY KEY (remarkId))",
             true, true/*noMRTableMode*/);

        /* put a row in before creating the index, to show that the index is
         * populated from existing rows. */
        writeJokeRow(tableAPI, 1, "pun",
                     "You can tune a piano but you can\'t tune a fish, " +
                     "unless you play bass");

        executeDdl
            ("CREATE FULLTEXT INDEX " + textIndex +
             " ON " + table + " (category, txt)");

        executeDdl
            ("CREATE FULLTEXT INDEX " + textIndexOnChildTable +
             " ON " + childTable + " (remark)");

        /* put a couple more rows after creating the index, to show that newly
         * added rows are indexed. */
        writeJokeRow(tableAPI, 2, "self-referential",
                     "Is it solipsistic in here, or is it just me?");
        writeJokeRow(tableAPI, 3, "stupid",
                     "Why is six afraid of seven?  Because seven ate nine");


        /* add some commentary on Joke #3, to test viability of text indexes on
         * child tables.
         */
        writeRemarkRow(tableAPI, 3, 1, "Heard that one before");
        writeRemarkRow(tableAPI, 3, 2,
                        "Wrong, it's because seven is prime, " +
                        "and primes can be intimidating.");

        /* Now query ES to see if the jokes are indexed.  Should be 3 Jokes + 2
         * remarks.
         */
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + textIndex,
                     3, /* 3 rows inserted */
                     getTotalHits(table, textIndex, 3, 10000));

        assertEquals("Unexpected count of indexed documents for table " +
                     childTable + ", text index " + textIndexOnChildTable,
                     2, /* 2 rows inserted in child table */
                     getTotalHits(childTable, textIndexOnChildTable, 2, 10000));

        /* load 997 dummy jokes (id 4 - 1000) */
        writeDummyJokes(tableAPI, 4, 1000);

        /* Final count should be 1000 jokes including 997 dummy jokes. */
        assertEquals("Unexpected count of indexed documents",
                     1000, /* 3 jokes + 997 dummy jokes */
                     getTotalHits(table, textIndex, 1000, 100000));

        /* no change to child table and its text index */
        assertEquals("Unexpected count of indexed documents for table " +
                     childTable + ", text index " + textIndexOnChildTable,
                     2, /* 2 rows inserted in child table */
                     getTotalHits(childTable, textIndexOnChildTable, 2, 10000));

        /* Test correct restarting of indexing after re-creation of an index. */
        executeDdl("DROP INDEX " + textIndex + " ON " + table);
        executeDdl
            ("CREATE FULLTEXT INDEX " + textIndex +
             " ON " + table + " (category, txt)");

        /* Final count should be 1000 jokes in the new index */
        assertEquals("Unexpected count of indexed documents",
                     1000, /* 3 jokes + 997 dummy jokes */
                     getTotalHits(table, textIndex, 1000, 100000));

        /* Also re-check the child table */
        assertEquals("Unexpected count of indexed documents for table " +
                     childTable + ", text index " + textIndexOnChildTable,
                     2, /* 2 rows inserted in child table */
                     getTotalHits(childTable, textIndexOnChildTable, 2, 10000));

        /* Now delete a couple of records */

        deleteJokeRow(tableAPI, 1);
        deleteJokeRow(tableAPI, 2);

        /* Final count should be 998 jokes after deleting 2 */
        assertEquals("Unexpected count of indexed documents",
                     998, /* 1 joke + 997 dummy jokes */
                     getTotalHits(table, textIndex, 998, 100000));
    }


    /**
     * Test using DROP INDEX to drop a text index as well as its ES index. It
     * does follows:
     *
     * 1. Create two tables with few rows
     * 2. Create two text indices on each of table
     * 3. Verify two text indices
     * 4. Drop one text index
     * 5. Verify dropped index is gone, the other stays
     * 6. Drop the other text index
     * 7. Verify both text indices are gone
     */
    @Test
    public void testDropTextIndex () throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*Not secure*/ false,
             /* force clear to ensure a clean start */
             true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        TableAPI tableAPI = store.getTableAPI();

        /* create two tables */
        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, category STRING, txt STRING," +
                   "PRIMARY KEY (id))", true, true/*noMRTableMode*/);
        /* Ensure that the previous create has completed.  See [#25187] */
        tableAPI.getTable(getNamespace(), table);
        executeDdl("CREATE TABLE " + childTable + " " +
                   "(remarkId INTEGER, remark STRING, PRIMARY KEY (remarkId))",
                   true, true/*noMRTableMode*/);

        /* put few rows in the tables */
        writeJokeRow(tableAPI, 1, "pun",
                     "You can tune a piano but you can\'t tune a fish, " +
                     "unless you play bass");
        writeJokeRow(tableAPI, 2, "self-referential",
                     "Is it solipsistic in here, or is it just me?");
        writeJokeRow(tableAPI, 3, "stupid",
                     "Why is six afraid of seven?  Because seven ate nine");
        writeRemarkRow(tableAPI, 3, 1, "Heard that one before");
        writeRemarkRow(tableAPI, 3, 2,
                       "Wrong, it's because seven is prime, " +
                       "and primes can be intimidating.");

        /* create two text indices */
        executeDdl("CREATE FULLTEXT INDEX " + textIndex +
                   " ON " + table + " (category, txt)");
        executeDdl("CREATE FULLTEXT INDEX " + textIndexOnChildTable +
                   " ON " + childTable + " (remark)");


        /* Now query ES to verify text index */
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + textIndex,
                     3, /* 3 rows inserted */
                     getTotalHits(table, textIndex, 3, 10000));
        assertEquals("Unexpected count of indexed documents for table " +
                     childTable + ", text index " + textIndexOnChildTable,
                     2, /* 2 rows inserted in child table */
                     getTotalHits(childTable, textIndexOnChildTable, 2, 10000));


        /* now drop the text index on parent table! */
        executeDdl("DROP INDEX " + textIndex + " ON " + table);

        /* verify that the ES index has been deleted */
        String esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              namespace, table, textIndex);
        assertFalse("ES index " + esIndexName + " should be gone",
                    ElasticsearchHandler.existESIndex(esIndexName,
                    		restClient.admin()));
        /* but the other text index on child table should still be there */
        esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              namespace,
                                              childTable,
                                              textIndexOnChildTable);
        assertTrue("ES index " + esIndexName + " should stay",
                   ElasticsearchHandler.existESIndex(esIndexName,
                                                     restClient.admin()));


        /* finally drop the remaining text index on child table */
        executeDdl("DROP INDEX " + textIndexOnChildTable +
                   " ON " + childTable);
        /* verify it is gone */
        assertFalse("ES index " + esIndexName + " should be gone",
                    ElasticsearchHandler.existESIndex(esIndexName,
                    		restClient.admin()));
    }

    /**
     * Test text index and secondary index can both defined on the same
     * columns, instead of incorrectly being treated as duplicates.
     */
    @Test
    public void testTextIndexOnSameFieldsWithSecondaryIndex ()
        throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*secure ES*/ false,
             /* force clear to ensure a clean start */
             true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        TableAPI tableAPI = store.getTableAPI();

        final String secondaryIndex = "JokeSecondaryIndex";
        final String fields = "(category, txt)";

        /* create a table */
        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, category STRING, txt STRING," +
                   " PRIMARY KEY (id))");
        /* Ensure that the previous create has completed.  See [#25187] */
        tableAPI.getTable(getNamespace(), table);

        /* create text and secondary index on the same columns */
        executeDdl("CREATE FULLTEXT INDEX " + textIndex + " ON " + table + " " +
                   fields);

        /* if SR [#24923] is not fixed, exception would be raised here */
        executeDdl("CREATE INDEX " + secondaryIndex + " ON " + table +
                   " " + fields);

        TableImpl t = getTable(getNamespace(), table);
        roundTripTable(t);

        Index indexResult = t.getTextIndex(textIndex);
        assertEquals(textIndex, indexResult.getName());

        try {
            t.getTextIndex(secondaryIndex);
            fail("Specified index is not a text index so throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
    }

    /**
     * Test any stale ES index should be deleted before a new text index and
     * its corresponding ES index is created.
     */
    @Test
    public void testDelStaleESIndex () throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*secure*/ false,
             /* force clear to ensure a clean start */
             true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        TableAPI tableAPI = store.getTableAPI();

        final ESAdminClient esAdminClient = restClient.admin();
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              namespace,
                                              table, textIndex);

        /* create a stale index with random data */
        ElasticsearchHandler.createESIndex(esIndexName, esAdminClient);
        assertTrue("Stale ES index " + esIndexName + " should exist.",
                   (ElasticsearchHandler.existESIndex(esIndexName,
                                                      esAdminClient)));

        createESIndexMapping(esIndexName, esAdminClient);
        indexOneDoc(esIndexName, restClient.dml());

        /* verify we have a doc in stale index */
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + textIndex,
                     1,
                     getTotalHits(table, textIndex, 1, 10000));

        /* create a table and new text index */
        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, category STRING, txt STRING," +
                   " PRIMARY KEY (id))");
        /* Ensure that the previous create has completed.  See [#25187] */
        tableAPI.getTable(getNamespace(), table);
        executeDdl("CREATE FULLTEXT INDEX " + textIndex +
                   " ON " + table + " (category, txt) OVERRIDE");

        /* verify we have a empty new es index without any data */
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + textIndex,
                     0,   /* no doc should be found in new es index */
                     getTotalHits(table, textIndex, 0, 10000));

        /* put few rows in the tables */
        writeJokeRow(tableAPI, 1, "pun",
                     "You can tune a piano but you can\'t tune a fish, " +
                     "unless you play bass");
        writeJokeRow(tableAPI, 2, "self-referential",
                     "Is it solipsistic in here, or is it just me?");
        writeJokeRow(tableAPI, 3, "stupid",
                     "Why is six afraid of seven?  Because seven ate nine");

        /* Now query ES to verify text index */
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + textIndex,
                     3, /* 3 rows inserted */
                     getTotalHits(table, textIndex, 3, 10000));
    }

    /**
     * Tests for text-indexing non-scalar types.
     */
    @Test
    public void testComplexTypes () throws Exception {

        final int timeOutMs = 120000;

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,/*not secure*/ false, true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        TableAPI tableAPI = store.getTableAPI();

        final String arrayOfScalarIndex = "categoryIndex";
        final String arrayOfRecordIndex = "historyIndex";
        final String recordIndex = "totalsIndex";
        final String mapKeyIndex = "remarkAuthorsIndex";
        final String mapValIndex = "remarkIndex";
        final String mapSpecificKeyIndex = "guysRemarksIndex";

        /* Create a table with a mix of complex types: records, arrays, maps */
        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, txt STRING, category ARRAY(STRING), " +
                   " remarks MAP(STRING)," +
                   " history ARRAY(RECORD(views integer, edits INTEGER))," +
                   " totals RECORD(chars integer, words integer)," +
                   " PRIMARY KEY (id))");
        /* Ensure that the previous create has completed.  See [#25187] */
        tableAPI.getTable(getNamespace(), table);

        /* Since we are creating several indexes here, piggyback a test for
         * index properties (ES_SHARDS and ES_REPLICAS).
         *
         * The first index also exercises allowing multiple multi-key fields in
         * a single index.  This is not allowed for secondaries, but is allowed
         * for text indexes. [#24966]
         */
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfScalarIndex +
                   " ON " + table +
                   " (category[], remarks.keys(), totals.words) ES_SHARDS=5");
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfRecordIndex +
                   " ON " + table +
                   " (history[].views) ES_SHARDS=1 ES_REPLICAS=0");
        executeDdl("CREATE FULLTEXT INDEX " + recordIndex +
                   " ON " + table + " (totals.words) ES_SHARDS=2");
        executeDdl("CREATE FULLTEXT INDEX " + mapKeyIndex + " ON " + table +
                   " (remarks.keys()) ES_SHARDS=5 ES_REPLICAS = 0");
        executeDdl("CREATE FULLTEXT INDEX " + mapValIndex +
                   " ON " + table + " (remarks.values()) ES_SHARDS=5");
        executeDdl("CREATE FULLTEXT INDEX " + mapSpecificKeyIndex +
                   " ON " + table + " (remarks.guy) ES_SHARDS=5");

        Table t = tableAPI.getTable(getNamespace(), table);

        roundTripTable((TableImpl)t);

        Row row = t.createRow();

        row.put("id", 1);
        row.put("txt", "Pete and re-Pete were walking down the street." +
                " Pete fell down.  Who was left? [REPEAT]");
        row.putArray("category").add("pedro-repedro").add("cyclic");
        row.putMap("remarks").
            put("notguy", "Humor quotient of zero").
            put("soupy", "HAHAHA");
        row.putArray("history").addRecord().put("views", 23).put("edits", 7);
        row.putRecord("totals").put("chars", 192).put("words", 15);

        tableAPI.put(row, null, null);

        row = t.createRow();

        row.put("id", 2);
        row.put("txt", "Often, Bach and Offenbach walked down the street." +
                " Bach would write a fugue.  Who was left? [OFTEN BACH]");
        row.putArray("category").add("bach-offenbach").add("nonsensical");
        row.putMap("remarks").
            put("guy", "unsuccessful variation on pedro-repedro").
            put("soupy", "HEEHEEHEE");
        row.putArray("history").addRecord().put("views", 9).put("edits", 127);
        row.putRecord("totals").put("chars", 97).put("words", 20);

        tableAPI.put(row, null, null);

        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + arrayOfScalarIndex,
                     2,
                     getTotalHits(table, arrayOfScalarIndex, 2, timeOutMs));
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + arrayOfRecordIndex,
                     2,
                     getTotalHits(table, arrayOfRecordIndex, 2, timeOutMs));
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + recordIndex,
                     2,
                     getTotalHits(table, recordIndex, 2, timeOutMs));
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + mapKeyIndex,
                     2,
                     getTotalHits(table, mapKeyIndex, 2, timeOutMs));
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + mapValIndex,
                     2,
                     getTotalHits(table, mapValIndex, 2, timeOutMs));
        /* For KVSTORE-959 fix, search for specific keyword in mapValIndx. */
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + mapValIndex,
                     1,
                     getTotalHits(table, mapValIndex, 1, timeOutMs,
                                  HitsPrecision.EXACTLY, "HAHAHA"));
        assertEquals("Unexpected count of indexed documents for table " +
                     table + ", text index " + mapSpecificKeyIndex,
                     1, /* Only one remark will match key value of "guy" */
                     getTotalHits(table, mapSpecificKeyIndex, 1, timeOutMs));

        /* Check the index properties. */
        verifyIndexProperties(table, arrayOfScalarIndex, 5, 0);
        verifyIndexProperties(table, arrayOfRecordIndex, 1, 0);
        verifyIndexProperties(table, recordIndex, 2, 0);
        verifyIndexProperties(table, mapKeyIndex, 5, 0);
        verifyIndexProperties(table, mapValIndex, 5, 0);
        verifyIndexProperties(table, mapSpecificKeyIndex, 5, 0);
    }

    /**
     * Tests for text-indexing timestamp type.
     */
    @Test
    public void testTimestampType() throws Exception {

        final int timeOutMs = 600000;

        CommandServiceAPI cs = createStore.getAdmin();


        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,/*not secure*/false, true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        TableAPI tableAPI = store.getTableAPI();

        final String ts0ts3Index = "ts0ts3Index";
        final String ts3ts9Index = "ts3ts9Index";
        final String arrayOfTs3Index = "arrayOfTs3Index";
        final String arrayOfTs6Index = "arrayOfTs6Index";
        final String arrayOfRecTs2Index = "arrayOfRecTs2Index";
        final String arrayOfRecTs5Index = "arrayOfRecTs5Index";
        final String mapOfTs1Index = "mapOfTs1Index";
        final String mapOfTs7Index = "mapOfTs7Index";
        final String recordOfTs0Ts9Index = "recordOfTs0Ts9Index";
        final String arrayOfTs6_Ts0_recTs9Index = "arrayOfTs6_Ts0_recTs9Index";
        final String mapOfTs7_recTs0_recTs9Index = "mapOfTs7_recTs0_recTs9Index";

        /*
         * Create a table that includes simple Timestamp type or complex type
         * with nested Timestamp type.
         */
        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), " +
                   " ts9 TIMESTAMP(9), " +
                   " arrayOfTs3 ARRAY(TIMESTAMP(3)), " +
                   " arrayOfTs6 ARRAY(TIMESTAMP(6)), " +
                   " arrayOfRecordTs2Ts5 " +
                   "ARRAY(RECORD(ts2 TIMESTAMP(2), ts5 TIMESTAMP(5))), " +
                   " mapOfTs1 MAP(TIMESTAMP(0)), " +
                   " mapOfTs7 MAP(TIMESTAMP(7)), " +
                   " rec RECORD(ts0 TIMESTAMP(0), ts9 TIMESTAMP(9)), " +
                   " PRIMARY KEY (shard(id), ts3, ts9))");
        tableAPI.getTable(getNamespace(), table);
        String settings = " ES_SHARDS=1 ES_REPLICAS=1";
        executeDdl("CREATE FULLTEXT INDEX " + ts0ts3Index +
                    " ON " + table + " (ts0, ts3)" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + ts3ts9Index +
                   " ON " + table + " (ts3, ts9)" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfTs3Index +
                   " ON " + table + " (arrayOfTs3[])" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfTs6Index +
                   " ON " + table + " (arrayOfTs6[])" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfRecTs2Index +
                   " ON " + table + " (arrayOfRecordTs2Ts5[].ts2)" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfRecTs5Index +
                   " ON " + table + " (arrayOfRecordTs2Ts5[].ts5)"+ settings);
        executeDdl("CREATE FULLTEXT INDEX " + mapOfTs1Index +
                   " ON " + table + " (mapOfTs1.values())" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + mapOfTs7Index +
                   " ON " + table + " (mapOfTs7.values())" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + recordOfTs0Ts9Index +
                   " ON " + table + " (rec.ts0, rec.ts9)" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + arrayOfTs6_Ts0_recTs9Index +
                   " ON " + table + " (arrayOfTs6[], ts0, rec.ts9)" + settings);
        executeDdl("CREATE FULLTEXT INDEX " + mapOfTs7_recTs0_recTs9Index +
                   " ON " + table +
                   " (mapOfTs7.values(), rec.ts0, rec.ts9, id)" + settings);

        Table t = tableAPI.getTable(getNamespace(), table);

        roundTripTable((TableImpl)t);

        final Timestamp[] values = new Timestamp[] {
            new Timestamp(0),
            TimestampUtils.parseString("2016-08-19T15:43:27.987654321"),
            TimestampUtils.parseString("-2016-08-19T15:43:27.987654321"),
            TimestampDefImpl.MAX_VALUE,
            TimestampDefImpl.MIN_VALUE
        };

        for (int i = 0; i < values.length; i++) {
            Timestamp ts = values[i];
            Row row = t.createRow();
            row.put("id", i + 1);
            row.put("ts0", ts);
            row.put("ts3", ts);
            row.put("ts9", ts);
            row.putArray("arrayOfTs3")
                .add(ts)
                .add(TimestampDefImpl.MAX_VALUE)
                .add(TimestampDefImpl.MIN_VALUE);
            row.putArray("arrayOfTs6")
                .add(ts)
                .add(TimestampDefImpl.MAX_VALUE)
                .add(TimestampDefImpl.MIN_VALUE);
            ArrayValue av = row.putArray("arrayOfRecordTs2Ts5");
            av.addRecord().put("ts2", ts).put("ts5", ts);
            av.addRecord().put("ts2", TimestampDefImpl.MAX_VALUE)
                          .put("ts5", TimestampDefImpl.MIN_VALUE);
            row.putMap("mapOfTs1")
                .put("now", ts)
                .put("min", TimestampDefImpl.MIN_VALUE)
                .put("max", TimestampDefImpl.MAX_VALUE);
            row.putMap("mapOfTs7")
                .put("now", ts)
                .put("min", TimestampDefImpl.MIN_VALUE)
                .put("max", TimestampDefImpl.MAX_VALUE);
            row.putRecord("rec").put("ts0", ts).put("ts9", ts);

            Version v = tableAPI.put(row, null, null);
            assertTrue("Failed to put row: " + row.toJsonString(false),
                       v != null);
        }

        Set<String> indexes = t.getIndexes(IndexType.TEXT).keySet();
        assertEquals("Unexpected count of text indexes for table " + table,
                     11, indexes.size());

        for (String index : indexes) {
            assertEquals("Unexpected count of indexed documents for table " +
                         table + ", text index " + index,
                         values.length,
                         getTotalHits(table, index, values.length, timeOutMs));
            /* Check the index properties. */
            verifyIndexProperties(table, index, 1, 1);
        }
    }

    @Test
    public void testSecureStore() throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*not secure */ false,
             /* force clear to ensure a clean start */
             true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        final SecurityConfigAdd configAdd =
            new SecurityConfigAdd(createStore.getRootDir());
        configAdd.testAddWithErrors();
    }

    /**
     * Test for dropping an index while continuing to populate its table;
     * exercises coordination between TransactionAgenda and ES index deletion.
     */
    @Test
    public void dropIndexDuringPopulation () throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();

        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*not secure*/ false,
             true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        final TableAPI tableAPI = store.getTableAPI();

        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, category STRING, txt STRING," +
                   "PRIMARY KEY (id))");

        executeDdl("CREATE FULLTEXT INDEX " + textIndex +
                   " ON " + table + " (category, txt)");

        /* Run a thread to populate the table for a while.  It will keep going
         * until keepPopulating is false
         */
        keepPopulating = true;
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    int start = 0;
                    while(keepPopulating) {
                        int end = start + 1000;
                        writeDummyJokes(tableAPI, start, end);
                        start += end + 1;
                    }
                } catch (MetadataNotFoundException e) {
                    /* This happens if we delete the table while a write
                     * operation is pending.  It's OK, the test is over by then.
                     */
                }
            }
        };
        t.start();

        /* Wait until we have at least 3000 documents showing in the index */
        int currentTotal = getTotalHitsAtLeast(table, textIndex, 3000, 100000);
        trace("currentTotal = " + currentTotal);

        /* now drop the text index while the population continues. */
        executeDdl("DROP INDEX " + textIndex + " ON " + table);

        /* Wait for at least one TransactionAgenda flush cycle. */
        Thread.sleep(10000);

        /* verify that the ES index has not come back */
        String esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              namespace, table, textIndex);
        /* This fails without the fix to [#25253] */
        assertFalse("ES index " + esIndexName + " should be gone",
                    ElasticsearchHandler.existESIndex(esIndexName,
                                                      restClient.admin()));

        keepPopulating = false;
        executeDdl("DROP TABLE " + table);
    }

    @Test
    public void testCreateTextIndexIfNotExists()
        throws RemoteException {

        CommandServiceAPI cs = createStore.getAdmin();

        /* Inform the store about the ES instance. */
        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort, /*not secure*/ false,true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        executeDdl("CREATE TABLE " + table + " (id INTEGER, txt STRING, " +
                   "PRIMARY KEY (id))");

        executeDdl("CREATE FULLTEXT INDEX idxTxt ON " + table + " (txt)",
                   namespace);
        executeDdl("CREATE FULLTEXT INDEX IF NOT EXISTS idxTxt ON " +
                   table + " (txt)");

        executeDdl("CREATE FULLTEXT INDEX idxIdTxt ON " + table +
                   " (id, txt) ES_SHARDS=2");
        executeDdl("CREATE FULLTEXT INDEX IF NOT EXISTS idxIdTxt ON " + table +
                   " (id, txt) ES_SHARDS=2");
    }

    volatile boolean keepPopulating;

    private enum HitsPrecision {
        EXACTLY, ATLEAST
    }

    private int getTotalHits(String tableName,
                             String textIndexName,
                             int expectedHits,
                             long timeoutMillis)
        throws InterruptedException {

        return getTotalHits(tableName, textIndexName,
                            expectedHits, timeoutMillis, HitsPrecision.EXACTLY,
                            wildcard);
    }

    private int getTotalHitsAtLeast(String tableName,
                                    String textIndexName,
                                    int expectedHits,
                                    long timeoutMillis)
        throws InterruptedException {

        return getTotalHits(tableName, textIndexName,
                            expectedHits, timeoutMillis, HitsPrecision.ATLEAST,
                            wildcard);
    }


    /*
     * Repeatedly check total hits from an unconditional ES search of JokeIndex
     * until it reaches the expected value, or until the timeout expires.
     */
    private int getTotalHits(String tableName,
                             String textIndexName,
                             int expectedHits,
                             long timeoutMillis,
                             HitsPrecision precision,
                             String keyword)
        throws InterruptedException {

        final long startTime = System.currentTimeMillis();
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              namespace,
                                              tableName,
                                              textIndexName);
        Integer totalHits = 0;

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            /* Refresh the search engine to ensure all indexed items are
             * available to search. */
            try {
                restClient.dml().refresh(esIndexName);
            } catch (IOException e1) {
                /*
                 * It's OK if refresh failed.
                 */
            }
            Thread.sleep(500);

            SearchRequest searchRequest = new SearchRequest(
                esIndexName, keyword);

            SearchResponse response = null;
			try {
				response = restClient.dml().search(searchRequest);
				//System.out.println("Search Response:" + response);
			} catch (IOException e) {
				fail(e.getMessage());
			}
			if( response != null ) {
                totalHits = response.hits().totalValue();
            }

            switch (precision) {
            case EXACTLY:
                if (totalHits == expectedHits) {
                    return totalHits;
                }
                break;
            case ATLEAST:
                if (totalHits >= expectedHits) {
                    return totalHits;
                }
            }
            trace("Re-trying search after " +
                  (System.currentTimeMillis() - startTime) +
                  "ms, totalHits = " + totalHits);
        }
        fail("Timed out waiting " + timeoutMillis +
             "ms for ES to catch up, totalHits = " + totalHits +
             ", expected " + expectedHits);
        return 0;
    }

    private void verifyIndexProperties(String tableName,
                                       String indexName,
                                       int expectedShards,
                                       int expectedReplicas) throws IOException {
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              getNamespace(),
                                              tableName,
                                              indexName);

        GetIndexRequest req = new GetIndexRequest(esIndexName);
        Map<String,Object> settings =
        		restClient.admin().getIndexSettings(req)
        		.settings();
        assertEquals(settings.get("number_of_shards"),
                     Integer.toString(expectedShards));
        assertEquals(settings.get("number_of_replicas"),
                     Integer.toString(expectedReplicas));
    }

    private void writeDummyJokes(TableAPI t, int startId, int endId) {

        for (int i = startId; i <= endId; i++) {
            String category = "category" + UUID.randomUUID();
            String txt = "text-" + UUID.randomUUID() + ", " +
                UUID.randomUUID() + ", " + UUID.randomUUID();
            writeJokeRow(t, i, category, txt);
        }
    }

    private static void writeJokeRow(TableAPI t, int id,
                                     String category, String txt) {
        Table jokeTable = t.getTable(namespace, "Joke");

        Row row = jokeTable.createRow();

        row.put("id", id);
        row.put("category", category);
        row.put("txt", txt);

        t.put(row, null, null);
    }

    private static void deleteJokeRow(TableAPI t, int id) {
        Table jokeTable = t.getTable(namespace, "Joke");
        PrimaryKey pkey = jokeTable.createPrimaryKey();
        pkey.put("id", id);
        t.delete(pkey, null, null);
    }

    private static void writeRemarkRow(TableAPI t, int jokeid, int remarkid,
                                       String remark) {
        Table remarkTable = t.getTable(namespace, "Joke.JokeRemarks");

        Row row = remarkTable.createRow();

        row.put("id", jokeid);
        row.put("remarkId", remarkid);
        row.put("remark", remark);

        t.put(row, null, null);
    }


    /* Creates a JSON to describe an ES type mapping */
    private JsonGenerator buildTestMappingSpec() {

        	ByteArrayOutputStream baos = new ByteArrayOutputStream();

    		try {
    			ESJsonBuilder mapping = new ESJsonBuilder(baos);
    			mapping.startStructure()       //mapping start
    			.field("dynamic","false")
    			.startStructure("properties"); // properties start

                HashSet<String> allFields = new HashSet<>();
                allFields.add("name");

                for (String key : allFields) {
                    mapping.startStructure(key)
                        .field("index", "false")
                        .field("type", "text")
                        .endStructure();
                }
                mapping.endStructure(); //properties end
                mapping.endStructure(); //mapping end

                return mapping.jsonGenarator();


        } catch (IOException e) {
            throw new IllegalStateException
                ("Unable to serialize ES mapping", e);
        }
    }

    private void createESIndexMapping(String esIndexName,
                                      ESAdminClient esAdminClient)
        throws IllegalStateException {

        final JsonGenerator mappingSpec = buildTestMappingSpec();

        try {
        PutMappingRequest putMappingRequest =
        		new PutMappingRequest(esIndexName, mappingSpec);

        PutMappingResponse putMappingResponse =
            esAdminClient.createMapping(putMappingRequest);

        if (!putMappingResponse.isAcknowledged()) {
            String msg = "Cannot install ES mapping for ES index " +
                         esIndexName;
            throw new IllegalStateException(msg);
        }

        } catch(IOException ioe) {
        	throw new IllegalStateException(ioe);
        }
    }

    private IndexDocumentResponse indexOneDoc(String esIndexName,
                                      ESDMLClient esClient) throws IOException {

        final String doc =
            "{\"_pkey\":{\"_table\":\"Joke\",\"id\":\"1\"}, " +
            "\"name\":\"oracle\"}";

        IndexDocumentRequest indexReq =
        		new IndexDocumentRequest(esIndexName, "1");
        indexReq.source(doc);
        return esClient.index(indexReq);

    }

    private String getTime() {
        final long timeStamp = System.currentTimeMillis();
        SimpleDateFormat df =
            new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
        Date date = new Date(timeStamp);
        return  df.format(date);
    }

    private void trace(final String msg) {
       if (traceOnConsole) {
            System.out.println(msg);
        } else {
            logger.info(msg);
        }
    }

    class SecurityConfigAdd extends SecurityConfigTestBase {

        private File testRoot = null;
        private final String aSecurityConfig = "securityx";

        TestPasswordReader pwReader = new TestPasswordReader(null);

        SecurityConfigAdd(String rootDir) throws Exception {
            testRoot = new File(rootDir);
            makeSecurityConfig(testRoot, new File(aSecurityConfig));
        }

        public void testAddWithErrors()
            throws Exception {

            String s;

            /* Auto-login wallets need no passphrase */
            pwReader.setPassword(null);

            s = runCLICommand(pwReader,
                    new String[]{"config", "add-security",
                                 "-root", testRoot.toString(),
                                 "-config", "config0.xml",
                                 "-secdir", aSecurityConfig});
            assertTrue(s.indexOf("The configuration cannot be enabled in a " +
                                 "store with registered ES cluster " +
                                 esClusterName +
                                 ", please first " +
                                 "deregister the ES cluster from the non-secure store," +
                                 "and reconfigure.") != -1);
        }
    }
}
