/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import oracle.kv.KVStoreFactory;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.tif.ElasticsearchHandler;
import oracle.kv.impl.tif.TextIndexFeeder;
import oracle.kv.impl.tif.esclient.esRequest.SearchRequest;
import oracle.kv.impl.tif.esclient.esResponse.SearchResponse;
import oracle.kv.impl.tif.esclient.restClient.ESRestClient;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.impl.util.VersionUtil;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * Test TIF when partition migrates between shards
 */
public class TextIndexPartMigrationTest extends TableTestBase {

    private int esHttpPort;
    private static final String esClusterName = "tif-test-es-cluster";
    private static final String namespace = "es";
    private ESRestClient restClient;

    private final String table = "Joke";
    private final String textIndex = "JokeIndex";

    @Override
    void startStoreDynamic()
        throws Exception {

        staticTearDown(); /* clean up first */
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      4, /* n SNs */
                                      1, /* rf */
                                      24, /* n partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true,
                                      null);
        createStore.setPoolSize(3); /* reserve one SN for later. */
        setDynamicStorePolicies(createStore);
        createStore.start();
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        tableImpl = (TableAPIImpl) store.getTableAPI();
    }

    private void setDynamicStorePolicies(CreateStore cstore) {
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        map.setParameter(ParameterState.RN_TIF_BULK_OP_SIZE, "1");
        cstore.setPolicyMap(map);
    }

    @BeforeClass
    public static void staticSetUp() throws Exception {
        Assume.assumeFalse(
            "FTS Unit tests are not currently compatible with security",
            SECURITY_ENABLE);
        Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
        Assume.assumeTrue("Skipping test suite due to missing http port",
            System.getProperty("es.http.port") != null); 
        TableTestBase.staticSetUp();
        
    }

    @Override
    public void setUp() throws Exception {
        kvstoreName =
            "kvtest-" + getClass().getName() + "-" +
            /* Filter out illegal characters */
            testName.getMethodName().replaceAll("[^-_.a-zA-Z0-9]", "-");
        esHttpPort = Integer.parseInt(System.getProperty("es.http.port"));

        startStoreDynamic();
        /* started a multi-region table agent if needed. */
        mrTableTearDown();
        mrTableSetUp(store);

        /* Elasticsearch logging is noisy by default on stdout; just disable it
         * unless debugging.
         */
        org.apache.log4j.Logger log4jRoot =
            org.apache.log4j.Logger.getRootLogger();
        log4jRoot.removeAllAppenders();
        log4jRoot.addAppender
            (org.apache.log4j.varia.NullAppender.getNullAppender());

        restClient = ElasticsearchHandler.createESRestClient(
                esClusterName, "localhost:"+esHttpPort, logger);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        restClient.close();
    }

    @Test
    public void testPartMigration() throws Exception {

        final int numJokes = 1000;
        CommandServiceAPI cs = createStore.getAdmin();

        int planId = cs.createRegisterESClusterPlan
            ("register-es-cluster", esClusterName,
             "localhost:" + esHttpPort,
             /*Not secure*/ false,true);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        final TableAPI tableAPI = store.getTableAPI();

        executeDdl("CREATE TABLE " + table + " " +
                   "(id INTEGER, category STRING, txt STRING," +
                   "PRIMARY KEY (id))", namespace);

        /* Ensure that the previous create has completed.  See [#25187] */
        tableAPI.getTable(namespace, table);

        writeDummyJokes(tableAPI, 1, numJokes);

        executeDdl("CREATE FULLTEXT INDEX " + textIndex +
                   " ON " + table + " (category, txt)", namespace);

        /* Wait until enough documents showing in the index */
        getTotalHitsAtLeast(table, textIndex, numJokes, 100000);
        final String topo = "newTopo";
        /* Now deploy a new topology that includes the spare SN. */
        cs.copyCurrentTopology(topo);
        cs.redistributeTopology(topo, "AllStorageNodes");
        planId = cs.createDeployTopologyPlan("deploy-new-topo", topo, null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        /* Now let's see if we lost any updates. */
        assertEquals("Unexpected count of indexed documents for " + table +
                     "." + textIndex,
                     numJokes,
                     getTotalHits(table, textIndex, numJokes, 20000));
    }

    private enum HitsPrecision {
        EXACTLY, ATLEAST
    }

    private int getTotalHits(String tableName,
                             String textIndexName,
                             int expectedHits,
                             long timeoutMillis)
        throws InterruptedException{

        return getTotalHits(tableName, textIndexName,
                            expectedHits, timeoutMillis, HitsPrecision.EXACTLY);
    }

    private int getTotalHitsAtLeast(String tableName,
                                    String textIndexName,
                                    int expectedHits,
                                    long timeoutMillis)
        throws InterruptedException {

        return getTotalHits(tableName, textIndexName,
                            expectedHits, timeoutMillis, HitsPrecision.ATLEAST);
    }

    /*
     * Repeatedly check total hits from an unconditional ES search of JokeIndex
     * until it reaches the expected value, or until the timeout expires.
     */
    private int getTotalHits(String tableName,
                             String textIndexName,
                             int expectedHits,
                             long timeoutMillis,
                             HitsPrecision precision)
        throws InterruptedException {

        final long startTime = System.currentTimeMillis();

        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(createStore.getStoreName(),
                                              namespace,
                                              tableName,
                                              textIndexName);
        int totalHits = 0;
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            /* Refresh the search engine to ensure all indexed items are
             * available to search. */
        	try {
        	restClient.dml().refresh(esIndexName);
        	} catch (Exception e) {

        	}
            Thread.sleep(1000);

            SearchRequest searchRequest = new SearchRequest(esIndexName);
            SearchResponse response = null;
			try {
				response = restClient.dml().search(searchRequest);
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
            logger.info("Re-trying search after " +
                        (System.currentTimeMillis() - startTime) +
                        "ms, totalHits = " + totalHits);
        }
        fail("Timed out waiting " + timeoutMillis +
             "ms for ES to catch up, totalHits = " + totalHits +
             ", expected " + expectedHits);
        return 0;
    }

    private void writeDummyJokes(TableAPI t, int startId, int numJokes) {

        for (int i = startId; i < startId + numJokes; i++) {
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
}
