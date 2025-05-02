/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Version;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.tif.esclient.esResponse.GetResponse;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.table.Index;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests to test TextIndexFeeder.
 */
public class TextIndexFeederTest extends TextIndexFeederTestBase {

    private ElasticsearchHandler esHandler;
    private int esHttpPort;

    @BeforeClass
    public static void staticSetUp() throws Exception {
         Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        config = new KVRepTestConfig(this, NUM_DC, NUM_SN, REP_Factor,
                                     numPartitions);

        System.setProperty("es.path.home", System.getProperty("testdestdir"));

        esHttpPort = Integer.parseInt(System.getProperty("es.http.port"));
        final String esClusterHttpMembers =
                "localhost:" + Integer.toString(esHttpPort);

        final String esClusterName = "tif-test-es-cluster";

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("setUp: started ES Server Node");
            logger.info("setUp: cluster.name       = " + esClusterName);
            logger.info("setUp: http.port          = " + esHttpPort);
            logger.info(" ");
        }

        esHandler =
            ElasticsearchHandler.newInstance(esClusterName,
                                             esClusterHttpMembers, false, null,
                                             5000, logger);

        logger.info("setUp: created ElasticsearchHandler");

        createSnaConfig(esClusterHttpMembers, esClusterName);
        logger.info("setUp: created SNA Config");
    }

    @Override
    public void tearDown() throws Exception {

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("-----------------------------------");
            logger.info("------------- tearDown ------------");
            logger.info("-----------------------------------");
            logger.info(" ");
            logger.info("tearDown: stopping the RNs ...");
            logger.info(" ");
        }

        config.stopRNs();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("tearDown: RNs stopped");
            logger.info(" ");
            logger.info("tearDown: closing ElasticsearchHandler ...");
            logger.info(" ");
        }

        if (esHandler != null) {
            esHandler.close();
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("tearDown: ElasticsearchHandler closed");
            logger.info(" ");
            logger.info("tearDown: closing elastic search cluster ...");
            logger.info(" ");
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("tearDown: elasticsearch cluster closed");
            logger.info(" ");
        }

        super.tearDown();

        if (logger.isLoggable(Level.INFO)) {
            logger.info("tearDown: complete");
            logger.info("-----------------------------------");
            logger.info(" ");
        }
    }

    /**
     * Test of a full data path between kv store and es index. In particular,
     * in test
     *
     * - load real jokes into kvstore;
     * - start TIF to receive data via replication stream from source node;
     * - TIF would process and send documents in ES for indexing;
     * - after test done, verify all jokes sent to ES
     *
     * This test uses a tiny set of real jokes.
     *
     * @throws Exception
     */
    @Test
    public void testRealJokes() throws Exception {

        final String curTestName = "testRealJokes";

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------- " + curTestName + " ----------");
            logger.info(" ");
        }

        /* create env without loading any data */
        prepareTestEnv(false);

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        /* mark the text index as ready */
        final boolean succ =
            metadata.updateIndexStatus(null, "JokeIndex",
                                       jokeTable.getFullName(),
                                       IndexImpl.IndexStatus.READY);
        assertTrue("expect successful update index status ", succ);
        updateMetadata(rg1rn1, metadata);

        /* load real jokes into kv store */
        loadJokeTable(apiImpl, realJokes);

        if (logger.isLoggable(Level.FINE)) {

            final Table table = apiImpl.getTable(jokeTable.getFullName());

            if (table == null) {
                if (logger.isLoggable(Level.INFO)) {
                    final String msg = curTestName + ": " + "Store does not " +
                       "contain table [name=" +  jokeTable.getFullName() + "]";
                    logger.info(" ");
                    logger.info("-----------------------------------");
                    logger.info(msg);
                    logger.info("-----------------------------------");
                    logger.info(" ");
                }
            } else {

                if (logger.isLoggable(Level.FINEST)) {
                    final TableIterator<Row> itr =
                    apiImpl.tableIterator(
                        table.createPrimaryKey(), null, null);

                    final String tblName = jokeTable.getFullName();

                    logger.finest(" ");
                    logger.finest("-----------------------------------");
                    logger.finest(curTestName + ": Contents of table ['" +
                                  tblName + "']:");
                    logger.finest(" ");
                    while (itr.hasNext()) {
                        logger.finest(itr.next().toString());
                    }
                    itr.close();

                    logger.finest("----- End table contents ['" +
                                  tblName + "'] -----");
                }
            }
        }

        final TextIndexFeeder tif =
            new TextIndexFeeder(new SourceRepNode(kvstoreName, rg1rn1),
                                new HostRepNode(tifNodeName, rg1rn2),
                                esHandler, new ParameterMap(), null, logger);
        logger.info("created TextIndexFeeder");

        /* set post commit cbk */
        final TransactionPostCommitCounter cc =
            new TransactionPostCommitCounter(realJokes.length, 0);
        tif.setPostCommitCbk(cc);

        /* The mapping should become available */
        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": creating text index mapping ...");
            logger.info(" ");
        }

        /* The mapping should become available to both TIFs */
        createMappingIfNotExist(tif, kvstoreName,
                                jokeTable.getInternalNamespace(),
                                jokeTable.getFullName(),
                                "JokeIndex");

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": text index mapping created");
            logger.info(" ");
            logger.info(curTestName + ": start text index feeder");
            logger.info(" ");
        }

        /* fire TIF */
        tif.startFeeder();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": text index feeder started");
            logger.info(" ");
            logger.info(curTestName + ": wait for jokes to be indexed ...");
            logger.info(" ");
        }

        /* wait for test done */
        waitForTestDone(cc);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": joke indexing complete");
            logger.info(" ");
            logger.info(curTestName + ": verifying indexed jokes " +
                        "committed to elasticsearch ...");
            logger.info(" ");
        }

        /* verify */
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(kvstoreName,
                                              jokeTable.getInternalNamespace(),
                                              jokeTable.getFullName(),
                                              "JokeIndex");
        verifyIndexedJokes(esIndexName, realJokes);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": verification complete [index=" +
                        esIndexName + "]");
            logger.info(" ");
            logger.info(curTestName + ": cleanup - stopping the " +
                        "TextIndexFeeder ...");
        }

        stopFeeder(tif);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(curTestName + ": cleanup - TextIndexFeeder stopped " +
                        "and index deleted [" + esIndexName + "]");
            logger.info(curTestName + ": cleanup - closing the kvstore ...");
        }

        kvs.close();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(curTestName + ": cleanup - kvstore closed");
            logger.info(" ");
            logger.info(curTestName + ": PASSED");
            logger.info(" ");
        }
    }

    /**
     * Test of a full data path between kv store and es index. In particular,
     * it tests that
     *
     * - load dummy jokes into kvstore;
     * - start TIF to receive preloaded dummy jokes via replication
     * stream from source node;
     * - TIF would process and send preloaded dummy jokes in ES for indexing;
     * - after preloaded dummy jokes are all indexed, verify all preloaded
     * dummy jokes indexed in ES;
     * - update a subset of dummy jokes in kvstore, TIF would receive the
     * updates and send to ES for indexing via replication stream
     * - verify all these updated dummy jokes indexed in ES.
     *
     * This test uses a reasonable set of dummy jokes
     *
     * @throws Exception
     */
    @Test
    public void testDummyJokes() throws Exception {

        final String curTestName = "testDummyJokes";

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------- " + curTestName + " ---------");
            logger.info(" ");
        }

        /* create env without loading any data */
        prepareTestEnv(false);

        /* number of preloaded dummy jokes */
        final int numDummyJokes = 100 * numPartitions;
        final int numUpdatedJokes = numDummyJokes / 10;
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();
        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final Joke[] allDummyJokes = createDummyJokes(numDummyJokes);

        final String tableName = jokeTable.getFullName();
        final String indexName = "JokeIndex";
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(kvstoreName,
                                              null, /* namespace */
                                              tableName,
                                              indexName);
        /* mark the text index as ready */
        metadata.updateIndexStatus(null, indexName, tableName,
                                   IndexImpl.IndexStatus.READY);
        updateMetadata(rg1rn1, metadata);

        /* load dummy jokes into kv store */
        loadJokeTable(apiImpl, allDummyJokes);

        if (logger.isLoggable(Level.FINE)) {

            final Table table = apiImpl.getTable(jokeTable.getFullName());

            if (table == null) {
                if (logger.isLoggable(Level.INFO)) {
                    final String msg = curTestName + ": Store does not " +
                        "contain table [name=" + jokeTable.getFullName() + "]";
                    logger.info(" ");
                    logger.info("-----------------------------------");
                    logger.info(msg);
                    logger.info("-----------------------------------");
                    logger.info(" ");
                }
            } else {

                final TableIterator<Row> itr =
                apiImpl.tableIterator(table.createPrimaryKey(), null, null);

                final String tblName = jokeTable.getFullName();

                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(" ");
                    logger.finest("-----------------------------------");
                    logger.finest(curTestName + ": Contents of table ['" +
                                  tblName + "']:");
                    logger.finest(" ");
                    while (itr.hasNext()) {
                        logger.finest(itr.next().toString());
                    }
                    itr.close();

                    logger.finest("----- End table contents ['" +
                                  tblName + "'] -----");
                }
            }
        }

        final TextIndexFeeder tif =
            new TextIndexFeeder(new SourceRepNode(kvstoreName, rg1rn1),
                                new HostRepNode(tifNodeName, rg1rn1),
                                esHandler, new ParameterMap(), null, logger);
        logger.info("created TextIndexFeeder");

        /* set post commit cbk */
        final TransactionPostCommitCounter cc =
            new TransactionPostCommitCounter(allDummyJokes.length, 0);
        tif.setPostCommitCbk(cc);

        /* The mapping should become available */
        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": creating text index mapping ...");
            logger.info(" ");
        }

        createMappingIfNotExist(tif, kvstoreName, null, tableName, indexName);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": text index mapping created");
            logger.info(" ");
            logger.info(curTestName + ": start text index feeder");
            logger.info(" ");
        }

        tif.startFeeder();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": text index feeder started");
            logger.info(" ");
            logger.info(curTestName + ": wait for jokes to be indexed ...");
            logger.info(" ");
        }

        /* wait for all preloaded jokes to be indexed */
        waitForTestDone(cc);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": joke indexing complete");
            logger.info(" ");
            logger.info(curTestName + ": verifying indexed jokes " +
                        "committed to elasticsearch ...");
            logger.info(" ");
        }

        /* verify all preloaded dummy joke commit to ES */
        verifyIndexedJokes(esIndexName, allDummyJokes);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": verification complete [" +
                        allDummyJokes.length + " indexed jokes from index=" +
                        esIndexName + "]");
            logger.info(" ");
            logger.info(curTestName + ": update the table with a new set " +
                        "of jokes ...");
            logger.info(" ");
        }

        /* create a subset of dummy jokes */
        final Joke[] allUpdatedJokes =
            createUpdatedDummyJokes(allDummyJokes, numUpdatedJokes);

        /* adjust expected puts to include the updates */
        cc.setExpectedPuts(allDummyJokes.length + numUpdatedJokes);

        /* update test db */
        loadJokeTable(apiImpl, allUpdatedJokes);

        if (logger.isLoggable(Level.FINE)) {

            final Table table = apiImpl.getTable(jokeTable.getFullName());

            if (table == null) {
                if (logger.isLoggable(Level.INFO)) {
                    final String msg =
                        curTestName + ": Store does not contain updated " +
                        "table [name=" + jokeTable.getFullName() + "]";
                    logger.info(" ");
                    logger.info("-----------------------------------");
                    logger.info(msg);
                    logger.info("-----------------------------------");
                    logger.info(" ");
                }
            } else {

                if (logger.isLoggable(Level.FINEST)) {
                    final TableIterator<Row> itr =
                    apiImpl.tableIterator(
                        table.createPrimaryKey(), null, null);

                    final String tblName = jokeTable.getFullName();

                    logger.finest(" ");
                    logger.finest("-----------------------------------");
                    logger.finest(curTestName + ": Contents of updated " +
                                  "table ['" + tblName + "']:");
                    logger.finest(" ");
                    while (itr.hasNext()) {
                        logger.finest(itr.next().toString());
                    }
                    itr.close();

                    logger.finest("----- End updated table contents ['" +
                                  tblName + "'] -----");
                }
            }
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": table update complete");
            logger.info(" ");
            logger.info(curTestName + ": wait for updated jokes to be " +
                        "indexed ...");
            logger.info(" ");
        }

        /* wait for updated dummy jokes committed */
        waitForTestDone(cc);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": updated joke indexing complete");
            logger.info(" ");
            logger.info(curTestName + ": verifying newly indexed jokes " +
                        "committed to elasticsearch ...");
            logger.info(" ");
        }

        verifyIndexedJokes(esIndexName, allUpdatedJokes);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": verification complete [" +
                        allUpdatedJokes.length + " indexed updated jokes " +
                        "from index=" + esIndexName + "]");
            logger.info(" ");
            logger.info(curTestName + ": cleanup - stopping the " +
                        "TextIndexFeeder ...");
        }

        /* clean up */
        stopFeeder(tif);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(curTestName + ": cleanup - TextIndexFeeder stopped " +
                        "and index deleted [" + esIndexName + "]");
            logger.info(curTestName + ": cleanup - closing the kvstore ...");
        }

        kvs.close();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(curTestName + ": cleanup - kvstore closed");
            logger.info(" ");
            logger.info(curTestName + ": PASSED");
            logger.info(" ");
        }
    }

    /**
     * Test of a full data path between kv store and es index. In particular,
     * it tests that
     *
     * - load dummy jokes into kvstore;
     * - start TIF to receive preloaded dummy jokes via partition transfer;
     * - TIF would process and send preloaded dummy jokes in ES for indexing.
     * - verify all preloaded dummy jokes sent to ES
     *
     * @throws Exception
     */
    @Test
    public void testInitialRepOnly() throws Exception {

        final String curTestName = "testInitialRepOnly";

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------- " + curTestName + " ----------");
            logger.info(" ");
        }

        /* create env without loading any data */
        prepareTestEnv(false);
        /* number of preloaded dummy jokes */
        final int numDummyJokes = 100 * numPartitions;
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();
        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final Joke[] allDummyJokes = createDummyJokes(numDummyJokes);

        final String tableName = jokeTable.getFullName();
        final String indexName = "JokeIndex";
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(kvstoreName,
                                              null, /* namespace */
                                              tableName,
                                              indexName);


        /* mark the text index as ready */
        metadata.updateIndexStatus(null, indexName, tableName,
                                   IndexImpl.IndexStatus.READY);
        updateMetadata(rg1rn1, metadata);

        loadJokeTable(apiImpl, allDummyJokes);

        if (logger.isLoggable(Level.FINE)) {

            final Table table = apiImpl.getTable(jokeTable.getFullName());

            if (table == null) {
                if (logger.isLoggable(Level.INFO)) {
                    final String msg = curTestName + ": " + "Store does not " +
                       "contain table [name=" +  jokeTable.getFullName() + "]";
                    logger.info(" ");
                    logger.info("-----------------------------------");
                    logger.info(msg);
                    logger.info("-----------------------------------");
                    logger.info(" ");
                }
            } else {

                if (logger.isLoggable(Level.FINEST)) {
                    final TableIterator<Row> itr =
                    apiImpl.tableIterator(
                        table.createPrimaryKey(), null, null);

                    final String tblName = jokeTable.getFullName();

                    logger.finest(" ");
                    logger.finest("-----------------------------------");
                    logger.finest(curTestName + ": Contents of table ['" +
                                  tblName + "']:");
                    logger.finest(" ");
                    while (itr.hasNext()) {
                        logger.finest(itr.next().toString());
                    }
                    itr.close();

                    logger.finest("----- End table contents ['" +
                                  tblName + "'] -----");
                }
            }
        }

        final TextIndexFeeder tif =
            new TextIndexFeeder(new SourceRepNode(kvstoreName, rg1rn1),
                                new HostRepNode(tifNodeName, rg1rn1),
                                esHandler, new ParameterMap(), null, logger);
        logger.info("created TextIndexFeeder");

        /* set post commit cbk */
        final TransactionPostCommitCounter cc =
            new TransactionPostCommitCounter(allDummyJokes.length, 0);
        tif.setPostCommitCbk(cc);

        /* The mapping should become available */
        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": creating text index mapping ...");
            logger.info(" ");
        }

        /* The mapping should become available */
        createMappingIfNotExist(tif, kvstoreName, null, tableName, indexName);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": text index mapping created");
            logger.info(" ");
            logger.info(curTestName + ": start text index feeder");
            logger.info(" ");
        }

        /* start initial phase */
        tif.startFeederFromInitPhase();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": text index feeder started");
            logger.info(" ");
            logger.info(curTestName + ": wait for jokes to be indexed ...");
            logger.info(" ");
        }

        /* wait for all puts committed , no deletion */
        waitForTestDone(cc);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": joke indexing complete");
            logger.info(" ");
            logger.info(curTestName + ": verifying indexed jokes " +
                        "committed to elasticsearch ...");
            logger.info(" ");
        }

        /* verify */
        verifyIndexedJokes(esIndexName, allDummyJokes);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info(curTestName + ": verification complete [index=" +
                        esIndexName + "]");
            logger.info(" ");
            logger.info(curTestName + ": cleanup - stopping the " +
                        "TextIndexFeeder ...");
        }

        /* clean up */
        stopFeeder(tif);

        if (logger.isLoggable(Level.INFO)) {
            logger.info(curTestName + ": cleanup - TextIndexFeeder stopped " +
                        "and index deleted [" + esIndexName + "]");
            logger.info(curTestName + ": cleanup - closing the kvstore ...");
        }

        kvs.close();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(curTestName + ": cleanup - kvstore closed");
            logger.info(" ");
            logger.info(curTestName + ": PASSED");
            logger.info(" ");
        }
    }

    /**
     * Test of a full data path between a kv store table with json column
     * consisting of fields containing only basic scalar mappings and
     * elasticsearch index. In particular,
     *
     * - load json documents into table in kvstore;
     * - start TIF to receive data via replication stream from source node;
     * - TIF would process and send documents in ES for indexing;
     * - after test done, verify all documents sent to ES
     *
     * @throws Exception
     */
    @Test
    public void testJsonIndexScalar() throws Exception {

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------- testJsonIndexScalar ----------");
            logger.info(" ");
        }
        
        /* create env without loading any data */
        prepareTestEnv(false);

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI tableApi = kvs.getTableAPI();
        final String jsonTableName = jsonTableScalar.getFullName();

        /* mark the index as ready */
        final boolean succ =
            metadata.updateIndexStatus(null, JSON_INDEX_NAME_SCALAR,
                                       jsonTableName,
                                       IndexImpl.IndexStatus.READY);
        assertTrue("expect successful update index status ", succ);
        updateMetadata(rg1rn1, metadata);

        final Table table = tableApi.getTable(jsonTableScalar.getFullName());
        populateJsonTableScalar(
            tableApi, table, jsonTableName, N_JSON_ROWS_SCALAR);

        final Index index = table.getIndex(JSON_INDEX_NAME_SCALAR);
        if (index == null) {
            fail("error: store table [" + jsonTableName + "] does " +
                 "not contain the expected index [" +
                 JSON_INDEX_NAME_SCALAR + "]");
        } else {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("[" + JSON_INDEX_NAME_SCALAR + "]: type = " +
                            index.getType() + ", fields = " +
                            index.getFields());
                logger.fine(" ");
            }
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("------------------------");
            logger.info("---- BEGIN TESTING -----");
            logger.info("------------------------");
        }

        final TextIndexFeeder tif =
            new TextIndexFeeder(new SourceRepNode(kvstoreName, rg1rn1),
                                new HostRepNode(tifNodeName, rg1rn2),
                                esHandler, new ParameterMap(), null, logger);
        /* set post commit cbk */
        final TransactionPostCommitCounter cc =
            new TransactionPostCommitCounter(N_JSON_ROWS_SCALAR, 0);
        tif.setPostCommitCbk(cc);

        /* The mapping should become available to both TIFs */
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableScalar.getInternalNamespace(),
                                jsonTableName, JSON_INDEX_NAME_SCALAR);
        
        /* Start the TextIndexFeeder */
        tif.startFeeder();

        /* wait for test done */
        waitForTestDone(cc);

        /* verify */
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(
                kvstoreName, jsonTableScalar.getInternalNamespace(),
                jsonTableName, JSON_INDEX_NAME_SCALAR);
        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------------------------------------------");
            logger.info("---------------- VERIFICATION ---------------");
            logger.info("---------------------------------------------");
            logger.info(" ");
        }
        verifyIndexedJsonScalar(
            tableApi, table, esIndexName, N_JSON_ROWS_SCALAR);

        stopFeeder(tif);
        kvs.close();
    }

    /**
     * Test of a full data path between a kv store table with json column
     * consisting of fields containing complex json objects (objects, arrays,
     * scalars) and elasticsearch index. In particular,
     *
     * - load json documents into table in kvstore;
     * - start TIF to receive data via replication stream from source node;
     * - TIF would process and send documents in ES for indexing;
     * - after test done, verify all documents sent to ES
     *
     * @throws Exception
     */
    @Test
    public void testJsonIndexSenators() throws Exception {

        createJsonIndexSenators();
        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------- testJsonIndexSenators ----------");
            logger.info(" ");
        }

        /* create env without loading any data */
        prepareTestEnv(false);

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI tableApi = kvs.getTableAPI();
        final String jsonTableName = jsonTableSenators.getFullName();

        /* mark the index as ready */
        final boolean succ =
            metadata.updateIndexStatus(null, JSON_INDEX_NAME_SENATORS,
                                       jsonTableName,
                                       IndexImpl.IndexStatus.READY);
        assertTrue("expect successful update index status ", succ);
        updateMetadata(rg1rn1, metadata);

        final Table table = tableApi.getTable(jsonTableSenators.getFullName());
        populateJsonTableSenators(
            tableApi, table, jsonTableName, N_JSON_ROWS_SENATORS);

        final Index index = table.getIndex(JSON_INDEX_NAME_SENATORS);
        if (index == null) {
            fail("error: store table [" + jsonTableName + "] does " +
                 "not contain the expected index [" +
                 JSON_INDEX_NAME_SENATORS + "]");
        } else {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("[" + JSON_INDEX_NAME_SENATORS + "]: type = " +
                            index.getType() + ", fields = " +
                            index.getFields());
                logger.fine(" ");
            }
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("------------------------");
            logger.info("---- BEGIN TESTING -----");
            logger.info("------------------------");
        }

        final TextIndexFeeder tif =
            new TextIndexFeeder(new SourceRepNode(kvstoreName, rg1rn1),
                                new HostRepNode(tifNodeName, rg1rn2),
                                esHandler, new ParameterMap(), null, logger);
        /* set post commit cbk */
        final TransactionPostCommitCounter cc =
            new TransactionPostCommitCounter(N_JSON_ROWS_SENATORS, 0);
        tif.setPostCommitCbk(cc);

        /* The mapping should become available to both TIFs */
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableSenators.getInternalNamespace(),
                                jsonTableName,
                                JSON_INDEX_NAME_SENATORS);
        /* Start the TextIndexFeeder */
        tif.startFeeder();

        /* wait for test done */
        waitForTestDone(cc);

        /* verify */
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(
                kvstoreName, jsonTableSenators.getInternalNamespace(),
                jsonTableName, JSON_INDEX_NAME_SENATORS);
        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------------------------------------------");
            logger.info("---------------- VERIFICATION ---------------");
            logger.info("---------------------------------------------");
            logger.info(" ");
        }

        verifyIndexedJsonSenators(
            tableApi, table, esIndexName, N_JSON_ROWS_SENATORS);
        stopFeeder(tif);
        kvs.close();
    }

    /**
     * Test the below situations:
     *
     * - Create two indexes with same content but in different order (both
     *   indexes should be created successfully)
     * - Create an index that contains invalid field - the field has wrong path
     * - Create an index that contains invalid field - the field has wrong case
     * - Create an index that the mapping specification of the field is not
     *   consistent with the value that stores in the store.
     * - Insert another row into "senators" table, the content of the json
     *   field in this row does not have the same structure with the content of
     *   the json fields in other rows.
     *
     * For more details, please check the test spec in https://goo.gl/PzWEFN.
     * Related SRs: 27551 & 27552
     *
     * @throws Exception
     */
    @Test
    public void testJsonIndexBehaviors() throws Exception {

        /* Create two indexes with same content but in different order (both
         * indexes should be created successfully)
         */
        createJsonIndexSenatorsForBehaviorsTest();

        if (logger.isLoggable(Level.INFO)) {
            logger.info(" ");
            logger.info("---------- testJsonIndexBehaviors ----------");
            logger.info(" ");
        }

        /* create env without loading any data */
        prepareTestEnv(false);

        final RepNode rg1rn1 = config.getRN(new RepNodeId(1, 1));
        final RepNode rg1rn2 = config.getRN(new RepNodeId(1, 2));

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI tableApi = kvs.getTableAPI();
        final String jsonTableName = jsonTableSenators.getFullName();
        /* mark the index as ready */
        final boolean succ =
            metadata.updateIndexStatus(null, JSON_INDEX_NAME_SENATORS_ORDER,
                                       jsonTableName,
                                       IndexImpl.IndexStatus.READY);
        assertTrue("expect successful update index status ", succ);
        updateMetadata(rg1rn1, metadata);

        final Table table = tableApi.getTable(jsonTableSenators.getFullName());
        populateJsonTableSenators(
            tableApi, table, jsonTableName, N_JSON_ROWS_SENATORS);
        /* Add a row that its json column does not have congress_numbers field,
         * Which means this json document has different structure from json
         * documents of other rows.
         * So these two indexes: "JSON_INDEX_NAME_SENATORS_ORDER" and
         * "JSON_INDEX_NAME_SENATORS_OPP_ORDER" can be used to verify two
         * test cases: test case 101 and 201.
         * See "Test Specification for Full Text Search JSON Support" on
         * Confluence, test case 101 and 201.
         */
        addInconsistentJsonRowSenators(tableApi, table, N_JSON_ROWS_SENATORS);
        final int n_json_rows_senators_behavior = N_JSON_ROWS_SENATORS + 1;

        if (logger.isLoggable(Level.INFO)) {
            logger.info("------------------------");
            logger.info("---- BEGIN TESTING -----");
            logger.info("------------------------");
        }

        TextIndexFeeder tif =
            new TextIndexFeeder(new SourceRepNode(kvstoreName, rg1rn1),
                                new HostRepNode(tifNodeName, rg1rn2),
                                esHandler, new ParameterMap(), null, logger);
        /* set post commit cbk */
        final TransactionPostCommitCounter cc =
            new TransactionPostCommitCounter(20, 0);
        tif.setPostCommitCbk(cc);

        /* The mapping should become available to both TIFs */
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableSenators.getInternalNamespace(),
                                jsonTableName,
                                JSON_INDEX_NAME_SENATORS_ORDER);
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableSenators.getInternalNamespace(),
                                jsonTableName,
                                JSON_INDEX_NAME_SENATORS_OPP_ORDER);
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableSenators.getInternalNamespace(),
                                jsonTableName,
                                JSON_INDEX_NAME_SENATORS_WRONG_PATH);
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableSenators.getInternalNamespace(),
                                jsonTableName,
                                JSON_INDEX_NAME_SENATORS_WRONG_CASE);
        createMappingIfNotExist(tif, kvstoreName,
                                jsonTableSenators.getInternalNamespace(),
                                jsonTableName,
                                JSON_INDEX_NAME_SENATORS_INCONS_TYPE);
        /* Start the TextIndexFeeder */
        tif.startFeeder();
        /* wait for test done */
        waitForTestDone(cc);

        /* verify */
        final String esIndexName1 = TextIndexFeeder.deriveESIndexName(
                    kvstoreName, jsonTableSenators.getInternalNamespace(),
                    jsonTableName, JSON_INDEX_NAME_SENATORS_ORDER);
        verifyIndexedJsonSenatorsInBehaviorTests(
                    tableApi, table, esIndexName1,
                    n_json_rows_senators_behavior);
        final String esIndexName2 =
                TextIndexFeeder.deriveESIndexName(
                    kvstoreName, jsonTableSenators.getInternalNamespace(),
                    jsonTableName, JSON_INDEX_NAME_SENATORS_OPP_ORDER);
        verifyIndexedJsonSenatorsInBehaviorTests(
                    tableApi, table, esIndexName2,
                    n_json_rows_senators_behavior);

        /* verify the existence of these indexes */
        final String esIndexName3 =
                TextIndexFeeder.deriveESIndexName(
                    kvstoreName, jsonTableSenators.getInternalNamespace(),
                    jsonTableName, JSON_INDEX_NAME_SENATORS_WRONG_PATH);
        assertTrue("Failed, " + esIndexName3 + " does not exist in ES cluster",
                   esHandler.existESIndex(esIndexName3));

        final String esIndexName4 =
                TextIndexFeeder.deriveESIndexName(
                    kvstoreName, jsonTableSenators.getInternalNamespace(),
                    jsonTableName, JSON_INDEX_NAME_SENATORS_WRONG_CASE);
        assertTrue("Failed, " + esIndexName4 + " does not exist in ES cluster",
                   esHandler.existESIndex(esIndexName4));
        final String esIndexName5 =
                TextIndexFeeder.deriveESIndexName(
                    kvstoreName, jsonTableSenators.getInternalNamespace(),
                    jsonTableName, JSON_INDEX_NAME_SENATORS_INCONS_TYPE);
        assertTrue("Failed, " + esIndexName5 + " does not exist in ES cluster",
                   esHandler.existESIndex(esIndexName5));

        stopFeeder(tif);
        kvs.close();
    }

    private void stopFeeder(TextIndexFeeder tif) {
        /* drop index if exists */
        final String indexName =
            TextIndexFeeder.deriveESIndexName(
                kvstoreName, jsonTableScalar.getInternalNamespace(),
                jsonTableScalar.getFullName(), JSON_INDEX_NAME_SCALAR);
        tif.dropIndex(indexName);
        tif.stopFeeder(true, false);
    }

    void createMappingIfNotExist(TextIndexFeeder tif,
                                 String storeName,
                                 String namespace,
                                 String tableName,
                                 String textIndexName) throws IOException {
        final String esIndexName =
            TextIndexFeeder.deriveESIndexName(storeName, namespace,
                                              tableName, textIndexName);
        if (esHandler.existESIndexMapping(esIndexName)) {
            return;
        }
        try {
            tif.createMappingForTextIndex(storeName, namespace,
                                          tableName, textIndexName);
        } catch (Exception exp) {
            /*
             * Seems checking existing mapping may return false negative, and
             * cause failure in creating new mapping. Since the mapping does
             * not change, we can reuse the existing mapping and ignore the
             * exception.
             */
            logger.info("Fail to create the mapping:" + exp.getMessage() +
                        "\n" + LoggerUtils.getStackTrace(exp));
        }
    }

    /**
     * Given an array of jokes, verify each joke has been sent to ES index
     * correctly by querying the ES index using the key.
     *
     * @param esIndexName name of es index
     * @param allJokes array of all jokes
     */
    private void verifyIndexedJokes(String esIndexName,
                                    Joke[] allJokes) {

        /* constant mapping name */

        final ArrayList<String> missedKeys = new ArrayList<>();
        final ArrayList<String> failedKeys = new ArrayList<>();

        /* Read jokes back from ES and verify. */
        for (int i = 0; i < allJokes.length; i++) {

            GetResponse getResponse = null;
            try {
                getResponse = esHandler.get(esIndexName,
                                            allJokes[i].getKey());
            } catch (IOException e) {
                fail("GetResponse failed due to:" + e);
                return;
            }

            if (!getResponse.isFound()) {
                missedKeys.add(allJokes[i].getKey());
                logger.info("Joke " + i + " missing! Key: " +
                        allJokes[i].getKey());

            } else {
                final String text =
                    (String) getResponse.sourceAsMap().get("text");
                final String category =
                    (String) getResponse.sourceAsMap().get("category");

                if (!allJokes[i].getText().equals(text) ||
                    !allJokes[i].getCategory().equals(category)) {
                    failedKeys.add(allJokes[i].getKey());
                    logger.info("Joke " + i + " failed verification: " +
                                "");
                    logger.info("Expected joke text: " +
                                allJokes[i].getText() + ",  category " +
                                allJokes[i].getCategory());

                    logger.info("Actual joke text: " +
                                text + ",  category " +
                                category);

                }
            }
        }

        if (!missedKeys.isEmpty()) {
            fail(missedKeys.size() + " keys missed on ES!");
        } else if (!failedKeys.isEmpty()) {
            fail(failedKeys.size() + " keys found on ES but failed " +
                 "verification!");
        } else {
            logger.info("All " + allJokes.length + " jokes verified");
        }
    }

    /* load Joke table into kv store */
    private void loadJokeTable(TableAPI apiImpl,
                               Joke[] allJokes) {
        /* Add the joke table rows via public API. */
        int id = 0;
        long lastVLSN = 0;
        UUID lastGroupUUID = new UUID(0, 0);
        Version version;
        for (Joke joke : allJokes) {
            final RowImpl row = makeJokeRow(jokeTable, joke, id++);
            /* Save the row's key in the data source array */
            joke.setKey(TableKey.createKey(jokeTable, row, false)
                                .getKey().toString());

            version = apiImpl.put(row, null,
                                  new WriteOptions(Durability.COMMIT_NO_SYNC,
                                                   10000,
                                                   MILLISECONDS));
            lastVLSN = version.getVLSN();
            lastGroupUUID = version.getRepGroupUUID();
        }

        logger.info(allJokes.length + " items in Joke table have " +
                    "been loaded into store " +
                    config.getKVSConfig().getStoreName() +
                    ", the last entry is loaded to group: " + lastGroupUUID +
                    " with vlsn: " + lastVLSN);
    }

    /* create a set of dummy jokes */
    private Joke[] createDummyJokes(int numDummyJokes) {
        final Joke[] ret = new Joke[numDummyJokes];

        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < numDummyJokes; i++) {
            final String type = "type" + UUID.randomUUID();
            final String text = "text-original-" + UUID.randomUUID() + ", " +
                                UUID.randomUUID() + ", " + UUID.randomUUID();
            final float humor = random.nextFloat();

            final Timestamp originDate =
                TimestampUtils.parseString("1963-02-17T01:02:03");
            final Timestamp firstUseDate =
                TimestampUtils.parseString("1983-06-04T08:01:01.123");
            final Timestamp lastUseDate =
                TimestampUtils.parseString("2001-12-31T11:59:59.546789321");

            ret[i] = new Joke(type, text, humor,
                              originDate, firstUseDate, lastUseDate);
        }

        logger.info(ret.length + " dummy jokes created.");
        return ret;
    }

    /* update a subset of jokes */
    private Joke[] createUpdatedDummyJokes(Joke[] allJokes,
                                           int samples) {
        final int sz = (samples < allJokes.length) ? samples : allJokes.length;
        final Joke[] ret = new Joke[sz];

        for (int i = 0; i < sz; i++) {
            final String type = allJokes[i].getCategory();
            final float humor = allJokes[i].getHumorQuotient();
            final Timestamp originDate = allJokes[i].getOriginDate();
            final Timestamp firstUseDate = allJokes[i].getFirstUseDate();
            final Timestamp lastUseDate = allJokes[i].getLastUseDate();

            /* create a new text to simulate update */
            final String text = "text-updated-" + UUID.randomUUID() + ", " +
                                UUID.randomUUID() + ", " + UUID.randomUUID();
            final Joke updatedJoke = new Joke(type, text, humor, originDate,
                                              firstUseDate, lastUseDate);
            /* same key as original joke */
            updatedJoke.setKey(allJokes[i].getKey());
            ret[i] = updatedJoke;
        }

        logger.info(sz + " dummy jokes updated.");
        return ret;
    }

    /* wait for test done, by checking subscription callback */
    private void waitForTestDone(final TransactionPostCommitCounter callBack)
        throws TimeoutException {

        final boolean success = new PollCondition(TEST_POLL_INTERVAL_MS,
                                                  TEST_POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
                return callBack.isTestDone();
            }
        }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    private class TransactionPostCommitCounter
        implements TransactionPostCommitCallback {

        private int expectedPuts;
        private int expectedDels;
        private int totalputs;
        private int totaldels;

        TransactionPostCommitCounter(int puts, int dels) {
            expectedDels = dels;
            expectedPuts = puts;
            totaldels = 0;
            totalputs = 0;
        }

        public synchronized void setExpectedPuts(int puts) {
            expectedPuts = puts;
        }

        @Override
        public synchronized void postCommit(IndexOperation op) {
            final IndexOperation.OperationType type = op.getOperation();
            
            if (type.equals(IndexOperation.OperationType.PUT)) {
                totalputs++;
            } else if (type.equals(IndexOperation.OperationType.DEL)) {
                totaldels++;
            }
            notify();
        }

        @Override
        public synchronized void postCommit(TransactionAgenda.Transaction txn,
                                            long vlsn) {
            for (IndexOperation op : txn.getOps()) {
                final IndexOperation.OperationType type = op.getOperation();
                if (type.equals(IndexOperation.OperationType.PUT)) {
                    totalputs++;
                } else if (type.equals(IndexOperation.OperationType.DEL)) {
                    totaldels++;
                }
            }
            notify();
        }

        public boolean isTestDone() {
            return (totalputs == expectedPuts) && (totaldels == expectedDels);
        }

    }

    private void populateJsonTableScalar(final TableAPI tableApi,
                                         final Table table,
                                         final String tableName,
                                         final int nRows) {
        if (tableApi == null) {
            fail("populateJsonTableScalar: tableApi == null");
        }
        if (table == null) {
            fail("populateJsonTableScalar: table == null");
        }
        if (tableName == null) {
            fail("populateJsonTableScalar: tableName == null");
        }
        if (nRows <= 0) {
            fail("populateJsonTableScalar: nRows must be positive " +
                 "[nRows = " + nRows + "]");
        }

        for (int i = 0; i < nRows; i++) {
            addJsonRowScalar(tableApi, table, i);
        }

        displayRows(tableApi, table);

        final long nRowsAdded = nRowsInTable(tableApi, table);

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("populateJsonTableScalar: # of rows requested = " +
                        nRows + ", # of rows added to table [" + tableName +
                        "] = " + nRowsAdded);
            logger.fine(" ");
        }
    }

    private void addJsonRowScalar(final TableAPI tableApi,
                                  final Table table,
                                  final int rowIndx) {

        final Integer jsonFieldIntegerVal = INTEGER_VALS[rowIndx];
        final Long jsonFieldLongVal = LONG_VALS[rowIndx];
        final Double jsonFieldDoubleVal = DOUBLE_VALS[rowIndx];
        final Number jsonFieldNumberVal = NUMBER_VALS[rowIndx];
        final String jsonFieldStringVal = STRING_VALS[rowIndx];
        final Boolean jsonFieldBooleanVal = BOOLEAN_VALS[rowIndx];
        final String jsonFieldDateVal = DATE_VALS[rowIndx];

        final String jsonFieldVal =
            "{" +
                  "\"" +
                  JSON_FIELD_INTEGER_NAME +
                  "\"" +
                  ":" +
                  jsonFieldIntegerVal +

                  "," +

                  "\"" +
                  JSON_FIELD_LONG_NAME +
                  "\"" +
                  ":" +
                  jsonFieldLongVal +

                  "," +

                  "\"" +
                  JSON_FIELD_DOUBLE_NAME +
                  "\"" +
                  ":" +
                  jsonFieldDoubleVal +

                  "," +

                  "\"" +
                  JSON_FIELD_NUMBER_NAME +
                  "\"" +
                  ":" +
                  jsonFieldNumberVal +

                  "," +

                  "\"" +
                  JSON_FIELD_STRING_NAME +
                  "\"" +
                  ":" +
                  "\"" +
                  jsonFieldStringVal +
                  "\"" +

                  "," +

                  "\"" +
                  JSON_FIELD_BOOLEAN_NAME +
                  "\"" +
                  ":" +
                  jsonFieldBooleanVal +

                  "," +

                  "\"" +
                  JSON_FIELD_DATE_NAME +
                  "\"" +
                  ":" +
                  "\"" +
                  jsonFieldDateVal +
                  "\"" +
            "}";

        final Row row = table.createRow();

        row.put(JSON_ID_FIELD_NAME, rowIndx);
        try {
            row.putJson(JSON_FIELD_NAME, jsonFieldVal);
        } catch (Exception e) {
            logger.severe("Exception on put: name = " + JSON_FIELD_NAME +
                          ", val = " + jsonFieldVal);
            throw e;
        }
        tableApi.putIfAbsent(row, null, null);
    }

    private void populateJsonTableSenators(final TableAPI tableApi,
                                        final Table table,
                                        final String tableName,
                                        final int nRows) {
        if (tableApi == null) {
            fail("populateJsonTableSenators: tableApi == null");
        }
        if (table == null) {
            fail("populateJsonTableSenators: table == null");
        }
        if (tableName == null) {
            fail("populateJsonTableSenators: tableName == null");
        }
        if (nRows <= 0) {
            fail("populateJsonTableSenators: nRows must be positive " +
                 "[nRows = " + nRows + "]");
        }

        for (int i = 0; i < nRows; i++) {
            addJsonRowSenators(tableApi, table, i);
        }

        displayRows(tableApi, table);

        final long nRowsAdded = nRowsInTable(tableApi, table);

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("populateJsonTableSenators: # of rows requested = " +
                        nRows + ", # of rows added to table [" + tableName +
                        "] = " + nRowsAdded);
            logger.fine(" ");
        }
    }

    private void addJsonRowSenators(final TableAPI tableApi,
                                    final Table table,
                                    final int rowIndx) {

        final String jsonFieldVal = JSON_SENATOR_INFO[rowIndx];

        final Row row = table.createRow();

        row.put(JSON_ID_FIELD_NAME, rowIndx);
        row.putJson(JSON_FIELD_NAME, jsonFieldVal);

        tableApi.putIfAbsent(row, null, null);
    }

    private void addInconsistentJsonRowSenators(final TableAPI tableApi,
                                                final Table table,
                                                final int rowIndx) {

        final Row row = table.createRow();

        row.put(JSON_ID_FIELD_NAME, rowIndx);
        row.putJson(JSON_FIELD_NAME, JSON_SENATOR_INCONSISTENT_STRUCTURE);

        tableApi.putIfAbsent(row, null, null);
    }

    private long nRowsInTable(final TableAPI tableApi, final Table tbl) {
        final TableIterator<PrimaryKey> itr =
            tableApi.tableKeysIterator(tbl.createPrimaryKey(), null, null);
        long cnt = 0;
        while (itr.hasNext()) {
            itr.next();
            cnt++;
        }
        itr.close();
        return cnt;
    }

    private void displayRows(final TableAPI tableApi, final Table tbl) {
        final TableIterator<Row> itr =
            tableApi.tableIterator(tbl.createPrimaryKey(), null, null);
        if (logger.isLoggable(Level.FINE)) {
            while (itr.hasNext()) {
                logger.fine(itr.next().toString());
            }
        }
        itr.close();
    }

    private void verifyIndexedJsonScalar(final TableAPI tableApi,
                                         final Table table,
                                         final String esIndexName,
                                         final int nRows) {

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("elasticsearch index name: " + esIndexName);
        }

        final Set<String> missedKeys = new HashSet<>();
        final Set<String> failedKeys = new HashSet<>();

        GetResponse getResponse = null;

        final TableIterator<Row> itr =
            tableApi.tableIterator(table.createPrimaryKey(), null, null);

        while (itr.hasNext()) {

            final Row row = itr.next();
            final int curId = row.get(JSON_ID_FIELD_NAME).asInteger().get();

            final String keyStr =
                TableKey.createKey(
                    jsonTableScalar, row, false).getKey().toString();

            try {
                getResponse = esHandler.get(esIndexName, keyStr);
            } catch (IOException e) {
                fail("verifyIndexedJsonScalar: GetResponse failed " +
                     "[" + e + "]");
                return;
            }

            if (!getResponse.isFound()) {

                missedKeys.add(keyStr);
                logger.info("missing key [" + keyStr + "]: row = " + row);

            } else {

                ObjectNode root = JsonUtils.parseJsonObject(
                        new String(getResponse.source()));
                final String stringField = JSON_FIELD_STRING_NAME;
                final String integerField = JSON_FIELD_INTEGER_NAME;
                final String longField = JSON_FIELD_LONG_NAME;
                final String doubleField = JSON_FIELD_DOUBLE_NAME;
                final String numberField = JSON_FIELD_NUMBER_NAME;
                final String booleanField = JSON_FIELD_BOOLEAN_NAME;
                final String dateField = JSON_FIELD_DATE_NAME;

                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(" ");
                    logger.finest("[key=" + keyStr + "]: row = " + row);
                    logger.finest("[key=" + keyStr + "]: map = " + root);
                }
                final String stringVal = root.findFirst(stringField).asText();
                final String expectString = STRING_VALS[curId];

                Integer integerVal = root.findFirst(integerField).asInt();
                final Integer expectInteger = INTEGER_VALS[curId];

                Long longVal = root.findFirst(longField).asLong();
                final Number expectNumber = NUMBER_VALS[curId];

                Double doubleVal = root.findFirst(doubleField).asDouble();
                final Double expectDouble = DOUBLE_VALS[curId];

                Number numberVal = root.findFirst(numberField).asLong();
                final Long expectLong = LONG_VALS[curId];

                final Boolean booleanVal =
                    root.findFirst(booleanField).asBoolean();
                final Boolean expectBoolean = BOOLEAN_VALS[curId];

                final String dateVal = root.findFirst(dateField).asText();
                final String expectDate = DATE_VALS[curId];

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(" ");
                    logger.fine("-------------------------------------------");
                    logger.fine(stringField + "  retrieved: " + stringVal);
                    logger.fine(stringField + "  expected:  " + expectString);
                    logger.fine(integerField + " retrieved: " + integerVal);
                    logger.fine(integerField + " expected:  " + expectInteger);
                    logger.fine(longField + "    retrieved: " + longVal);
                    logger.fine(longField + "    expected:  " + expectLong);
                    logger.fine(doubleField + "  retrieved: " + doubleVal);
                    logger.fine(doubleField + "  expected:  " + expectDouble);
                    logger.fine(numberField + "  retrieved: " + numberVal);
                    logger.fine(numberField + "  expected:  " + expectNumber);
                    logger.fine(booleanField + " retrieved: " + booleanVal);
                    logger.fine(booleanField + " expected:  " + expectBoolean);
                    logger.fine(dateField + "    retrieved: " + dateVal);
                    logger.fine(dateField + "    expected:  " + expectDate);
                    logger.fine("-------------------------------------------");
                    logger.fine(" ");
                }

                if (!expectString.equals(stringVal)) {
                    failedKeys.add(keyStr);
                    logger.info(stringField + " failed verification: " +
                                "retrieved: " + stringVal + ", expected: " +
                                expectString);
                }

                if (!expectInteger.equals(integerVal)) {
                    failedKeys.add(keyStr);
                    logger.info(integerField + " failed verification: " +
                                "retrieved: " + integerVal + ", expected: " +
                                expectInteger);
                }

                if (!expectLong.equals(longVal)) {
                    failedKeys.add(keyStr);
                    logger.info(longField + " failed verification: " +
                                "retrieved: " + longVal + ", expected: " +
                                expectLong);
                }

                /*
                 * The retrieved double may have lost some accuracy
                 * when being handled by elasticsearch. So rather than
                 * comparing the expected double and retrieved double
                 * directly, compare their respective unit numbers (ulps).
                 */
                final double ulpExpected = Math.ulp(expectDouble);
                final double ulpRetrieved = Math.ulp(doubleVal);
                final double deltaUlp = ulpExpected - ulpRetrieved;
                if (deltaUlp != 0.0) {
                    failedKeys.add(keyStr);
                    logger.info(doubleField + " failed verification: " +
                                "retrieved: " + doubleVal +
                                " (ulp=" + ulpRetrieved + "), expected: " +
                                expectDouble + "(ulp=" + ulpExpected + ")");
                }

                if (!expectBoolean.equals(booleanVal)) {
                    failedKeys.add(keyStr);
                    logger.info(booleanField + " failed verification: " +
                                "retrieved: " + booleanVal + ", expected: " +
                                expectBoolean);
                }

                if (!expectDate.equals(dateVal)) {
                    failedKeys.add(keyStr);
                    logger.info(dateField + " failed verification: " +
                                "retrieved: " + dateVal + ", expected: " +
                                expectDate);
                }
            }
        }
        itr.close();

        if (!missedKeys.isEmpty()) {
            fail("verifyIndexedJsonScalar: failed to retrieve " +
                 missedKeys.size() + " keys from elasticsearch");
        } else if (!failedKeys.isEmpty()) {
            fail("verifyIndexedJsonScalar: verification failed for " +
                 failedKeys.size() + " keys retrieved from elasticsearch");
        } else {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("verifyIndexedJsonScalar: all " + nRows +
                            " JSON documents from elasticsearch " +
                            "successfully verified");
                logger.info(" ");
            }
        }
    }

    private void verifyIndexedJsonSenators(final TableAPI tableApi,
                                           final Table table,
                                           final String esIndexName,
                                           final int nRows) {

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("elasticsearch index name: " + esIndexName);
        }

        GetResponse getResponse = null;

        final TableIterator<Row> itr =
            tableApi.tableIterator(table.createPrimaryKey(), null, null);

        while (itr.hasNext()) {

            final Row row = itr.next();
            final int curId = row.get(JSON_ID_FIELD_NAME).asInteger().get();

            final String keyStr =
                TableKey.createKey(
                    jsonTableSenators, row, false).getKey().toString();

            try {
                getResponse = esHandler.get(esIndexName, keyStr);
            } catch (IOException e) {
                fail("verifyIndexedJsonSenators: GetResponse failed " +
                     "[" + e + "]");
                return;
            }

            if (!getResponse.isFound()) {

                fail("verifyIndexedJsonSenators: response not found, " +
                     "missing key [" + keyStr + "]: row = " + row);

            } else {

                final Map<String, Object> sourceMap =
                    getResponse.sourceAsMap();

                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(" ");
                    logger.finest("verifyIndexedJsonSenators: " +
                                  "[key=" + keyStr + "]: row = " + row);
                    logger.finest("verifyIndexedJsonSenators: " +
                                  "[key=" + keyStr + "]: map = " + sourceMap);
                }

                final Map<String, String> responseMap =
                            EXPECTED_SENATOR_RESPONSE.get(curId);

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("verifyIndexedJsonSenators: " +
                                "expectedResponseRow[id=" + curId + "]");
                    for (Map.Entry<String, String> entry :
                                 responseMap.entrySet()) {
                        logger.fine("  " + entry.getKey() + " : " +
                                    entry.getValue());
                    }
                }

                ObjectNode root = JsonUtils.parseJsonObject(
                        new String(getResponse.source()));
                for (Map.Entry<String, String> entry :
                        responseMap.entrySet()) {

                    final String fieldName = entry.getKey();
                    String fName =
                        fieldName.substring(fieldName.lastIndexOf(".") + 1);
                    final String expectedVal =
                        entry.getValue().replace(" ", "");
                    JsonNode leaf = root.findFirst(fName);
                    final String receivedValStr =
                        leaf.toString().replace("\"", "").replace(" ", ""); 
                    assertEquals("for indexed document, expected value not " +
                            "equal to indexed value: id= " + curId +
                            ", fieldName=" + fieldName + ", expected=" +
                            expectedVal + ", received=" + receivedValStr,
                            expectedVal, receivedValStr);
                }
            }
        }
        itr.close();

        if (logger.isLoggable(Level.INFO)) {
            logger.info("verifyIndexedJsonSenators: all " + nRows +
                        " JSON sentator documents from elasticsearch " +
                        "successfully verified");
            logger.info(" ");
        }

    }

    private void verifyIndexedJsonSenatorsInBehaviorTests
                                          (final TableAPI tableApi,
                                           final Table table,
                                           final String esIndexName,
                                           final int nRows) {


        if (logger.isLoggable(Level.FINE)) {
            logger.fine("elasticsearch index name: " + esIndexName);
        }

        GetResponse getResponse = null;

        final TableIterator<Row> itr =
            tableApi.tableIterator(table.createPrimaryKey(), null, null);

        while (itr.hasNext()) {

            final Row row = itr.next();
            final int curId = row.get(JSON_ID_FIELD_NAME).asInteger().get();

            final String keyStr =
                TableKey.createKey(
                    jsonTableSenators, row, false).getKey().toString();

            try {
                getResponse = esHandler.get(esIndexName, keyStr);
            } catch (IOException e) {
                fail("verifyIndexedJsonSenators: GetResponse failed " +
                     "[" + e + "]");
                return;
            }

            if (!getResponse.isFound()) {

                fail("verifyIndexedJsonSenators: response not found, " +
                     "missing key [" + keyStr + "]: row = " + row);

            } else {

                final Map<String, Object> sourceMap =
                    getResponse.sourceAsMap();

                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(" ");
                    logger.finest("verifyIndexedJsonSenators: " +
                                  "[key=" + keyStr + "]: row = " + row);
                    logger.finest("verifyIndexedJsonSenators: " +
                                  "[key=" + keyStr + "]: map = " + sourceMap);
                }

                final Map<String, String> responseMap =
                        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(curId);

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("verifyIndexedJsonSenators: " +
                                "expectedResponseRow[id=" + curId + "]");
                    for (Map.Entry<String, String> entry :
                                 responseMap.entrySet()) {
                        logger.fine("  " + entry.getKey() + " : " +
                                    entry.getValue());
                    }
                }

                ObjectNode root = JsonUtils.parseJsonObject(
                        new String(getResponse.source()));

                for (Map.Entry<String, String> entry :
                        responseMap.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        continue;
                    }
                    final String fieldName = entry.getKey();
                    String fName =
                        fieldName.substring(fieldName.lastIndexOf(".") + 1);
                    final String expectedVal = entry.getValue().replace(" ", "");
                    JsonNode leaf = root.findFirst(fName);
                    final String receivedValStr = leaf.toString().
                        replace("\"", "").replace(" ", ""); 
                    assertEquals("for indexed document, expected value not " +
                            "equal to indexed value: id= " + curId +
                            ", fieldName=" + fieldName + ", expected=" +
                            expectedVal + ", received=" + receivedValStr,
                            expectedVal, receivedValStr);
                }
            }
        }
        itr.close();

        if (logger.isLoggable(Level.INFO)) {
            logger.info("verifyIndexedJsonSenators: all " + nRows +
                        " JSON sentator documents from elasticsearch " +
                        "successfully verified");
            logger.info(" ");
        }
    }

    /*
     * Make sure each array containing the expected JSON values are
     * initialized with exactly N_JSON_ROWS_XXXX (N_JSON_ROWS_SCALAR,
     * N_JSON_ROWS_SENATORS, etc.) number of elements; otherwise
     * an ArrayIndexOutOfBoundException will occur.
     */
    private static final int N_JSON_ROWS_SCALAR = 10;

    private static final Integer[] INTEGER_VALS =
    {
        1234, 2134, 2314, 2341, 3241, 3421, 3412, 2341, 2431, 2431
    };

    private static final Long[] LONG_VALS =
    {
        987654321L, 897654321L, 879654321L, 876954321L, 876594321L,
        876549321L, 876543921L, 876543291L, 876543219L, 123456789L
    };

    private static final Double[] DOUBLE_VALS =
    {
        987654321.1234D, 897654321.2134D, 879654321.2314D, 87695432.2341D,
        876594321.3241D, 876549321.3421D, 876543921.3412D, 876543291.2341D,
        876543219.2431D, 123456789.2431
    };

    private static final Number[] NUMBER_VALS =
    {
        123456789L, 987654321L, 213456789L, 231456789L, 234156789L,
        234516789L, 234561789L, 234567189L, 234567819L, 234567891L
    };

    private static final String[] STRING_VALS =
    {
      "String 0 - fruit: apple peach blueberry pear",
      "String 1 - vehicle: auto truck suv pickup camper",
      "String 2 - city: detroit denver desmoines chicago rochester",
      "String 3 - state: michigan colorado iowa illinois new york",
      "String 4 - county: wayne denver polk cook westchester",
      "String 5 - gender: male female",
      "String 6 - street: vaughan oak mill wesley county road 17 spring",
      "String 7 - color: red green yellow blue orange gold purple",
      "String 8 - tree: oak maple elm cherry apple christmas cottonwood",
      "String 9 - tool: hammer screw driver saw wrench pliers scissors"
    };

    private static final Boolean[] BOOLEAN_VALS =
    {true, false, true, false, true, false, true, false, true, false};

    private static final String[] DATE_VALS =
    {
      "1955-10-26T01:02:03.123",
      "2019-12-08T01:02:03.000",
      "1964-09-04T01:02:03.789",
      "2010-11-02T01:02:03.557",
      "1959-08-06T01:02:03.000",
      "1953-09-15T01:02:03.546",
      "2006-02-17T01:02:03.223",
      "1956-04-06T01:02:03.321",
      "1957-10-17T01:02:03.000",
      "1954-06-04T01:02:03.778"
    };

    private static final int N_JSON_ROWS_SENATORS = 3;
    private static final String DQ = "\"";

    /*
     * Each of the 3 elements of the JSON_SENATOR_INFO array initialized here
     * is written to the kvstore senators table as a json document when the
     * table is populated for the associated test.
     */
    private static final String[] JSON_SENATOR_INFO =
    {
"{" +
 DQ + "caucus" + DQ + ": null," +
 DQ + "congress_numbers" + DQ + ": [ " +
   "113, " +
   "114, " +
   "115" +
   " ], " +
 DQ + "current" + DQ + ": true, " +
 DQ + "description" + DQ + ": " + DQ + "Junior Senator for Wisconsin" +
      DQ + ", " +
 DQ + "district" + DQ + ": null, " +
 DQ + "enddate" + DQ + ": " + DQ + "2019-01-03T01:02:03.123456789" +
      DQ + ", " +
 DQ + "extra" + DQ + ": { " +
      DQ + "address" + DQ + ": " + DQ +
          "709 Hart Senate Office Building Washington DC 20510" + DQ + ", " +
      DQ + "contact_form" + DQ + ": " + DQ +
          "https://www.baldwin.senate.gov/feedback" + DQ + ", " +
      DQ + "fax" + DQ + ": " + DQ + "202-225-6942" + DQ + ", " +
      DQ + "office" + DQ + ": " + DQ + "709 Hart Senate Office Building" +
          DQ + ", " +
      DQ + "rss_url" + DQ + ": " + DQ +
          "http://www.baldwin.senate.gov/rss/feeds/?type=all" + DQ +
      "}, " +
 DQ + "leadership_title" + DQ + ": null, " +
 DQ + "party" + DQ + ": " + DQ + "Democrat" + DQ + ", " +
 DQ + "person" + DQ + ": { " +
      DQ + "bioguideid" + DQ + ": " + DQ + "B001230" + DQ + ", " +
      DQ + "birthday" + DQ + ": " + DQ + "1962-02-11" + DQ + ", " +
      DQ + "cspanid" + DQ + ": 57884, " +
      DQ + "firstname" + DQ + ": " + DQ + "Tammy" + DQ + ", " +
      DQ + "gender" + DQ + ": " + DQ + "female" + DQ + ", " +
      DQ + "gender_label" + DQ + ": " + DQ + "Female" + DQ + ", " +
      DQ + "lastname" + DQ + ": " + DQ + "Baldwin" + DQ + ", " +
      DQ + "link" + DQ + ": " + DQ +
          "https://www.govtrack.us/congress/members/tammy_baldwin/400013" +
          DQ + ", " +
      DQ + "middlename" + DQ + ": " + DQ + DQ + ", " +
      DQ + "name" + DQ + ": " + DQ + "Sen. Tammy Baldwin [D-WI]" + DQ + ", " +
      DQ + "namemod" + DQ + ": " + DQ + DQ + ", " +
      DQ + "nickname" + DQ + ": " + DQ + DQ + ", " +
      DQ + "osid" + DQ + ": "  + DQ + "N00004367" + DQ + ", " +
      DQ + "pvsid" + DQ + ": " + DQ + "3470" + DQ + ", " +
      DQ + "sortname" + DQ + ": " + DQ + "Baldwin, Tammy (Sen.) [D-WI]" +
          DQ + ", " +
      DQ + "twitterid" + DQ + ": " + DQ + "SenatorBaldwin" + DQ + ", " +
      DQ + "youtubeid" + DQ + ": " + DQ + "witammybaldwin" + DQ +
      "}, " +
 DQ + "phone" + DQ + ": " + DQ + "202-224-5653" + DQ + ", " +
 DQ + "role_type" + DQ + ": " + DQ + "senator" + DQ + ", " +
 DQ + "role_type_label" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "senator_class" + DQ + ": " + DQ + "class1" + DQ + ", " +
 DQ + "senator_class_label" + DQ + ": " + DQ + "Class 1" + DQ + ", " +
 DQ + "senator_rank" + DQ + ": " + DQ + "junior" + DQ + ", " +
 DQ + "senator_rank_label" + DQ + ": " + DQ + "Junior" + DQ + ", " +
 DQ + "startdate" + DQ + ": " + DQ + "2013-01-03T03:02:01.123" + DQ + ", " +
 DQ + "state" + DQ + ": " + DQ + "WI" + DQ + ", " +
 DQ + "title" + DQ + ": " + DQ + "Sen." + DQ + ", " +
 DQ + "title_long" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "website" + DQ + ": " + DQ + "https://www.baldwin.senate.gov" + DQ +
" }",

"{" +
 DQ + "caucus" + DQ + ": null, " +
 DQ + "congress_numbers" + DQ + ": [ " +
  "213, " +
  "214, " +
  "215" +
  " ], " +
 DQ + "current" + DQ + ": true, " +
 DQ + "description" + DQ + ": " + DQ + "Senior Senator for Ohio" + DQ + ", " +
 DQ + "district" + DQ + ": null, " +
 DQ + "enddate" + DQ + ": " + DQ + "2020-01-05T03:01:02.567812349" +
      DQ + ", " +
 DQ + "extra" + DQ + ": { " +
      DQ + "address" + DQ + ": " + DQ +
          "713 Hart Senate Office Building Washington DC 20510" +  DQ + ", " +
      DQ + "contact_form" + DQ + ": " + DQ +
          "http://www.brown.senate.gov/contact/" + DQ + ", " +
      DQ + "fax" + DQ + ": " + DQ + "202-228-6321" + DQ + ", " +
      DQ + "office" + DQ + ": " + DQ + "713 Hart Senate Office Building" +
          DQ + ", " +
      DQ + "rss_url" + DQ + ": " + DQ +
          "http://www.brown.senate.gov/rss/feeds/?type=all&amp;" + DQ +
      "}, " +
 DQ + "leadership_title" + DQ + ": null, " +
 DQ + "party" + DQ + ": " + DQ + "Republican" + DQ + ", " +
 DQ + "person" + DQ + ": { " +
      DQ + "bioguideid" + DQ + ": " + DQ + "B000944" + DQ + ", " +
      DQ + "birthday" + DQ + ": " + DQ + "1952-11-09" + DQ + ", " +
      DQ + "cspanid" + DQ + ": 5051, " +
      DQ + "firstname" + DQ + ": " + DQ + "Sherrod" + DQ + ", " +
      DQ + "gender" + DQ + ": " + DQ + "male" + DQ + ", " +
      DQ + "gender_label" + DQ + ": " + DQ + "Male" + DQ + ", " +
      DQ + "lastname" + DQ + ": " + DQ + "Brown" + DQ + ", " +
      DQ + "link" + DQ + ": " + DQ +
          "https://www.govtrack.us/congress/members/sherrod_brown/400050" +
          DQ + ", " +
      DQ + "middlename" + DQ + ": " + DQ + DQ + ", " +
      DQ + "name" + DQ + ": " + DQ + "Sen. Sherrod Brown [D-OH]" + DQ + ", " +
      DQ + "namemod" + DQ + ": " + DQ + DQ + ", " +
      DQ + "nickname" + DQ + ": " + DQ + DQ + ", " +
      DQ + "osid" + DQ + ": " + DQ + "N00003535" + DQ + ", " +
      DQ + "pvsid" + DQ + ": " + DQ + "27018" + DQ + ", " +
      DQ + "sortname" + DQ + ": " + DQ + "Brown, Sherrod (Sen.) [D-OH]" +
          DQ + ", " +
      DQ + "twitterid" + DQ + ": " + DQ + "SenSherrodBrown" + DQ + ", " +
      DQ + "youtubeid" + DQ + ": " + DQ + "SherrodBrownOhio" + DQ +
      "}, " +
 DQ + "phone" + DQ + ": " + DQ + "202-224-2315" + DQ + ", " +
 DQ + "role_type" + DQ + ": " + DQ + "senator" + DQ + ", " +
 DQ + "role_type_label" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "senator_class" + DQ + ": " + DQ + "class1" + DQ + ", " +
 DQ + "senator_class_label" + DQ + ": " + DQ + "Class 1" + DQ + ", " +
 DQ + "senator_rank" + DQ + ": " + DQ + "senior" + DQ + ", " +
 DQ + "senator_rank_label" + DQ + ": " + DQ + "Senior" + DQ + ", " +
 DQ + "startdate" + DQ + ": " + DQ + "2010-01-03T05:04:09.456" + DQ + ", " +
 DQ + "state" + DQ + ": " + DQ + "OH" + DQ + ", " +
 DQ + "title" + DQ + ": " + DQ + "Sen." + DQ + ", " +
 DQ + "title_long" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "website" + DQ + ": " + DQ + "https://www.brown.senate.gov" + DQ +
" }",

"{ " +
 DQ + "caucus" + DQ + ": null, " +
 DQ + "congress_numbers" + DQ + ": [ " +
   "313, " +
   "314, " +
   "315" +
   " ], " +
 DQ + "current" + DQ + ": true, " +
 DQ + "description" + DQ + ": " + DQ + "Senior Senator for Maryland" +
      DQ + ", " +
 DQ + "district" + DQ + ": null, " +
 DQ + "enddate" + DQ + ": " + DQ + "2022-01-03T11:59:59.918273645" +
      DQ + ", " +
 DQ + "extra" + DQ + ": { " +
      DQ + "address" + DQ + ": " + DQ +
          "509 Hart Senate Office Building Washington DC 20510" + DQ + ", " +
      DQ + "contact_form" + DQ + ": " + DQ +
          "http://www.cardin.senate.gov/contact/" + DQ + ", " +
      DQ + "fax" + DQ + ": " + DQ + "202-224-1651" + DQ + ", " +
      DQ + "office" + DQ + ": " + DQ + "509 Hart Senate Office Building" +
          DQ + ", " +
      DQ + "rss_url" + DQ + ": " + DQ +
          "http://www.cardin.senate.gov/rss/feeds/?type=all" + DQ +
      "}, " +
 DQ + "leadership_title" + DQ + ": null, " +
 DQ + "party" + DQ + ": " + DQ + "Democrat" + DQ + ", " +
 DQ + "person" + DQ + ": { " +
      DQ + "bioguideid" + DQ + ": " + DQ + "C000141" + DQ + ", " +
      DQ + "birthday" + DQ + ": " + DQ + "1943-10-05" + DQ + ", " +
      DQ + "cspanid" + DQ + ": 4004, " +
      DQ + "firstname" + DQ + ": " + DQ + "Benjamin" + DQ + ", " +
      DQ + "gender" + DQ + ": " + DQ + "male" + DQ + ", " +
      DQ + "gender_label" + DQ + ": " + DQ + "Male" + DQ + ", " +
      DQ + "lastname" + DQ + ": " + DQ + "Cardin" + DQ + ", " +
      DQ + "link" + DQ + ": " + DQ +
          "https://www.govtrack.us/congress/members/benjamin_cardin/400064" +
          DQ + ", " +
      DQ + "middlename" + DQ + ": " + DQ + "L." + DQ + ", " +
      DQ + "name" + DQ + ": " + DQ + "Sen. Benjamin Cardin [D-MD]" +
          DQ + ", " +
      DQ + "namemod" + DQ + ": " + DQ + DQ + ", " +
      DQ + "nickname" + DQ + ": " + DQ + DQ + ", " +
      DQ + "osid" + DQ + ": " + DQ + "N00001955" + DQ + ", " +
      DQ + "pvsid" + DQ + ": " + DQ + "26888" + DQ + ", " +
      DQ + "sortname" + DQ + ": " +  DQ + "Cardin, Benjamin (Sen.) [D-MD]" +
          DQ + ", " +
      DQ + "twitterid" + DQ + ": " + DQ + "SenatorCardin" + DQ + ", " +
      DQ + "youtubeid" + DQ + ": " + DQ + "senatorcardin" + DQ +
      "}, " +
 DQ + "phone" + DQ + ": " + DQ + "202-224-4524" + DQ + ", " +
 DQ + "role_type" + DQ + ": " + DQ + "senator" + DQ + ", " +
 DQ + "role_type_label" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "senator_class" + DQ + ": " + DQ + "class1" + DQ + ", " +
 DQ + "senator_class_label" + DQ + ": " + DQ + "Class 1" + DQ + ", " +
 DQ + "senator_rank" + DQ + ": " + DQ + "senior" + DQ + ", " +
 DQ + "senator_rank_label" + DQ + ": " + DQ + "Senior" + DQ + ", " +
 DQ + "startdate" + DQ + ": " + DQ + "2009-01-03T12:00:00.789" + DQ + ", " +
 DQ + "state" + DQ + ": " + DQ + "MD" + DQ + ", " +
 DQ + "title" + DQ + ": " + DQ + "Sen." + DQ + ", " +
 DQ + "title_long" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "website" + DQ + ": " + DQ + "https://www.cardin.senate.gov" + DQ +
" }"
    };

    /*
     * This is written to the kvstore senators table as a json document
     * when doing the testJsonIndexBehaviors test. This json document has
     * different structure compare to the rest of the json documents, that is,
     * missing field congress_numbers.
     */
    private static final String JSON_SENATOR_INCONSISTENT_STRUCTURE =
"{" +
 DQ + "caucus" + DQ + ": null," +
 /* congress_numbers is removed to make this json doc has different structure*/
// DQ + "congress_numbers" + DQ + ": [ " +
//   "413, " +
//   "414, " +
//   "415" +
//   " ], " +
 DQ + "current" + DQ + ": true, " +
 DQ + "description" + DQ + ": " + DQ + "Junior Senator for California" +
      DQ + ", " +
 DQ + "district" + DQ + ": null, " +
 DQ + "enddate" + DQ + ": " + DQ + "2019-01-03T01:02:03.123456789" +
      DQ + ", " +
 DQ + "extra" + DQ + ": { " +
      DQ + "address" + DQ + ": " + DQ +
          "709 Hart Senate Office Building Washington DC 20510" + DQ + ", " +
      DQ + "contact_form" + DQ + ": " + DQ +
          "https://www.baldwin.senate.gov/feedback" + DQ + ", " +
      DQ + "fax" + DQ + ": " + DQ + "202-225-6942" + DQ + ", " +
      DQ + "office" + DQ + ": " + DQ + "709 Hart Senate Office Building" +
          DQ + ", " +
      DQ + "rss_url" + DQ + ": " + DQ +
          "http://www.baldwin.senate.gov/rss/feeds/?type=all" + DQ +
      "}, " +
 DQ + "leadership_title" + DQ + ": null, " +
 DQ + "party" + DQ + ": " + DQ + "Democrat" + DQ + ", " +
 DQ + "person" + DQ + ": { " +
      DQ + "bioguideid" + DQ + ": " + DQ + "B001230" + DQ + ", " +
      DQ + "birthday" + DQ + ": " + DQ + "1962-02-11" + DQ + ", " +
      DQ + "cspanid" + DQ + ": 57884, " +
      DQ + "firstname" + DQ + ": " + DQ + "Tammy" + DQ + ", " +
      DQ + "gender" + DQ + ": " + DQ + "female" + DQ + ", " +
      DQ + "gender_label" + DQ + ": " + DQ + "Female" + DQ + ", " +
      DQ + "lastname" + DQ + ": " + DQ + "Baldwin" + DQ + ", " +
      DQ + "link" + DQ + ": " + DQ +
          "https://www.govtrack.us/congress/members/tammy_baldwin/400013" +
          DQ + ", " +
      DQ + "middlename" + DQ + ": " + DQ + DQ + ", " +
      DQ + "name" + DQ + ": " + DQ + "Sen. Tammy Baldwin [D-WI]" + DQ + ", " +
      DQ + "namemod" + DQ + ": " + DQ + DQ + ", " +
      DQ + "nickname" + DQ + ": " + DQ + DQ + ", " +
      DQ + "osid" + DQ + ": "  + DQ + "N00004367" + DQ + ", " +
      DQ + "pvsid" + DQ + ": " + DQ + "3470" + DQ + ", " +
      DQ + "sortname" + DQ + ": " + DQ + "Baldwin, Tammy (Sen.) [D-WI]" +
          DQ + ", " +
      DQ + "twitterid" + DQ + ": " + DQ + "SenatorBaldwin" + DQ + ", " +
      DQ + "youtubeid" + DQ + ": " + DQ + "witammybaldwin" + DQ +
      "}, " +
 DQ + "phone" + DQ + ": " + DQ + "202-224-5653" + DQ + ", " +
 DQ + "role_type" + DQ + ": " + DQ + "senator" + DQ + ", " +
 DQ + "role_type_label" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "senator_class" + DQ + ": " + DQ + "class1" + DQ + ", " +
 DQ + "senator_class_label" + DQ + ": " + DQ + "Class 1" + DQ + ", " +
 DQ + "senator_rank" + DQ + ": " + DQ + "junior" + DQ + ", " +
 DQ + "senator_rank_label" + DQ + ": " + DQ + "Junior" + DQ + ", " +
 DQ + "startdate" + DQ + ": " + DQ + "2013-01-03T03:02:01.123" + DQ + ", " +
 DQ + "state" + DQ + ": " + DQ + "WI" + DQ + ", " +
 DQ + "title" + DQ + ": " + DQ + "Sen." + DQ + ", " +
 DQ + "title_long" + DQ + ": " + DQ + "Senator" + DQ + ", " +
 DQ + "website" + DQ + ": " + DQ + "https://www.baldwin.senate.gov" + DQ + " }";

    /*
     * The contents of the EXPECTED_SENATOR_RESPONS list is used in the
     * verification process by the associated test.
     */
    private static final List<Map<String, String>> EXPECTED_SENATOR_RESPONSE
        = new ArrayList<>();
    static {
        EXPECTED_SENATOR_RESPONSE.add(new HashMap<>());
        EXPECTED_SENATOR_RESPONSE.add(new HashMap<>());
        EXPECTED_SENATOR_RESPONSE.add(new HashMap<>());

        EXPECTED_SENATOR_RESPONSE.get(0).put(congressNumbers,
        "[113, 114, 115]");
        EXPECTED_SENATOR_RESPONSE.get(0).put(description,
        "Junior Senator for Wisconsin");
        EXPECTED_SENATOR_RESPONSE.get(0).put(extraAddress,
        "709 Hart Senate Office Building Washington DC 20510");
        EXPECTED_SENATOR_RESPONSE.get(0).put(party,
        "Democrat");
        EXPECTED_SENATOR_RESPONSE.get(0).put(personCspanId,
        "57884");
        EXPECTED_SENATOR_RESPONSE.get(0).put(personGender,
        "female");
        EXPECTED_SENATOR_RESPONSE.get(0).put(website,
        "https://www.baldwin.senate.gov");

        EXPECTED_SENATOR_RESPONSE.get(1).put(congressNumbers,
        "[213, 214, 215]");
        EXPECTED_SENATOR_RESPONSE.get(1).put(description,
        "Senior Senator for Ohio");
        EXPECTED_SENATOR_RESPONSE.get(1).put(extraAddress,
        "713 Hart Senate Office Building Washington DC 20510");
        EXPECTED_SENATOR_RESPONSE.get(1).put(party,
        "Republican");
        EXPECTED_SENATOR_RESPONSE.get(1).put(personCspanId,
        "5051");
        EXPECTED_SENATOR_RESPONSE.get(1).put(personGender,
        "male");
        EXPECTED_SENATOR_RESPONSE.get(1).put(website,
        "https://www.brown.senate.gov");

        EXPECTED_SENATOR_RESPONSE.get(2).put(congressNumbers,
        "[313, 314, 315]");
        EXPECTED_SENATOR_RESPONSE.get(2).put(description,
        "Senior Senator for Maryland");
        EXPECTED_SENATOR_RESPONSE.get(2).put(extraAddress,
        "509 Hart Senate Office Building Washington DC 20510");
        EXPECTED_SENATOR_RESPONSE.get(2).put(party,
        "Democrat");
        EXPECTED_SENATOR_RESPONSE.get(2).put(personCspanId,
        "4004");
        EXPECTED_SENATOR_RESPONSE.get(2).put(personGender,
        "male");
        EXPECTED_SENATOR_RESPONSE.get(2).put(website,
        "https://www.cardin.senate.gov");
    }

    /*
     * The contents of the EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST list is
     * used in the verification process by the testing the creation of two
     * indexes that have same fields but in different orders.
     */
    private static final List<Map<String, String>>
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST = new ArrayList<>();
    static {
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.add(new HashMap<>());
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.add(new HashMap<>());
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.add(new HashMap<>());
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.add(new HashMap<>());

        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(0).put(congressNumbers,
        "[113, 114, 115]");
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(0).put(description,
        "Junior Senator for Wisconsin");

        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(1).put(congressNumbers,
        "[213, 214, 215]");
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(1).put(description,
        "Senior Senator for Ohio");

        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(2).put(congressNumbers,
        "[313, 314, 315]");
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(2).put(description,
        "Senior Senator for Maryland");

        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(3).put(congressNumbers,
        "");
        EXPECTED_SENATOR_RESPONSE_BEHAVIOR_TEST.get(3).put(description,
        "Junior Senator for California");
    }
}
