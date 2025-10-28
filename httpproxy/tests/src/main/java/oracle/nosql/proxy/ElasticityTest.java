/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.StringValue;

import oracle.nosql.proxy.security.SecureTestUtil;
import oracle.nosql.proxy.util.PortFinder;
import oracle.nosql.proxy.util.CreateStoreUtils;
import oracle.nosql.proxy.util.CreateStore;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the correctness of query under elasticity operations.
 *
 * Two kind of elasticity operations are used in this tests. A store expansion
 * expand a 3x1 store into a 6x1 store. A store contraction contracts a 6x1
 * store into a 3x1 store. A series of such expansion and contraction can be
 * conducted in a test.
 *
 * The secondary query tests work in the folloiwng pattern. The table rows have
 * the following schema:
 *     userId    name    count
 * where userId is an integer which is the primary key, name is a string which
 * is not unique and count is the number of rows that has the same name. When
 * we insert the rows, we group rows of the same name together, i.e., into
 * consecutive userId blocks. The index is on the name field and the tests
 * query the rows with a specified name. The test then start up two threads, an
 * elasticity thread and a query thread. The elasticity thread does a series of
 * elasticity operations described above while in the mean time the query
 * thread does the query and verify the results. We also verify the query
 * results before and after the elasticity operations to make sure there is no
 * problem with the test insertion or elasticity operation.
 *
 * [KVSTORE-1518]
 */
public class ElasticityTest extends ProxyTestBase {

    private static boolean trace = false;

    private static final int startPort = 5000;
    private static final int haRange = 5;
    private static final Random rand = new Random();

    private static final int POLL_CONDITION_INTERVAL = 1000;
    private static final int NUM_ERRORS_TO_DISPLAY = 10;
    private static final AtomicInteger topoCandidateSequencer =
        new AtomicInteger(0);

    private final Logger logger = Logger.getLogger(getClass().getName());
    private CreateStore createStore = null;
    private KVStore kvstore = null;
    private StorageNodeAgent[] extraSNAs = new StorageNodeAgent[3];

    private Proxy proxy;
    private NoSQLHandle handle = null;

    private int maxReadKB = 25;

    String usersDDL =
        "CREATE TABLE IF NOT EXISTS users ( " +
        "   uid integer, "   +
        "   name string, "   +
        "   int integer, "   +
        "   count long, "    +
        " PRIMARY KEY (uid))";

    String childDDL =
        "CREATE TABLE IF NOT EXISTS users.child ( " +
        "   cid integer, "   +
        "   cname string, "  +
        "   cint integer, "  +
        "   count integer, " +
        " PRIMARY KEY (cid))";

    String idxNameDDL =
        "CREATE INDEX IF NOT EXISTS idx_name ON users(name)";

    String idxIntDDL =
            "CREATE INDEX IF NOT EXISTS idx_int ON users(int)";

    String users2DDL =
        "CREATE TABLE IF NOT EXISTS users2 ( " +
        "   uid1 integer, "  +
        "   uid2 integer, "  +
        "   name string, "   +
        "   int integer, "   +
        "   count long, "    +
        " PRIMARY KEY(shard(uid1), uid2))";

    String idx2IntDDL =
        "CREATE INDEX IF NOT EXISTS idx_int ON users2(int)";

    String[] queries = {
        // 0
        "declare $name string; " +
        "select * from users where name = $name",
        // 1
        "declare $low integer; $high integer; " +
        "select * from users where $low <= int and int <= $high " +
        "order by int",
        // 2
        "declare $low integer; $high integer; " +
        "select * from users where $low <= int and int <= $high " +
        "order by int desc",
        // 3
        "declare $low integer; $high integer; " +
        "select int, count(*) as count " +
        "from users " +
        "where $low <= int and int <= $high " +
        "group by int",
        // 4
        "declare $low integer; $high integer; " +
        "select int, count(*) as count " +
        "from users " +
        "group by int",
        // 5
        "declare $low integer; $high integer; " +
        "select * from users where $low <= uid and uid <= $high ",
        // 6
        "declare $uid1 integer; $low integer; $high integer; " +
        "select * from users2 where uid1 = $uid1 and $low <= int and int <= $high",
        // 7
        "declare $name string; " +
        "select p.uid, p.name, p.count as pcount, c.cid, c.count as ccount " +
        "from nested tables(users p descendants(users.child c)) " +
        "where p.name = $name",
        // 8
        "declare $name string; " +
        "select p.uid, p.name, p.count as pcount, c.cid, c.count as ccount " +
        "from users p, users.child c " +
        "where p.uid = c.uid and p.name = $name",
    };

    int numRows;
    int maxNameId1;
    int maxNameId2;
    int minNumRowsPerName = 1;

    final int maxChildRows = 30;

    // Maps nameId to count
    final Map<Integer, Long> countMap = new HashMap<Integer, Long>();

    /** Represents the test state. */
    private class TestState {

        TestState(int numQueryThreads) {
            this.numQueryThreads = numQueryThreads;
        }

        int numQueryThreads;

        private final AtomicInteger elasticityCount =
            new AtomicInteger(0);
        private final AtomicBoolean elasticityDone =
            new AtomicBoolean(false);
        private final AtomicInteger queryThreadDoneCount =
            new AtomicInteger(0);
        private final ConcurrentLinkedQueue<Throwable> errors =
            new ConcurrentLinkedQueue<>();

        private int getElasticityCount() {
            return elasticityCount.get();
        }

        private void incElasticityCount() {
            elasticityCount.getAndIncrement();
        }

        private boolean isElasticityDone() {
            return elasticityDone.get();
        }

        private void setElasticityDone() {
            elasticityDone.set(true);
        }

        private boolean areQueriesDone() {
            return queryThreadDoneCount.get() >= numQueryThreads;
        }

        private void setQueryThreadDone() {
            int done = queryThreadDoneCount.getAndIncrement();
        }

        private void reportError(Throwable t) {
            errors.add(t);
        }

        private Collection<Throwable> getErrors() {
            return errors;
        }
    }

    private class QueryException extends RuntimeException {

        public static final long serialVersionUID = 1L;

        private long timestamp;
        private String qname;

        private QueryException(String qname, String message) {
            super(message);
            this.timestamp = System.currentTimeMillis();
            this.qname = qname;
        }

        @Override
        public String toString() {
            return String.format(
                "Error executing query at %s with name=%s : %s",
                FormatUtils.formatDateTimeMillis(timestamp),
                qname,
                getMessage());
        }
    }

    private class ElasticityException extends RuntimeException {

        public static final long serialVersionUID = 1L;

        private ElasticityException(Throwable cause) {
            super(cause);
        }
    }

    private static void trace(String msg) {

        if (trace) {
            System.out.println(msg);
        }
    }


    /* these override the Before/AfterClass methods in ProxyTestBase */
    @BeforeClass
    public static void staticSetUp() {
        assumeTrue("Skipping ElasticityTest for minicloud or cloud test",
                   !Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));
    }

    @AfterClass
    public static void staticTearDown() {}

    @Override
    @Before
    public void setUp() throws Exception {
    }

    @Override
    @After
    public void tearDown() throws Exception {

        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
            proxy = null;
        }

        if (handle != null) {
            handle.close();
            handle = null;
        }

        if (kvstore != null) {
            kvstore.close();
            kvstore = null;
        }

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
    }

    private static void cleanupTestDir(String testDir) {
        File testDirFile = new File(testDir);
        if (!testDirFile.exists()) {
            return;
        }
        clearDirectory(testDirFile);
    }

     private void createStore(
        String testSubDir,
        int capacity,
        int partitions) throws Exception {

        int port = getKVPort();
        String testDir = getTestDir() + "/" + testSubDir;

        cleanupTestDir(testDir);

        createStore =
            new CreateStore(
                testDir,
                getStoreName(),
                port,
                3, /* nsns */
                3, /* rf */
                partitions,
                capacity,
                256, /* mb */
                false, /* use threads */
                null);
        final File root = new File(testDir);
        root.mkdirs();
        createStore.start();

        kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(getStoreName(),
                              String.format("%s:%s", getHostName(), port)));

        proxy = ProxyTestBase.startProxy();

        handle = createHandle();
    }

    private NoSQLHandle createHandle() {

        NoSQLHandleConfig hconfig =
            new NoSQLHandleConfig(ProxyTestBase.getProxyEndpoint());

        /* 5 retries, default retry algorithm */
        hconfig.configureDefaultRetryHandler(5, 0);

        hconfig.setRequestTimeout(30000);
        //hconfig.setNumThreads(20);

        SecureTestUtil.setAuthProvider(hconfig,
                                       ProxyTestBase.SECURITY_ENABLED,
                                       ProxyTestBase.onprem(),
                                       ProxyTestBase.getTenantId());
        hconfig.setLogger(logger);

        /* Open the handle */
        NoSQLHandle h = NoSQLHandleFactory.createNoSQLHandle(hconfig);

        /* do a simple op to set the protocol version properly */
        try {
            GetTableRequest getTable =
                new GetTableRequest().setTableName("noop");
            h.getTable(getTable);
        } catch (TableNotFoundException e) {}

        return h;
    }

    private void expandStore(int capacity) throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final String hostname = createStore.getHostname();
        final String poolname = CreateStore.STORAGE_NODE_POOL_NAME;
        final int portsPerFinder = 20;

        /* deploy 3 more sns */
        for (int i = 0; i < 3; ++i) {
            int sid = i + 4;
            PortFinder pf = new PortFinder(
                startPort + (3 + i) * portsPerFinder, haRange, hostname);
            int port = pf.getRegistryPort();

            extraSNAs[i] = CreateStoreUtils.createUnregisteredSNA(
                createStore.getRootDir(),
                pf,
                capacity,
                String.format("config%s.xml", i + 3),
                false /* useThreads */,
                false /* createAdmin */,
                2 /* mb */,
                null /* extra params */);

            CreateStoreUtils.waitForAdmin(hostname, port, 20, logger);
            createStore.setExpansionSnas(extraSNAs);

            int planId = cs.createDeploySNPlan(
                String.format("deploy sn%s", sid),
                new DatacenterId(1),
                hostname,
                port,
                "comment");

            runPlan(planId);

            StorageNodeId snid = extraSNAs[i].getStorageNodeId();
            cs.addStorageNodeToPool(poolname, snid);
        }

        String expandTopoName =
            String.format("expand-%s",
                          topoCandidateSequencer.getAndIncrement());
        cs.copyCurrentTopology(expandTopoName);
        cs.redistributeTopology(expandTopoName, poolname);

        int planId = cs.createDeployTopologyPlan(
            "deploy expansion", expandTopoName, null);
        runPlan(planId);
    }

    private void contractStore() throws Exception {

        final CommandServiceAPI cs = createStore.getAdmin();
        final String poolname = CreateStore.STORAGE_NODE_POOL_NAME;

        for (int i = 0; i < 3; ++i) {
            cs.removeStorageNodeFromPool(poolname, new StorageNodeId(i + 4));
        }

        String contractTopoName =
            String.format("contract-%s",
                          topoCandidateSequencer.getAndIncrement());
        cs.copyCurrentTopology(contractTopoName);
        cs.contractTopology(contractTopoName, poolname);

        verbose("Elasticity: Starting deploy-contraction plan");

        int planId = cs.createDeployTopologyPlan(
            "deploy contraction", contractTopoName, null);
        runPlan(planId);

        for (int i = 0; i < 3; ++i) {

            verbose("Elasticity: Starting remove SN plan for SN " + (i+4));

            planId = cs.createRemoveSNPlan(
                String.format("remove sn%s", i + 4),
                new StorageNodeId(i + 4));
            runPlan(planId);

            extraSNAs[i].shutdown(true, true, "contration");
            extraSNAs[i] = null;

            Files.deleteIfExists(
                Paths.get(createStore.getRootDir(),
                          String.format("config%s.xml", i + 3)));
            FileUtils.deleteDirectory(
                Paths.get(createStore.getRootDir(),
                          createStore.getStoreName(),
                          String.format("sn%s", i + 4))
                .toFile());
        }
    }

    private void runPlan(int planId) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    private void createTableAndIndex() {

        TableLimits limits = new TableLimits(90000, 15000, 50);
        int timeout = 20000;

        ProxyTestBase.tableOperation(handle, usersDDL, limits,
                                     TableResult.State.ACTIVE, timeout);
        ProxyTestBase.tableOperation(handle, childDDL, null,
                                     TableResult.State.ACTIVE, timeout);
        ProxyTestBase.tableOperation(handle, idxNameDDL, null,
                                     TableResult.State.ACTIVE, timeout);
        ProxyTestBase.tableOperation(handle, idxIntDDL, null,
                                     TableResult.State.ACTIVE, timeout);
        ProxyTestBase.tableOperation(handle, users2DDL, limits,
                                     TableResult.State.ACTIVE, timeout);
        ProxyTestBase.tableOperation(handle, idx2IntDDL, null,
                                     TableResult.State.ACTIVE, timeout);
    }

    /**
     * Populates {@code numRows} of rows and returns the maximum name ID.
     *
     * The userID is the natural series. The name is in the form of name.id
     * where id is also the natural series but multiple rows can have the same
     * id. The number of rows having the same name.id is picked at random
     * between 1 and 5% {@code numRows}. The count stores the total number of
     * rows that has the same name.id with that row so that by looking at the
     * count field, we know how many rows should be returned for a query on
     * that name.id.
     */
    private void populateRows(String tableName, String testName) {

        TableAPI tableAPI = kvstore.getTableAPI();
        Table table = tableAPI.getTable("in.valid.iac.name.space:" + tableName);
        Table childTable = tableAPI.getTable("in.valid.iac.name.space:users.child");
        boolean users2 = tableName.equals("users2");
        boolean innerJoin = testName.contains("InnerJoin");

        MapValue row = new MapValue();
        int maxRowsPerNameId = Math.max(1, numRows * 5 / 100);
        int uid1 = 0;
        int uid2 = 0;
        int nameId = 0;
        int nrows = 0;

        Row kvrow = null;
        if (table != null) {
            kvrow = table.createRow();
        }

        PutRequest putRequest = new PutRequest()
            .setValue(row)
            .setTableName(tableName);

        if (users2) {
            uid1 = 2; // all rows will go to partition 4
        }

        while (nrows < numRows) {
            int maxRowsPerNameId2 = Math.min(numRows - nrows, maxRowsPerNameId);
            long rowsPerNameId = (
                maxRowsPerNameId2 <= minNumRowsPerName ?
                minNumRowsPerName :
                minNumRowsPerName
                + rand.nextInt(maxRowsPerNameId2 - minNumRowsPerName));

            String name = ("name." + nameId);
            if (!users2) {
                countMap.put(nameId, rowsPerNameId);
            }

            for (int i = 0; i < rowsPerNameId; ++i) {
                if (users2) {
                    row.put("uid1", uid1);
                    row.put("uid2", uid2);
                    if (kvrow != null) {
                        kvrow.put("uid1", uid1);
                        kvrow.put("uid2", uid2);
                    }
                } else {
                    row.put("uid", uid1);
                    if (kvrow != null) {
                        kvrow.put("uid", uid1);
                    }
                }
                row.put("name", name);
                row.put("int", nameId);
                row.put("count", rowsPerNameId);

                PutResult res = handle.put(putRequest);
                assertNotNull("Put failed", res.getVersion());

                if (users2) {
                    ++uid2;
                } else {
                    int numChildRows = rand.nextInt(maxChildRows);
                    if (numChildRows == 0 && innerJoin) {
                        numChildRows = 1;
                    }

                    if (numChildRows > 0) {

                        MapValue childRow = new MapValue();
                        PutRequest cputRequest = new PutRequest()
                            .setValue(childRow)
                            .setTableName("users.child");

                        childRow.put("uid", uid1);

                        for (int j = 0; j < numChildRows; ++j) {
                            childRow.put("cid", j);
                            childRow.put("cint", rand.nextInt(10));
                            childRow.put("count", numChildRows);

                            PutResult cres = handle.put(cputRequest);
                            assertNotNull("Put failed", cres.getVersion());
                        }
                    }

                    ++uid1;
                }
                ++nrows;

                /*
                if (!users2) {
                PartitionId pid = ((KVStoreImpl)kvstore).
                    getPartitionId(TableKey.createKey(table, kvrow, false).getKey());
                trace("Inserted row " + row +
                                   " in P-" + pid.getPartitionId());
               }
                */
            }

            ++nameId;
        }

        if (users2) {
            maxNameId2 = nameId;
        } else {
            maxNameId1 = nameId;
        }
    }

    private int getUserId(MapValue row) {
        return row.get("uid").getInt();
    }

    private int getUserId2(MapValue row) {
        return row.get("uid2").getInt();
    }

    private String getName(MapValue row) {
        return row.get("name").getString();
    }

    private int getInt(MapValue row) {
        return row.get("int").getInt();
    }

    private long getCount(MapValue row) {
        return row.get("count").getLong();
    }

    private PartitionId getPartition(Table table, RecordValue rec) {
        Row row = table.createRow(rec);
        return ((KVStoreImpl) kvstore).getPartitionId(
            TableKey.createKey(table, row, false).getKey());
    }

    /** Executes a secondary query and verifies the result. */
    private void queryAndVerify(
        TestState testState,
        int qid,
        int qcount) {

        String qname = ("Q" + qid + "-" + qcount);

        PrepareRequest preq = new PrepareRequest();
        preq.setGetQueryPlan(true);
        preq.setStatement(queries[qid]);
        PrepareResult pres = handle.prepare(preq);
        PreparedStatement prep = pres.getPreparedStatement();

        //verbose("driver topo seq num = " + prep.topologySeqNum());

        QueryRequest qreq = new QueryRequest();
        qreq.setPreparedStatement(prep);
        qreq.setTimeout(30000);
        qreq.setQueryName(qname);
        qreq.setMaxReadKB(maxReadKB);
        qreq.setTraceLevel(3);
        //if (qcount > 4800) {
        //    qreq.setTraceLevel(0);
        //}

        int maxNameId = maxNameId1;
        int searchPKey = -1;
        if (qid == 5) {
            searchPKey = rand.nextInt(numRows);
        } else if (qid == 6) {
            searchPKey = 2;
            maxNameId = maxNameId2;
        }

        int searchNameId = rand.nextInt(maxNameId);

        String searchKey = ("name." + searchNameId);

        //verbose("Executing query " + qname + " with search pkey " +
        //                   searchPKey + " and search key " + searchKey);

        if (qid == 0 || qid == 7 || qid == 8) {
            prep.setVariable("$name", new StringValue(searchKey));
        } else if (qid == 5) {
            int lowKey = searchPKey - 20;
            int highKey = searchPKey + 20;
            prep.setVariable("$low", new IntegerValue(lowKey));
            prep.setVariable("$high", new IntegerValue(highKey));
        } else if (qid == 6) {
            int lowKey = searchNameId - 100;
            int highKey = searchNameId + 100;
            prep.setVariable("$uid1", new IntegerValue(searchPKey));
            prep.setVariable("$low", new IntegerValue(lowKey));
            prep.setVariable("$high", new IntegerValue(highKey));
        } else {
            int lowKey = searchNameId - 2;
            int highKey = searchNameId + 2;
            prep.setVariable("$low", new IntegerValue(lowKey));
            prep.setVariable("$high", new IntegerValue(highKey));
        }

        List<MapValue> results = new ArrayList<>();

        try {
            do {
                QueryResult res = handle.query(qreq);
                List<MapValue> list = res.getResults();
                for (MapValue val : list) {
                    results.add(val);
                }
            } while (!qreq.isDone());

            if (results.isEmpty()) {
                throw new QueryException(qname, "no records found");
            }

            verifyQueryResults(qid, qname,
                               maxNameId, searchNameId, searchPKey,
                               results);
        } catch (QueryException e) {
            testState.reportError(e);
            if (testState.errors.size() == 1) {
                trace("Elasticity:-1 query " + qname + " failed");
                trace(CommonLoggerUtils.getStackTrace(e));
                if (trace) {
                    qreq.printTrace(System.out);
                }
            }
            throw e;
        } catch (RequestTimeoutException rte) {
            /* Don't fail the test. Due to the high load imposed to the store
             * by ElasticityTest, it is possible for queries to timeout or
             * get a "failed to read base topology" errors. We don't want
             * to consider such erros as failures; only wrong query results
             * are considered failures. */
            testState.reportError(rte);
            verbose("Elasticity:-2 query " + qname + " failed");
            verbose(CommonLoggerUtils.getStackTrace(rte));
        } catch (TableNotFoundException tnfe) {
            /* Don't fail the test. Same reason as above */
            testState.reportError(tnfe);
            verbose("Elasticity:-2 query " + qname + " failed");
            verbose(CommonLoggerUtils.getStackTrace(tnfe));
        } catch (Throwable t) {
            testState.reportError(t);
            if (testState.errors.size() <= 10) {
                trace("Elasticity:-2 query " + qname + " failed: " + t);
            }
            verbose("Elasticity:-2 query " + qname + " failed");
            verbose(CommonLoggerUtils.getStackTrace(t));
            throw new QueryException(qname, t.getMessage());
        }
    }

    /**
     * Verifies that the query result consists of a block of consecutive rows
     * and the count matches the specified value.
     */
    private void verifyQueryResults(
        int qid,
        String qname,
        int maxNameId,
        int searchNameId,
        int searchPKey,
        List<MapValue> results) {

        if (qid == 7 || qid == 8) {
            verifyQ7Results(qid, qname, results);
            return;
        }

        long expectedCount = 0;
        long actualCount = 0;
        int prevInt = -1;

        /* Queries with range scan: compute expected count
         * Make sure results are sorted and  */
        if (qid == 1 || qid == 2 || qid == 6) {

            for (MapValue row : results) {

                ++actualCount;
                int currInt = getInt(row);

                if (prevInt < 0) {
                    expectedCount += getCount(row);
                    prevInt = currInt;
                    continue;
                }

                if (qid == 1 && prevInt > currInt) {
                    throw new QueryException(qname, "Query results are out of order");
                }

                if (qid == 2 && prevInt < currInt) {
                    throw new QueryException(qname, "Query results are out of order");
                }

                if (prevInt != currInt) {
                    expectedCount += getCount(row);
                    prevInt = currInt;
                }
            }
        }

        /* Group-by queries */
        if (qid == 3 || qid == 4) {
            if (qid == 4) {
                expectedCount = maxNameId;
            } else {
                expectedCount = 5;
                if (searchNameId == 0 || searchNameId == maxNameId - 1) {
                    expectedCount = 3;
                } else if (searchNameId == 1 || searchNameId == maxNameId - 2) {
                    expectedCount = 4;
                }
            }

            for (MapValue row : results) {

                ++actualCount;
                int currInt = getInt(row);
                long currCount = getCount(row);

                if (countMap.get(currInt) != currCount) {
                    throw new QueryException(qname,
                        "Unexpected group count. Expected = " + countMap.get(currInt) +
                        " actual = " + currCount);
                }

                if (prevInt < 0) {
                    prevInt = currInt;
                    continue;
                }

                if (prevInt > currInt) {
                    throw new QueryException(qname, "Query results are out of order");
                }

                if (prevInt != currInt) {
                    prevInt = currInt;
                }
            }
        }

        if (qid == 5) {
            expectedCount = 41;

            if (searchPKey - 20 < 0) {
                expectedCount -= (20 - searchPKey);
            } else if (searchPKey + 20 >= numRows) {
                expectedCount -= (searchPKey + 20 - numRows + 1);
            }
        }

        if (qid < 3 || qid == 5) {
            /* Make sure the row Ids are consecutive without missing or duplicate */
            Collections.sort(results, Comparator.comparingInt((r) -> getUserId(r)));

            MapValue firstRow = results.get(0);
            if (qid == 0) {
                expectedCount = getCount(firstRow);
            }
            int startId = getUserId(firstRow);
            int prevId = startId - 1;

            actualCount = 0;
            for (MapValue row : results) {
                ++actualCount;
                int currId = getUserId(row);
                if (prevId < currId - 1) {
                    notifyIncorrectRows("missing", qid, qname,
                                        currId-1, prevId, currId);
                } else if (prevId == currId) {

                    notifyIncorrectRows("duplicating", qid, qname,
                                        currId, prevId, currId);
                }
                prevId = currId;
            }
        }

        if (qid == 6) {
            /* Make sure the row Ids are consecutive without missing or duplicate */
            Collections.sort(results, Comparator.comparingInt((r) -> getUserId2(r)));

            MapValue firstRow = results.get(0);
            int startId = getUserId2(firstRow);
            int prevId = startId - 1;

            actualCount = 0;
            for (MapValue row : results) {
                ++actualCount;
                int currId = getUserId2(row);
                if (prevId < currId - 1) {
                    notifyIncorrectRows("missing", qid, qname,
                                        currId-1, prevId, currId);
                } else if (prevId == currId) {

                    notifyIncorrectRows("duplicating", qid, qname,
                                        currId, prevId, currId);
                }
                prevId = currId;
            }
        }

        /* Make sure the count is correct. */
        if (actualCount != expectedCount) {
            throw new QueryException(
                qname, "incorrect count, expected = " +
                expectedCount + " actual = " + actualCount);
        }
    }

    private void verifyQ7Results(
        int qid,
        String qname,
        List<MapValue> results) {

        /* Make sure the row Ids are consecutive without missing or duplicate */
        Collections.sort(results, Comparator.comparingInt((r) -> getUserId(r)));

        MapValue firstRow = results.get(0);
        int startUid = getUserId(firstRow);
        int prevUid = startUid - 1;
        long numExpectedParentRows = firstRow.get("pcount").getLong();
        long numActualParentRows = 0;
        int prevCid = -1;
        int numExpectedChildRows = -1;
        int numActualChildRows = -1;

        for (MapValue row : results) {

            //verbose(row);

            int currUid = getUserId(row);

            if (prevUid < currUid - 1) {
                notifyIncorrectRows("missing", qid, qname,
                                    currUid-1, prevUid, currUid);
            }

            if (prevUid == currUid) {

                if (prevCid == -1) {
                    notifyIncorrectRows("duplicating", qid, qname,
                                        currUid, prevUid, currUid);
                }

                ++numActualChildRows;
                int currCid = row.get("cid").getInt();
                if (prevCid < currCid - 1) {
                    throw new QueryException(
                        qname,
                       "missing row (" + currUid + ", " + (prevCid+1) + ")" +
                        "prevCid = " + prevCid + " currCid = " + currCid);
                } else if (prevCid == currCid) {
                    throw new QueryException(
                        qname,
                        "duplicating row (" + currUid + ", " + currCid + ")");
                }

                prevCid = currCid;
            } else {
                if (numActualChildRows != numExpectedChildRows) {
                    throw new QueryException(
                        qname,
                        "incorrect number of child rows for parent uid : " +
                        currUid + " expected = " + numExpectedChildRows +
                        " actual = " + numActualChildRows);
                }

                ++numActualParentRows;

                if (row.get("ccount").isNull()) {
                    prevCid = -1;
                    numExpectedChildRows = 0;
                    numActualChildRows = 0;
                } else {
                    int currCid = row.get("cid").getInt();
                    if (currCid != 0) {
                        throw new QueryException(
                            qname,
                            "missing row (" + currUid + ", " + 0 + ")" +
                            " currCid = " + currCid);
                    }
                    prevCid = 0;
                    numExpectedChildRows = row.get("ccount").getInt();
                    numActualChildRows = 1;
                }
            }

            prevUid = currUid;
        }

        if (numActualParentRows != numExpectedParentRows) {
            throw new QueryException(
                qname,
                "incorrect number of parent rows: expected = " +
                numExpectedParentRows + " actual = " +
                numActualParentRows);
        }
    }

    private void notifyIncorrectRows(String cause,
                                     int qid,
                                     String qname,
                                     int problemId,
                                     int prevId,
                                     int currId) {

        String tableName;
        if (qid == 6) {
            tableName = "users2";
        } else {
            tableName = "users";
        }

        Table table = kvstore.getTableAPI().getTable("in.valid.iac.name.space:" +
                                                     tableName);
        final Row row = table.createRow();
        if (qid == 6) {
            row.put("uid", problemId);
        } else {
            row.put("uid", problemId);
        }
        throw new QueryException(
            qname,
            String.format(
                "%s row with userId %s, "
                + "prevId=%s, currId=%s, "
                + "partition of the %s row: %s",
                cause, problemId, prevId, currId, cause,
                getPartition(table, row)));

    }

    /**
     * Verifies the test state after elasticity ops and queries are done.
     */
    private void verifyTestState(TestState testState) {

        if (testState.isElasticityDone() &&
            testState.areQueriesDone() &&
            testState.getErrors().isEmpty()) {
            return;
        }

        List<String> errorMessages = new ArrayList<>();

        /*
        Predicate<Throwable> unexpectedError =
            (t) -> (!(t instanceof ElasticityException)) &&
                   (!(t instanceof QueryException));

        if (testState.getErrors().stream()
            .filter(unexpectedError).count() != 0) {
            errorMessages.add(
                String.format(
                    "Unexpected exceptions. %s",
                    toErrorString(testState, unexpectedError)));
        }
        */

        Predicate<Throwable> elasticityError =
            (t) -> (t instanceof ElasticityException);

        if (testState.getErrors().stream()
            .filter(elasticityError).count() != 0) {
            errorMessages.add(
                String.format(
                    "Unexpected elasticity exceptions. "
                    + "Total elasticity routines done: %s. "
                    + "%s",
                    testState.getElasticityCount(),
                    toErrorString(testState, elasticityError)));
        }

        collectQueryErrors(testState, errorMessages);

        assertTrue(errorMessages.stream()
                   .collect(Collectors.joining("\n")),
                   errorMessages.isEmpty());
    }

    private String toErrorString(TestState testState,
                                 Predicate<Throwable> filter) {
        StringBuilder sb = new StringBuilder();
        long totalCount = testState.getErrors().stream().filter(filter).count();
        sb.append("Total number of errors: ").append(totalCount).append("\n");

        testState.getErrors().stream().filter(filter)
            .limit(NUM_ERRORS_TO_DISPLAY)
            .forEach((t) -> {
                sb.append("> ").append(CommonLoggerUtils.getStackTrace(t)).
                   append("\n");
            });
        return sb.toString();
    }

    private void collectQueryErrors(TestState testState,
                                    List<String> errorMessages) {

        List<QueryException> queryErrors = new ArrayList<>();
        for (Throwable t : testState.getErrors()) {
            if (!(t instanceof QueryException)) {
                continue;
            }
            QueryException qe = (QueryException) t;
            queryErrors.add(qe);
        }

        if (queryErrors.isEmpty()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        queryErrors.stream()
            .limit(NUM_ERRORS_TO_DISPLAY)
            .forEach((qe) -> { sb.append("> ").append(qe).append("\n"); });
        errorMessages.add(
            String.format("Unexpected query exceptions. " +
                          "Total number of failures: %s.\n" + "%s",
                          queryErrors.size(),
                          sb.toString()));
    }

    /* Start a thread to execute queries and verify the results. */
    private void startQueryThread(TestState testState, int qid) {

        Thread th = new Thread(() -> {
                try {
                    int count = 0;
                    //while (count < 200) {
                    while (!testState.isElasticityDone()) {
                        queryAndVerify(testState, qid, count);
                        count++;
                    }
                    testState.setQueryThreadDone();
                } catch (Throwable t) {
                    /* make sure all query threads exit on failure */
                    testState.setQueryThreadDone();
                    testState.setElasticityDone();
                    fail(t.getMessage());
                }
            });
        th.setDaemon(true);
        th.start();
    }

    private void startElasticityThread(TestState testState,
                                       List<ElasticityRoutine> routines) {
        Thread th = new Thread(() -> {
            try {
                for (ElasticityRoutine routine : routines) {
                    routine.run();
                    testState.incElasticityCount();
                }
            } catch (Throwable t) {
                testState.reportError(new ElasticityException(t));
            } finally {
                testState.setElasticityDone();
            }
            });
        th.setDaemon(true);
        th.start();
    }

    private interface ElasticityRoutine {
        void run() throws Exception;
    }

    /**
     * Tests the basic case that a query is executed under store expansion.
     * This test is expected to exercise the most basic interaction between
     * query and partition migration.
     */
    @Test
    public void testSmallExpansion() throws Exception {
        int[] qids = { 0 };
        numRows = 1000;
        testExpansion("smallExpansion", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionSort() throws Exception {
        int[] qids = { 1 };
        numRows = 1000;
        testExpansion("smallExpansionSort", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionSortDesc() throws Exception {
        int[] qids = { 2 };
        numRows = 1000;
        testExpansion("smallExpansionSortDesc", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionGroup() throws Exception {
        int[] qids = { 3 };
        numRows = 1000;
        testExpansion("smallExpansionGroup", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionGroup2() throws Exception {
        int[] qids = { 4 };
        numRows = 1000;
        testExpansion("smallExpansionGroup2", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionAllPartitions() throws Exception {
        int[] qids = { 5 };
        numRows = 1000;
        testExpansion("smallExpansionAllParititions", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionSinglePartition() throws Exception {
        int[] qids = { 6, 6, 6, 6, 6 };
        numRows = 3000;
        testExpansion("smallExpansionSinglePartition", 1, 10, qids, "users2");
    }

    @Test
    public void testSmallExpansionJoin() throws Exception {
        int[] qids = { 7 };
        numRows = 1000;
        testExpansion("smallExpansionJoin", 1, 10, qids, "users");
    }

    @Test
    public void testSmallExpansionInnerJoin() throws Exception {
        int[] qids = { 8 };
        numRows = 1000;
        testExpansion("smallExpansionInnerJoin", 1, 10, qids, "users");
    }

    @Test
    public void testBigExpansionJoin() throws Exception {
        org.junit.Assume.assumeTrue(!isLinux); /* skip if linux */
        int[] qids = { 7 };
        numRows = 10000;
        testExpansion("bigExpansionJoin", 3, 20, qids, "users");
    }

    @Test
    public void testBigExpansion() throws Exception {
        org.junit.Assume.assumeTrue(!isLinux); /* skip if linux */
        int[] qids = { 0, 1, 4 };
        numRows = 10000;
        testExpansion("bigExpansion", 3, 20, qids, "users");
    }

    private void testExpansion(
        String testSubDir,
        int capacity,
        int partitions,
        int[] qids,
        String tableName)
        throws Exception {

        createStore(testSubDir, capacity, partitions);
        createTableAndIndex();
        populateRows(tableName, testSubDir);

        TestState testState = new TestState(qids.length);

        startElasticityThread(testState, Arrays.asList(() -> expandStore(capacity)));
        //testState.setElasticityDone();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }

        for (int qid : qids) {
            startQueryThread(testState, qid);
        }

        final long timeoutMillis = 600 * 1000;
        PollCondition.await(
            POLL_CONDITION_INTERVAL, timeoutMillis,
            () ->
            testState.isElasticityDone() && testState.areQueriesDone());

        verifyTestState(testState);
    }

    /**
     * Tests the basic case that a query is executed under store contraction.
     * This test is expected to exercise the most basic interaction between
     * query and partition migration.
     */
    @Test
    public void testSmallContraction() throws Exception {
        int[] qids = { 0 };
        numRows = 1000;
        testContraction("smallContraction", 1, 10, qids);
    }

    @Test
    public void testSmallContractionSort() throws Exception {
        int[] qids = { 1 };
        numRows = 1000;
        testContraction("smallContractionSort", 1, 10, qids);
    }

    public void testContraction(
        String testSubDir,
        int capacity,
        int partitions,
        int[] qids) throws Exception {

        createStore(testSubDir, capacity, partitions);
        expandStore(capacity);
        createTableAndIndex();
        populateRows("users", testSubDir);
        TestState testState = new TestState(qids.length);

        verbose("Elasticity: Starting store contraction");

        for (int qid : qids) {
            startQueryThread(testState, qid);
        }
        //testState.setQueryAndVerifyDone();
        startElasticityThread(testState, Arrays.asList(() -> contractStore()));

        /* Waits for both to finish. */
        final long timeoutMillis = 60 * 1000;
        PollCondition.await(
            POLL_CONDITION_INTERVAL, timeoutMillis,
            () ->
            testState.isElasticityDone() && testState.areQueriesDone());

        verbose("Elasticity: Store contraction done");

        /* Verify the results. */
        verifyTestState(testState);
    }
}
