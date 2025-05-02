/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.query;

import static oracle.kv.impl.util.ThreadUtils.threadId;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.async.AsyncTestUtils;
import oracle.kv.impl.async.exception.DialogNoSuchTypeException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;

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
@TestClassTimeoutMillis(60*60*1000)
public class ElasticityTest extends TestBase {

    private static final int startPort = 5000;
    private static final int haRange = 5;
    private static final Random rand = new Random();

    private static final int POLL_CONDITION_INTERVAL = 1000;
    private static final int NUM_ERRORS_TO_DISPLAY = 10;
    private static final AtomicInteger topoCandidateSequencer =
        new AtomicInteger(0);

    private CreateStore createStore = null;
    private KVStore kvstore = null;
    private StorageNodeAgent[] extraSNAs = new StorageNodeAgent[3];

    private int batchsize = 5;

    private boolean inContraction;
    private boolean inSNRemoval;

    String usersDDL =
        "CREATE TABLE IF NOT EXISTS users ( " +
        "   uid integer, "   +
        "   name string, "   +
        "   int integer, "   +
        "   ikey integer,"   +
        "   count long, "    +
        " PRIMARY KEY (uid))";

    String idxNameDDL =
        "CREATE INDEX IF NOT EXISTS idx_name ON users(name)";

    String idxIntDDL =
            "CREATE INDEX IF NOT EXISTS idx_int ON users(int)";

    String idxKeyDDL =
            "CREATE INDEX IF NOT EXISTS idx_key ON users(ikey)";

    String childDDL =
        "CREATE TABLE IF NOT EXISTS users.child ( " +
        "   cid integer, "   +
        "   cname string, "  +
        "   cint integer, "  +
        "   count integer, " +
        " PRIMARY KEY (cid))";

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
        "declare $ikey integer; " +
        "select * from users where ikey = $ikey",
        // 9
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

    // Maps parent pk to number of matching child rows
    //final Map<Integer, Integer> childRowsMap = new HashMap<Integer, Long>();

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
            queryThreadDoneCount.getAndIncrement();
            //System.out.println("Query thread " + Thread.currentThread().getId() +
            //                   " is done. num query threads done = " + (done+1));
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

    @Override
    public void tearDown() throws Exception {

        if (kvstore != null) {
            kvstore.close();
        }

        if (createStore != null) {
            createStore.shutdown(true);
        }
        for (final StorageNodeAgent sna : extraSNAs) {
            if (sna != null) {
                sna.shutdown(true /* stopServices */, false /* force */);
            }
        }

        /* Wait for all async servers to shutdown */
        final long stop = System.currentTimeMillis() + 60_000;
        while (true) {
            if (AsyncTestUtils.getActiveDialogTypes().isEmpty()) {
                break;
            }
            if (System.currentTimeMillis() >= stop) {
                AsyncTestUtils.checkActiveDialogTypes();
                break;
            }
            Thread.sleep(10_000);
        }
    }

     private void createStore(
         int capacity,
         int partitions) throws Exception {

         createStore(3, 3, capacity, partitions);
    }

     private void createStore(
         int nsns,
         int rf,
         int capacity,
         int partitions) throws Exception {

        final String storeName = "kvstore";
        final int port =
            (new PortFinder(startPort, haRange)).getRegistryPort();
        createStore =
            new CreateStore(
                storeName, port,
                nsns, /* nsns */
                rf, /* rf */
                partitions,
                capacity,
                2, /* mb */
                true, /* useThreads */
                null);

        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.COMMON_HIDE_USERDATA, "false");
        UserDataControl.setKeyHiding(false);
        UserDataControl.setValueHiding(false);
        createStore.setPolicyMap(map);

        createStore.start();
        kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(storeName, String.format("localhost:%s", port)));
    }

    private void expandStore(int capacity) throws Exception {

        logger.fine(() -> "Expanding store");
        final CommandServiceAPI cs = createStore.getAdmin();
        final String hostname = createStore.getHostname();
        final String poolname = CreateStore.STORAGE_NODE_POOL_NAME;
        final int portsPerFinder = 20;

        /* deploy 3 more sns */
        for (int i = 0; i < 3; ++i) {
            int sid = i + 4;
            logger.fine(() -> String.format(
                    "Starting StorageNodeAgent for sn%s", sid));
            PortFinder pf = new PortFinder(
                startPort + (3 + i) * portsPerFinder, haRange, hostname);
            int port = pf.getRegistryPort();

            extraSNAs[i] = StorageNodeUtils.createUnregisteredSNA(
                createStore.getRootDir(),
                pf,
                capacity,
                String.format("config%s.xml", i + 3),
                true /* useThreads */,
                false /* createAdmin */,
                null /* mgmtImpl */,
                0 /* mgmtPollPort */,
                0 /* mgmtTrapPort */,
                2 /* mb */);

            StorageNodeUtils.waitForAdmin(hostname, port);

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

        //System.out.println("XXXX Deploying expanded topology");

        int planId = cs.createDeployTopologyPlan(
            "deploy expansion", expandTopoName, null);
        runPlan(planId);
        logger.fine(() -> "Expanded store");
    }

    private void contractStore() throws Exception {

        logger.fine(() -> "Contracting store");
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

        System.out.println("XXXX Starting deploy-contraction plan");

        int planId = cs.createDeployTopologyPlan(
            "deploy contraction", contractTopoName, null);
        runPlan(planId);

        inSNRemoval = true;

        for (int i = 0; i < 3; ++i) {

            System.out.println("XXXX Starting remove SN plan for SN " + (i+4));

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

        inSNRemoval = false;
        logger.fine(() -> "Contracted store");
    }

    private void runPlan(int planId) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    private void createTableAndIndex() {

        kvstore.executeSync(usersDDL);
        kvstore.executeSync(childDDL);
        kvstore.executeSync(idxNameDDL);
        kvstore.executeSync(idxIntDDL);
        kvstore.executeSync(idxKeyDDL);
        kvstore.executeSync(users2DDL);
        kvstore.executeSync(idx2IntDDL);
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
    private void populateRows(String tableName, String testCaseName) {

        TableAPI tableAPI = kvstore.getTableAPI();
        Table table = tableAPI.getTable(tableName);
        Table childTable = tableAPI.getTable("users.child");
        boolean users2 = tableName.equals("users2");
        boolean innerJoin = testCaseName.contains("InnerJoin");

        Row row = table.createRow();
        int maxRowsPerNameId = Math.max(1, numRows * 5 / 100);
        int uid1 = 0;
        int uid2 = 0;
        int nameId = 0;
        int nrows = 0;

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
                } else {
                    row.put("uid", uid1);
                    row.put("ikey", uid1);
                }
                row.put("name", name);
                row.put("int", nameId);
                row.put("count", rowsPerNameId);
                tableAPI.put(row, null, null);

                int numChildRows = 0;

                if (users2) {
                    ++uid2;
                } else if (testCaseName.contains("Join")) {
                    numChildRows = rand.nextInt(maxChildRows);
                    if (numChildRows == 0 && innerJoin) {
                        numChildRows = 1;
                    }
                    Row childRow = childTable.createRow();
                    if (numChildRows > 0) {
                        childRow.put("uid", uid1);
                        for (int j = 0; j < numChildRows; ++j) {
                            childRow.put("cid", j);
                            childRow.put("cint", rand.nextInt(10));
                            childRow.put("count", numChildRows);

                            tableAPI.put(childRow, null, null);
                        }
                    }

                    ++uid1;
                }
                ++nrows;
                /*
                PartitionId pid = ((KVStoreImpl)kvstore).
                    getPartitionId(TableKey.createKey(table, row, false).getKey());
                System.out.println("Inserted row " + row +
                                   " in P-" + pid.getPartitionId() +
                                   " numChildRows = " + numChildRows);
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

    private int getUserId(RecordValue row) {
        return row.get("uid").asInteger().get();
    }

    private int getUserId2(RecordValue row) {
        return row.get("uid2").asInteger().get();
    }

    private int getInt(RecordValue row) {
        return row.get("int").asInteger().get();
    }

    private long getCount(RecordValue row) {
        return row.get("count").asLong().get();
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

        long timeoutMillis = 100000;

        String qname = ("Q" + qid + "-" + qcount + "-" +
                        threadId(Thread.currentThread()));

        List<RecordValue> results = new ArrayList<>();

        ExecuteOptions options = new ExecuteOptions();
        options.setAsync(false);
        options.setTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        options.setQueryName(qname);
        options.setResultsBatchSize(batchsize);
        options.setDoLogFileTracing(false);
        options.setTraceLevel((byte)3);

        PreparedStatement ps = kvstore.prepare(queries[qid], options);
        BoundStatement bs = ps.createBoundStatement();

        //if (qcount == 0) {
        //    System.out.println("Query plan:\n" + ps);
        //}

        int maxNameId = maxNameId1;
        int searchPKey = -1;
        if (qid == 5) {
            searchPKey = rand.nextInt(numRows);
        } else if (qid == 6) {
            searchPKey = 2;
            maxNameId = maxNameId2;
        } else if (qid == 8) {
            searchPKey = rand.nextInt(numRows);
        }

        int searchNameId = rand.nextInt(maxNameId);

        String searchKey = ("name." + searchNameId);

        //System.out.println("Executing query " + qname + " with search pkey " +
        //                   searchPKey + " and search key " + searchKey);

        if (qid == 0 || qid == 7 || qid == 9) {
            bs.setVariable("$name", searchKey);
        } else if (qid == 5) {
            int lowKey = searchPKey - 20;
            int highKey = searchPKey + 20;
            bs.setVariable("$low", lowKey);
            bs.setVariable("$high", highKey);
        } else if (qid == 6) {
            int lowKey = searchNameId - 100;
            int highKey = searchNameId + 100;
            bs.setVariable("$uid1", searchPKey);
            bs.setVariable("$low", lowKey);
            bs.setVariable("$high", highKey);
        } else if (qid == 8) {
            bs.setVariable("$ikey", searchPKey);
        } else {
            int lowKey = searchNameId - 2;
            int highKey = searchNameId + 2;
            bs.setVariable("$low", lowKey);
            bs.setVariable("$high", highKey);
        }

        StatementResult qsr = null;

        try {
            qsr = kvstore.executeSync(bs, options);
            Iterator<RecordValue> iter = qsr.iterator();
            while (iter.hasNext()) {
                RecordValue res = iter.next();
                results.add(res);
            }

            //if (qcount == 1) {
            //    qsr.printTrace(System.out);
            //}

            if (results.isEmpty()) {
                throw new QueryException(qname, "no results found");
            }
            verifyQueryResults(qid, qname,
                               maxNameId, searchNameId, searchPKey,
                               results);
        } catch (Throwable t) {

            if (inContraction && t instanceof RequestTimeoutException) {
                Throwable cause = t.getCause();
                if (cause != null && cause instanceof IOException) {
                    cause = cause.getCause();
                    if (cause != null && cause instanceof DialogNoSuchTypeException) {
                        System.out.println("XXXX query " + qname +
                                           " failed during RN removal");
                        return;
                    }
                } else if (inSNRemoval && cause == null) {
                    System.out.println("XXXX query " + qname +
                                       " failed during SN removal");
                    return;
                }
            }

            if (qsr != null) {
                qsr.printTrace(System.out);
            }

            testState.reportError(t);
            System.out.println("XXXX query " + qname + " failed: \n" + t);
            System.out.println(CommonLoggerUtils.getStackTrace(t));
            try {
                tearDown();
            } catch (Exception e) {
                System.out.println("Exception during shutdown: " + e);
            }
            System.exit(1);
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
        List<RecordValue> results) {

        if (qid == 7 || qid == 9) {
            verifyQ7Results(qid, qname, results);
            return;
        }

        if (qid == 8) {
            if (results.size() != 1) {
                throw new QueryException(qname,
                    "Unexpected number of results: " + results.size());
            }
            RecordValue res = results.get(0);
            int ikey = res.get("ikey").asInteger().get();
            if (ikey != searchPKey) {
                throw new QueryException(qname,
                "Wrong query result : " + ikey + " expected : " + searchPKey);
            }
            return;
        }

        long expectedCount = 0;
        long actualCount = 0;
        int prevInt = -1;

        /* Queries with range scan: compute expected count
         * Make sure results are sorted and  */
        if (qid == 1 || qid == 2 || qid == 6) {

            for (RecordValue row : results) {

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

            //System.out.println("\nGrouping query results:\n");

            for (RecordValue row : results) {

                //System.out.println(row);

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

            RecordValue firstRow = results.get(0);
            if (qid == 0) {
                expectedCount = getCount(firstRow);
            }
            int startId = getUserId(firstRow);
            int prevId = startId - 1;

            actualCount = 0;
            for (RecordValue row : results) {
                ++actualCount;
                int currId = getUserId(row);
                if (prevId < currId - 1) {
                    notifyIncorrectRows("missing", qname, qid,
                                        currId-1, prevId, currId);
                } else if (prevId == currId) {

                    notifyIncorrectRows("duplicating", qname, qid,
                                        currId, prevId, currId);
                }
                prevId = currId;
            }
        }

        if (qid == 6) {
            /* Make sure the row Ids are consecutive without missing or duplicate */
            Collections.sort(results, Comparator.comparingInt((r) -> getUserId2(r)));

            RecordValue firstRow = results.get(0);
            int startId = getUserId2(firstRow);
            int prevId = startId - 1;

            actualCount = 0;
            for (RecordValue row : results) {
                ++actualCount;
                int currId = getUserId2(row);
                if (prevId < currId - 1) {
                    notifyIncorrectRows("missing", qname, qid,
                                        currId-1, prevId, currId);
                } else if (prevId == currId) {

                    notifyIncorrectRows("duplicating", qname, qid,
                                        currId, prevId, currId);
                }
                prevId = currId;
            }
        }

        /* Make sure the count is correct. */
        if (qid != 7 && actualCount != expectedCount) {
            throw new QueryException(
                qname,
                String.format("incorrect count, expected=%s, actual=%s",
                              expectedCount, actualCount));
        }

    }

    private void verifyQ7Results(
        int qid,
        String qname,
        List<RecordValue> results) {

        //"declare $name string; " +
        //"select p.uid, p.name, p.count as pcount, c.cid, c.count as ccount " +
        //"from nested tables(users p descendants(users.child c)) " +
        //"where p.name = $name",

        /* Make sure the row Ids are consecutive without missing or duplicate */
        Collections.sort(results, Comparator.comparingInt((r) -> getUserId(r)));

        RecordValue firstRow = results.get(0);
        int startUid = getUserId(firstRow);
        int prevUid = startUid - 1;
        long numExpectedParentRows = firstRow.get("pcount").asLong().get();
        long numActualParentRows = 0;
        int prevCid = -1;
        int numExpectedChildRows = -1;
        int numActualChildRows = -1;

        for (RecordValue row : results) {

            //System.out.println(row);

            int currUid = getUserId(row);

            if (prevUid < currUid - 1) {
                notifyIncorrectRows("missing", qname, qid,
                                    currUid-1, prevUid, currUid);
            }

            if (prevUid == currUid) {

                if (prevCid == -1) {
                    notifyIncorrectRows("duplicating", qname, qid,
                                        currUid, prevUid, currUid);
                }

                ++numActualChildRows;
                int currCid = row.get("cid").asInteger().get();
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
                        prevUid + " expected = " + numExpectedChildRows +
                        " actual = " + numActualChildRows);
                }

                ++numActualParentRows;

                if (row.get("ccount").isNull()) {
                    prevCid = -1;
                    numExpectedChildRows = 0;
                    numActualChildRows = 0;
                } else {
                    int currCid = row.get("cid").asInteger().get();
                    if (currCid != 0) {
                        throw new QueryException(
                            qname,
                            "missing row (" + currUid + ", " + 0 + ")" +
                            " currCid = " + currCid);
                    }
                    prevCid = 0;
                    numExpectedChildRows = row.get("ccount").asInteger().get();
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
                                     String qname,
                                     int qid,
                                     int problemId,
                                     int prevId,
                                     int currId) {
        String tableName = (qid == 6 ? "users2" : "users");
        Table table = kvstore.getTableAPI().getTable(tableName);
        final Row row = table.createRow();
        if (qid == 6) {
            row.put("uid2", problemId);
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

    private void updateRequestDispacherTopology() {
        final TableAPI tableAPI = kvstore.getTableAPI();
        final Table table = tableAPI.getTable("users");
        final PrimaryKey key = table.createPrimaryKey();
        key.put("uid", 1);
        tableAPI.get(key, new ReadOptions(null, 0, null));
    }

    /** Start a thread to execute queries and verify the results. */
    private void startQueryThread(TestState testState, int qid) {

        new Thread(() -> {
            int count = 0;
            //while (count < 2) {
            while (!testState.isElasticityDone()) {
                queryAndVerify(testState, qid, count);
                count++;
            }
            testState.setQueryThreadDone();
            //System.out.println("Executed " + count + " queries");
        }).start();
    }

    private void startElasticityThread(TestState testState,
                                       List<ElasticityRoutine> routines) {
        new Thread(() -> {
            try {
                for (ElasticityRoutine routine : routines) {
                    routine.run();
                    testState.incElasticityCount();
                }
            } catch (Throwable t) {
                testState.reportError(new ElasticityException(t));
                System.out.println("XXXX Got elasticity exception:\n" + t);
            } finally {
                testState.setElasticityDone();
            }
        }).start();
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
        System.out.println("XXXX testSmallExpansion");
        testExpansion(1, 10, qids, "testSmallExpansion");
    }

    @Test
    public void testSmallExpansionSort() throws Exception {
        int[] qids = { 1 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionSort");
        testExpansion(1, 10, qids, "testSmallExpansionSort");
    }

    @Test
    public void testSmallExpansionSortDesc() throws Exception {
        int[] qids = { 2 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionSortDesc");
        testExpansion(1, 10, qids, "testSmallExpansionSortDesc");
    }

    @Test
    public void testSmallExpansionGroup() throws Exception {
        int[] qids = { 3 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionGroup");
        testExpansion(1, 10, qids, "testSmallExpansionGroup");
    }

    @Test
    public void testSmallExpansionGroup2() throws Exception {
        int[] qids = { 4 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionGroup2");
        testExpansion(1, 10, qids, "testSmallExpansionGroup2");
    }

    @Test
    public void testSmallExpansionAllPartitions() throws Exception {
        int[] qids = { 5 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionAllPartitions");
        testExpansion(1, 10, qids, "testSmallExpansionAllPartitions");
    }

    @Test
    public void testSmallExpansionSinglePartition() throws Exception {
        int[] qids = { 6, 6, 6, 6, 6 };
        numRows = 3000;
        batchsize = 50;
        System.out.println("XXXX testSmallExpansionSinglePartition");
        testExpansion(1, 10, qids, "testSmallExpansionSinglePartition");
        batchsize = 5;
    }

    @Test
    public void testSmallExpansionJoin() throws Exception {
        int[] qids = { 7 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionJoin");
        testExpansion(1, 10, qids, "testSmallExpansionJoin");
    }

    @Test
    public void testSmallExpansionInnerJoin() throws Exception {
        int[] qids = { 9 };
        numRows = 1000;
        System.out.println("XXXX testSmallExpansionInnerJoin");
        testExpansion(1, 10, qids, "testSmallExpansionInnerJoin");
    }

    @Test
    public void testBigExpansionJoin() throws Exception {
        int[] qids = { 7 };
        numRows = 10000;
        batchsize = 50;
        System.out.println("XXXX testBigExpansionJoin");
        testExpansion(3, 20, qids, "testBigExpansionJoin");
        batchsize = 5;
    }

    @Test
    public void testBigExpansionInnerJoin() throws Exception {
        int[] qids = { 9 };
        numRows = 10000;
        System.out.println("XXXX testBigExpansionInnerJoin");
        testExpansion(3, 20, qids, "testBigExpansionInnerJoin");
    }

    @Test
    public void testBigExpansion() throws Exception {
        int[] qids = { 8 };
        numRows = 100000;
        System.out.println("XXXX testBigExpansion");
        testExpansion(3, 50, qids, "testBigExpansion");
    }

    @Test
    public void testBigExpansionMix() throws Exception {
        int[] qids = { 0, 1, 4 };
        numRows = 10000;
        batchsize = 50;
        System.out.println("XXXX testBigExpansionMix");
        testExpansion(3, 20, qids, "testBigExpansionMix");
        batchsize = 5;
    }

    private void testExpansion(
        int capacity,
        int partitions,
        int[] qids,
        String testCaseName)
        throws Exception {

        logger.fine(() -> "Initializing");
        createStore(capacity, partitions);
        createTableAndIndex();
        if (testName.equals("testSmallExpansionSinglePartition")) {
            populateRows("users2", testCaseName);
        } else {
            populateRows("users", testCaseName);
        }

        TestState testState = new TestState(qids.length);

        logger.fine(() -> "Starting elasticity and query threads");
        startElasticityThread(testState, Arrays.asList(() -> expandStore(capacity)));
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }

        for (int qid : qids) {
            startQueryThread(testState, qid);
        }

        /*
        try {
            Thread.sleep(50000);
            testState.setElasticityDone();
        } catch (InterruptedException e) {
        }
        */

        logger.fine(() -> "Waiting for threads to finish");
        final long timeoutMillis = 20 * 60 * 1000;
        PollCondition.await(
            POLL_CONDITION_INTERVAL, timeoutMillis,
            () ->
            testState.isElasticityDone() && testState.areQueriesDone());

        updateRequestDispacherTopology();
        logger.fine(() -> "Verifying results");
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
        System.out.println("XXXX testSmallContraction");
        testContraction(1, 10, qids);
    }

    @Test
    public void testSmallContractionSort() throws Exception {
        int[] qids = { 1 };
        numRows = 1000;
        System.out.println("XXXX testSmallContractionSort");
        testContraction(1, 10, qids);
    }

    public void testContraction(
        int capacity,
        int partitions,
        int[] qids) throws Exception {

        logger.fine(() -> "Initializing");
        createStore(capacity, partitions);
        expandStore(capacity);
        createTableAndIndex();
        populateRows("users", "contraction");
        TestState testState = new TestState(qids.length);

        System.out.println("XXXX Starting store contraction");

        logger.fine(() -> "Starting elasticity and query threads");

        for (int qid : qids) {
            startQueryThread(testState, qid);
        }

        inContraction = true;

        //testState.setQueryAndVerifyDone();
        startElasticityThread(testState, Arrays.asList(() -> contractStore()));

        /* Waits for both to finish. */
        logger.fine(() -> "Waiting for threads to finish");
        final long timeoutMillis = 1000 * 1000;
        PollCondition.await(
            POLL_CONDITION_INTERVAL, timeoutMillis,
            () ->
            testState.isElasticityDone() && testState.areQueriesDone());

        System.out.println("XXXX Store contraction done");

        inContraction = false;

        /* Verify the results. */
        updateRequestDispacherTopology();
        logger.fine(() -> "Verifying results");
        verifyTestState(testState);
    }

    /**
     * Tests the basic case that a query is executed under multiple elasticity
     * operations.. This test is expected to exercise the most basic
     * interaction between query and partition migration.
     */
    //@Test
    public void testQueryUnderMultipleElasticityBasic() throws Exception {
        /* Initializes */
        logger.fine(() -> "Initializing");
        createStore(1/*capacity*/, 10/*partitions*/);
        createTableAndIndex();
        populateRows("users", "foo");
        final TestState testState = new TestState(1);
        /* Starts the elasticity and query threads. */
        logger.fine(() -> "Starting elasticity and query threads");
        startQueryThread(testState, 0);
        startElasticityThread(
            testState,
            Arrays.asList(
                () -> expandStore(1),
                () -> contractStore(),
                () -> expandStore(1)));
        /* Waits for both to finish. */
        logger.fine(() -> "Waiting for threads to finish");
        final long timeoutMillis = 180 * 1000;
        PollCondition.await(
            POLL_CONDITION_INTERVAL, timeoutMillis,
            () ->
            testState.isElasticityDone() && testState.areQueriesDone());
        /* Verify the results. */
        updateRequestDispacherTopology();
        logger.fine(() -> "Verifying results");
        verifyTestState(testState);
    }
}
