/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.pubsub;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVStoreFactory;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.pubsub.StreamDelEvent;
import oracle.kv.impl.pubsub.StreamPutEvent;
import oracle.kv.impl.pubsub.StreamSequenceId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.WriteOptions;

import org.junit.After;
import org.junit.Before;

public class StreamRowMDTestBase extends PubSubTestBase {

    private static final WriteOptions WRITE_OPTIONS =
        new WriteOptions(Durability.COMMIT_NO_SYNC, 10000, MILLISECONDS);

    private static final String TEST_MRT_NAME = "MRTableUser";
    private static final String TEST_TABLE_NAME = "User";
    private static final int INSERTS = 10;
    private static final int UPDATES = 10;
    private static final int DELETES = 10;
    private NoSQLPublisher publisher;

    @Before
    @Override
    public void setUp() throws Exception {

        super.setUp();

        /* override default */
        repFactor = 1;
        numStorageNodes = 1;
        numPartitions = 12;
        useFeederFilter = true;

        startStore();
        kvStoreConfig = createKVConfig(createStore);
        store = KVStoreFactory.getStore(kvStoreConfig);
        createDefaultTestNameSpace();
        tableAPI = store.getTableAPI();
        publisher = null;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
        if (createStore != null) {
            createStore.shutdown();
        }
        super.tearDown();
    }

    void testBody(boolean mrTable) {

        /* create table */
        final TableImpl tb = (TableImpl) creatTable(mrTable);
        assertEquals(mrTable, tb.isMultiRegion());

        /* create stream */
        final NoSQLSubscription stream =
            createStream(tb.getFullNamespaceName());

        /* do insert, update and delete */
        doInsertUpdateDelete(tb.getFullNamespaceName());

        /* stream all operations */
        waitForStreamDone(stream);

        /* verify the row md in streamed put and deletes */
        verifyRowMDInStream(stream);

        if (mrTable) {
            verifyRowMDTombstone();
        }
    }

    private void doInsertUpdateDelete(String tbName) {
        final TableAPI tapi = store.getTableAPI();
        final Table table = tapi.getTable(tbName);
        assertNotNull("Not fond table=" + tbName, table);

        /* insert */
        IntStream.range(0, INSERTS).forEach(i -> {
            final Row row = putRow(table, i);
            trace("Insert row=" + row.toJsonString(false) +
                  ", row md=" + row.getRowMetadata());
        });

        /* updates */
        IntStream.range(0, UPDATES).forEach(i -> {
            final Row row = putRow(table, i);
            trace("Update row=" + row.toJsonString(false) +
                  ", row md=" + row.getRowMetadata());
        });

        /* deletes */
        final PrimaryKey pk = table.createPrimaryKey();
        IntStream.range(0, DELETES).forEach(i -> {
            pk.put("id", i);
            pk.setRowMetadata(TEST_ROW_MD);
            final boolean succ = tapi.delete(pk, null, WRITE_OPTIONS);
            assertTrue("Fail to delete pk=" + pk.toJsonString(false), succ);
            trace("Delete primary key=" + pk.toJsonString(false) +
                  ", row md=" + pk.getRowMetadata());
        });
    }

    private Row putRow(Table table, int i) {
        final TableAPI tapi = store.getTableAPI();
        final Row row = table.createRow();
        row.put("id", i);
        row.put("desc", UUID.randomUUID().toString().substring(0, 4));
        row.setRowMetadata(TEST_ROW_MD);
        final Version ver = tapi.put(row, null, WRITE_OPTIONS);
        assertNotNull("Fail to write row=" + row.toJsonString(false), ver);
        return row;
    }

    private void verifyRowMDInStream(NoSQLSubscription stream) {
        final TestSubscriber subscriber =
            (TestSubscriber) (((NoSQLSubscriptionImpl) stream).getSubscriber());

        final List<StreamOperation> puts = subscriber.getPutOps();

        puts.stream().map(sp -> sp.asPut().getRow())
            .forEach(row -> {
                trace("[PUT]" + row.toJsonString(false) +
                      ", row md=" + row.getRowMetadata());
                assertEquals(TEST_ROW_MD, row.getRowMetadata());
            });
        trace("Done verifying all row md in stream put operations");


        final List<StreamOperation> dels = subscriber.getDelOps();
        dels.stream().map(sp -> sp.asDelete().getPrimaryKey())
            .forEach(pk -> {
                trace("[DELETE]" + pk.toJsonString(false) +
                      ", row md=" + pk.getRowMetadata());
                assertEquals(TEST_ROW_MD, pk.getRowMetadata());
            });
        trace("Done verifying all row md in stream delete operations");
    }

    private void waitForStreamDone(NoSQLSubscription stream) {

        final TestSubscriber subscriber =
            (TestSubscriber) (((NoSQLSubscriptionImpl) stream).getSubscriber());
        final int expPuts = INSERTS + UPDATES;
        final int expDels = DELETES;
        try {
            waitFor(new PollCondition(TEST_POLL_INTERVAL_MS,
                                      TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final int actPuts = subscriber.getPutOps().size();
                    final int actDels = subscriber.getDelOps().size();
                    trace("#puts=" + actPuts + "(exp=" + expPuts + ")" +
                          ", #dels=" + actDels + "(exp=" + expDels + ")");
                    return actPuts == expPuts && actDels == expDels;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
    }

    private NoSQLSubscription createStream(String tableName) {

        ckptTableName = "TestCkptTable";
        /* create a publisher */
        final NoSQLPublisherConfig pconf =
            new NoSQLPublisherConfig.Builder(kvStoreConfig, testPath)
                .build();

        publisher = NoSQLPublisher.get(pconf, logger);
        trace("publisher created");

        /* create a subscriber */
        final NoSQLSubscriptionConfig subConf =
            new NoSQLSubscriptionConfig.Builder(ckptTableName)
                .setSubscribedTables(tableName)
                .build();
        final TestSubscriber subscriber = new TestSubscriber(subConf);
        trace("subscriber created: " + subscriber.getSubscriptionConfig());

        publisher.subscribe(subscriber);
        final boolean succ = new PollCondition(TEST_POLL_INTERVAL_MS,
                                               TEST_POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
                return subscriber.isSubscriptionSucc();
            }
        }.await();
        if (!succ) {
            fail("Timeout in waiting");
        }

        final NoSQLSubscription subscription = subscriber.getSubscription();
        subscription.request(Long.MAX_VALUE);
        trace("Stream started");
        return subscription;
    }

    private Table creatTable(boolean mrTable) {
        if (mrTable) {
            mrTableRegionSetUp(store);
            return createTestTable(TEST_MRT_NAME, true);
        }
        return createTestTable(TEST_TABLE_NAME, false);
    }

    private Table createTestTable(String tableName, boolean mrtable) {
        /* create test tables */
        String ddl = "CREATE TABLE " + tableName + " " +
                     "(id INTEGER, desc STRING, PRIMARY KEY (id))";
        if (mrtable) {
            ddl += " IN REGIONS " + LOCAL_REGION + ", " + REMOTE_REGION;
        }
        store.executeSync(ddl);
        /* Ensure table created */
        final Table table = tableAPI.getTable(tableName);
        assertNotNull("table " + tableName + " not created", table);
        trace("src table " + tableName + " have been created, " +
              "ddl=" + ddl);
        return table;
    }

    private void verifyRowMDTombstone() {
        final List<Row> rows = getTombstoneFromTable();
        rows.forEach(row -> {
            assertNotNull(row);
            final RowImpl tombstoneRow = (RowImpl) row;
            trace("Tombstone row=" + tombstoneRow.toJsonString(false) +
                  ", creation=" + tombstoneRow.getCreationTime() +
                  ", last update=" + tombstoneRow.getLastModificationTime() +
                  ", region id=" + tombstoneRow.getRegionId() +
                  ", row md=" + tombstoneRow.getRowMetadata());
            assertTrue(tombstoneRow.isTombstone());
            /* tombstone has some metadata */

            // todo: uncomment check when creation time feature is enabled
            //assertTrue(row.getCreationTime() > 0);
            assertTrue(row.getLastModificationTime() > 0);
            assertTrue(tombstoneRow.getRegionId() > 0);
            assertEquals(TEST_ROW_MD, row.getRowMetadata());
        });
    }

    private List<Row> getTombstoneFromTable() {
        final List<Row> ret = new ArrayList<>();
        final TableAPI tapi = store.getTableAPI();
        final Table table = tapi.getTable(TEST_MRT_NAME);
        assertNotNull(table);

        final TableIteratorOptions iter_opt = new TableIteratorOptions(
            Direction.FORWARD,
            Consistency.ABSOLUTE,
            /* iterator timeout upper bounded by store read timeout */
            ((KVStoreImpl) store).getReadTimeoutMs(),
            TimeUnit.MILLISECONDS,
            1,
            16);

        /* always include tombstones in MR table initialization */
        iter_opt.setIncludeTombstones();

        final TableIterator<Row> iter = tapi.tableIterator(
            table.createPrimaryKey(), null, iter_opt);
        while (iter.hasNext()) {
            final RowImpl tombstoneRow = (RowImpl) iter.next();
            if (tombstoneRow.isTombstone()) {
                assertNotNull(tombstoneRow);
                ret.add(tombstoneRow);
            }
        }
        return ret;
    }

    private class TestSubscriber extends TestNoSQLSubscriberBase {

        private final List<StreamOperation> putOps;
        private final List<StreamOperation> delOps;

        TestSubscriber(NoSQLSubscriptionConfig config) {
            super(config);
            putOps = Collections.synchronizedList(new ArrayList<>());
            delOps = Collections.synchronizedList(new ArrayList<>());
        }

        @Override
        public void onNext(StreamOperation t) {
            if (t instanceof StreamPutEvent) {
                putOps.add(t);
                return;
            }

            if (t instanceof StreamDelEvent) {
                delOps.add(t);
                return;
            }

            throw new IllegalStateException("Receive unsupported stream " +
                                            "operation from shard " +
                                            t.getRepGroupId() +
                                            ", seq: " +
                                            ((StreamSequenceId)
                                                t.getSequenceId())
                                                .getSequence());
        }

        List<StreamOperation> getPutOps() {
            return putOps;
        }

        List<StreamOperation> getDelOps() {
            return delOps;
        }
    }
}
