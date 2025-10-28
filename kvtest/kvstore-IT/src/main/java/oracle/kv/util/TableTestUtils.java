/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Map;

import oracle.kv.KVStore;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;

/**
 * This is a utility class for Table and Index test, which encapsulates
 * methods to add and remove table or index.
 */
public class TableTestUtils {


    public static void addTable(TableBuilder builder,
                                boolean shouldSucceed,
                                boolean noMRTableMode,
                                KVStore kvStore)
        throws Exception {
        TableTestBase.addTable(builder.buildTable(), shouldSucceed,
                               builder.getNamespace(), noMRTableMode,
                               kvStore);
    }

    public static void evolveTable(TableEvolver evolver,
                                   boolean shouldSucceed,
                                   CommandServiceAPI cs,
                                   KVStore storeAPI)
        throws Exception {

        try {
            TableImpl table = evolver.getTable();
            int planId = cs.createEvolveTablePlan("EvolveTable",
                                                  table.getInternalNamespace(),
                                                  table.getFullName(),
                                                  evolver.getTableVersion(),
                                                  table.getFieldMap(),
                                                  table.getDefaultTTL(),
                                                  table.getBeforeImageTTL(),
                                                  table.getRemoteRegions());
            execPlan(cs, planId, "evolveTable", shouldSucceed, storeAPI);
        } catch (AdminFaultException ice) {
            if (shouldSucceed) {
                fail("evolveTable failed");
            }
        }
    }

    public static void removeTables(Map<String, Table> tables,
                                    CommandServiceAPI cs,
                                    KVStore kvStore)
        throws Exception {

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getValue().getChildTables().size() > 0) {
                removeTables(entry.getValue().getChildTables(), cs, kvStore);
            }
            TableImpl table = (TableImpl)entry.getValue();

            if (table.isSystemTable()) {
                continue;
            }
            removeTable(table.getInternalNamespace(), table.getFullName(),
                        null, cs, kvStore);
        }
    }

    /**
     * Remove a table, either checking for success or failure, or ignoring the
     * result if shouldSucceed is null.
     */
    public static void removeTable(String namespace,
                                   String tableName,
                                   Boolean shouldSucceed,
                                   CommandServiceAPI cs,
                                   KVStore kvStore)
        throws Exception {

        /**
         * TODO: With the drop table optimization, record deletion is async and
         * therefore records may still be accessible via the KV API after plan
         * completion. If the caller will use the KV API to check for records
         * (as in TableNestingTest.nestedTables), then we may need to call a JE
         * API here to wait for the async operation to complete.
         */
        try {
            int planId = cs.createRemoveTablePlan("RemoveTable",
                                                  namespace,
                                                  tableName);
            execPlan(cs, planId, "removeTable", shouldSucceed, kvStore);
        } catch (AdminFaultException ice) {
            if (Boolean.TRUE.equals(shouldSucceed)) {
                throw new RuntimeException(
                    "removeTable failed for table " + tableName + ": " + ice,
                    ice);
            }
        }
    }

    public static void addIndex(TableImpl table, String indexName,
                                String[] indexFields,
                                boolean shouldSucceed,
                                CommandServiceAPI cs,
                                KVStore kvStore)
            throws Exception {
        addIndex(table, indexName, indexFields, null, shouldSucceed, cs,
                 kvStore);
    }

    public static void addIndex(TableImpl table,
                                String indexName,
                                String[] indexFields,
                                FieldDef.Type[] indexTypes,
                                boolean shouldSucceed,
                                CommandServiceAPI cs,
                                KVStore kvStore)
        throws Exception {

        try {
            int planId = cs.createAddIndexPlan("AddIndex",
                                               table.getInternalNamespace(),
                                               indexName,
                                               table.getFullName(),
                                               indexFields,
                                               indexTypes,
                                               true,
                                               null);
            execPlan(cs, planId, "addIndex", shouldSucceed, kvStore);
        } catch (AdminFaultException ice) {
            if (shouldSucceed) {
                ice.printStackTrace();
                fail("addIndex failed");
            }
        }
    }

    public static void removeIndex(String namespace,
                                   String tableName, String indexName,
                                   boolean shouldSucceed, CommandServiceAPI cs,
                                   KVStore kvStore)
        throws Exception {

        try {
            int planId = cs.createRemoveIndexPlan("RemoveIndex",
                                                  namespace,
                                                  indexName,
                                                  tableName);
            execPlan(cs, planId, "removeIndex", shouldSucceed, kvStore);
        } catch (AdminFaultException ice) {
            if (shouldSucceed) {
                fail("removeIndex failed");
            }
        }
    }

    private static void execPlan(CommandServiceAPI cs, int planId,
                                 String testCase, Boolean shouldSucceed,
                                 KVStore kvStore)
        throws Exception {

        try {
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            Plan.State state = cs.awaitPlan(planId, 0, null);
            /*
             * Since we are executing plans directly vs. DDL and bypassing the
             * the client APIs the table cache must be manually cleared.
             */
            clearTableCache(kvStore);
            if (Boolean.TRUE.equals(shouldSucceed)) {
                assertSame(Plan.State.SUCCEEDED, state);
            } else if (Boolean.FALSE.equals(shouldSucceed)) {
                assertSame(Plan.State.ERROR, state);
            }
        } catch (AdminFaultException ice) {
            if (Boolean.TRUE.equals(shouldSucceed)) {
                fail("Plan testcase failed: " + testCase);
            }
        }
    }

    /**
     * Clears the cache of the specified client API.
     */
    public static void clearTableCache(KVStore kvStore) {
        ((TableAPIImpl)kvStore.getTableAPI()).clearCache();
    }
}
