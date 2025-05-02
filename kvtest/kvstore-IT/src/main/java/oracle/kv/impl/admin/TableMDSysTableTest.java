/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.api.table.TableSysTableUtil.getGCSeqNum;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_DELETED;
import static oracle.kv.impl.systables.TableMetadataDesc.NAMESPACE_TYPE;
import static oracle.kv.impl.systables.TableMetadataDesc.TABLE_NAME;
import static oracle.kv.impl.systables.TableMetadataDesc.TABLE_TYPE;
import static org.junit.Assert.assertEquals;

import oracle.kv.StatementResult;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.util.ShutdownThread;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

import org.junit.Test;

import java.util.logging.Logger;

/**
 * Tests the table metadata system table operations not covered by normal
 * DDL/DML and table tests.
 */
public class TableMDSysTableTest extends TableTestBase {

    /**
     * Test the table garbage collector.
     */
    @Test
    public void testTableGC() {
        final int N_TABLES = 10;

        waitForStoreReady(createStore.getAdmin(), true);
        assertEquals("Initial deleted rows", 0, countDeletedTables());
        assertEquals("Initial namespaces", 0, countNamespaces());

        final int nSysTables = countTables();

        for (int i = 0; i < N_TABLES; i++) {
            store.executeSync("CREATE TABLE Users" + i +
                    " (id INTEGER, firstName STRING, lastName STRING," +
                    " PRIMARY KEY (id))");
            store.executeSync( "CREATE NAMESPACE IF NOT EXISTS ns" + i);
        }
        assertEquals("Deleted table markers after create",
                     0, countDeletedTables());
        assertEquals("Namespaces after after create",
                     N_TABLES, countNamespaces());

        for (int i = 0; i < N_TABLES; i++) {
            store.executeSync("DROP TABLE Users" + i);
            store.executeSync("DROP NAMESPACE ns" + i);
        }
        assertEquals("Deleted table markers after delete",
                     N_TABLES, countDeletedTables());
        assertEquals("Deleted namespace markers after GC",
                     N_TABLES, countDeletedNamespaces());

        /*
         * Create more tables to push the deleted entries down the in seq
         * number range
         */
        for (int i = 0; i < N_TABLES*2; i++) {
            store.executeSync("CREATE TABLE UsersII" + i +
                    " (id INTEGER, firstName STRING, lastName STRING," +
                    " PRIMARY KEY (id))");
        }

        final Table sysTable = tableImpl.getTable(TABLE_NAME);
        assertEquals("GS seq number", 1, getGCSeqNum(sysTable, tableImpl, true));

        /*
         * High seq number should be around: tables/namespaces created plus
         * tables/namespaces deleted plus the system tables. We double that
         * to make sure the GC seq num is set high enough to catch all the
         * deleted markers.
         */
        int highSeqNum = (nSysTables + (N_TABLES * 6)) * 2;
        int nRemoved = MDTableUtil.gcTable(highSeqNum, sysTable, tableImpl,
                               new Dummy(),
                               LoggerUtils.getLogger(this.getClass(),
                                                 "TableMDSysTableTest"));
        assertEquals("Number of rows GCed", N_TABLES*2, nRemoved);
        assertEquals("Deleted table markers after GC",
                0, countDeletedTables());
        assertEquals("Deleted namespace markers after GC",
                0, countDeletedNamespaces());
    }

    private int countTables() {
        return countRows("SELECT * FROM " + TABLE_NAME +
                         " WHERE Type = \"" + TABLE_TYPE + "\" AND " +
                         COL_NAME_DELETED + " = false");
    }

    private int countDeletedTables() {
        return countRows("SELECT * FROM " + TABLE_NAME +
                " WHERE Type = \"" + TABLE_TYPE + "\" AND " +
                COL_NAME_DELETED + " = true");
    }

    private int countNamespaces() {
        return countRows("SELECT * FROM " + TABLE_NAME +
                " WHERE Type = \"" + NAMESPACE_TYPE + "\" AND " +
                COL_NAME_DELETED + " = false");
    }

    private int countDeletedNamespaces() {
        return countRows("SELECT * FROM " + TABLE_NAME +
                " WHERE Type = \"" + NAMESPACE_TYPE + "\" AND " +
                COL_NAME_DELETED + " = true");
    }

    private int countRows(String statement) {
        StatementResult res =
                store.executeSync(statement,
                                  new ExecuteOptions().setResultsBatchSize(10));
        int count = 0;
        final TableIterator<RecordValue> iterator = res.iterator();
        try {
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
        } finally {
            iterator.close();
        }
        return count;
    }

    /* Dummy shutdown thread to pass to GC method */
    private static class Dummy extends ShutdownThread {
        protected Dummy() {
            super("DUMMY");
        }

        @Override
        protected Logger getLogger() {
            return null;
        }
    }
}
