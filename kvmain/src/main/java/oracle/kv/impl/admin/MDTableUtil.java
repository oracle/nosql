/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */
package oracle.kv.impl.admin;

import static oracle.kv.impl.api.table.TableMetadata.createCompareMap;
import static oracle.kv.impl.systables.TableMetadataDesc.TABLE_NAME;

import oracle.kv.MetadataNotFoundException;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableSysTableUtil;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.ShutdownThread;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility methods for accessing the table metadata system table by the Admin.
 */
public class MDTableUtil extends TableSysTableUtil {

    /**
     * Overrides the deleted threshold for testing.
     */
    static volatile Optional<Integer> deletedThresholdOverride =
        Optional.empty();

    /*
     * The number of deleted that will trigger cleaning of the system table.
     *
     * TODO - parameter?
     */
    private static final int DEFAULT_DELETED_THRESHOLD = 1000;
    private static final Supplier<Integer> GET_DELETED_THRESHOLD =
        () -> deletedThresholdOverride.orElse(DEFAULT_DELETED_THRESHOLD);

    /* Prevent construction */
    private MDTableUtil() {}

    /* -- Table MD update methods */
    /*
     * In general all write operations should go through here. The
     * pattern is to check if writes are enabled, then do the operation.
     * Writes are enabled by detecting user table activity, basically any
     * operation that isn't related to a system table.
     *
     * These methods catch MetadataNotFoundException and ignore them. This
     * exception is thrown if the RN does not yet have the system table,
     * usually at store startup either by a test or system table creation.
     * It is ok to ignore. Eventually the MD table will be updated by check()
     * when the system table monitor runs.
     */

    /**
     * Update a table.
     *
     * See TableSysTableUtil.updateTable()
     */
    public static void updateTable(TableImpl table,
                                   TableMetadata md,
                                   Admin admin) {
        if (!admin.systemTablesEnabled(!table.isSystemTable())) {
            return;
        }
        final Table sysTable = md.getTable(TABLE_NAME);
        if (sysTable != null) {
            try {
                updateTable(table, md, sysTable,
                        admin.getInternalKVStore().getTableAPI(),
                        admin.getLogger());
            } catch (MetadataNotFoundException mnfe) {
                /*  System table not ready, ignore. */
            }
        }
    }

    /**
     * Remove a table.
     *
     * See TableSysTableUtil.removeTable()
     */
    public static void removeTable(TableImpl table,
                                   TableMetadata md,
                                   boolean markForDelete,
                                   Admin admin) {
        if (!admin.systemTablesEnabled(!table.isSystemTable())) {
            return;
        }
        final Table sysTable = md.getTable(TABLE_NAME);
        if (sysTable != null) {
            try {
                removeTable(table, md, markForDelete, sysTable,
                            admin.getInternalKVStore().getTableAPI(),
                            admin.getLogger());
            } catch (MetadataNotFoundException mnfe) {
                /*  System table not ready, ignore. */
            }
        }
    }

    /**
     * Add a namespace.
     *
     * See TableSysTableUtil.addNamespace()
     */
    public static void addNamespace(TableMetadata.NamespaceImpl ns,
                                    TableMetadata md,
                                    Admin admin) {
        if (!admin.systemTablesEnabled(true)) {
            return;
        }
        final Table sysTable = md.getTable(TABLE_NAME);
        if (sysTable != null) {
            try {
                addNamespace(ns, md.getSequenceNumber(), sysTable,
                             admin.getInternalKVStore().getTableAPI(),
                             admin.getLogger());
            } catch (MetadataNotFoundException mnfe) {
                /*  System table not ready, ignore. */
            }
        }
    }

    /**
     * Remove a namespace.
     *
     * See: TableSysTableUtil.removeNamespace()
     */
    public static void removeNamespace(String namespace,
                                       TableMetadata md,
                                       Admin admin) {
        if (!admin.systemTablesEnabled(true)) {
            return;
        }
        final Table sysTable = md.getTable(TABLE_NAME);
        if (sysTable != null) {
            try {
                removeNamespace(namespace, md.getSequenceNumber(), sysTable,
                                admin.getInternalKVStore().getTableAPI(),
                                admin.getLogger());
            } catch (MetadataNotFoundException mnfe) {
                /*  System table not ready, ignore. */
            }
        }
    }

    /**
     * Update a region.
     *
     * See TableSysTableUtil.updateRegion()
     */
    public static void updateRegion(Region region,
                                    TableMetadata md,
                                    Admin admin) {
        if (!admin.systemTablesEnabled(true)) {
            return;
        }
        final Table sysTable = md.getTable(TABLE_NAME);
        if (sysTable != null) {
            try {
                updateRegion(region, md.getSequenceNumber(), sysTable,
                             admin.getInternalKVStore().getTableAPI(),
                             admin.getLogger());
            } catch (MetadataNotFoundException mnfe) {
                /*  System table not ready, ignore. */
            }
        }
    }

    /* -- Table metadata system table maintenance -- */

    /**
     * Checks whether the table MD system table exist and, if writes to system
     * tables is permitted, its entries match the current Admin table metadata.
     * System table writes may not be permitted because 1) the Admin just
     * started (this state is not persisted), 2) there are no user tables
     * (it's likely the store is used as a Key/Value store), or 3) the store
     * has not yet been deployed.
     *
     * Returns true if the check is complete, i.e. the system table is
     * up-to-date, or if writes to the system table are disabled.
     */
    static boolean check(Admin admin, ShutdownThread caller) {
        final TableMetadata md = admin.getMetadata(TableMetadata.class,
                                                   Metadata.MetadataType.TABLE);
        if (md == null) {
            return false;
        }
        final Table sysTable = md.getTable(TABLE_NAME);
        if (sysTable == null) {
            return false;
        }

        /*
         * Maps of the current metadata. As we scan through the system table,
         * entries are removed which do not need updating. The entries
         * remaining (if any) are then written to the system table.
         */
        final Map<String, TableMetadata.NamespaceImpl> namespaces =
                                            createCompareMap();
        namespaces.putAll(md.getNamespaces());
        final Map<Integer, Region> regions = new HashMap<>();
        for (Region region : md.getKnownRegions()) {
            regions.put(region.getId(), region);
        }
        final Map<String, Table> tables = createCompareMap();
        tables.putAll(md.getTables());

        if (!admin.systemTablesEnabled(false)) {
            /*
             * If writes are not enabled, check whether there are user
             * tables present and try again. This will usually happen when
             * the Admin is first started.
             */
            boolean userTablesPresent = !namespaces.isEmpty() ||
                                        !regions.isEmpty() ||
                                        checkForUserTables(tables.values());
            if (!admin.systemTablesEnabled(userTablesPresent)){
                /*
                 * Writes to system tables are not yet enabled. If user tables
                 * are present, then the store is not ready (this should only
                 * happen during store startup). In that case return false
                 * which will keep the monitor thread running.
                 */
                return !userTablesPresent;
            }
        }

        final Logger logger = admin.getLogger();
        logger.info("Verifying table system table against " + md);

        final AtomicInteger deleted = new AtomicInteger(0);
        final AtomicInteger highSeqNum = new AtomicInteger(0);

        /*
         * Iterate through the system table, checking whether the entries
         * are present in the MD, or otherwise need to be updated or
         * removed.
         */
        try {
            final TableAPI tableAPI = admin.getInternalKVStore().getTableAPI();
            getAllRows(sysTable, tableAPI, new RowCallback() {
                @Override
                public boolean namespaceRow(String name, int seqNum, Row row) {
                    if (caller.isShutdown()) {
                        return false;
                    }
                    if (seqNum > highSeqNum.get()) {
                        highSeqNum.set(seqNum);
                    }
                    logger.fine(() ->
                            "Checking system table record for " + name);
                    final TableMetadata.NamespaceImpl ns = getNamespace(row);
                    if (ns == null) {
                        /* Deleted */
                        deleted.incrementAndGet();
                        return true;
                    }
                    final TableMetadata.NamespaceImpl currentNS =
                                                    namespaces.get(name);
                    if (currentNS == null) {
                        logger.info("Replacing active namespace with" +
                                " deleted marker");
                        /* Namespace is gone, replace with a deleted marker */
                        removeNamespace(name, md.getSequenceNumber(),
                                sysTable, tableAPI, logger);
                        return true;
                    }
                    if (currentNS.equals(ns)) {
                        /* No change */
                        namespaces.remove(name);
                    }
                    return true;
                }

                @Override
                public boolean regionRow(String key, int seqNum, Row row) {
                    if (caller.isShutdown()) {
                        return false;
                    }
                    if (seqNum > highSeqNum.get()) {
                        highSeqNum.set(seqNum);
                    }
                    /* Regions are never deleted */
                    final Region region = getRegion(row);
                    final Region currentRegion = regions.get(region.getId());
                    if (currentRegion.equals(region)) {
                        /* No change */
                        regions.remove(region.getId());
                    }
                    return true;
                }

                @Override
                public boolean tableRow(String name, int seqNum, Row row) {
                    if (caller.isShutdown()) {
                        return false;
                    }
                    if (seqNum > highSeqNum.get()) {
                        highSeqNum.set(seqNum);
                    }
                    final TableImpl table = getTable(row);
                    if (table == null) {
                        /* Deleted */
                        deleted.incrementAndGet();
                        return true;
                    }
                    final TableImpl current = (TableImpl) tables.get(name);
                    if (current == null) {
                        /*
                         * The table is gone. This should not happen, but it
                         * can be fixed by replacing the record with a deleted
                         * marker
                         */
                        logger.info("Stale table record for " +
                                table.getFullName() +
                                " in system table, inserting a" +
                                " deleted marker");
                        removeTable(table, md, false,
                                sysTable, tableAPI, logger);
                        return true;
                    }
                    if (table.getSequenceNumber() ==
                            current.getSequenceNumber()) {
                        /* No changes, can remove from update list */
                        tables.remove(getNameString(table));
                    } else if (table.getSequenceNumber() >
                            current.getSequenceNumber()) {
                        /* Table in system table is newer? */
                        // TODO - error!!!
                        logger.warning("Table record for " +
                                table.getFullName() +
                                " in system table is newer" +
                                " than metadata!");
                    } /* else { table is old, and needs updating } */
                    return true;
                }
            });

            logger.info("Verify found " + tables.size() + " tables, " +
                    namespaces.size() + " namspaces, " +
                    regions.size() + " regions in need of updating, " +
                    deleted.get() + " DELETED markers");

            /* What remains in the maps need to be written */
            for (TableMetadata.NamespaceImpl ns : namespaces.values()) {
                if (caller.isShutdown()) {
                    return false;
                }
                addNamespace(ns, md.getSequenceNumber(),
                             sysTable, tableAPI, logger);
            }
            for (Region region  : regions.values()) {
                if (caller.isShutdown()) {
                    return false;
                }
                updateRegion(region, md.getSequenceNumber(),
                             sysTable, tableAPI, logger);
            }
            for (Table table : tables.values()) {
                if (caller.isShutdown()) {
                    return false;
                }
                updateTable((TableImpl)table, md, sysTable, tableAPI, logger);
            }

            /*
             * Attempt to set the GC seq number to 1. This will write
             * a one if the seq number has not been written before. If
             * there is a number it will be >= 1 and this set will do
             * nothing.
             */
            int gcSeqNum = setGCSeqNum(1, sysTable, tableAPI, logger);
            logger.info("Verifying table system table successful," +
                        " GCseq#=" + gcSeqNum);
            admin.systemTablesReady();
        } catch (Exception ex) {
            logger.log(Level.WARNING,
                    "Unexpected exception checking table metadata" +
                            " system table", ex);
            return false;
        }

        /* Check successful, GC table if needed */
        if (deleted.get() > GET_DELETED_THRESHOLD.get()) {
            try {
                gcTable(highSeqNum.get(), sysTable,
                        admin.getInternalKVStore().getTableAPI(),
                        caller, logger);
            } catch (Exception ex) {
                logger.log(Level.WARNING,
                        "Unexpected exception garbage collecting table" +
                                " metadata system table", ex);
            }
        }
        return true;
    }

    private static boolean checkForUserTables(Collection<Table> tables) {
        for (Table table : tables) {
            if (!((TableImpl)table).isSystemTable()) {
                return true;
            }
        }
        return false;
    }

    /*
     * GC the system table. This may set the GC sequence number (GCSN), and
     * then scan the rows in sequence number order, removing any DELETED
     * namespace or table rows with sequence numbers up to, but not including
     * the GCSN. Returns the number of rows removed.
     *
     * Package private for unit test.
     */
    static int gcTable(int highSeqNum,
                       Table sysTable,
                       TableAPI tableAPI,
                       ShutdownThread caller,
                       Logger logger) {
        /*
         * Move the GC to halfway between its current position
         * and highSeqNum.
         */
        int currentGCSN = getGCSeqNum(sysTable, tableAPI, true);
        int newGCSN = (highSeqNum - currentGCSN) / 2;
        if (newGCSN <= 0) {
            return 0;
        }
        final int gcSeqNum = setGCSeqNum(newGCSN, sysTable, tableAPI, logger);
        logger.info("Scanning for DELETED rows up to sequence number " +
                gcSeqNum);

        final AtomicInteger nRows = new AtomicInteger(0);

        /*
         * Do a sequential scan by sequence number, deleting any DELETED
         * rows encountered that have sequence numbers less than GCSN.
         * Quit the scan if shutdown or when the seqNum reach GCSN.
         */
        getSequentialRows(sysTable, tableAPI, new RowCallback() {
            @Override
            public boolean namespaceRow(String name, int seqNum, Row row) {
                if (caller.isShutdown() || (seqNum >= gcSeqNum)) {
                    return false;
                }
                if (isDeleted(row)) {
                    logger.fine("Removing DELETED namespace " + name +
                            ", seqNum=" + seqNum);
                    delete(row, sysTable, tableAPI);
                    nRows.incrementAndGet();
                }
                return true;
            }

            @Override
            public boolean tableRow(String name, int seqNum, Row row) {
                if (caller.isShutdown() || (seqNum >= gcSeqNum)) {
                    return false;
                }
                if (isDeleted(row)) {
                    logger.fine("Removing DELETED table " + name +
                            ", seqNum=" + seqNum);
                    delete(row, sysTable, tableAPI);
                    nRows.incrementAndGet();
                }
                return true;
            }
        });
        return nRows.get();
    }

    /**
     * Waits for the store to be ready for testing and returns {@code true} if
     * the store is ready. If forTables is true, also waits for all system
     * tables to be created and the MD system table populated.
     */
    public static boolean waitForStoreReady(CommandServiceAPI cs,
                                            boolean forTables,
                                            int checkIntervalMillis,
                                            int timeoutMillis)
    {
        return new PollCondition(checkIntervalMillis, timeoutMillis) {
            @Override
            protected boolean condition() {
                try {
                    return cs.isStoreReady(forTables);
                } catch (RemoteException e) {
                    return false;
                }
            }
        }.await();
    }

}
