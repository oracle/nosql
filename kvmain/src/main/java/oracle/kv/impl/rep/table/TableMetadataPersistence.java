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

package oracle.kv.impl.rep.table;

import static oracle.kv.impl.util.SerializationUtil.readNonNullString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.test.TestIOHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.Table;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Helper methods for persisting table metadata. This class assumes that the
 * table metadata container (TableMetadata) is persisted by the caller.
 * These methods handle the table instances, specifically the table
 * hierarchies (a top-level table and all of its child tables and indexes).
 *
 * Two types of objects are stored in the table metadata DB:
 *
 * 1. A single table TableMetadata instance (handled elsewhere).
 *
 * 2. Table hierarchies (one record per top-level table). The key is the table
 * ID as a long. The IDs are short and unique. The RN does not need to access
 * these records by table name (or ID for that matter).
 *
 * The table ID does not conflict with the metadata key which is the string
 * returned from MetadataType.getType(). The iterations over the table records
 * in this class assume that the metadata object appears BEFORE the table
 * records by key order.
 */
public class TableMetadataPersistence {
    public volatile static TestIOHook<TableImpl> PERSISTENCE_TEST_HOOK;

    /* Switch from using FastExternializable to Java serialization */
    final static short SWITCH_TO_JAVA_SERIAL = SerialVersion.V30;

    /* Record format version (post switch to Java serialization) */
    private final static short TABLE_RECORD_JAVA_V1 = SerialVersion.V30;

    private TableMetadataPersistence() {}

    /**
     * Writes the changed table instances, or all of the tables in the
     * metadata. If changed is null, all of the top level tables in the
     * metadata are persisted, and tables no longer in the metadata are
     * deleted. Otherwise only the tables in the changed list are added,
     * updated, or removed from the database as necessary.
     * The input metadata is returned.
     */
    static void writeTables(TableMetadata md,
                            Map<Long, TableImpl> changed,
                            short storeSerialVersion,
                            Database db,
                            Transaction txn,
                            TableKeyGenerator keyGen,
                            Logger logger) {
        assert txn != null;

        if (changed == null) {
            writeAll(md, db, txn, keyGen, storeSerialVersion, logger);
            return;
        }
        logger.log(Level.FINE, () -> "Partial update of " + md + " " +
                                     changed.size() + " changes");

        /* Reusable database entries */
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();

        for (TableImpl table : changed.values()) {
            assert table.isTop();

            /*
             * If the top table was dropped (no longer in the metadata), remove
             * it from the DB
             */
            if (md.getTable(table.getFullNamespaceName()) == null) {
                deleteTable(table, key, db, txn, keyGen);
                continue;
            }
            putTable(table, key, value, db, txn, keyGen, storeSerialVersion);
        }
    }

    /**
     * Writes the specified table to the database.
     */
    private static void putTable(TableImpl table,
                                 DatabaseEntry key,
                                 DatabaseEntry value,
                                 Database db, Transaction txn,
                                 TableKeyGenerator keyGen,
                                 short storeSerialVersion) {
        assert storeSerialVersion >= SerialVersion.MINIMUM;
        keyGen.setKey(key, table);
        serializeTable(table, value, storeSerialVersion);
        db.put(txn, key, value);
    }

    /*
     * Serialize a table into the database entry. The type of serialization
     * is based on the input serial version and the tables required version.
     */
    private static void serializeTable(TableImpl table,
                                       DatabaseEntry value,
                                       short storeSerialVersion) {
        final short requiredSerialVersion = table.getRequiredSerialVersion();

        /*
         * The required version could be greater than the store version if a
         * new format table was deployed before the store version was updated.
         * In this case, go ahead and serialize with the required version.
         */
        final short useVersion = (short)Math.max(storeSerialVersion,
                                                 requiredSerialVersion);

        /*
         * If the version to use is less than SWITCH_TO_JAVA_SERIAL we
         * still need to serialize using FastExternalizable.
         */
        if (useVersion < SWITCH_TO_JAVA_SERIAL) {
            value.setData(SerializationUtil.getBytes(table, useVersion));
            return;
        }

        /* OK to use Java serialization */
        serializeTable(table, value);
    }

    /*
     * Serialize a table into the database entry using Java serialization.
     */
    private static void serializeTable(TableImpl table, DatabaseEntry value) {

        /*
         * Serialize using Java serialization.
         */
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {

            /*
             * In the serialization format for FastExternalizable the serial
             * version was the first short followed by the table bytes (See
             * SerializationUtil.getBytes(FastExternalizable, short)). We
             * continue that here using TABLE_RECORD_JAVA_V1 for the serial
             * version. There isn't a need to use the required version since
             * the serial version is not used during Java de-serialization
             * after the initial check (See getTableInternal(DatabaseEntry)).
             */
            dos.writeShort(TABLE_RECORD_JAVA_V1);
            SerializationUtil.writeJavaSerial(dos, table);
            value.setData(baos.toByteArray());
        } catch (IOException ioe) {
            throw new IllegalStateException("Exception serializing table: " +
                                            table.getClass().getName(), ioe);
        }
    }

    /**
     * Deletes the specified table from the database.
     */
    private static void deleteTable(TableImpl table,
                                    DatabaseEntry key,
                                    Database db, Transaction txn,
                                    TableKeyGenerator keyGen) {
        keyGen.setKey(key, table);
        db.delete(txn, key);
    }

    /**
     * Persists all of the tables in the metadata. Tables no longer in the
     * metadata are deleted. The input metadata is returned.
     */
    private static void writeAll(TableMetadata md,
                                 Database db,
                                 Transaction txn,
                                 TableKeyGenerator keyGen,
                                 short storeSerialVersion,
                                 Logger logger) {
        logger.log(Level.FINE, () -> "Full update of " + md);

        /* Reusable database entries */
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();

        /*
         * This is a map of the tables to write. They are removed from the map
         * as they are checked against the existing tables.
         */
        final Map<String, Table> tableMap = new HashMap<>(md.getTables());

        /*
         * Read each of the existing top level table records to see if the table
         * still exists and if it has changed.
         */
        try (Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT)) {
            final DatabaseEntry newValue = new DatabaseEntry();

            /* Move the cursor to the first table record */
            keyGen.setStartKey(key);
            OperationStatus status = cursor.getSearchKeyRange(key, value,
                                                              LockMode.RMW);
            while (status == OperationStatus.SUCCESS) {
                final TableImpl oldTable;
                try {
                    oldTable = getTable(value);
                } catch (IllegalStateException ise) {
                    /*
                     * If there is an exception de-serializing the table delete
                     * the record and continue. An update of the table will
                     * restore the entry and make the table's data accessible
                     * again.
                     *
                     * If the table had indexes operations on an index will
                     * result in a SecondaryIntegrityException. In that event
                     * the index database will be removed, but will be
                     * re-created when the table is restored.
                     */
                    logSerializationFailure(logger, value, ise);
                    cursor.delete();
                    status = cursor.getNext(key, value, LockMode.RMW);
                    continue;
                }

                /* The name of the table we are currently checking */
                final String tableName = oldTable.getFullNamespaceName();

                /*
                 * If the old table is gone (not in the new MD) or the old
                 * table has a different ID delete the old table.
                 */
                final TableImpl newTable = (TableImpl)tableMap.get(tableName);
                if ((newTable == null) ||
                    (newTable.getId() != oldTable.getId())) {
                    logger.log(Level.FINE, () -> "Removing old table " +
                                                 tableName);
                    cursor.delete();
                } else {
                    /*
                     * If here, the old table and new table are the same tables,
                     * i.e. they have the same ID. Check whether the old table
                     * has changed and if so update it here.
                     */
                    serializeTable(newTable, newValue, storeSerialVersion);
                    if (!Arrays.equals(value.getData(), newValue.getData())) {
                        logger.log(Level.FINE, () -> "Detected change in " +
                                                     tableName + " updating");
                        cursor.put(key, newValue);
                    }

                    /* Remove the entry since it has been checked */
                    tableMap.remove(tableName);
                }
                status = cursor.getNext(key, value, LockMode.RMW);
            }
        }

        /*
         * At this point all changed tables have been updated and have been
         * removed from the idMap. Any remaining are new tables.
         */
        for (Table t : tableMap.values()) {
            logger.log(Level.FINE, () -> "Found new table: " +
                                         t.getFullNamespaceName());
            putTable((TableImpl)t, key, value, db, txn, keyGen,
                     storeSerialVersion);
        }
    }

    /**
     * Reads all table records and populates the specified table metadata
     * instance. Returns true if any records require updating.
     */
    public static boolean readTables(TableMetadata fetchedMD,
                                     Database db,
                                     Transaction txn,
                                     TableKeyGenerator keyGen,
                                     Logger logger) {
        fetchedMD.initializeTables(null);

        /* Reusable database entries */
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();

        boolean needsUpdate = false;

        try (Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT)) {
            keyGen.setStartKey(key);

            OperationStatus status = cursor.getSearchKeyRange(key, value, null);

            while (status == OperationStatus.SUCCESS) {
                try {
                    final TableReturn ret = getTableInternal(value);
                    if (ret.serialVersionUsed < TABLE_RECORD_JAVA_V1) {
                        needsUpdate = true;
                    }
                    final TableImpl table = ret.table;
                    fetchedMD.addTableHierarchy(table);
                    logger.log(Level.FINE, () -> "Inserted " + table +
                                                 " to " + fetchedMD);
                } catch (IllegalStateException ise) {
                    /*
                     * If there is an exception de-serializing the table ignore
                     * the table (leave it out of the in-memory copy) and
                     * continue. The table will not be accessible, but its
                     * data will remain.
                     *
                     * If the table had indexes operations on an index will
                     * result in a SecondaryIntegrityException. In that event
                     * the index database will be removed, but will be
                     * re-created when the table is restored.
                     */
                    logSerializationFailure(logger, value, ise);
                }
                status = cursor.getNext(key, value, null);
            }
            return needsUpdate;
        }
    }

    private static void logSerializationFailure(Logger logger,
                                                DatabaseEntry entry,
                                                IllegalStateException ise) {
        /*
         * Attempt to extract the table name from a database entry. It assumes
         * the entry is for a TableImpl that has been persisted via PutTable.
         */
        String tableName;
        try (final DataInputStream dis = new DataInputStream(
                   new ByteArrayInputStream(entry.getData()))) {
            /*
             * The first field is the serial version (short written by
             * SerializationUtil.getBytes()), followed by the table name string
             * (written by TableImpl.writeFastExternal()).
             */
            short serialVersion = dis.readShort();
            tableName = readNonNullString(dis, serialVersion);
        } catch (Exception e) {
            tableName = "(unknown)";
        }
        logger.log(Level.SEVERE,
                   "Unexpected exception reading table " + tableName +
                   ". The table will not be available for access but its" +
                   " data has been preserved. Access can be restored by" +
                   " updating the table via an evolve table operation.",
                   ise);
    }

    /**
     * Rewrites up to maxWrites table records that were written using
     * FastExternalizable. Returns the number of records written. Record
     * scan starts at key. On return key contains the next record to read.
     */
    static int updateTables(Database db,
                            Transaction txn,
                            DatabaseEntry key,
                            int maxWrites,
                            Logger logger) {
        final DatabaseEntry value = new DatabaseEntry();
        int nWrites = 0;

        try (Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT)) {

            OperationStatus status = cursor.getSearchKeyRange(key, value, null);
            while (status == OperationStatus.SUCCESS) {
                try {
                    final TableReturn ret = getTableInternal(value);
                    if (ret.serialVersionUsed < TABLE_RECORD_JAVA_V1) {
                        final TableImpl table = ret.table;
                        logger.log(Level.FINE, () -> "Re-writing " + table +
                                                     " using Java" +
                                                     " serialization");
                        serializeTable(table, value);
                        cursor.put(key, value);
                        nWrites++;
                    }
                } catch (IllegalStateException ise) {
                    logSerializationFailure(logger, value, ise);
                }
                if (nWrites >= maxWrites) {
                    break;
                }
                status = cursor.getNext(key, value, null);
            }
        }
        return nWrites;
    }

    /* Dumps the table record keys from the DB. For debug. */
    @SuppressWarnings("unused")
    private static void dump(Database db, Transaction txn, Logger logger) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();

        key.setData(new byte[0]);
        try (Cursor cursor = db.openCursor(txn, CursorConfig.DEFAULT)) {
            OperationStatus status = cursor.getSearchKeyRange(key, value, null);

            while (status == OperationStatus.SUCCESS) {
                logger.warning("Found table key: " + key);
                status = cursor.getNext(key, value, null);
            }
        }
    }

    /**
     * Returns a table instance from the input database entry.
     */
    public static TableImpl getTable(DatabaseEntry entry) {
        return getTableInternal(entry).table;
    }

    private static TableReturn getTableInternal(DatabaseEntry entry) {
        try (final DataInputStream dis = new DataInputStream(
                 new ByteArrayInputStream(entry.getData()))) {
            final short serialVersion = dis.readShort();
            if (serialVersion > SerialVersion.CURRENT) {
                throw new IllegalStateException("Error deserializing table," +
                                                " unknown serial version: " +
                                                serialVersion);
            }
            final TableImpl table =
                (serialVersion >= TABLE_RECORD_JAVA_V1) ?
                       SerializationUtil.readJavaSerial(dis, TableImpl.class) :
                       new TableImpl(dis, serialVersion, null);

            assert TestHookExecute.doIOHookIfSet(PERSISTENCE_TEST_HOOK, table);
            // Convert to runtime exception?
            assert table.isTop();
            return new TableReturn(table, serialVersion);
        } catch (IOException ioe) {
            throw new IllegalStateException ("Exception deserializing table",
                                             ioe);
        }
    }

    /*
     * Container for a table instance and the serial version used to
     * de-serialize it.
     */
    private static class TableReturn {
        final TableImpl table;
        final short serialVersionUsed;

        TableReturn(TableImpl table, short serialVersionUsed) {
            this.table = table;
            this.serialVersionUsed = serialVersionUsed;
        }
    }

    public interface TableKeyGenerator {
        void setStartKey(DatabaseEntry key);
        void setKey(DatabaseEntry key, TableImpl table);
    }
}

