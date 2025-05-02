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

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableMetadata.filterTable;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_CONSTANT;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_DATA;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_DELETED;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_DESCRIPTION;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_KEY;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_SEQ_NUM;
import static oracle.kv.impl.systables.TableMetadataDesc.COL_NAME_TYPE;
import static oracle.kv.impl.systables.TableMetadataDesc.GC_SEQ_NUM;
import static oracle.kv.impl.systables.TableMetadataDesc.METADATA_TABLE_ID;
import static oracle.kv.impl.systables.TableMetadataDesc.NAMESPACE_TYPE;
import static oracle.kv.impl.systables.TableMetadataDesc.OTHER_TYPE;
import static oracle.kv.impl.systables.TableMetadataDesc.REGION_TYPE;
import static oracle.kv.impl.systables.TableMetadataDesc.ROW_CONSTANT;
import static oracle.kv.impl.systables.TableMetadataDesc.SEQ_INDEX_NAME;
import static oracle.kv.impl.systables.TableMetadataDesc.TABLE_NAME;
import static oracle.kv.impl.systables.TableMetadataDesc.TABLE_TYPE;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.Version;
import oracle.kv.impl.api.table.TableMetadata.NamespaceImpl;
import oracle.kv.impl.systables.TableMetadataDesc;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.FieldValue;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.WriteOptions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Utility methods for accessing the table metadata system table.
 *
 * Callers are responsible for all exceptions.
 */
public class TableSysTableUtil {

    /**
     * Overrides batch result size for testing.
     */
    public static volatile Optional<Integer> batchResultSizeOverride =
        Optional.empty();

    /* Iterator options */
    private static final long TIMEOUT = 10000L;
    private static final int MAX_CONCURRENT_REQUESTS = 2;
    private static final int DEFAULT_BATCH_RESULT_SIZE = 10;

    private static final Function<Direction, TableIteratorOptions>
       GET_ITERATOR_OPTIONS =
        (d) -> new TableIteratorOptions(d, Consistency.ABSOLUTE, TIMEOUT,
            TimeUnit.MILLISECONDS, MAX_CONCURRENT_REQUESTS,
            batchResultSizeOverride.orElse(DEFAULT_BATCH_RESULT_SIZE));

    /* Read options. */

    private static final ReadOptions NO_CONSISTENCY_READ =
            new ReadOptions(Consistency.NONE_REQUIRED, 0, null);

    private static final ReadOptions ABSOLUTE_READ =
            new ReadOptions(Consistency.ABSOLUTE, 0, null);

    /* Write options. */
    private static final WriteOptions WRITE_OPTIONS =
            new WriteOptions(Durability.COMMIT_SYNC, 0, null);


    /* Number of retries when accessing the system table */
    private static final int MAX_RETRIES = 10;

    /* Delay between retries */
    private static final int RETRY_SLEEP_MS = 1000;

    /**
     * The magic bootstrap table key. This uses the system table
     * descriptor for the metadata table to build the instance to
     * generate the key. We set the ID to the fixed ID of the table.
     */
    private static final PrimaryKey bootstrapMDKey;
    static {
        final TableImpl t = new TableMetadataDesc().buildTable();
        t.setId(METADATA_TABLE_ID);
        bootstrapMDKey = t.createPrimaryKey();
        setTableKey(bootstrapMDKey, t);
    }

    /* Allow subclassing */
    protected TableSysTableUtil() {}

    /* -- Table metadata system table -- */

    /**
     * Gets the table metadata table from the metadata table. This uses a
     * bootstrap primary key to read the table row. Null is returned if
     * the table does not yet exist or has not been initialized.
     *
     * @return the table metadata system table or null
     *
     * @throws FaultException if the underlying read throws a FaultException
     * and the number of retries is exhausted.
     *
     * @see <a href="../KVStore.html#readExceptions">Read exceptions</a>
     *
     * Public for unit tests
     */
    public static Table getMDTable(TableAPI tableAPI) {
        try {
            /*
             * Once the table metadata system table is IN the system table it
             * will not change, so we should be able to get it from anywhere.
             */
            final Table sysTable = retry(() ->
                   getTable(tableAPI.get(bootstrapMDKey, NO_CONSISTENCY_READ)));
            /*
             * Return the table only if it has been initialized (GCSN > 0).
             * Since we are only checking for existence of GCSN an absolute
             * read is not necessary.
             */
            return (sysTable != null) &&
                    (getGCSeqNum(sysTable, tableAPI, false) > 0) ?
                                                            sysTable : null;
        } catch (MetadataNotFoundException mnfe) {
            return null;
        }
    }

    protected static Table getSysTable(TableAPI tableAPI) {
        return tableAPI.getTable(TABLE_NAME);
    }

    /* -- Row/key helpers -- */

    /**
     * Sets the type and key on the specified row. The key is the
     * string returned by key.toLowerCase().
     */
    private static void setKey(Row row, String type, String key) {
        setType(row, type);
        row.put(COL_NAME_KEY, key.toLowerCase());
    }

    private static String getKey(Row row) {
        return row.get(COL_NAME_KEY).asString().get();
    }

    private static void setType(Row row, String type) {
        row.put(COL_NAME_CONSTANT, ROW_CONSTANT);
        row.put(COL_NAME_TYPE, type);
    }

    private static String getType(Row row) {
        return row.get(COL_NAME_TYPE).asString().get();
    }

    private static void setSequenceNumber(Row row, int seqNum) {
        row.put(COL_NAME_SEQ_NUM, seqNum);
    }

    private static int getSequenceNumber(Row row) {
        return (row == null) ? 0 : row.get(COL_NAME_SEQ_NUM).asInteger().get();
    }

    /**
     * Sets the description field. If the input JSON is not valid the
     * and replacement is non-null the description is set to the value
     * provided by the replacement otherwise the field is set to null.
     */
    private static void setDescription(Row row,
                                       String json,
                                       Supplier<String> replacement,
                                       Logger logger) {
        try {
            row.putJson(COL_NAME_DESCRIPTION, json);
            return;
        } catch (IllegalArgumentException iae) {
            /* Log the failure, hopefully it will be noticed and fixed */
            logger.warning("Exception setting JSON description " +
                           iae.getMessage());
            logger.warning("JSON: " + json);
        }
        /* Try the replacement if provided */
        if (replacement != null) {
            /* Note the recursion. Setting replacement to null will exit */
            setDescription(row, replacement.get(), null, logger);
            return;
        }
        /* No more options, set to null */
        row.putJsonNull(COL_NAME_DESCRIPTION);
    }

    /**
     * Gets the Json description. If the row is a deleted marker null
     * is returned.
     */
    @SuppressWarnings("unused")
    private static String getDescription(Row row) {
        final FieldValue value = row.get(COL_NAME_DESCRIPTION);
        return value.isJsonNull() ? null : value.toJsonString(false);
    }

    private static void setDeleted(Row row) {
        row.put(COL_NAME_DELETED, true);
        row.putJsonNull(COL_NAME_DESCRIPTION);
    }

    protected static boolean isDeleted(Row row) {
        return row.get(COL_NAME_DELETED).asBoolean().get();
    }

    private static void setData(Row row, byte[] bytes) {
        assert bytes != null;
        row.put(COL_NAME_DATA, bytes);
    }

    private static byte[] getData(Row row) {
        return row.get(COL_NAME_DATA).asBinary().get();
    }

    /* -- Tables -- */

    /**
     * Gets a table instance from a row. Null is returned if the row represents
     * a deleted table.
     */
    protected static TableImpl getTable(Row row) {
        if (row == null) {
            return null;
        }
        return isDeleted(row) ?
                null :
                SerializationUtil.getObject(getData(row), TableImpl.class);
    }

    /**
     * Updates the system table with the specified table. If there is an
     * existing row for this table, it will be updated if the specified
     * table's sequence number is greater than the existing row.
     */
    protected static void updateTable(TableImpl table,
                                      TableMetadata md,
                                      Table sysTable,
                                      TableAPI tableAPI,
                                      Logger logger) {
        logger.fine(() -> "Updating " + table.getFullNamespaceName() +
                    " seqNum=" + table.getSequenceNumber());

        /* Only table hierarchies are stored */
        final TableImpl top = table.getTopLevelTable();
        final Row row = sysTable.createRow();
        setTableKey(row, top);
        setSequenceNumber(row, top.getSequenceNumber());
        setDescription(row,
                       top.toJsonString(false,
                               true,
                                        md.getRegionMapper()),
                       () -> makeSimpleJson(top),
                       logger);
        setData(row, SerializationUtil.getBytes(top));
        write(row, true, tableAPI, sysTable, logger);
    }

    private static String makeSimpleJson(TableImpl table) {
        return "{\"type\":\"table\",\"namespace\":\"" + table.getNamespace() +
                "\",\"name\":\"" + table.getName() + "\"}";
    }

    /**
     * Updates or removes the specified table. If a child table or the table
     * is marked for delete, the table hierarchy record is updated. If the
     * table being removed is a top level table and markForDelete is false
     * the row is replaced with a deleted marker.
     */
    protected static void removeTable(TableImpl table,
                                      TableMetadata md,
                                      boolean markForDelete,
                                      Table sysTable,
                                      TableAPI tableAPI,
                                      Logger logger) {
        /*
         * If the table is a child table or it is mark-for-delete, just
         * update the table hierarchy.
         */
        if (!table.isTop() || markForDelete) {
            updateTable(table, md, sysTable, tableAPI, logger);
            return;
        }

        /*
         * Top level table is being removed.
         * Overwrite the row with a deleted maker. The data field
         * will be the table ID.
         * Note that if a new table with the same name exist, its
         * seq number will be higher than the deleted table. In
         * that case the write will fail.
         */
        // TODO - what if the table's row is not there?
        final Row row = sysTable.createRow();
        setTableKey(row, table);
        setSequenceNumber(row, table.getSequenceNumber());
        setDeleted(row);
        setData(row, SerializationUtil.getBytes(table.getId()));
        write(row, true, tableAPI, sysTable, logger);
    }

    private static void setTableKey(Row row, TableImpl table) {
        setTableKey(row, getNameString(table));
    }

    private static void setTableKey(Row row,
                                    String namespace,
                                    String tableName) {
        /* The key is the namespace + the top level table name: path[0] */
        final String[] path = TableImpl.parseFullName(tableName);
        setTableKey(row, NameUtils.makeQualifiedName(namespace, path[0]));
    }

    private static void setTableKey(Row row, String name) {
        setKey(row, TABLE_TYPE, name);
    }

    protected static String getNameString(TableImpl table) {
        assert table.isTop();
        return table.getFullNamespaceName();
    }

    /* -- Namespaces -- */

    /**
     * Adds the specified namespace to the table.
     */
    protected static void addNamespace(NamespaceImpl ns,
                                       int seqNum,
                                       Table sysTable,
                                       TableAPI tableAPI,
                                       Logger logger) {
        final Row row = sysTable.createRow();
        setNamespaceKey(row, ns.getNamespace());
        setSequenceNumber(row, seqNum);
        setDescription(row, ns.toJsonString(), null, logger);
        setData(row, SerializationUtil.getBytes(ns));
        /*
         * Namespaces do not change once created (only removed). So rows don't
         * need to be overwritten
         */
        write(row, false, tableAPI, sysTable, logger);
    }

    /**
     * Removes the specified namespace from the table.
     */
    protected static void removeNamespace(String namespace,
                                          int seqNum,
                                          Table sysTable,
                                          TableAPI tableAPI,
                                          Logger logger) {
        /*
         * Overwrite the row with a deleted maker. The data field
         * will be null.
         */
        final Row row = sysTable.createRow();
        setNamespaceKey(row, namespace);
        setSequenceNumber(row, seqNum);
        setDeleted(row);
        write(row, true, tableAPI, sysTable, logger);
    }

    private static void setNamespaceKey(Row row, String namespace) {
        setKey(row, NAMESPACE_TYPE, namespace);
    }

    /**
     * Gets a namespace instance from a row. Returns null if the row is a
     * deleted marker.
     */
    protected static NamespaceImpl getNamespace(Row row) {
        return isDeleted(row) ?
                null :
                SerializationUtil.getObject(getData(row), NamespaceImpl.class);
    }

    /* -- Regions -- */

    /**
     * Updates the specified region in the system table.
     */
    protected static void updateRegion(Region region,
                                       int seqNum,
                                       Table sysTable,
                                       TableAPI tableAPI,
                                       Logger logger) {
        final Row row = sysTable.createRow();
        setRegionKey(row, region.getId());
        setSequenceNumber(row, seqNum);
        setDescription(row, region.toJsonString(), null, logger);
        setData(row, SerializationUtil.getBytes(region));
        write(row, true, tableAPI, sysTable, logger);
    }

    private static void setRegionKey(Row row, int regionId) {
        setKey(row, REGION_TYPE, Integer.toString(regionId));
    }

    protected static Region getRegion(Row row) {
        /* Regions are not deleted */
        return SerializationUtil.getObject(getData(row), Region.class);
    }

    /* -- GC sequence number -- */

    /**
     * Sets the GC sequence number (GCSN). Returns the seq number
     * recorded in the table. The GCSN should be written before any delete
     * records are pruned. Any delete records with sequence numbers less
     * than the GCSN can be pruned.
     *
     * Note that the sequence number of this row may match an existing
     * metadata row. This should only be noticeable during an index scan
     * when two rows with the same sequence number are read.
     *
     * @return the GC sequence number in the table or 0 if the table metadata
     * system table does not exist.
     */
    protected static int setGCSeqNum(int seqNum,
                                     Table sysTable,
                                     TableAPI tableAPI,
                                     Logger logger) {
        assert seqNum > 0;

        /* The data field is empty */
        final Row row = sysTable.createRow();
        setKey(row, OTHER_TYPE, GC_SEQ_NUM);
        setSequenceNumber(row, seqNum);
        setDescription(row,
                "{\"key\" : \"GC sequence number\", \"seqNum\" : " +
                        seqNum + "}",
                null, logger);
        int ret = write(row, true, tableAPI, sysTable, logger);
        logger.fine(() -> "Set GCSN to=" + seqNum + " returned=" + ret);
        return ret;
    }

    /**
     * Gets the GC sequence number.
     *
     * Public for unit test.
     */
    public static int getGCSeqNum(Table sysTable, TableAPI tableAPI,
                                  boolean absolute) {
        final PrimaryKey pk = sysTable.createPrimaryKey();
        setKey(pk, OTHER_TYPE, GC_SEQ_NUM);
        return getSequenceNumber(tableAPI.get(pk,
                                              absolute ? ABSOLUTE_READ :
                                                         NO_CONSISTENCY_READ));
    }

    /* -- Rows -- */

    /**
     * Conditionally writes the specified row in the system table.
     * If the row does not already exist in the table the row is written.
     * If overwrite is true and a row does exist or the existing row is a
     * deleted marker, the sequence number of the existing row is compared
     * to the new row. If the new row is newer (higher sequence number) the
     * table is updated with the new row.
     * Returns the sequence number of the resulting row in the table or
     * 0 if there was a metadata not found error.
     *
     * @throws FaultException if the underlying write throws a FaultException
     * and the number of retries is exhausted.
     *
     * @see <a href="../KVStore.html#writeExceptions">Write exceptions</a>
     */
    @SuppressWarnings("unused")
    private static int write(Row row, boolean overwrite,
                             TableAPI tableAPI,
                             Table sysTable,
                             Logger logger) {
        final int seqNum = getSequenceNumber(row);
        assert seqNum > 0;
        final ReturnRow rr = sysTable.createReturnRow(ReturnRow.Choice.ALL);

        /* Attempt to write the row */
        Version v = retry(() -> tableAPI.putIfAbsent(row, rr, WRITE_OPTIONS));
        if (v != null) {
            return seqNum;
        }
        /* There is an existing row */
        int existingSeqNum = getSequenceNumber(rr);

        /*
         *  Continue if overwrite is set or the existing row is a
         *  deleted marker
         */
        if (!(overwrite || isDeleted(rr))) {
            return existingSeqNum;
        }

        /* Update if the row is newer than existing row */
        while (seqNum > existingSeqNum) {
            v = retry(() -> tableAPI.putIfVersion(row,
                                                  rr.getVersion(),
                                                  rr,
                                                  WRITE_OPTIONS));
            if (v != null) {
                return seqNum;
            }
            existingSeqNum = getSequenceNumber(rr);
        }
        return existingSeqNum;
    }

    protected static void delete(Row row, Table sysTable, TableAPI tableAPI) {
        final PrimaryKey pk = sysTable.createPrimaryKey();
        setKey(pk, getType(row), getKey(row));
        retry(() -> tableAPI.delete(pk, null, WRITE_OPTIONS));
    }

    /*  -- Client get metadata/table support -- */

    /**
     * Tables returned by these methods have been filtered to remove
     * indexes that have not finished populating.
     *
     * The get metadata and wildcard get table calls are expensive
     * as they need to read the entire system table. This is mentioned
     * in the TableAPI methods so their use should only be as necessary.
     */

    /**
     * Gets the specified table. Returns null if the table does not exist.
     *
     * @return a table instance or null
     *
     * @throws FaultException if the underlying read throws a FaultException
     * and the number of retries is exhausted.
     *
     * @see <a href="../KVStore.html#readExceptions">Read exceptions</a>
     *
     * @see TableAPIImpl#getTable(String)
     */
    public static TableImpl getTable(String namespace,
                                     String tableName,
                                     Table sysTable,
                                     TableAPI tableAPI) {
        final PrimaryKey pk = sysTable.createPrimaryKey();
        setTableKey(pk, namespace, tableName);
        try {
            return filterTable(retry(() ->
                    getTable(tableAPI.get(pk, ABSOLUTE_READ))));
        } catch (FaultException fe) {
            /*
             * An absolute read can throw a fault exception if the shard
             * has lost quorum. Make a last ditch effort with no consistency.
             */
            return filterTable(getTable(tableAPI.get(pk, NO_CONSISTENCY_READ)));
        }
    }

    /**
     * Gets a table by table ID. This method scans the system table
     * until it finds a table with the matching ID. Therefore, this
     * method may be expensive. The returned table should be cached
     * to avoid this operation.
     *
     * @return a table instance or null
     *
     * @see TableAPIImpl#getTableById(long)
     */
    public static TableImpl getTable(long tableId,
                                     Table sysTable,
                                     TableAPI tableAPI) {
        final TableImpl[] table = {null};
        getAllTables(sysTable, tableAPI, new RowCallback() {
            @Override
            public boolean tableRow(String key, int seqNum, Row row) {
                final TableImpl t = getTable(row);
                if ((t != null) && (t.getId() == tableId)) {
                    table[0] = t;
                    return false;
                }
                return true;
            }
        });
        return filterTable(table[0]);
    }

    /**
     * Gets a metadata instance. The instance is populated from the system
     * table.
     *
     * @see TableAPIImpl#getTableMetadata()
     */
    public static TableMetadata getTableMetadata(Table sysTable,
                                                 TableAPIImpl tableAPI) {
        final TableMetadata md = new TableMetadata(false);
        getAllRows(sysTable, tableAPI, new RowCallback() {
            @Override
            public boolean namespaceRow(String key, int seqNum, Row row) {
                final NamespaceImpl ns = getNamespace(row);
                if (ns != null) {
                    md.addNamespace(ns);
                }
                md.setSeqNum(seqNum);
                return true;
            }

            @Override
            public boolean regionRow(String key, int seqNum, Row row) {
                /* Regions are never deleted */
                md.addRegion(getRegion(row));
                md.setSeqNum(seqNum);
                return true;
            }

            @Override
            public boolean tableRow(String key, int seqNum, Row row) {
                final TableImpl table = getTable(row);
                if (table != null) {
                    md.addTableHierarchy(filterTable(table));
                }
                md.setSeqNum(seqNum);
                return true;
            }
        });
        return md;
    }

    /**
     * Gets a region mapper.
     *
     * @see TableAPIImpl#getRegionMapper()
     */
    public static RegionMapper getRegionMapper(Table sysTable,
                                               TableAPIImpl tableAPI) {
        /*
         * Populate an empty metadata instance from region data to
         * create a mapper
         */
        final TableMetadata md = new TableMetadata(false);
        final PrimaryKey pk = sysTable.createPrimaryKey();
        setType(pk, REGION_TYPE);
        getAllRows(pk, tableAPI,
                new RowCallback() {
                    @Override
                    public boolean regionRow(String key, int seqNum, Row row) {
                        /* Regions are never deleted */
                        md.addRegion(getRegion(row));
                        md.setSeqNum(seqNum);
                        return true;
                    }
                });
        return md.getRegionMapper();
    }

    /**
     * Gets a map of tables with the specified namespace or all
     * tables if namespace is null.
     *
     * @see TableAPIImpl#getTables(String)
     */
    public static Map<String, Table> getTables(String namespace,
                                               Table sysTable,
                                               TableAPIImpl tableAPI) {
        /*
         * Populate an empty metadata instance with the target tables
         * and then get the map from that instance. This ensures
         * that the returned map is the same as earlier versions.
         */
        final TableMetadata md = new TableMetadata(false);
        getAllTables(sysTable, tableAPI,
                new RowCallback() {
                    @Override
                    public boolean tableRow(String key, int seqNum, Row row) {
                        final TableImpl table = getTable(row);
                        if (table != null) {
                            md.addTableHierarchy(filterTable(table));
                        }
                        return true;
                    }
                });
        return md.getTables(namespace);
    }

    /**
     * Gets a list of multi region tables.
     *
     * @see TableAPIImpl#getMultiRegionTables(boolean)
     */
    static List<Table> getMultiRegionTables(boolean includeLocalOnly,
                                            Table sysTable,
                                            TableAPIImpl tableAPI) {
        /*
         * Populate an empty metadata instance with the target tables
         * and then get the list from that instance. This ensures
         * that the returned map is the same as earlier versions.
         */
        final TableMetadata md = new TableMetadata(false);
        getAllTables(sysTable, tableAPI,
                new RowCallback() {
                    @Override
                    public boolean tableRow(String key, int seqNum, Row row) {
                        final TableImpl table = getTable(row);
                        if ((table != null) && table.isMultiRegion()) {
                            md.addTableHierarchy(filterTable(table));
                        }
                        return true;
                    }
                });
        return md.getMRTables(includeLocalOnly).get();
    }

    /**
     * Gets a list of system tables.
     *
     * @see TableAPIImpl#getSystemTables()
     */
    static List<Table> getSystemTables(Table sysTable,
                                       TableAPIImpl tableAPI) {
        /*
         * Populate an empty metadata instance with the target tables
         * and then get the list from that instance. This ensures
         * that the returned list is the same as earlier versions.
         */
        final TableMetadata md = new TableMetadata(false);
        getAllTables(sysTable, tableAPI,
                new RowCallback() {
                    @Override
                    public boolean tableRow(String key, int seqNum, Row row) {
                        final TableImpl table = getTable(row);
                        if ((table != null) && table.isSystemTable()) {
                            md.addTableHierarchy(filterTable(table));
                        }
                        return true;
                    }
                });
        return md.getSystemTables().get();
    }

    public static Set<String> listNamespaces(Table sysTable,
                                             TableAPIImpl tableAPI) {
        /*
         * Populate an empty metadata instance with namespaces
         * and then get the set from that instance. This ensures
         * that the returned set is the same as earlier versions.
         */
        final TableMetadata md = new TableMetadata(false);
        final PrimaryKey pk = sysTable.createPrimaryKey();
        setType(pk, NAMESPACE_TYPE);
        getAllRows(pk, tableAPI,
                new RowCallback() {
                    @Override
                    public boolean namespaceRow(String key,
                                                int seqNum,
                                                Row row) {
                        final NamespaceImpl ns = getNamespace(row);
                        if (ns != null) {
                            md.addNamespace(ns);
                        }
                        return true;
                    }
                });
        return md.listNamespaces();
    }

    /* -- System table iteration -- */

    /**
     * Iterator callback. One of the methods is called for every row in the
     * iteration based on row type. Implementers can override the methods they
     * are interested in. Methods return true if the iteration should continue.
     */
    @SuppressWarnings("unused")
    protected interface RowCallback {
        default boolean namespaceRow(String key, int seqNum, Row row) {
            return true;
        }

        default boolean regionRow(String key, int seqNum, Row row) {
            return true;
        }

        default boolean tableRow(String key, int seqNum, Row row) {
            return true;
        }

        default boolean gcSeqNum(int seqNum) {
            return true;
        }
    }

    /**
     * Iterates through metadata rows in sequence number in increasing order
     * calling a callback method for each row. Note that duplicate sequence
     * numbers are allowed and the order of iteration of rows with
     * duplicate sequence numbers is not specified.
     */
    protected static void getSequentialRows(Table sysTable,
                                            TableAPI tableAPI,
                                            RowCallback callback) {
        final IndexKey indexKey =
                sysTable.getIndex(SEQ_INDEX_NAME).createIndexKey();
        iterate(tableAPI.tableIterator(indexKey, null,
            GET_ITERATOR_OPTIONS.apply(Direction.FORWARD)), callback);
    }

    private static void getAllTables(Table sysTable,
                                     TableAPI tableAPI,
                                     RowCallback callback) {
        final PrimaryKey key = sysTable.createPrimaryKey();
        setType(key, TABLE_TYPE);
        getAllRows(key, tableAPI, callback);
    }

    /**
     * Iterates through all table metadata rows calling a callback method
     * for each row.
     */
    protected static void getAllRows(Table sysTable,
                                     TableAPI tableAPI,
                                     RowCallback callback) {
        final PrimaryKey key = sysTable.createPrimaryKey();
        getAllRows(key, tableAPI, callback);
    }

    /**
     * Iterates through all table metadata rows using the specified key.
     */
    private static void getAllRows(PrimaryKey key,
                                   TableAPI tableAPI,
                                   RowCallback callback) {
        iterate(tableAPI.tableIterator(key, null,
            GET_ITERATOR_OPTIONS.apply(Direction.UNORDERED)), callback);
    }

    private static void iterate(TableIterator<Row> itr,
                                RowCallback callback) {
        try {
            boolean cont = true;
            while (cont && itr.hasNext()) {
                final Row row = itr.next();
                final String key = getKey(row);
                final int seqNum = getSequenceNumber(row);
                switch (getType(row)) {
                case NAMESPACE_TYPE:
                    cont = callback.namespaceRow(key, seqNum, row);
                    break;
                case REGION_TYPE:
                    cont = callback.regionRow(key, seqNum, row);
                    break;
                case TABLE_TYPE:
                    cont = callback.tableRow(key, seqNum, row);
                    break;
                case OTHER_TYPE:
                    /* Currently only one OTHER_TYPE */
                    if (key.equalsIgnoreCase(GC_SEQ_NUM)) {
                        cont = callback.gcSeqNum(seqNum);
                    }
                    break;
                }
            }
        } finally {
            itr.close();
        }
    }

    /**
     * Execute the specified operation. If it throws a FaultException
     * the operation is retried.
     *
     * @throws FaultException if the underlying operation throws a
     * FaultException and the number of retries is exhausted.
     */
    private static <T> T retry(Supplier<T> op) {
        FaultException lastFE = null;
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
                return op.get();
            } catch (FaultException fe) {
                lastFE = fe;
                try {
                    Thread.sleep(RETRY_SLEEP_MS);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }
        if (lastFE == null) {
            throw new IllegalStateException("Unexpected null exception");
        }
        throw lastFE;
    }

    /* -- Dump table utilities for testing-- */

    public static void dumpTable(TableAPI tableAPI, Logger logger) {
        logger.info("DUMP TABLE");
        final Table sysTable = getSysTable(tableAPI);
        if (sysTable == null) {
            logger.warning("System table does not yet exist");
            return;
        }

        getAllRows(sysTable, tableAPI, new RowCallback() {
            @Override
            public boolean namespaceRow(String key, int seqNum, Row row) {
                logger.info("Namespace: " + key +
                        " " + seqNum + (isDeleted(row) ? " DELETED" : ""));
                return true;
            }

            @Override
            public boolean regionRow(String key, int seqNum, Row row) {
                logger.info("Region: " + key + " " + seqNum +
                            " " + getRegion(row));
                return true;
            }

            @Override
            public boolean tableRow(String key, int seqNum, Row row) {
                logger.info("Table: " + key +
                        " " + seqNum + (isDeleted(row) ? " DELETED" : ""));
                return true;
            }

            @Override
            public boolean gcSeqNum(int seqNum){
                logger.info("GC seq num: " + seqNum);
                return true;
            }
        });
    }

    public static void dumpTable(TableAPI tableAPI) {
        final Table sysTable = getSysTable(tableAPI);
        if (sysTable == null) {
            System.out.println("DUMP TABLES - System table unavailable");
            return;
        }
        System.out.println("DUMP TABLES");
        final AtomicInteger nRows = new AtomicInteger(0);
        getAllRows(sysTable, tableAPI, new RowCallback() {
            @Override
            public boolean namespaceRow(String key, int seqNum, Row row) {
                System.out.println("Namespace: " + key + " " + seqNum +
                                   (isDeleted(row) ? " DELETED" : ""));
                nRows.incrementAndGet();
                return true;
            }

            @Override
            public boolean regionRow(String key, int seqNum, Row row) {
                System.out.println("Region: " + key + " " + seqNum +
                                   " " + getRegion(row));
                nRows.incrementAndGet();
                return true;
            }

            @Override
            public boolean tableRow(String key, int seqNum, Row row) {
                System.out.println("Table: " + key + " " + seqNum +
                                   (isDeleted(row) ? " DELETED" :
                                                     (" " + getTable(row))));
                nRows.incrementAndGet();
                return true;
            }

            @Override
            public boolean gcSeqNum(int seqNum){
                System.out.println("GC seq num: " + seqNum);
                nRows.incrementAndGet();
                return true;
            }
        });
        System.out.println("Number of Rows " + nRows.get());
    }
}
