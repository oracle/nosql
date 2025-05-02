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

package oracle.kv.impl.xregion.service;

import static oracle.kv.impl.systables.MRTableInfoDesc.COL_INFO;
import static oracle.kv.impl.systables.MRTableInfoDesc.COL_REGION_NAME;
import static oracle.kv.impl.systables.MRTableInfoDesc.COL_TABLE_NAME;

import java.util.Optional;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Version;
import oracle.kv.impl.systables.MRTableInfoDesc;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.json.JsonUtils;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Object managing and querying the system table that stores the multi-region
 * table information. It encapsulates the details of the table and provides
 * utility APIs to read and write the content of the system table. This
 * object is embedded in {@link ServiceMDMan}. The system table is defined in
 * {@link oracle.kv.impl.systables.MRTableInfoDesc}.
 */
public class XRegionTableInfo {

    /** the name of the system table */
    private static final String SYS_MRT_INFO_NAME = MRTableInfoDesc.TABLE_NAME;

    /** max attempts to access the table */
    private static final long MAX_ATTEMPTS = 3;

    /** read option */
    private static final ReadOptions READ_OPT =
        new ReadOptions(Consistency.ABSOLUTE, 0, null);

    /** write options */
    private static final WriteOptions WRITE_OPT =
        new WriteOptions(Durability.COMMIT_SYNC, 0, null);

    /** parent service metadata manager */
    private final ServiceMDMan parent;

    /** private logger */
    private final Logger logger;

    /**
     * System table containing MR table information. The field may be null
     * during upgrade. If the field is null, all writes will be noop and all
     * reads will return null and caller should ignore the result and move
     * forward.
     */
    private volatile Table sysTable = null;

    public XRegionTableInfo(ServiceMDMan parent, Logger logger) {
        this.parent = parent;
        this.logger = logger;
    }

    /**
     * Initializes the system table
     */
    public void initialize() {
        /* fetch system table from local store */
        sysTable = parent.getLocalTableRetry(SYS_MRT_INFO_NAME);
        if (sysTable == null) {
            logger.warning(lm("System table=" + SYS_MRT_INFO_NAME + " " +
                              "unavailable"));
        }
    }

    /**
     * Inserts a table id for a given MR table into the system table
     * @param tableName name of MR table
     * @param regionName name of region, can be local or remote region
     * @return true if table id inserted, false otherwise
     */
    public boolean insert(String tableName, String regionName, long tableId) {
        if (sysTable == null) {
            return true;
        }
        final XRegionTableInfoJson json = new XRegionTableInfoJson(tableName,
                                                                   regionName,
                                                                   tableId);
        if (!write(json)) {
            logger.warning(lm("Cannot insert table id=" + tableId +
                              " for table=" + tableName +
                              ", region=" + regionName +
                              " in table=" + SYS_MRT_INFO_NAME));
            return false;
        }
        logger.fine(() -> lm("Done insert json=" + json +
                             " in table=" + SYS_MRT_INFO_NAME));
        return true;
    }

    /**
     * Deletes a table id for a given MR table into the system table
     * @param tableName name of MR table
     * @param regionName name of region, can be local or remote region
     * @return true if entry is deleted or does not exist, false otherwise
     */
    public boolean delete(String tableName, String regionName) {
        if (sysTable == null) {
            return true;
        }
        if (getRecordedTableId(tableName, regionName) == 0) {
            /* entry already deleted */
            return true;
        }

        final PrimaryKey key = sysTable.createPrimaryKey();
        key.put(MRTableInfoDesc.COL_TABLE_NAME, tableName);
        key.put(MRTableInfoDesc.COL_REGION_NAME, regionName);
        if (!del(regionName, tableName)) {
            logger.warning(lm("Fail to delete entry for table=" + tableName +
                              ", region=" + regionName));
            return false;
        }
        logger.fine(() -> lm("Done delete entry for table=" + tableName +
                             ", region=" + regionName +
                             " in table=" + SYS_MRT_INFO_NAME));
        return true;
    }

    /**
     * Gets the table id of a given MR table at a give region recorded in
     * system table
     * @param table   name of the table
     * @param region  name of region, can be local or remote region
     * @return table id of the given table if exists, or 0 if the entry
     * does not exist or the system table is not available.
     */
    public long getRecordedTableId(String table, String region) {
        if (sysTable == null) {
            return 0;
        }
        final Optional<XRegionTableInfoJson> json = read(region, table);
        if (!json.isPresent()) {
            logger.fine(() -> lm("No entry exists for table=" + table +
                                 ", region=" + region +
                                 " in table=" + SYS_MRT_INFO_NAME));
            return 0;
        }
        return json.get().getTableId();
    }

    private String lm(String msg) {
        return "[MRInfo-" + parent.getSid() + "] " + msg;
    }

    boolean verifyTableInfo(String tableName, String regionName, long tableId) {
        /* check with stored table id in table info */
        final long recorded = getRecordedTableId(tableName, regionName);
        if (recorded == 0) {
            logger.info(lm("No recorded id for table=" + tableName));
            return true;
        }

        if (recorded == tableId) {
            logger.fine(() -> lm("Matched table=" + tableName +
                                 " at region=" + regionName +
                                 " id=" + tableId));
            return true;
        }

        logger.info(lm("Table=" + tableName + " at region=" + regionName +
                       ", id=" + tableId + " does not match the " +
                       "recorded=" + recorded));
        return false;
    }

    /**
     * Reads an JSON doc of table info
     *
     * @param region       region name
     * @param table        table name
     * @return an optional JSON doc of table info
     */
    private Optional<XRegionTableInfoJson> read(String region, String table) {
        final PrimaryKey pkey = sysTable.createPrimaryKey();
        pkey.put(COL_TABLE_NAME, table);
        pkey.put(COL_REGION_NAME, region);
        final Row row = parent.readRetry(pkey, READ_OPT, MAX_ATTEMPTS);
        if (row == null) {
            return Optional.empty();
        }
        final MapValue map = row.get(MRTableInfoDesc.COL_INFO).asMap();
        try {
            final XRegionTableInfoJson val = XRegionTableInfoJson.readMap(map);
            /* verify version match */
            final int recordVer = val.getVersion();
            final int currVer = XRegionTableInfoJson.CURRENT_VERSION;
            if (recordVer == currVer) {
                return Optional.of(val);
            }
            final String msg = "table info was recorded by a different " +
                               "ver=" + recordVer + ", current ver=" + currVer;
            logger.info(lm(msg));
            return Optional.of(val);
        } catch (RuntimeException re) {
            final String err = "Cannot convert json to table info" +
                               ", current version="+
                               XRegionTableInfoJson.CURRENT_VERSION +
                               ", row=" + row.toJsonString(true) +
                               ", error=" + re;
            logger.warning(lm(err));
            return Optional.empty();
        }
    }

    /**
     * Writes table info to system table
     *
     * @param info     table info object to write
     * @return an optional version
     */
    private boolean write(XRegionTableInfoJson info) {
        final Row row = sysTable.createRow();
        row.put(COL_TABLE_NAME, info.getTable());
        row.put(COL_REGION_NAME, info.getRegion());
        final MapValue jsonMap = row.putMap(COL_INFO);
        info.writeMap(jsonMap);
        final Version ver = parent.putRetry(row, WRITE_OPT, MAX_ATTEMPTS);
        return ver != null;
    }

    /**
     * Deletes table info from system table
     *
     * @param region   region name
     * @param table    table name
     * @return true if deleted successfully false otherwise
     */
    private boolean del(String region, String table) {
        final PrimaryKey pkey = sysTable.createPrimaryKey();
        pkey.put(COL_TABLE_NAME, table);
        pkey.put(COL_REGION_NAME, region);
        return parent.deleteRetry(pkey, WRITE_OPT, MAX_ATTEMPTS);
    }

    /**
     * Object to represent the JSON column that stores the multi-region table
     * info, this class should be only used by the enclosing class. Defined as
     * public only because of unit tests.
     */
    public static class XRegionTableInfoJson {

        /** current version */
        public static final short CURRENT_VERSION = 1;

        /** version */
        private static final String VERSION_KEY = "version";
        private final int version;

        /** table name */
        private static final String TABLE_NAME_KEY = "table";
        private final String table;

        /** region name */
        private static final String REGION_NAME_KEY = "region";
        private final String region;

        /** table id of the table at the region name */
        private static final String TABLE_ID_KEY = "tableId";
        private final long tableId;

        public XRegionTableInfoJson(@NonNull String table,
                                    @NonNull String region,
                                    long tableId) {
            this(table, region, tableId, CURRENT_VERSION);
        }

        private XRegionTableInfoJson(@NonNull String table,
                                     @NonNull String region,
                                     long tableId,
                                     int version) {
            if (table == null || table.isEmpty()) {
                throw new IllegalArgumentException(
                    "Table name cannot be null or empty");
            }
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException(
                    "Region name cannot be null or empty");
            }
            this.table = table;
            this.region= region;
            this.tableId = tableId;
            this.version = version;
        }

        public int getVersion() {
            return version;
        }

        public String getTable() {
            return table;
        }


        public String getRegion() {
            return region;
        }

        public long getTableId() {
            return tableId;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof XRegionTableInfoJson)) {
                return false;
            }
            final XRegionTableInfoJson other = (XRegionTableInfoJson) obj;
            return version == other.version &&
                   table.equals(other.table) &&
                   region.equals(other.region) &&
                   tableId == other.tableId;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(version) +
                   table.hashCode() +
                   region.hashCode() +
                   Long.hashCode(tableId);
        }

        @Override
        public String toString() {
            return JsonUtils.prettyPrint(this);
        }

        /**
         * Populates the map with fields in the object
         * @param jsonMap an empty map
         */
        void writeMap(MapValue jsonMap) {
            jsonMap.put(VERSION_KEY, version);
            jsonMap.put(TABLE_NAME_KEY, table);
            jsonMap.put(REGION_NAME_KEY, region);
            jsonMap.put(TABLE_ID_KEY, tableId);
        }

        /**
         * Returns an object of table info from given json map
         * @param jsonMap a json column from row in the system table
         * {@link MRTableInfoDesc}
         * @return object of XRegionTableInfoJson
         */
        public static XRegionTableInfoJson readMap(MapValue jsonMap) {
            return new XRegionTableInfoJson(
                jsonMap.get(TABLE_NAME_KEY).asString().get(),
                jsonMap.get(REGION_NAME_KEY).asString().get(),
                jsonMap.get(TABLE_ID_KEY).asLong().get(),
                jsonMap.get(VERSION_KEY).asInteger().get());
        }
    }
}
