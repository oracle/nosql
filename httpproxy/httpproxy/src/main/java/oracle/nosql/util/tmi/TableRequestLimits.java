/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * A class to encapsulate table request limits per store. This format of the
 * information saved about store table request limits, as persisted in the
 * metadata catalog.
 * SC will return TableRequestLimits with StoreInfo to Proxy, so we can
 * configure table request limits per store at SC.
 * Note: TableRequestLimits is internal class, will not expose to customer.
 * We put it in cloudutils project so it can be used by Proxy to set store
 * table request limits for test against minicloud.
 */
public class TableRequestLimits {

    /*
     * - version 2, added rowSizeLimit, requestSizeLimit, readWriteKBLimit
     *   and queryStringSizeLimit
     */
    private static final int CURRENT_VERSION = 2;

    private int version;

    /*
     * These limits will be enforced in SC side
     */
    private int tableReadUnits;      /* Maximum read throughput allowed for a
                                      * single table. ReadUnits. */
    private int tableWriteUnits;     /* Maximum write throughput allowed for a
                                      * single table. WriteUnits. */
    private int tableSize;           /* Maximum individual table size
                                      * for a single table. GBs */
    private int indexesPerTable;     /* Maximum number of indexes allowed on
                                      * a single table */
    private int columnsPerTable;     /* Maximum number of columns allowed on
                                      * a single table */
    private int schemaEvolutions;    /* The maximum number of schema
                                      * evolutions that can take place over
                                      * the lifetime of a table */
    private int indexKeySizeLimit;   /* Index key size limit, bytes */

    /*
     * These limits will be enforced in Proxy side
     */
    private int primaryKeySizeLimit; /* Primary key size limit, bytes */
    private int rowSizeLimit;        /* Per single row size limit */
    private int requestSizeLimit;    /* Per request size limit */
    private int requestReadKBLimit;  /* Read data size limit per request */
    private int requestWriteKBLimit; /* Write data size limit per request */
    private int queryStringSizeLimit;/* Query string size limit per request */
    private int batchOpNumberLimit;  /* The number of operations allowed in a
                                      * single batch */
    private int batchRequestSizeLimit;/* The limit on total size of a batch
                                       * operation */

    /*
     * Default values for standard table
     */
    private static final int TABLE_SIZE = 5000; /* size in GB -- 5TB */
    private static final int TABLE_READ_UNITS = 40000;
    private static final int TABLE_WRITE_UNITS = 20000;
    private static final int INDEXES_PER_TABLE = 5;
    private static final int COLUMNS_PER_TABLE = 50;
    private static final int SCHEMA_EVOLUTIONS = 100;
    private static final int PRIMARY_KEY_SIZE_LIMIT = 64; /* 64b */
    private static final int INDEX_KEY_SIZE_LIMIT = 64; /* 64b */
    private static final int ROW_SIZE_LIMIT = 512 * 1024; /* 512kb */
    private static final int REQUEST_SIZE_LIMIT = 2 * 1024 * 1024; /* 2mb */
    private static final int REQUEST_READ_KB_LIMIT = 2 * 1024; /* 2mb */
    private static final int REQUEST_WRITE_KB_LIMIT = 2 * 1024; /* 2mb */
    private static final int QUERY_STRING_SIZE_LIMIT = 10 * 1024; /* 10kb */
    private static final int BATCH_OP_NUMBER_LIMIT = 50;
    private static final int BATCH_REQUEST_SIZE_LIMIT =
        25 * 1024 * 1024; /* 25mb */

    /*
     * Different default values for free table
     */
    private static final int FREE_TABLE_SIZE = 25;  /* size in GB */
    private static final int FREE_TABLE_READ_UNITS = 50;
    private static final int FREE_TABLE_WRITE_UNITS = 50;

    public static TableRequestLimits getStandardTableDefaultInstance() {
        return new TableRequestLimits(TABLE_SIZE,
                                      TABLE_READ_UNITS,
                                      TABLE_WRITE_UNITS,
                                      INDEXES_PER_TABLE,
                                      COLUMNS_PER_TABLE,
                                      SCHEMA_EVOLUTIONS,
                                      PRIMARY_KEY_SIZE_LIMIT,
                                      INDEX_KEY_SIZE_LIMIT,
                                      ROW_SIZE_LIMIT,
                                      REQUEST_SIZE_LIMIT,
                                      REQUEST_READ_KB_LIMIT,
                                      REQUEST_WRITE_KB_LIMIT,
                                      QUERY_STRING_SIZE_LIMIT,
                                      BATCH_OP_NUMBER_LIMIT,
                                      BATCH_REQUEST_SIZE_LIMIT);
    }

    public static TableRequestLimits getFreeTableDefaultInstance() {
        return new TableRequestLimits(FREE_TABLE_SIZE,
                                      FREE_TABLE_READ_UNITS,
                                      FREE_TABLE_WRITE_UNITS,
                                      INDEXES_PER_TABLE,
                                      COLUMNS_PER_TABLE,
                                      SCHEMA_EVOLUTIONS,
                                      PRIMARY_KEY_SIZE_LIMIT,
                                      INDEX_KEY_SIZE_LIMIT,
                                      ROW_SIZE_LIMIT,
                                      REQUEST_SIZE_LIMIT,
                                      REQUEST_READ_KB_LIMIT,
                                      REQUEST_WRITE_KB_LIMIT,
                                      QUERY_STRING_SIZE_LIMIT,
                                      BATCH_OP_NUMBER_LIMIT,
                                      BATCH_REQUEST_SIZE_LIMIT);
    }

    /* Needed for serialization */
    public TableRequestLimits() {
    }

    public TableRequestLimits(int tableSize,
                              int tableReadUnits,
                              int tableWriteUnits,
                              int indexesPerTable,
                              int columnsPerTable,
                              int schemaEvolutions,
                              int primaryKeySizeLimit,
                              int indexKeySizeLimit,
                              int rowSizeLimit,
                              int requestSizeLimit,
                              int requestReadKBLimit,
                              int requestWriteKBLimit,
                              int queryStringSizeLimit,
                              int batchOpNumberLimit,
                              int batchRequestSizeLimit) {
        version = CURRENT_VERSION;
        this.tableReadUnits = tableReadUnits;
        this.tableWriteUnits = tableWriteUnits;
        this.tableSize = tableSize;
        this.indexesPerTable = indexesPerTable;
        this.columnsPerTable = columnsPerTable;
        this.schemaEvolutions = schemaEvolutions;
        this.primaryKeySizeLimit = primaryKeySizeLimit;
        this.indexKeySizeLimit = indexKeySizeLimit;
        this.rowSizeLimit = rowSizeLimit;
        this.requestSizeLimit = requestSizeLimit;
        this.requestReadKBLimit = requestReadKBLimit;
        this.requestWriteKBLimit = requestWriteKBLimit;
        this.queryStringSizeLimit = queryStringSizeLimit;
        this.batchOpNumberLimit = batchOpNumberLimit;
        this.batchRequestSizeLimit = batchRequestSizeLimit;
    }

    public TableRequestLimits(TableRequestLimits other) {
        this(other.tableSize,
             other.tableReadUnits,
             other.tableWriteUnits,
             other.indexesPerTable,
             other.columnsPerTable,
             other.schemaEvolutions,
             other.primaryKeySizeLimit,
             other.indexKeySizeLimit,
             other.rowSizeLimit,
             other.requestSizeLimit,
             other.requestReadKBLimit,
             other.requestWriteKBLimit,
             other.queryStringSizeLimit,
             other.batchOpNumberLimit,
             other.batchRequestSizeLimit);
    }

    public boolean evolveIfOldVersion() {
        if (version < CURRENT_VERSION) {
            version = CURRENT_VERSION;
            rowSizeLimit = ROW_SIZE_LIMIT;
            requestSizeLimit = REQUEST_SIZE_LIMIT;
            requestReadKBLimit = REQUEST_READ_KB_LIMIT;
            requestWriteKBLimit = REQUEST_WRITE_KB_LIMIT;
            queryStringSizeLimit = QUERY_STRING_SIZE_LIMIT;
            batchOpNumberLimit = BATCH_OP_NUMBER_LIMIT;
            batchRequestSizeLimit = BATCH_REQUEST_SIZE_LIMIT;
            return true;
        }
        return false;
    }

    public int getVersion() {
        return version;
    }

    public int getTableReadUnits() {
        return tableReadUnits;
    }

    public int getTableWriteUnits() {
        return tableWriteUnits;
    }

    public int getTableSize() {
        return tableSize;
    }

    public int getIndexesPerTable() {
        return indexesPerTable;
    }

    public int getColumnsPerTable() {
        return columnsPerTable;
    }

    public int getSchemaEvolutions() {
        return schemaEvolutions;
    }

    public int getIndexKeySizeLimit() {
        return indexKeySizeLimit;
    }

    public int getPrimaryKeySizeLimit() {
        return primaryKeySizeLimit;
    }

    public int getRowSizeLimit() {
        return rowSizeLimit;
    }

    public int getRequestSizeLimit() {
        return requestSizeLimit;
    }

    public int getRequestReadKBLimit() {
        return requestReadKBLimit;
    }

    public int getRequestWriteKBLimit() {
        return requestWriteKBLimit;
    }

    public int getQueryStringSizeLimit() {
        return queryStringSizeLimit;
    }

    public int getBatchOpNumberLimit() {
        return batchOpNumberLimit;
    }

    public int getBatchRequestSizeLimit() {
        return batchRequestSizeLimit;
    }

    public TableRequestLimits setTableReadUnits(int tableReadUnits) {
        this.tableReadUnits = tableReadUnits;
        return this;
    }

    public TableRequestLimits setTableWriteUnits(int tableWriteUnits) {
        this.tableWriteUnits = tableWriteUnits;
        return this;
    }

    public TableRequestLimits setTableSize(int tableSize) {
        this.tableSize = tableSize;
        return this;
    }

    public TableRequestLimits setIndexesPerTable(int indexesPerTable) {
        this.indexesPerTable = indexesPerTable;
        return this;
    }

    public TableRequestLimits setColumnsPerTable(int columnsPerTable) {
        this.columnsPerTable = columnsPerTable;
        return this;
    }

    public TableRequestLimits setSchemaEvolutions(int schemaEvolutions) {
        this.schemaEvolutions = schemaEvolutions;
        return this;
    }

    public TableRequestLimits setIndexKeySizeLimit(int indexKeySizeLimit) {
        this.indexKeySizeLimit = indexKeySizeLimit;
        return this;
    }

    public TableRequestLimits setPrimaryKeySizeLimit(int primaryKeySizeLimit) {
        this.primaryKeySizeLimit = primaryKeySizeLimit;
        return this;
    }

    public TableRequestLimits setRowSizeLimit(int rowSizeLimit) {
        this.rowSizeLimit = rowSizeLimit;
        return this;
    }

    public TableRequestLimits setRequestSizeLimit(int requestSizeLimit) {
        this.requestSizeLimit = requestSizeLimit;
        return this;
    }

    public TableRequestLimits setRequestReadKBLimit(int requestReadKBLimit) {
        this.requestReadKBLimit = requestReadKBLimit;
        return this;
    }

    public TableRequestLimits setRequestWriteKBLimit(int requestWriteKBLimit) {
        this.requestWriteKBLimit = requestWriteKBLimit;
        return this;
    }

    public TableRequestLimits
        setQueryStringSizeLimit(int queryStringSizeLimit) {
        this.queryStringSizeLimit = queryStringSizeLimit;
        return this;
    }

    public void setBatchOpNumberLimit(int batchOpNumberLimit) {
        this.batchOpNumberLimit = batchOpNumberLimit;
    }

    public void setBatchRequestSizeLimit(int batchRequestSizeLimit) {
        this.batchRequestSizeLimit = batchRequestSizeLimit;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + batchOpNumberLimit;
        result = prime * result + batchRequestSizeLimit;
        result = prime * result + columnsPerTable;
        result = prime * result + indexKeySizeLimit;
        result = prime * result + indexesPerTable;
        result = prime * result + primaryKeySizeLimit;
        result = prime * result + queryStringSizeLimit;
        result = prime * result + requestReadKBLimit;
        result = prime * result + requestSizeLimit;
        result = prime * result + requestWriteKBLimit;
        result = prime * result + rowSizeLimit;
        result = prime * result + schemaEvolutions;
        result = prime * result + tableReadUnits;
        result = prime * result + tableSize;
        result = prime * result + tableWriteUnits;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TableRequestLimits other = (TableRequestLimits) obj;
        if (batchOpNumberLimit != other.batchOpNumberLimit) {
            return false;
        }
        if (batchRequestSizeLimit != other.batchRequestSizeLimit) {
            return false;
        }
        if (columnsPerTable != other.columnsPerTable) {
            return false;
        }
        if (indexKeySizeLimit != other.indexKeySizeLimit) {
            return false;
        }
        if (indexesPerTable != other.indexesPerTable) {
            return false;
        }
        if (primaryKeySizeLimit != other.primaryKeySizeLimit) {
            return false;
        }
        if (queryStringSizeLimit != other.queryStringSizeLimit) {
            return false;
        }
        if (requestReadKBLimit != other.requestReadKBLimit) {
            return false;
        }
        if (requestSizeLimit != other.requestSizeLimit) {
            return false;
        }
        if (requestWriteKBLimit != other.requestWriteKBLimit) {
            return false;
        }
        if (rowSizeLimit != other.rowSizeLimit) {
            return false;
        }
        if (schemaEvolutions != other.schemaEvolutions) {
            return false;
        }
        if (tableReadUnits != other.tableReadUnits) {
            return false;
        }
        if (tableSize != other.tableSize) {
            return false;
        }
        if (tableWriteUnits != other.tableWriteUnits) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TableRequestLimits [version=" + version +
            ", tableReadUnits=" + tableReadUnits +
            ", tableWriteUnits=" + tableWriteUnits +
            ", tableSize=" + tableSize +
            ", indexesPerTable=" + indexesPerTable +
            ", columnsPerTable=" + columnsPerTable +
            ", schemaEvolutions=" + schemaEvolutions +
            ", indexKeySizeLimit=" + indexKeySizeLimit +
            ", primaryKeySizeLimit=" + primaryKeySizeLimit +
            ", rowSizeLimit=" + rowSizeLimit +
            ", requestSizeLimit=" + requestSizeLimit +
            ", requestReadKBLimit=" + requestReadKBLimit +
            ", requestWriteKBLimit=" + requestWriteKBLimit +
            ", queryStringSizeLimit=" + queryStringSizeLimit +
            ", batchOpNumberLimit=" + batchOpNumberLimit +
            ", batchRequestSizeLimit=" + batchRequestSizeLimit + "]";
    }
}
