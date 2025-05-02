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

package oracle.kv.impl.measurement;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Description of statistics recorded at the RepNode and displayed via the
 * monitoring system. To keep statistics compact, and to guard against enum
 * evolution, this class itself is not transmitted across the wire. Instead,
 * the id value alone is used, and any change or addition to the ids is a
 * protocol change.
 */
public enum PerfStatType implements StatType {

    /*
     * Status metrics for each type of operation.
     * When adding statistics, consider whether they should be added to
     * getDetailedStats, getSummaryStats, in order to be dumped to the
     * stats .csv files.
     */

        USER_SINGLE_OP_INT       (100, true,  "AllSingleKeyOperations"),
        USER_SINGLE_OP_CUM       (101, false, "AllSingleKeyOperations"),

        USER_MULTI_OP_INT        (120, true,  "AllMultiKeyOperations"),
        USER_MULTI_OP_CUM        (121, false, "AllMultiKeyOperations"),

        USER_SINGLE_READ_INT     (130, true,  "SingleKeyReadOperations"),
        USER_SINGLE_WRITE_INT    (140, true,  "SingleKeyWriteOperations"),
        USER_MULTI_READ_INT      (150, true,  "MultiKeyReadOperations"),
        USER_MULTI_WRITE_INT     (160, true,  "MultiKeyWriteOperations"),

        USER_SINGLE_READ_CUM     (131, false, "SingleKeyReadOperations"),
        USER_SINGLE_WRITE_CUM    (141, false, "SingleKeyWriteOperations"),
        USER_MULTI_READ_CUM      (151, false, "MultiKeyReadOperations"),
        USER_MULTI_WRITE_CUM     (161, false, "MultiKeyWriteOperations"),

        /* gets */
        GET_INT           (200, true, "Gets"),
        GET_CUM           (201, false, "Gets"),

        PUT_INT            (301, true, "Puts"),
        PUT_IF_ABSENT_INT  (302, true, "PutIfAbsent"),
        PUT_IF_PRESENT_INT (303, true, "PutIfPresent"),
        PUT_IF_VERSION_INT (304, true, "PutIfVersion"),
        PUT_RESOLVE_INT    (305, true, "PutResolve"),

        PUT_CUM            (321, false, "Puts"),
        PUT_IF_ABSENT_CUM  (322, false, "PutIfAbsent"),
        PUT_IF_PRESENT_CUM (323, false, "PutIfPresent"),
        PUT_IF_VERSION_CUM (324, false, "PutIfVersion"),
        PUT_RESOLVE_CUM    (325, false, "PutResolve"),

        DELETE_INT            (401, true,  "Deletes"),
        DELETE_IF_VERSION_INT (402, true, "DeleteIfVersion"),

        DELETE_CUM            (421, false, "Deletes"),
        DELETE_IF_VERSION_CUM (422, false, "DeleteIfVersion"),

        /* multiGet */
        MULTI_GET_INT               (500, true, "MultiGets"),
        MULTI_GET_KEYS_INT          (501, true, "MultiGetKeys"),
        MULTI_GET_ITERATOR_INT      (502, true, "MultiGetIterator"),
        MULTI_GET_KEYS_ITERATOR_INT (503, true, "MultiGetKeysIterator"),

        MULTI_GET_CUM               (520, false, "MultiGets"),
        MULTI_GET_KEYS_CUM          (521, false, "MultiGetKeys"),
        MULTI_GET_ITERATOR_CUM      (522, false, "MultiGetIterator"),
        MULTI_GET_KEYS_ITERATOR_CUM (523, false, "MultiGetKeysIterator"),

        /* storeIterator */
        STORE_ITERATOR_INT      (600, true, "StoreIterator"),
        STORE_KEYS_ITERATOR_INT (601, true, "StoreKeysIterator"),

        STORE_ITERATOR_CUM      (620, false, "StoreIterator"),
        STORE_KEYS_ITERATOR_CUM (621, false, "StoreKeysIterator"),

        /* multiDelete */
        MULTI_DELETE_INT (700, true, "MultiDeletes"),
        MULTI_DELETE_CUM (720, false, "MultiDeletes"),

        /* execute */
        EXECUTE_INT (800, true, "Executes"),
        EXECUTE_CUM (820, false, "Executes"),

        /* NOP */
        NOP_INT (900, true,  "NOPs"),
        NOP_CUM (920, false, "NOPs"),

        /* indexIterator */
        INDEX_ITERATOR_INT      (1000, true, "IndexIterator"),
        INDEX_KEYS_ITERATOR_INT (1001, true, "IndexKeysIterator"),

        INDEX_ITERATOR_CUM      (1020, false, "IndexIterator"),
        INDEX_KEYS_ITERATOR_CUM (1021, false, "IndexKeysIterator"),

        /* query types */
        QUERY_SINGLE_PARTITION_INT (1100, true, "QuerySinglePartition"),
        QUERY_MULTI_PARTITION_INT (1101, true, "QueryMultiPartition"),
        QUERY_MULTI_SHARD_INT (1202, true, "QueryMultiShard"),

        QUERY_SINGLE_PARTITION_CUM (1120, false, "QuerySinglePartition"),
        QUERY_MULTI_PARTITION_CUM (1121, false, "QueryMultiPartition"),
        QUERY_MULTI_SHARD_CUM (1122, false, "QueryMultiShard"),

        /*
         * bulk put. Note that QUERY_MULTI_SHARD, which was implemented earlier,
         * is defined as 1202, and that we must take care to avoid that value.
         */
        PUT_BATCH_INT (1200, true, "BulkPut"),
        PUT_BATCH_CUM (1220, false, "BulkPut"),

        /* bulk get types*/
        MULTI_GET_BATCH_INT (1300, true, "BulkGet"),
        MULTI_GET_BATCH_KEYS_INT (1301, true, "BulkGetKeys"),
        MULTI_GET_BATCH_TABLE_INT (1302, true, "BulkGetTable"),
        MULTI_GET_BATCH_TABLE_KEYS_INT (1303, true, "BulkGetTableKeys"),

        MULTI_GET_BATCH_CUM (1320, false, "BulkGet"),
        MULTI_GET_BATCH_KEYS_CUM (1321, false, "BulkGetKeys"),
        MULTI_GET_BATCH_TABLE_CUM (1322, false, "BulkGetTable"),
        MULTI_GET_BATCH_TABLE_KEYS_CUM (1323, false, "BulkGetTableKeys"),

        GET_IDENTITY_INT(1400, true, "AddSequence"),
        GET_IDENTITY_CUM(1420, false, "AddSequence");



    /** Catalog of all perf stat types. */
    public static Map<Integer, PerfStatType> idMap =
        new HashMap<Integer, PerfStatType>();

    /** Set ancestor relationships. **/
    static {
        USER_SINGLE_READ_INT.setParent(USER_SINGLE_OP_INT);
        USER_SINGLE_WRITE_INT.setParent(USER_SINGLE_OP_INT);
        USER_MULTI_READ_INT.setParent(USER_MULTI_OP_INT);
        USER_MULTI_WRITE_INT.setParent(USER_MULTI_OP_INT);

        USER_SINGLE_READ_CUM.setParent(USER_SINGLE_OP_CUM);
        USER_SINGLE_WRITE_CUM.setParent(USER_SINGLE_OP_CUM);
        USER_MULTI_READ_CUM.setParent(USER_MULTI_OP_CUM);
        USER_MULTI_WRITE_CUM.setParent(USER_MULTI_OP_CUM);

        GET_INT.setParent(USER_SINGLE_READ_INT);
        GET_CUM.setParent(USER_SINGLE_READ_CUM);

        PUT_INT.setParent(USER_SINGLE_WRITE_INT);
        PUT_IF_ABSENT_INT.setParent(USER_SINGLE_WRITE_INT);
        PUT_IF_PRESENT_INT.setParent(USER_SINGLE_WRITE_INT);
        PUT_IF_VERSION_INT.setParent(USER_SINGLE_WRITE_INT);

        PUT_CUM.setParent(USER_SINGLE_WRITE_CUM);
        PUT_IF_ABSENT_CUM.setParent(USER_SINGLE_WRITE_CUM);
        PUT_IF_PRESENT_CUM.setParent(USER_SINGLE_WRITE_CUM);
        PUT_IF_VERSION_CUM.setParent(USER_SINGLE_WRITE_CUM);

        DELETE_INT.setParent(USER_SINGLE_WRITE_INT);
        DELETE_IF_VERSION_INT.setParent(USER_SINGLE_WRITE_INT);

        DELETE_CUM.setParent(USER_SINGLE_WRITE_CUM);
        DELETE_IF_VERSION_CUM.setParent(USER_SINGLE_WRITE_CUM);

        MULTI_GET_INT.setParent(USER_MULTI_READ_INT);
        MULTI_GET_KEYS_INT.setParent(USER_MULTI_READ_INT);
        MULTI_GET_ITERATOR_INT.setParent(USER_MULTI_READ_INT);
        MULTI_GET_KEYS_ITERATOR_INT.setParent(USER_MULTI_READ_INT);

        MULTI_GET_CUM.setParent(USER_MULTI_READ_CUM);
        MULTI_GET_KEYS_CUM.setParent(USER_MULTI_READ_CUM);
        MULTI_GET_ITERATOR_CUM.setParent(USER_MULTI_READ_CUM);
        MULTI_GET_KEYS_ITERATOR_CUM.setParent(USER_MULTI_READ_CUM);

        STORE_ITERATOR_INT.setParent(USER_MULTI_READ_INT);
        STORE_KEYS_ITERATOR_INT.setParent(USER_MULTI_READ_INT);

        STORE_ITERATOR_CUM.setParent(USER_MULTI_READ_CUM);
        STORE_KEYS_ITERATOR_CUM.setParent(USER_MULTI_READ_CUM);

        /* multiDelete */
        MULTI_DELETE_INT.setParent(USER_MULTI_WRITE_INT);
        MULTI_DELETE_CUM.setParent(USER_MULTI_WRITE_CUM);

        /* execute */
        EXECUTE_INT.setParent(USER_MULTI_WRITE_INT);
        EXECUTE_CUM.setParent(USER_MULTI_WRITE_CUM);

        /* index iteration */
        INDEX_ITERATOR_INT.setParent(USER_MULTI_READ_INT);
        INDEX_KEYS_ITERATOR_INT.setParent(USER_MULTI_READ_INT);

        INDEX_ITERATOR_CUM.setParent(USER_MULTI_READ_CUM);
        INDEX_KEYS_ITERATOR_CUM.setParent(USER_MULTI_READ_CUM);

        /* queries */
        QUERY_SINGLE_PARTITION_INT.setParent(USER_MULTI_READ_INT);
        QUERY_MULTI_PARTITION_INT.setParent(USER_MULTI_READ_INT);
        QUERY_MULTI_SHARD_INT.setParent(USER_MULTI_READ_INT);

        QUERY_SINGLE_PARTITION_CUM.setParent(USER_MULTI_READ_CUM);
        QUERY_MULTI_PARTITION_CUM.setParent(USER_MULTI_READ_CUM);
        QUERY_MULTI_SHARD_CUM.setParent(USER_MULTI_READ_CUM);

        /* bulk put */
        PUT_BATCH_INT.setParent(USER_MULTI_WRITE_INT);
        PUT_BATCH_CUM.setParent(USER_MULTI_WRITE_CUM);

        /* bulk get types*/
        MULTI_GET_BATCH_INT.setParent(USER_MULTI_READ_INT);
        MULTI_GET_BATCH_KEYS_INT.setParent(USER_MULTI_READ_INT);
        MULTI_GET_BATCH_TABLE_INT.setParent(USER_MULTI_READ_INT);
        MULTI_GET_BATCH_TABLE_KEYS_INT.setParent(USER_MULTI_READ_INT);

        MULTI_GET_BATCH_CUM.setParent(USER_MULTI_READ_CUM);
        MULTI_GET_BATCH_KEYS_CUM.setParent(USER_MULTI_READ_CUM);
        MULTI_GET_BATCH_TABLE_CUM.setParent(USER_MULTI_READ_CUM);
        MULTI_GET_BATCH_TABLE_KEYS_CUM.setParent(USER_MULTI_READ_CUM);

        GET_IDENTITY_INT.setParent(USER_SINGLE_WRITE_INT);
        GET_IDENTITY_CUM.setParent(USER_SINGLE_WRITE_CUM);

        for (PerfStatType t : EnumSet.allOf(PerfStatType.class)) {
            idMap.put(t.getId(), t);
        }
    }

    /** Retrieve the enum from the catalog */
    public static PerfStatType getType(int id) {
        return idMap.get(id);
    }

    /**
     * The list of stats that are displayed in the <resource>_details.csv file
     */
    public static PerfStatType[] getDetailedStats() {
        return new PerfStatType[] {
            NOP_INT,
            GET_INT,
            PUT_INT,
            PUT_IF_ABSENT_INT,
            PUT_IF_PRESENT_INT,
            PUT_IF_VERSION_INT,
            DELETE_INT,
            DELETE_IF_VERSION_INT,
            MULTI_GET_INT,
            MULTI_GET_KEYS_INT,
            MULTI_GET_ITERATOR_INT,
            MULTI_GET_KEYS_ITERATOR_INT,
            STORE_ITERATOR_INT,
            STORE_KEYS_ITERATOR_INT,
            MULTI_DELETE_INT,
            EXECUTE_INT,
            INDEX_ITERATOR_INT,
            INDEX_KEYS_ITERATOR_INT,
            QUERY_SINGLE_PARTITION_INT,
            QUERY_MULTI_PARTITION_INT,
            QUERY_MULTI_SHARD_INT,
            PUT_BATCH_INT,
            MULTI_GET_BATCH_INT,
            MULTI_GET_BATCH_KEYS_INT,
            MULTI_GET_BATCH_TABLE_INT,
            MULTI_GET_BATCH_TABLE_KEYS_INT
        };
    }

    /**
     * The list of stats that are displayed in the <resource>_summary.csv file
     */
    public static PerfStatType[] getSummaryStats() {
        return new PerfStatType[] {
            USER_SINGLE_OP_INT, USER_MULTI_OP_INT,
            USER_SINGLE_READ_INT, USER_SINGLE_WRITE_INT,
            USER_MULTI_READ_INT, USER_MULTI_WRITE_INT };
    }
    /* The cumulative counterpart */
    public static PerfStatType[] getCumulativeSummaryStats() {
        return new PerfStatType[] {
            USER_SINGLE_OP_CUM, USER_MULTI_OP_CUM,
            USER_SINGLE_READ_CUM, USER_SINGLE_WRITE_CUM,
            USER_MULTI_READ_CUM, USER_MULTI_WRITE_CUM };
    }

    private final int id;
    private final boolean isInterval;
    private final String name;
    private PerfStatType parent;

    private PerfStatType(int id, boolean isInterval, String name) {
        this.id = id;
        this.isInterval = isInterval;
        this.name = name + (isInterval ? "_Interval" :
            "_Cumulative");
    }

    private void setParent(PerfStatType parent1) {
        this.parent = parent1;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean isInterval() {
        return isInterval;
    }

    public PerfStatType getParent() {
        return parent;
    }

    @Override
    public String toString() {
        return name;
    }
}
