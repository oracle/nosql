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

import java.util.List;
import java.util.Map;
import oracle.kv.table.FieldDef;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

/**
 */
public interface TableEventHandler {
    public void start(String namespace, String tableName);

    @SuppressWarnings("unused")
    default void tableId(long tableId) {}

    public void owner(String owner);

    public void ttl(TimeToLive ttl);

    public void systemTable(boolean value);

    public void description(String description);

    public void parent(String parentName);

    public void primaryKey(List<String> primaryKey);

    public void primaryKeySizes(List<Integer> primaryKeySizes);

    public void shardKey(List<String> shardKey);

    public void regions(Map<Integer, String> regions,
                        int localRegionId,
                        String localRegionName);

    public void jsonCollection(boolean value);

    public void jsonCollectionMRCounters(Map<String, FieldDef.Type> counters);

    public void limits(TableLimits limits);

    public void identity(String columnName,
                         boolean generatedAlways,
                         boolean onNull,
                         SequenceDef sequenceDef);

    /*
     * Use this to list child table names
     */
    public void children(List<String> childTables);

    public void startChildTables(int numChildTables);
    public void startChildTable(boolean isFirst);
    public void endChildTable();
    public void endChildTables();

    public void startIndexes(int numIndexes);

    public void index(Table table, /* can be null */
                      int indexNumber, /* starts with 1 */
                      String indexName,
                      String description,
                      String type,
                      List<String> fields,
                      List<String> types,
                      boolean indexesNulls,
                      boolean isUnique,
                      Map<String, String> annotations,
                      Map<String, String> properties);

    public void endIndexes();

    /*
     * o start/endFields is used for top-level tables as well as records
     * o start/endField is used for record fields as well as the type of
     * map and array types. E.g. for a record:
     *  startFields
     *    for each field, startField, ..., endField
     *  endFields
     *
     * For a map:
     *
     * startField
     *   startField // for the type
     *    ...
     *   endField
     * endField
     */
    public void startFields(int numFields);

    public void startField(boolean first);

    public void fieldInfo(String name,
                          FieldDef fieldDef,
                          Boolean nullable,
                          String defaultValue);

    /*
     * Used for map and array types
     */
    public void startCollection();
    public void endCollection();

    public void endField();
    public void endFields();

    public void end();
}
