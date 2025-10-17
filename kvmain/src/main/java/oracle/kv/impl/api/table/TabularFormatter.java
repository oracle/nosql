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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.query.shell.output.ResultOutputFactory;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.OutputMode;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.ResultOutput;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.FieldDef.Type;

/*
 * A class encapsulates methods to format table or index definition to a string
 * in tabular output.
 */
public class TabularFormatter {

    private final static String FIELD_NAME = "name";
    private final static String FIELD_TTL = "ttl";
    private final static String FIELD_BEFORE_IMAGE_TTL = "beforeImageTTL";
    private final static String FIELD_OWNER = "owner";
    private final static String FIELD_NAMESPACE = "namespace";
    private final static String FIELD_SYSTABLE = "sysTable";
    private final static String FIELD_JSON_COLLECTION = "jsonCollection";
    private final static String FIELD_PARENT = "parent";
    private final static String FIELD_CHILDREN = "children";
    private final static String FIELD_INDEXES = "indexes";
    private final static String FIELD_COMMENT = "description";
    private final static String FIELD_REGIONS = "regions";

    private final static String FIELD_ID = "id";
    private final static String FIELD_TYPE = "type";
    private final static String FIELD_NULLABLE = "nullable";
    private final static String FIELD_DEFAULT = "default";
    private final static String FIELD_SHARD_KEY = "shardKey";
    private final static String FIELD_PRIMARY_KEY = "primaryKey";
    private final static String FIELD_IDENTITY = "identity";

    private final static String FIELD_TABLE = "table";
    private final static String FIELD_FIELDS = "fields";
    private final static String FIELD_MULTI_KEY = "multiKey";
    private final static String FIELD_TYPES = "fieldType";
    private final static String FIELD_ANNOTATIONS = "annotations";
    private final static String FIELD_PROPERTIES = "properties";

    /* Table for basic table information */
    private static TableImpl tableInfo;
    /* Table for field definitions */
    private static TableImpl tableFields;
    /* Table for index definition */
    private static TableImpl tableIndex;
    /* Table for full text index definitions */
    private static TableImpl tableTextIndex;

    /* The position of namespace field, it is used to hide this column */
    private static int tableInfoNamespacePos;
    private static int tableIndexNamespacePos;

    static {
        tableInfo = buildTableInfoTable();
        tableFields = buildTableFieldsTable();
        tableIndex = buildTableIndexTable(false);
        tableTextIndex = buildTableIndexTable(true);

        /* Get the position of namespace field in tableInfo table */
        int i = 0;
        for (String field : tableInfo.getFields()) {
            if (field.equals(FIELD_NAMESPACE)) {
                tableInfoNamespacePos = i;
                break;
            }
            i++;
        }

        /* Get the position of namespace field in tableIndex table */
        i = 0;
        for (String field : tableIndex.getFields()) {
            if (field.equals(FIELD_NAMESPACE)) {
                tableIndexNamespacePos = i;
                break;
            }
            i++;
        }
    }

    /**
     * Formats the table definition in tabular output.
     */
    public static String formatTable(TableImpl table,
                                     Map<String, Object> fields,
                                     RegionMapper regionMapper) {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintStream outStream = new PrintStream(out);

        if (fields == null) {
            outStream.println(" === Information ===");

            /* Hidden the namespace column if the table's namespace is null */
            outputRecord(outStream, generateTableInfoRecord(table, regionMapper),
                         (table.getInternalNamespace() == null ?
                             new int[] {tableInfoNamespacePos} : null));
            outStream.println("\n === Fields ===");
        }
        outputRecords(outStream, generateFieldsRecords(table, fields), null);
        return out.toString();
    }

    /**
     * Formats the index definition in tabular output.
     */
    public static String formatIndex(IndexImpl index) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        /* Hidden the namespace column if the index's namespace is null */
        outputRecord(new PrintStream(out), generateIndexRecord(index),
                     (index.getTable().getInternalNamespace() == null ?
                         new int[] {tableIndexNamespacePos} : null));
        return out.toString();
    }

    /**
     * Generates the record of table basic information
     */
    private static RecordValue generateTableInfoRecord(
        TableImpl table,
        RegionMapper regionMapper) {

        RecordValue row = tableInfo.createRow();
        row.put(FIELD_NAMESPACE, emptyIfNull(table.getInternalNamespace()));
        row.put(FIELD_NAME, table.getFullName());
        row.put(FIELD_TTL, emptyIfNull(table.getDefaultTTL()));
        row.put(FIELD_BEFORE_IMAGE_TTL, emptyIfNull(table.getBeforeImageTTL()));
        row.put(FIELD_OWNER, emptyIfNull(table.getOwner()));
        row.put(FIELD_SYSTABLE, booleanYesNo(table.isSystemTable()));
        row.put(FIELD_JSON_COLLECTION, booleanYesNo(table.isJsonCollection()));
        row.put(FIELD_PARENT, emptyIfNull(table.getParentName()));
        ArrayValue av = row.putArray(FIELD_CHILDREN);
        if (!table.getChildTables().isEmpty()) {
            for (String cname : table.getChildTables().keySet()) {
                av.add(cname);
            }
        }
        row.put(FIELD_REGIONS,
                emptyIfNull(table.createRegionsString(regionMapper)));
        av = row.putArray(FIELD_INDEXES);
        if (!table.getIndexes().isEmpty()) {
            for (String iname : table.getIndexes().keySet()) {
                av.add(iname);
            }
        }
        row.put(FIELD_COMMENT, emptyIfNull(table.getDescription()));
        return row;
    }

    /**
     * Generates the records for fields definition.
     *
     * The fieldMap is a map of field path name and its definition, the field
     * definition can be either FieldDef or FieldMapEntry object.
     *
     * If the fieldMap is null, generates records for all fields of the given
     * table. Otherwise, generates records for the fields in fieldMap.
     */
    private static List<RecordValue>
        generateFieldsRecords(TableImpl table, Map<String, Object> fieldMap) {

        List<RecordValue> records = new ArrayList<RecordValue>();
        int ind = 1;
        if (fieldMap == null) {
            for (String name : table.getFields()) {
                FieldDef fdef = table.getField(name);
                boolean isIdentity = table.hasIdentityColumn() &&
                    table.getIdentityColumn() == table.getFieldMap()
                        .getFieldPos(name);

                RecordValue record =
                    createFieldRecord(ind++, name, fdef,
                                      table.isNullable(name),
                                      table.getDefaultValue(name),
                                      table.getShardKey().contains(name),
                                      table.getPrimaryKey().contains(name),
                                      isIdentity);
                records.add(record);
            }
            return records;
        }

        for (Entry<String, Object> e : fieldMap.entrySet()) {
            String name = e.getKey();
            RecordValue record;
            if (e.getValue() instanceof FieldDef) {
                record = createFieldRecord(ind++, name,
                                           (FieldDef)e.getValue(),
                                           false,  /* isNullable */
                                           null,   /* defVal */
                                           false,  /* isShardKey */
                                           false,  /* isPrimaryKey */
                                           false); /* isIdentity */
            } else {
                assert(e.getValue() instanceof FieldMapEntry);
                FieldMapEntry fme = (FieldMapEntry)e.getValue();

                boolean isIdentity = table.hasIdentityColumn() &&
                    table.getIdentityColumn() == table.getFieldMap()
                        .getFieldPos(name);

                record = createFieldRecord(ind++, name,
                                           fme.getFieldDef(),
                                           fme.isNullable(),
                                           fme.getDefaultValue(),
                                           table.getShardKey().contains(name),
                                           table.getPrimaryKey().contains(name),
                                           isIdentity);
            }
            records.add(record);
        }
        return records;
    }

    /**
     * Creates a record of tableFields table with the given information.
     */
    private static RecordValue createFieldRecord(int id,
                                                 String name,
                                                 FieldDef fdef,
                                                 boolean isNullable,
                                                 FieldValue defVal,
                                                 boolean isShardKey,
                                                 boolean isPrimaryKey,
                                                 boolean isIdentity) {
        Row row = tableFields.createRow();
        row.put(FIELD_ID, id);
        row.put(FIELD_NAME, name);

        String[] lines = ((FieldDefImpl)fdef).getDDLString().split("\\n");
        ArrayValue av = row.putArray(FIELD_TYPE);
        for (String line : lines) {
            av.add(line);
        }

        row.put(FIELD_NULLABLE, booleanYesNo(isNullable));
        row.put(FIELD_DEFAULT, emptyIfNull(defVal));
        row.put(FIELD_SHARD_KEY, booleanYes(isShardKey));
        row.put(FIELD_PRIMARY_KEY, booleanYes(isPrimaryKey));
        row.put(FIELD_IDENTITY, booleanYes(isIdentity));
        return row;
    }

    /**
     * Generates a record of tableIndex or tableTextIndex with the definition
     * of the given index.
     */
    private static RecordValue generateIndexRecord(IndexImpl index) {

        boolean isTextIndex = (index.getType() == Index.IndexType.TEXT);
        Row row = isTextIndex ? tableTextIndex.createRow() :
                                tableIndex.createRow();

        row.put(FIELD_NAMESPACE,
            emptyIfNull(index.getTable().getInternalNamespace()));
        row.put(FIELD_TABLE,index.getTable().getFullName());
        row.put(FIELD_NAME, index.getName());
        row.put(FIELD_TYPE, index.getType().name());
        row.put(FIELD_MULTI_KEY, booleanYesNo(index.isMultiKey()));

        ArrayValue av = row.putArray(FIELD_FIELDS);
        for (int i = 0; i < index.numFields(); i++) {
            String field = TableJsonUtils.toExternalIndexField(index, i, true);
            av.add(field);
        }

        if (!isTextIndex) {
            av = row.putArray(FIELD_TYPES);
            for (int i = 0; i < index.numFields(); i++) {
                IndexField ifield = index.getIndexPath(i);
                Type t;
                if (ifield.getFunction() != null) {
                    t = ifield.getType().getType();
                } else {
                    t = index.getFieldType(i);
                }
                av.add(emptyIfNull(t));
            }
        } else {
            av = row.putArray(FIELD_ANNOTATIONS);
            for (String field : index.getFields()) {
                av.add(emptyIfNull(index.getAnnotationForField(field)));
            }

            MapValue mv = row.putMap(FIELD_PROPERTIES);
            for (Entry<String, String> e : index.getProperties().entrySet()) {
                mv.put(e.getKey(), e.getValue());
            }
        }
        row.put(FIELD_COMMENT, emptyIfNull(index.getDescription()));
        return row;
    }

    /**
     * Outputs the record(s) to the PrintStream
     */
    private static void outputRecord(PrintStream output,
                                     RecordValue record,
                                     int[] hiddenColumns) {
        outputRecords(output, Arrays.asList(record), hiddenColumns);
    }

    private static void outputRecords(PrintStream output,
                                      List<RecordValue> records,
                                      int[] hiddenColumns) {

        RecordDef recordDef = records.get(0).getDefinition();
        ResultOutput ro = ResultOutputFactory.getOutput(OutputMode.COLUMN,
                                                        null, output,
                                                        recordDef,
                                                        records.iterator(),
                                                        false, 0,
                                                        hiddenColumns);
        try {
            ro.outputResultSet();
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to output records: " +
                                       ioe.getMessage());
        }
    }

    /**
     * Build a table to store the basic table information, its schema as below:
     *  CREATE TABLE tableInfo(namespace STRING,
     *                         name STRING,
     *                         ttl STRING,
     *                         owner STRING,
     *                         sysTable STRING,
     *                         parent STRING,
     *                         children ARRAY(STRING),
     *                         regions STRING,
     *                         indexes ARRAY(STRING),
     *                         description STRING,
     *                         PRIMARY KEY(namespace, name))
     */
    private static TableImpl buildTableInfoTable() {
        TableBuilder tb = TableBuilder.createTableBuilder("tableInfo");
        tb.addString(FIELD_NAMESPACE);
        tb.addString(FIELD_NAME);
        tb.addString(FIELD_TTL);
        tb.addString(FIELD_BEFORE_IMAGE_TTL);
        tb.addString(FIELD_OWNER);
        tb.addString(FIELD_JSON_COLLECTION);
        tb.addString(FIELD_SYSTABLE);
        tb.addString(FIELD_PARENT);
        tb.addField(FIELD_CHILDREN, TableBuilder.createArrayBuilder()
                                                .addString().build());
        tb.addString(FIELD_REGIONS);
        tb.addField(FIELD_INDEXES, TableBuilder.createArrayBuilder()
                                               .addString().build());
        tb.addString(FIELD_COMMENT);
        tb.primaryKey(FIELD_NAMESPACE, FIELD_NAME);
        return tb.buildTable();
    }

    /**
     * Build a table to store the table fields definition, its schema is as
     * below:
     *  CREATE TABLE tableFields(id INTEGER
     *                           name STRING,
     *                           type ARRAY(STRING),
     *                           nullable STRING,
     *                           default STRING,
     *                           shardKey STRING,
     *                           primaryKey STRING,
     *                           PRIMARY KEY(ID));
     */
    private static TableImpl buildTableFieldsTable() {
        TableBuilder tb = TableBuilder.createTableBuilder("tableFields");
        tb.addInteger(FIELD_ID);
        tb.addString(FIELD_NAME);
        tb.addField(FIELD_TYPE, TableBuilder.createArrayBuilder()
                                            .addString().build());
        tb.addString(FIELD_NULLABLE);
        tb.addString(FIELD_DEFAULT);
        tb.addString(FIELD_SHARD_KEY);
        tb.addString(FIELD_PRIMARY_KEY);
        tb.addString(FIELD_IDENTITY);
        tb.primaryKey(FIELD_ID);
        return tb.buildTable();
    }

    /**
     * Build a table to store the index definition,
     *
     *  CREATE TABLE tableIndex(namespace STRING,
     *                          table STRING,
     *                          name STRING,
     *                          type STRING,
     *                          multiKey STRING,
     *                          fields ARRAY(STRING),
     *                          declaredTypes ARRAY(STRING),
     *                          description STRING,
     *                          PRIMARY KEY(namespace, table, name))
     *
     *  CREATE TABLE tableTextIndex(namespace STRING,
     *                              table STRING,
     *                              name STRING,
     *                              type STRING,
     *                              multiKey STRING,
     *                              fields ARRAY(STRING),
     *                              annotations ARRAY(STRING),
     *                              properties ARRAY(STRING),
     *                              description STRING,
     *                              PRIMARY KEY(namespace, table, name))
     */
    private static TableImpl buildTableIndexTable(boolean isTextIndex) {
        String name = isTextIndex ? "tableTextIndex" : "tableIndex";
        TableBuilder tb = TableBuilder.createTableBuilder(name);
        tb.addString(FIELD_NAMESPACE);
        tb.addString(FIELD_TABLE);
        tb.addString(FIELD_NAME);
        tb.addString(FIELD_TYPE);
        tb.addString(FIELD_MULTI_KEY);
        tb.addField(FIELD_FIELDS,
                    TableBuilder.createArrayBuilder().addString().build());
        if (!isTextIndex) {
            tb.addField(FIELD_TYPES,
                        TableBuilder.createArrayBuilder().addString().build());
        } else {
            tb.addField(FIELD_ANNOTATIONS, TableBuilder.createArrayBuilder()
                                            .addString().build());
            tb.addField(FIELD_PROPERTIES, TableBuilder.createMapBuilder()
                                            .addString().build());
        }
        tb.addString(FIELD_COMMENT);
        tb.primaryKey(FIELD_NAMESPACE, FIELD_NAME);
        return tb.buildTable();
    }

    /**
     * Returns "Y" if val is true, otherwise return a empty string
     */
    private static String booleanYes(boolean val) {
        return val ? "Y" : "";
    }

    /**
     * Returns "Y" if val is true, otherwise return "N"
     */
    private static String booleanYesNo(boolean val) {
        return val ? "Y" : "N";
    }

    /**
     * Returns a empty string "" if the given string is null, otherwise
     * return string.
     */
    private static String emptyIfNull(String val) {
        return val == null ? "" : val;
    }

    /**
     * Returns a empty string "" if the given object is null, otherwise
     * return object.toString().
     */
    private static String emptyIfNull(Object val) {
        return val == null ? "" : val.toString();
    }
}
