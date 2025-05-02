/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import org.junit.Test;

/**
 * Test the records order in index.
 */
public class IndexRecordsOrderTest extends TableTestBase {

    private final static String TABLE_NAME = "tbOrder";
    private final static String FLD_ID = "id";
    private final static String FLD_INT = "intF";
    private final static String FLD_LONG = "longF";
    private final static String FLD_FLOAT = "floatF";
    private final static String FLD_DOUBLE = "doubleF";
    private final static String FLD_STRING = "stringF";
    private final static String FLD_ENUM = "enumF";
    private final static String FLD_TIMESTAMP0 = "timestampF0";
    private final static String FLD_TIMESTAMP3 = "timestampF3";
    private final static String FLD_TIMESTAMP6 = "timestampF6";
    private final static String FLD_TIMESTAMP9 = "timestampF9";
    private final static String FLD_DECIMAL = "decimalF";
    private final static String FLD_ARRAY = "arrayF";
    private final static String FLD_MAP = "mapF";
    private final static String FLD_RECORD = "recordF";
    private final static int MAX_STRING_LEN = 32;
    private final Random rand = new Random(System.currentTimeMillis());

    /**
     * Test records order in index which contains only a single field.
     */
    @Test
    public void testIndexOfSingleField() throws Exception {
        TableImpl table = createTable();
        final long numRows = 1000;
        final List<Long> idsOfRowWithNulls = new ArrayList<Long>();
        populateTable(table, numRows, idsOfRowWithNulls);
        List<String> allFields = table.getFields();

        for (String name : allFields) {

            final String[] idxFields;
            final FieldDef def = table.getField(name);
            if (def.isMap()) {
                idxFields = new String[] {name + ".keys()",
                                          name + ".values()",
                                          name + ".key1"};
            } else if (def.isRecord()) {
                List<String> recFields = def.asRecord().getFieldNames();
                idxFields = new String[recFields.size()];
                for (int i = 0; i < recFields.size(); i++) {
                    idxFields[i] = name + "." + recFields.get(i);
                }
            } else if (def.isArray()) {
                idxFields = new String[] {name + "[]"};
            } else {
                idxFields = new String[]{name};
            }

            for (String field : idxFields) {
                String idxName = field.replace('(', '_').replace(')', '_')
                                 .replace('.', '_').replace('[', '_')
                    .replace(']', '_');
                runTestIndexOfSingleField(table, idxName, field, numRows,
                                          idsOfRowWithNulls);
            }
        }
    }

    private void runTestIndexOfSingleField(TableImpl table, String idxName,
                                           String idxField, long numRows,
                                           List<Long> idsOfRowWithNulls)
        throws Exception {

        StringBuilder indexDDL = new StringBuilder();
        indexDDL.
            append("create index ").append(idxName).append(" on ").
            append(table.getName()).append("(").append(idxField).append(")");
        executeDdl(indexDDL.toString());

        table = getTable(table.getName());

        checkRecordsOrder(table, idxName, Direction.FORWARD, numRows,
                          idsOfRowWithNulls);
        checkRecordsOrder(table, idxName, Direction.REVERSE, numRows,
                          idsOfRowWithNulls);
    }

    /**
     * Test records order in index which contains multiple fields.
     */
    @Test
    public void testIndexOfMultiFields() throws Exception {
        TableImpl table = createTable();
        long numRows = 1000;
        List<Long> idsOfRowWithNulls = new ArrayList<Long>();
        populateTable(table, numRows, idsOfRowWithNulls);
        List<String> allFields = table.getFields();
        List<String> createdIdxFields = null;
        /* Randomly create five indices to test */
        int numOfIdx = 0;

        while (numOfIdx < 5) {
            String name = "index_" + numOfIdx;
            boolean exist = false;
            Map<String, Index> indexes = table.getIndexes();

            List<String> idxFields = getRandomIndexFields(table, allFields);

            for (Entry<String, Index> entry : indexes.entrySet()) {

                createdIdxFields = entry.getValue().getFields();

                if (idxFields.equals(createdIdxFields)) {
                    exist = true;
                    break;
                }
            }
            if (exist) {
                continue;
            }

            table = addIndex(table, name,
                             idxFields.toArray(new String[idxFields.size()]),
                             true);

            checkRecordsOrder(table, name, Direction.FORWARD, numRows,
                              idsOfRowWithNulls);
            checkRecordsOrder(table, name, Direction.REVERSE, numRows,
                              idsOfRowWithNulls);
            numOfIdx++;
        }
    }

    @Test
    public void testRecordIndex() throws Exception {

        String tableStatement =
            "CREATE TABLE testTable(                                 \n" +
            "    id INTEGER,                                         \n" +
            "    foo RECORD(f1 INTEGER, f2 INTEGER),                 \n" +
            "    primary key (id)                                    \n" +
            ")";

        String json0 = "{ \"f1\" : 1, \"f2\" : 20 }";
        String json1 = "{ \"f1\" : 2, \"f2\" : 10 }";
        String json2 = "{ \"f1\" : 3, \"f2\" :  5 }";
        String json3 = "{ \"f1\" : null, \"f2\" :  null}";

        String jdocs[] = { json0, json1, json2, json3};

        String indexStatement =
            "CREATE INDEX idx ON testTable (foo.f2)";

        executeDdl(tableStatement);
        executeDdl(indexStatement);

        Table table = tableImpl.getTable("testTable");

        Index index = table.getIndex("idx");

        int numRows = jdocs.length;

        for (int i = 0; i < numRows; i++) {

            Row row = table.createRow();
            row.put("id", i);
            row.putRecordAsJson("foo", jdocs[i], true);

            tableImpl.put(row, null, null);
        }

        IndexKey idxKey = index.createIndexKey();

        TableAPI tableApi = store.getTableAPI();

        TableIteratorOptions opts =
            new TableIteratorOptions(Direction.FORWARD,
                                     Consistency.ABSOLUTE,
                                     0, null, /* timeout, unit */
                                     0, 0);

        TableIterator<Row> iter = tableApi.tableIterator(idxKey, null, opts);

        final String[] expected;
        if (((IndexImpl)index).supportsSpecialValues()) {
            expected = new String[] {
                "{\"id\":2,\"foo\":{\"f1\":3,\"f2\":5}}",
                "{\"id\":1,\"foo\":{\"f1\":2,\"f2\":10}}",
                "{\"id\":0,\"foo\":{\"f1\":1,\"f2\":20}}",
                "{\"id\":3,\"foo\":{\"f1\":null,\"f2\":null}}"
            };
        } else {
            expected = new String[] {
                "{\"id\":2,\"foo\":{\"f1\":3,\"f2\":5}}",
                "{\"id\":1,\"foo\":{\"f1\":2,\"f2\":10}}",
                "{\"id\":0,\"foo\":{\"f1\":1,\"f2\":20}}"
            };
        }

        int i = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            assertTrue(expected[i].equals(row.toString()));
            ++i;
        }
    }

    /* Create table with field types can participate in an index. */
    private TableImpl createTable() throws Exception {
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addString(null);
        TableBuilderBase tb = TableBuilder.createTableBuilder(TABLE_NAME)
            .addLong(FLD_ID)
            .addInteger(FLD_INT)
            .addLong(FLD_LONG)
            .addFloat(FLD_FLOAT)
            .addDouble(FLD_DOUBLE)
            .addString(FLD_STRING)
            .addTimestamp(FLD_TIMESTAMP0, 0)
            .addTimestamp(FLD_TIMESTAMP3, 3)
            .addTimestamp(FLD_TIMESTAMP6, 6)
            .addTimestamp(FLD_TIMESTAMP9, 9)
            .addNumber(FLD_DECIMAL)
            .addEnum(FLD_ENUM,
                new String[]{"SINT","INT","LONG","FLOAT","DOUBLE"}, null)
            .addField(FLD_ARRAY, ab.build())
            .addField(FLD_MAP,
                      TableBuilder.createMapBuilder().addInteger().build())
            .addField(FLD_RECORD,
                      TableBuilder.createRecordBuilder(FLD_RECORD)
                      .addString("s1")
                      .addInteger("i1")
                      .build())
            .primaryKey(FLD_ID)
            .shardKey(FLD_ID);
        addTable((TableBuilder)tb, true);
        return getTable(TABLE_NAME);
    }

    /* Populate 1000 random rows to table. */
    private void populateTable(TableImpl table,
                               long numRows,
                               List<Long> idsOfRowWithNulls) {
        for (long i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put(FLD_ID, i);
            boolean setNull = useNulls(i);
            if (setNull) {
                if (idsOfRowWithNulls != null) {
                    idsOfRowWithNulls.add(i);
                }
            }
            for(String field : table.getFields()) {
                if (field.equals(FLD_ID)) {
                    continue;
                }
                if (setNull) {
                    row.putNull(field);
                } else {
                    FieldDef def = table.getField(field);
                    if (def.isArray()) {
                        def = def.asArray().getElement();
                        ArrayValue array = row.putArray(field);
                        FieldValue value = getFieldValue(def);
                        array.add(value);
                    } else if (def.isMap()) {
                        MapValue map = row.putMap(field);
                        def = def.asMap().getElement();
                        FieldValue value = getFieldValue(def);
                        map.put("key1", value);
                    } else if (def.isRecord()) {
                        RecordValue rec = row.putRecord(field);
                        for (String name : rec.getFieldNames()) {
                            def = rec.getDefinition().getFieldDef(name);
                            FieldValue value = getFieldValue(def);
                            rec.put(name, value);
                        }
                    } else {
                        row.put(field, getFieldValue(def));
                    }
                }
            }
            tableImpl.putIfAbsent(row, null, null);
        }
    }

    private boolean useNulls(long id) {
        return id % 100 == 1;
    }

    /* Get fields values according to the FieldDef. */
    private FieldValue getFieldValue(FieldDef def) {
        switch (def.getType()) {
        case INTEGER:
            return def.createInteger(rand.nextInt());
        case LONG:
            return def.createLong(rand.nextLong());
        case FLOAT:
            return def.createFloat(rand.nextFloat());
        case DOUBLE:
            return def.createDouble(rand.nextDouble());
        case STRING:
            return def.createString(
                    randomString(rand, rand.nextInt(MAX_STRING_LEN)));
        case TIMESTAMP: {
            return def.createTimestamp(randomTimestamp());
        }
        case NUMBER:
            return def.createNumber(randomBigDecimal());
        case ENUM:
            String[] values = def.asEnum().getValues();
            return def.createEnum(values[rand.nextInt(values.length)]);
        default:
            throw new IllegalStateException("Unsupported type:" +
                def.getType());
        }
    }

    private Timestamp randomTimestamp() {
        long min = TimestampDefImpl.MIN_VALUE.getTime();
        long max = TimestampDefImpl.MAX_VALUE.getTime();
        long millis = min + (long)(rand.nextDouble() * (max - min));
        Timestamp ts = new Timestamp(millis);
        ts.setNanos(rand.nextInt(1000000000));
        return ts;
    }

    /* Create a random string. */
    private String randomString(Random random, int len) {
        final String AB =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(len);
        for( int i = 0; i < len; i++ ) {
             sb.append(AB.charAt(random.nextInt(AB.length())));
         }
        return sb.toString();
    }

    /* Create a random BigDecimal value. */
    private BigDecimal randomBigDecimal() {
        int type = rand.nextInt(5);
        switch (type) {
        case 0:
            return BigDecimal.valueOf(rand.nextInt());
        case 1:
            return BigDecimal.valueOf(rand.nextLong());
        case 2:
            return BigDecimal.valueOf(rand.nextFloat());
        case 3:
            return BigDecimal.valueOf(rand.nextDouble());
        default:
            return new BigDecimal(BigInteger.valueOf(rand.nextLong()),
                                  rand.nextInt());
        }
    }

    /* Randomly choose some fields to create indices on. */
    private List<String> getRandomIndexFields(TableImpl table,
                                              List<String> allFields) {
        int nFields = rand.nextInt(allFields.size()) + 1;
        List<String> fields = new ArrayList<String>(allFields);
        final List<String> idxFields = new ArrayList<String>(nFields);
        boolean hasMultiKeyField = false;
        while (idxFields.size() < nFields) {
            String name = fields.get(rand.nextInt(fields.size()));
            FieldDef def = table.getField(name);
            if (hasMultiKeyField && def.isMap() || def.isArray()) {
                continue;
            }
            if (def.isMap()) {
                idxFields.add(name + ".keys()");
                idxFields.add(name + ".values()");
                hasMultiKeyField = true;
            } else if (def.isArray()) {
                hasMultiKeyField = true;
                idxFields.add(name);
            } else if (def.isRecord()) {
                for (String field : def.asRecord().getFieldNames()) {
                    idxFields.add(name + "." + field);
                }
            } else {
                idxFields.add(name);
            }
            fields.remove(name);
        }
        return idxFields;
    }

    /* Function to check the records order in an index. */
    private void checkRecordsOrder(TableImpl table, String idxName,
                                   Direction direction, long numRows,
                                   List<Long> idsOfRowWithNulls) {
        Index index = table.getIndex(idxName);
        IndexKey key = index.createIndexKey();
        TableIterator<KeyPair> kpIter =
            tableImpl.tableKeysIterator(key, null,
                    new TableIteratorOptions(direction, null, 0, null));
        KeyPair lastPair = null;
        long cnt = 0;

        boolean indexSupportNulls = ((IndexImpl)index).supportsSpecialValues();
        long expNumKeys = indexSupportNulls ?
                            numRows : numRows - idsOfRowWithNulls.size();
        while (kpIter.hasNext()) {
            KeyPair currentPair = kpIter.next();
            if (lastPair != null) {
                if (direction.equals(Direction.FORWARD)) {
                    assertTrue(lastPair.compareTo(currentPair) <= 0);
                } else if (direction.equals(Direction.REVERSE)) {
                    assertTrue(lastPair.compareTo(currentPair) >= 0);
                }
            }
            if (!index.getFields().get(0).equals(FLD_ID)) {
                checkRowWithNulls(indexSupportNulls, direction, expNumKeys,
                                  idsOfRowWithNulls, cnt, currentPair);
            }
            lastPair = currentPair;
            cnt++;
        }
        kpIter.close();
        assertTrue(cnt == expNumKeys);
    }

    /**
     * Check if the sorting order of those rows that contains index field with
     * NULLs:
     *  - Forward order: all index field with NULLs are at last.
     *  - Reverse order: all index field with NULLs are at first.
     */
    private void checkRowWithNulls(boolean indexSupportNulls,
                                   Direction direction,
                                   long numKeys,
                                   List<Long> idsOfRowWithNulls,
                                   long idx,
                                   KeyPair keyPair) {

        long id = keyPair.getPrimaryKey().get("id").asLong().get();
        if (!indexSupportNulls) {
            assertTrue(!idsOfRowWithNulls.contains(id));
            return;
        }

        if (direction.equals(Direction.FORWARD)) {
            if (idx > numKeys - idsOfRowWithNulls.size()) {
                int i = idsOfRowWithNulls.size() - (int)(numKeys - idx);
                assertTrue(id == idsOfRowWithNulls.get(i));
                checkIndexNulls(keyPair.getIndexKey());
            }
        } else if (direction.equals(Direction.REVERSE)) {
            if (idx < idsOfRowWithNulls.size()) {
                int i = idsOfRowWithNulls.size() - (int)idx - 1;
                assertTrue(id == idsOfRowWithNulls.get(i));
                checkIndexNulls(keyPair.getIndexKey());
            }
        }
    }

    /**
     * Check if index fields are all NULLs except "id" field
     */
    private void checkIndexNulls(IndexKey idxKey) {
        for (String field : idxKey.getIndex().getFields()) {
            if (field.equals(FLD_ID)) {
                continue;
            }
            assertTrue(idxKey.get(field).isNull());
        }
    }
}
