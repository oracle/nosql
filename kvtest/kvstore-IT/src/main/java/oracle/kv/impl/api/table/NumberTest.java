/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import oracle.kv.Direction;
import oracle.kv.StatementResult;
import oracle.kv.Version;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for NUMBER type.
 */
public class NumberTest extends TableTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        staticSetUp(true);
    }

    @Test
    public void testBasic()
        throws Exception {


        Table table = createTable();
        BigDecimal[] values = new BigDecimal[] {
            BigDecimal.ZERO,
            BigDecimal.ONE,
            BigDecimal.TEN,
            BigDecimal.valueOf(-1),
            BigDecimal.valueOf(Integer.MIN_VALUE),
            BigDecimal.valueOf(Integer.MAX_VALUE),
            BigDecimal.valueOf(Long.MIN_VALUE),
            BigDecimal.valueOf(Long.MAX_VALUE),
            BigDecimal.valueOf(-1 * Float.MIN_VALUE),
            BigDecimal.valueOf(-1 * Float.MAX_VALUE),
            BigDecimal.valueOf(Float.MIN_VALUE),
            BigDecimal.valueOf(Float.MAX_VALUE),
            BigDecimal.valueOf(-1 * Double.MIN_VALUE),
            BigDecimal.valueOf(-1 * Double.MAX_VALUE),
            BigDecimal.valueOf(Double.MIN_VALUE),
            BigDecimal.valueOf(Double.MAX_VALUE),
            randomBigDecimal(System.currentTimeMillis())
        };

        /* Put op */
        int id = 0;
        for (BigDecimal v : values) {
            Row row = table.createRow();
            row.put("id", id++);
            row.putNumber("pk", v);
            row.putNumber("dec", v);
            row.put("s", v.toString());
            Version ver = tableImpl.put(row, null, null);
            assertTrue(ver != null);
        }

        /* Get op */
        id = 0;
        for (BigDecimal v : values) {
            PrimaryKey pkey = table.createPrimaryKey();
            pkey.put("id", id);
            pkey.putNumber("pk", v);

            Row row = tableImpl.get(pkey, null);
            assertTrue(row != null);
            assertTrue(row.get("id").asInteger().get() == id);
            assertTrue(row.get("pk").asNumber().get().compareTo(v) == 0);
            assertTrue(row.get("dec").asNumber().get().compareTo(v) == 0);
            assertTrue(row.get("s").asString().get().equals(v.toString()));
            id++;
        }

        /* Query */
        final String numberQuery = "select id+pk from decimalTest ";
        try {
            StatementResult sr = store.executeSync(numberQuery);
            int count = 0;
            for (Iterator<RecordValue> iterator = sr.iterator();
                    iterator.hasNext();) {
                iterator.next();
                ++count;
            }
            assertTrue(count == 17);
        } catch (IllegalArgumentException iae) {
            fail("Exception: " + iae);
        }

        id = 0;
        /* Delete op */
        for (BigDecimal v : values) {
            PrimaryKey pkey = table.createPrimaryKey();
            pkey.put("id", id++);
            pkey.putNumber("pk", v);

            boolean deleted = tableImpl.delete(pkey, null, null);
            assertTrue(deleted);
        }
    }

    @Test
    public void testTableScan()
        throws Exception {

        final int nRows = 100;
        final Direction[] directions = new Direction[] {
            Direction.FORWARD, Direction.REVERSE
        };
        final int start = 10;
        final int end = 51;
        final boolean[] inclusiveFlags = new boolean[]{false, true};

        Table table = createTable();
        loadRows(table, nRows);

        BigDecimal valS = getDecimal(start);
        BigDecimal valE = getDecimal(end);

        PrimaryKey key = table.createPrimaryKey();
        for (Direction direction : directions) {
            /* Full scan */
            doTableScan(key, null, direction, nRows, false);

            /* Scan with range */
            FieldRange[] frs = createFieldRange(table, null,
                                                table.getPrimaryKey().get(0),
                                                valS, valE, inclusiveFlags);
            for (FieldRange fr : frs) {
                int expCnt = getExpectRangeCount(fr, start, end, nRows);
                doTableScan(key, fr, direction, expCnt, false);
            }
        }

        key = createRow(table, 10).createPrimaryKey();
        doTableScan(key, null, Direction.UNORDERED, 1, false);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testIndexScan()
        throws Exception {

        final int nRows = 100;
        final Direction[] directions = new Direction[] {
            Direction.FORWARD, Direction.REVERSE
        };

        final int start = 10;
        final int end = 51;
        final boolean[] inclusiveFlags = new boolean[]{false, true};

        Table table = createTable();
        loadRows(table, nRows);
        BigDecimal valS = randomBigDecimal(start);
        BigDecimal valE = randomBigDecimal(end);

        for (Direction direction : directions) {
            for (Entry<String, Index> e : table.getIndexes().entrySet()) {
                /* Full scan */
                Index index = e.getValue();
                IndexKey key = index.createIndexKey();
                doTableScan(key, null, direction, nRows, false);

                /* Scan with range */
                FieldRange[] ranges = createFieldRange(table, index,
                                                       index.getFields().get(0),
                                                       valS, valE,
                                                       inclusiveFlags);
                for (FieldRange range : ranges) {
                    int expCnt = getExpectRangeCount(range, start, end, nRows);
                    doTableScan(key, range, direction, expCnt, false);
                }

                key = index.createIndexKey(createRow(table, 10));
                doTableScan(key, null, Direction.UNORDERED, 1, false);
            }
        }
    }

    @Test
    public void testNestedDecimal() throws Exception {

        final Table table = createComplexTable();
        final int nRows = 100;

        loadRowsToComplexTable(table, nRows);

        /* Full table scan */
        doTableScan(table.createPrimaryKey(), null,
                    Direction.UNORDERED, 100, true);

        /* Index scan using idxArray index */
        Index index = table.getIndex("idxArray");
        FieldDef eDef = table.getField("arrayF").asArray().getElement();
        assertTrue(eDef.isNumber());

        IndexKey ikey = index.createIndexKey();
        BigDecimal bd = getDecimal(1);
        ikey.putNumber("arrayF[]", bd);
        doTableScan(ikey, null, Direction.FORWARD, 10, true);

        /* Range search with idxArray */
        ikey = index.createIndexKey();
        FieldRange fr = index.createFieldRange("arrayF[]");
        fr.setStart(getDecimal(15), true);
        doTableScan(ikey, fr, Direction.FORWARD, 50, true);

        fr.setEnd(getDecimal(18), false);
        doTableScan(ikey, fr, Direction.FORWARD, 10, true);

        /* Index scan using idxMapValue index */
        index = table.getIndex("idxMapValue");
        eDef = table.getField("mapF").asMap().getElement();
        assertTrue(eDef.isNumber());

        ikey = index.createIndexKey();
        bd = getDecimal(2);
        ikey.putNumber("mapF.values()", bd);
        doTableScan(ikey, null, Direction.FORWARD, 5, true);

        /* Range search with idxArray */
        ikey = index.createIndexKey();
        fr = index.createFieldRange("mapF.values()");
        fr.setStart(getDecimal(30), true);
        doTableScan(ikey, fr, Direction.FORWARD, 50, true);

        fr.setEnd(getDecimal(40), false);
        doTableScan(ikey, fr, Direction.FORWARD, 20, true);

        /* Index scan using idxRecordDec1Dec2 */
        index = table.getIndex("idxRecordDec1Dec2");
        RecordDef def = ((IndexImpl)index).getIndexKeyDef();
        ikey = index.createIndexKey();
        bd = getDecimal(5);
        for (String field : def.getFieldNames()) {
            eDef = def.getFieldDef(field);
            assertTrue(eDef.isNumber());
            ikey.putNumber(field, bd);
        }
        doTableScan(ikey, null, Direction.FORWARD, 2, true);

        /* Range second on indexRecordTs0Ts9 */
        fr = index.createFieldRange("recordF.dec1");
        eDef = def.getFieldDef("recordF.dec2");
        BigDecimal start = getDecimal(2);
        BigDecimal end = getDecimal(6);
        fr.setStart(start, true).setEnd(end, true);
        doTableScan(index.createIndexKey(), fr, Direction.FORWARD, 10, true);
    }

    private void doTableScan(RecordValue key, FieldRange range,
                             Direction direction, int expNumRows,
                             boolean isComplexTable) {

        TableIteratorOptions tio =
            new TableIteratorOptions(direction, null, 0, null);
        MultiRowOptions mro =
            (range == null) ? null : range.createMultiRowOptions();

        TableIterator<Row> itr = null;
        Table table = null;
        if (key.isIndexKey()) {
            itr = tableImpl.tableIterator(key.asIndexKey(), mro, tio);
            table = key.asIndexKey().getIndex().getTable();
        } else {
            itr = tableImpl.tableIterator(key.asPrimaryKey(), mro, tio);
            table = key.asPrimaryKey().getTable();
        }
        RecordValue prev = null;
        int cnt = 0;

        while(itr.hasNext()) {
            Row row = itr.next();

            int id = row.get("id").asInteger().get();
            Row expect = isComplexTable ? createComplexRow(table, id) :
                                          createRow(table, id);
            assertTrue(row.equals(expect));

            if (!key.isEmpty() || range != null) {
                checkRow(row, key, range);
            }

            if (direction != Direction.UNORDERED && !isComplexTable) {
                RecordValue[] keys = createKey(key, row);
                if (prev != null) {
                    checkOrdering(prev, keys[0], direction);
                }
                prev = keys[0].clone();
            }
            cnt++;
        }
        if (expNumRows > 0) {
            assertTrue(cnt == expNumRows);
        }
        itr.close();
    }

    private void checkRow(Row row, RecordValue key, FieldRange range) {
        RecordValue[] keysFromRow = createKey(key, row);
        if (key != null) {
            for (String name : key.getFieldNames()) {
                FieldValue fv = key.get(name);
                if (fv != null) {
                    checkIndexField(keysFromRow, name, fv,
                        new ValueComparator() {
                            @Override
                            public boolean compare(RecordValue rKey,
                                                   String field,
                                                   FieldValue value) {
                                return rKey.get(field).equals(value);
                            }
                    });
                }
            }
        }

        if (range != null) {
            String fname = range.getFieldName();
            FieldValue start = range.getStart();
            if (start != null) {
                if (range.getStartInclusive()) {
                    checkIndexField(keysFromRow, fname, start,
                        new ValueComparator() {
                            @Override
                            public boolean compare(RecordValue rKey,
                                                   String field,
                                                   FieldValue value) {
                                return rKey.get(field).compareTo(value) >= 0;
                            }
                    });
                } else {
                    checkIndexField(keysFromRow, fname, start,
                        new ValueComparator() {
                            @Override
                            public boolean compare(RecordValue rKey,
                                                   String field,
                                                   FieldValue value) {
                                return rKey.get(field).compareTo(value) > 0;
                            }
                    });
                }
            }

            FieldValue end = range.getEnd();
            if (end != null) {
                if (range.getEndInclusive()) {
                    checkIndexField(keysFromRow, fname, end,
                        new ValueComparator() {
                            @Override
                            public boolean compare(RecordValue rKey,
                                                   String field,
                                                   FieldValue value) {
                                return rKey.get(field).compareTo(value) <= 0;
                            }
                    });
                } else {
                    checkIndexField(keysFromRow, fname, end,
                        new ValueComparator() {
                            @Override
                            public boolean compare(RecordValue rKey,
                                                   String field,
                                                   FieldValue value) {
                                return rKey.get(field).compareTo(value) < 0;
                            }
                    });
                }
            }
        }
    }

    private interface ValueComparator {
        boolean compare(RecordValue key, String fieldName, FieldValue value);
    }

    private void checkIndexField(RecordValue[] keys,
                                 String fieldName,
                                 FieldValue value,
                                 ValueComparator comparator) {
        for (RecordValue key : keys) {
            if (comparator.compare(key, fieldName, value)) {
                return;
            }
        }
        fail("CheckIndexField failed");
    }

    private BigDecimal getDecimal(int index) {
        BigDecimal base = BigDecimal.ONE;
        long unscaledBValue = base.unscaledValue().longValue() + index;
        int scale = base.scale() - index;
        return new BigDecimal(BigInteger.valueOf(unscaledBValue), scale);
    }

    private void loadRows(Table table, int nRows) {
        for (int i = 0; i < nRows; i++) {
            Row row = createRow(table, i);
            Version v = tableImpl.put(row, null, null);
            assertTrue(v != null);
        }
    }

    private void loadRowsToComplexTable(Table table, int nRows) {
        for (int i = 0; i < nRows; i++) {
            Row row = createComplexRow(table, i);
            Version v = tableImpl.put(row, null, null);
            assertTrue(v != null);
        }
    }

    private void checkOrdering(RecordValue prev, RecordValue current,
                               Direction direction) {

        if (direction == Direction.FORWARD) {
            if (prev.isPrimaryKey()) {
                assertTrue("Expect prev=" + prev + " less than current=" +
                           current + " for FORWARD",
                           prev.compareTo(current) < 0);
            } else {
                assertTrue("Expect prev=" + prev + " less than or equal to" +
                           " current=" + current + " for FORWARD",
                           prev.compareTo(current) <= 0);
            }
        } else if (direction == Direction.REVERSE) {
            if (prev.isPrimaryKey()) {
                assertTrue(prev.compareTo(current) > 0);
            } else {
                assertTrue(prev.compareTo(current) >= 0);
            }
        }
    }

    private FieldRange[] createFieldRange(Table table,
                                          Index index,
                                          String fieldName,
                                          BigDecimal start,
                                          BigDecimal end,
                                          boolean[] inclusiveFlags) {

        List<FieldRange> frs = new ArrayList<FieldRange>();
        for (boolean isInclusive : inclusiveFlags) {
            FieldRange fr = (index != null) ?
                            index.createFieldRange(fieldName) :
                            table.createFieldRange(fieldName);
            fr.setStart(start, isInclusive);
            frs.add(fr);
        }
        for (boolean isInclusive : inclusiveFlags) {
            FieldRange fr = (index != null) ?
                            index.createFieldRange(fieldName) :
                            table.createFieldRange(fieldName);
            fr.setEnd(end, isInclusive);
            frs.add(fr);
        }
        for (boolean startInclusive : inclusiveFlags) {
            FieldRange fr = (index != null) ?
                            index.createFieldRange(fieldName) :
                            table.createFieldRange(fieldName);
            fr.setStart(start, startInclusive);
            for (boolean endInclusive : inclusiveFlags) {
                fr.setEnd(end, endInclusive);
                frs.add(fr);
            }
        }
        return frs.toArray(new FieldRange[frs.size()]);
    }

    private int getExpectRangeCount(FieldRange fr, int start,
                                    int end, int total) {
        int ret = total;
        if (fr.getStart() != null) {
            ret -= start;
            if (!fr.getStartInclusive()) {
                ret--;
            }
        }
        if (fr.getEnd() != null) {
            ret -= total - end;
            if (fr.getEndInclusive()) {
                ret++;
            }
        }
        return ret;
    }

    private Row createRow(Table table, int index) {
        Row row = table.createRow();
        row.put("id", index);
        row.putNumber("pk", getDecimal(index));
        BigDecimal v = randomBigDecimal(index);
        row.putNumber("dec", v);
        row.put("s", v.toString());
        return row;
    }

    private Row createComplexRow(Table table, int index) {
        Row row = table.createRow();
        row.put("id", index);

        ArrayValue av = row.putArray("arrayF");
        for (int i = 0; i < 3; i++) {
            int idx = (index % 10) * 3 + i;
            av.addNumber(getDecimal(idx));
        }

        MapValue mv = row.putMap("mapF");
        for (int i = 0; i < 3; i++) {
            int idx = (index % 20) * 3 + i;
            mv.putNumber("key" + i, getDecimal(idx));
        }

        BigDecimal bd = getDecimal(index % 50);
        row.putRecord("recordF").putNumber("dec1", bd).putNumber("dec2", bd);
        return row;
    }

    /* Ignore warnings about Index.createIndexKey */
    @SuppressWarnings("deprecation")
    private RecordValue[] createKey(RecordValue key, Row row) {
        if (key.isIndexKey()) {
            IndexImpl index = (IndexImpl)key.asIndexKey().getIndex();
            if (index.isMultiKey()) {
                List<byte[]> list = index.extractIndexKeys((RowImpl)row, 100);
                RecordValue[] keys = new RecordValue[list.size()];
                int i = 0;
                for (byte[] bytes: list) {
                    keys[i++] = index.deserializeIndexKey(bytes, false);
                }
                return keys;
            }
            return new RecordValue[]{index.createIndexKey(row)};
        }
        return new RecordValue[]{row.createPrimaryKey()};
    }

    private Table createTable()
        throws Exception {

        final String tableName = "decimalTest";
        TableImpl table = getTable(tableName);
        if (table != null) {
            deleteAllRows(table);
            return table;
        }

        table = TableBuilder.createTableBuilder(tableName)
            .addInteger("id")
            .addNumber("pk")
            .addNumber("dec")
            .addString("s")
            .shardKey("pk")
            .primaryKey("pk", "id")
            .buildTable();

        addTable(table, true);

        addIndex(table, "idx_dec", new String[] {"dec"}, true);
        addIndex(table, "idx_dec_s", new String[] {"dec", "s"}, true);

        table = getTable(tableName);
        assertTrue(table != null);
        return table;
    }

    private Table createComplexTable() throws Exception {

        final String tableName = "decimalComplex";
        TableImpl table = getTable(tableName);
        if (table != null) {
            deleteAllRows(table);
            return table;
        }

        table = TableBuilder.createTableBuilder(tableName)
            .addInteger("id")
            .addField("arrayF", TableBuilder.createArrayBuilder()
                               .addNumber().build())
            .addField("mapF", TableBuilder.createMapBuilder()
                             .addNumber().build())
            .addField("recordF", TableBuilder.createRecordBuilder("recordF")
                                 .addNumber("dec1")
                                 .addNumber("dec2").build())
            .primaryKey("id")
            .buildTable();

        addTable(table, true);
        addIndex(table, "idxArray", new String[]{"arrayF[]"}, true);
        addIndex(table, "idxMapValue", new String[]{"mapF.values()"}, true);
        addIndex(table, "idxRecordDec1Dec2",
                 new String[]{"recordF.dec1", "recordF.dec2"}, true);
        table = getTable(tableName);
        assertTrue(table != null);
        return table;
    }

    private void deleteAllRows(Table table) {
        TableIterator<PrimaryKey> itr =
            tableImpl.tableKeysIterator(table.createPrimaryKey(), null, null);
        while(itr.hasNext()) {
            PrimaryKey key = itr.next();
            tableImpl.delete(key, null, null);
        }
        itr.close();
    }

    /* Create a random BigDecimal value. */
    private BigDecimal randomBigDecimal(long index) {
        String s = index + "." + Math.abs(new Random(index).nextLong()) +
                   "E" + (int)(index & 0xFFF);
        return new BigDecimal(s);
    }

}
