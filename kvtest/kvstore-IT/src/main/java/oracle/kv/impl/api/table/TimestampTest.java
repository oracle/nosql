/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import oracle.kv.Direction;
import oracle.kv.Version;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValueFactory;
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
import oracle.kv.table.TimestampValue;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for Timestamp type.
 */
public class TimestampTest extends TableTestBase {
    final static Timestamp base =
        TimestampUtils.parseString("2016-07-06T16:24:44.987654321");

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        staticSetUp(true/*excludeTombstones*/);
    }

    @Test
    public void testBasic()
        throws Exception {

        Table table = createTable();
        Timestamp[] values = new Timestamp[] {
            new Timestamp(0),
            TimestampUtils.parseString("1970-01-01T00:00:00.000000001"),
            TimestampUtils.parseString("1969-12-31T23:59:59.987654321"),
            new Timestamp(System.currentTimeMillis()),
            new Timestamp(-1 * System.currentTimeMillis()),
            TimestampDefImpl.MIN_VALUE,
            TimestampDefImpl.MAX_VALUE,
        };

        /* Put op */
        Row row = table.createRow();
        int id = 0;
        for (Timestamp ts : values) {
            for (String field : table.getFields()) {
                if (field.equals("id")) {
                    row.put("id", id++);
                } else {
                    row.put(field, ts);
                }
            }
            Version ver = tableImpl.put(row, null, null);
            assertTrue(ver != null);
        }

        /* Get op */
        id = 0;
        for (Timestamp ts : values) {
            PrimaryKey pkey = table.createPrimaryKey();
            for (String field: table.getPrimaryKey()) {
                if (field.equals("id")) {
                    pkey.put(field, id);
                } else {
                    pkey.put(field, ts);
                }
            }
            row = tableImpl.get(pkey, null);
            assertTrue(row != null);
            for (String field: table.getFields()) {
                if (field.equals("id")) {
                    assertTrue(row.get("id").asInteger().get() == id);
                } else {
                    FieldDef def = table.getField(field);
                    assertTrue(def.isTimestamp());
                    Timestamp exp = TimestampUtils.roundToPrecision
                                    (ts, def.asTimestamp().getPrecision());
                    assertTrue(row.get(field).asTimestamp().get().equals(exp));
                }
            }
            id++;
        }

        id = 0;
        /* Delete op */
        for (Timestamp ts : values) {
            PrimaryKey pkey = table.createPrimaryKey();
            for (String field: table.getPrimaryKey()) {
                if (field.equals("id")) {
                    pkey.put(field, id);
                } else {
                    pkey.put(field, ts);
                }
            }
            boolean deleted = tableImpl.delete(pkey, null, null);
            assertTrue(deleted);
            id++;
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
        for (int p = 0; p < TimestampDefImpl.getMaxPrecision(); p++) {
            Table table = createSimpleTable(p);
            loadRows(table, nRows, base);
            Timestamp tsS = getTimestamp(p, base, start);
            Timestamp tsE = getTimestamp(p, base, end);
            PrimaryKey key = table.createPrimaryKey();
            for (Direction direction : directions) {
                /* Full scan */
                doTableScan(key, null, direction, base, nRows, false);

                /* Scan with range */
                FieldRange[] frs =
                    createFieldRange(table, null, table.getPrimaryKey().get(0),
                                     tsS, tsE, inclusiveFlags);
                for (FieldRange fr : frs) {
                    int expCnt = getExpectRangeCount(fr, start, end, nRows);
                    doTableScan(key, fr, direction, base, expCnt, false);
                }
            }

            key = createRow(table, 10, base).createPrimaryKey();
            doTableScan(key, null, Direction.UNORDERED, base, 1, false);
        }
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
        for (int p = 0; p < TimestampDefImpl.getMaxPrecision(); p++) {
            Table table = createSimpleTable(p);
            loadRows(table, nRows, base);
            Timestamp tsS = getTimestamp(p, base, start);
            Timestamp tsE = getTimestamp(p, base, end);
            for (Direction direction : directions) {
                for (Entry<String, Index> e : table.getIndexes().entrySet()) {
                    /* Full scan */
                    Index index = e.getValue();
                    IndexKey key = index.createIndexKey();
                    doTableScan(index.createIndexKey(), null, direction,
                                base, nRows, false);

                    /* Scan with range */
                    FieldRange[] frs =
                        createFieldRange(table, index, index.getFields().get(0),
                                         tsS, tsE, inclusiveFlags);
                    for (FieldRange fr : frs) {
                        int expCnt = getExpectRangeCount(fr, start, end, nRows);
                        doTableScan(key, fr, direction, base, expCnt, false);
                    }

                    key = index.createIndexKey(createRow(table, 10, base));
                    doTableScan(key, null, Direction.UNORDERED, base, 1, false);
                }
            }
        }
    }

    @Test
    public void testNestedTimestamp() throws Exception {

        final Table table = createComplexTable();
        final int nRows = 100;

        loadRowsToComplexTable(table, nRows, base);

        /* Full table scan */
        doTableScan(table.createPrimaryKey(), null, Direction.UNORDERED,
                    base, 100, true);

        /* Index scan using idxArray index */
        Index index = table.getIndex("idxArray");
        FieldDef eDef = table.getField("arrayF").asArray().getElement();
        assertTrue(eDef.isTimestamp());
        Timestamp ts = getTimestamp(eDef.asTimestamp().getPrecision(), base, 1);
        IndexKey ikey = index.createIndexKey();
        ikey.put("arrayF[]", ts);
        doTableScan(ikey, null, Direction.FORWARD, base, 10, true);

        /* Index scan using idxMapValue index */
        index = table.getIndex("idxMapValue");
        eDef = table.getField("mapF").asMap().getElement();
        assertTrue(eDef.isTimestamp());
        ts = getTimestamp(eDef.asTimestamp().getPrecision(), base, 1);
        ikey = index.createIndexKey();
        ikey.put("mapF.values()", ts);
        doTableScan(ikey, null, Direction.FORWARD, base, 5, true);

        /* Index scan using idxRecordTs0Ts9 index */
        index = table.getIndex("idxRecordTs0Ts9");
        RecordDef def = ((IndexImpl)index).getIndexKeyDef();
        ikey = index.createIndexKey();
        for (String field : def.getFieldNames()) {
            eDef = def.getFieldDef(field);
            assertTrue(eDef.isTimestamp());
            ts = getTimestamp(eDef.asTimestamp().getPrecision(), base, 1);
            ikey.put(field, ts);
        }
        doTableScan(ikey, null, Direction.FORWARD, base, 2, true);

        /* Range second on indexRecordTs0Ts9 */
        FieldRange fr = index.createFieldRange("recordF.ts0");
        eDef = def.getFieldDef("recordF.ts0");
        Timestamp start = getTimestamp(eDef.asTimestamp().getPrecision(),
                                       base, 2);
        Timestamp end = getTimestamp(eDef.asTimestamp().getPrecision(),
                                     base, 6);
        fr.setStart(start, true).setEnd(end, true);
        doTableScan(index.createIndexKey(), fr, Direction.FORWARD, base,
                    10, true);
    }

    /* Test FieldValueFactory.createTimestamp() methods */
    @Test
    public void testCreateTimestampValue() {
        Timestamp[] values = new Timestamp[] {
            new Timestamp(0),
            TimestampUtils.parseString("1970-01-01T00:00:00.123456789"),
            TimestampUtils.parseString("-1969-12-31T23:59:59.987654321"),
            new Timestamp(System.currentTimeMillis()),
            new Timestamp(-1 * System.currentTimeMillis()),
            TimestampDefImpl.MIN_VALUE,
            TimestampDefImpl.MAX_VALUE,
        };

        for (Timestamp ts : values) {
            byte[] bytes = TimestampUtils.toBytes(ts, 9);
            int year = TimestampUtils.getYear(bytes);
            int month = TimestampUtils.getMonth(bytes);
            int day = TimestampUtils.getDay(bytes);
            int hour = TimestampUtils.getHour(bytes);
            int minute = TimestampUtils.getMinute(bytes);
            int second = TimestampUtils.getSecond(bytes);
            int nano = TimestampUtils.getNano(bytes, 9);
            for (int p = 0; p <= 9; p++) {
                TimestampValue ts1 = FieldValueFactory.createTimestamp(ts, p);
                assertTrue(ts1.get().compareTo
                           (TimestampUtils.roundToPrecision(ts, p)) == 0);

                String str = TimestampUtils.formatString(ts);
                TimestampValue ts2 = FieldValueFactory.createTimestamp(str, p);
                assertTrue(ts1.equals(ts2));

                int fracSecs = TimestampUtils.fracSecondToPrecision(nano, p);
                TimestampValue ts3 =
                    FieldValueFactory.createTimestamp(year, month, day, hour,
                                                      minute, second, fracSecs,
                                                      p);
                assertTrue(ts3.getYear() == year);
                assertTrue(ts3.getMonth() == month);
                assertTrue(ts3.getDay() == day);
                assertTrue(ts3.getHour() == hour);
                assertTrue(ts3.getMinute() == minute);
                assertTrue(ts3.getSecond() == second);
                assertTrue(ts3.getNano() ==
                           nano - (nano % (int)(Math.pow(10, 9 - p))));
            }
        }

        /*
         * Test on fracSeconds argument of FieldValueFactory.createTimestamp(..)
         */
        int year = 2016;
        int month = 10;
        int day = 17;
        int hour = 13;
        int minute = 37;
        int second = 40;
        for (int p = 0; p <= 9; p++) {
            TimestampValue tsv =
                FieldValueFactory.createTimestamp(year, month, day,
                                                  hour, minute, second,
                                                  0, p);
            assertTrue(tsv.getYear() == year);
            assertTrue(tsv.getMonth() == month);
            assertTrue(tsv.getDay() == day);
            assertTrue(tsv.getHour() == hour);
            assertTrue(tsv.getMinute() == minute);
            assertTrue(tsv.getSecond() == second);
            assertTrue(tsv.getNano() == 0);
        }

        for (int p = 1; p <= 9; p++) {
            TimestampValue tsv =
                FieldValueFactory.createTimestamp(year, month, day,
                                                  hour, minute, second,
                                                  1, p);
            assertTrue(tsv.getYear() == year);
            assertTrue(tsv.getMonth() == month);
            assertTrue(tsv.getDay() == day);
            assertTrue(tsv.getHour() == hour);
            assertTrue(tsv.getMinute() == minute);
            assertTrue(tsv.getSecond() == second);
            assertTrue(tsv.getNano() == (int)Math.pow(10, 9 - p));
        }

        int fracSecs = 9;
        for (int p = 1; p <= 9; p++) {
            TimestampValue tsv =
                FieldValueFactory.createTimestamp(year, month, day,
                                                  hour, minute, second,
                                                  fracSecs, p);
            assertTrue(tsv.getYear() == year);
            assertTrue(tsv.getMonth() == month);
            assertTrue(tsv.getDay() == day);
            assertTrue(tsv.getHour() == hour);
            assertTrue(tsv.getMinute() == minute);
            assertTrue(tsv.getSecond() == second);
            assertTrue(tsv.getNano() == (fracSecs * (int)Math.pow(10, 9 - p)));
            fracSecs = fracSecs * 10 + 9;
        }

        int nanos = 123456789;
        for (int p = 0; p <= 9; p++) {
            for (int i = 0; i <= p; i++) {
                int fsecs = (i == 0)? 0 : (nanos / (int)Math.pow(10, 9 - i));
                int expNanos = (i == 0)? 0 : (fsecs * (int)Math.pow(10, 9 - p));
                TimestampValue tsv =
                    FieldValueFactory.createTimestamp(year, month, day,
                                                      hour, minute, second,
                                                      fsecs, p);
                assertTrue(tsv.getYear() == year);
                assertTrue(tsv.getMonth() == month);
                assertTrue(tsv.getDay() == day);
                assertTrue(tsv.getHour() == hour);
                assertTrue(tsv.getMinute() == minute);
                assertTrue(tsv.getSecond() == second);
                assertTrue(tsv.getNano() == expNanos);
            }
        }

        /* Invalid arguments */
        Timestamp ts = new Timestamp(0);
        testCreateTimstampInvalidArgument(ts, 10);
        testCreateTimstampInvalidArgument(ts, -1);
        ts = new Timestamp(TimestampDefImpl.MAX_VALUE.getTime() + 1);
        testCreateTimstampInvalidArgument(ts, 9);
        ts = new Timestamp(TimestampDefImpl.MIN_VALUE.getTime() - 1);
        testCreateTimstampInvalidArgument(ts, 9);

        testCreateTimstampInvalidArgument("2016-10-09", 10);
        testCreateTimstampInvalidArgument("2016-10-09", -1);
        testCreateTimstampInvalidArgument("10000-01-01", 0);
        testCreateTimstampInvalidArgument("-6384-12-31T23:59:59.999999999", 9);

        testCreateTimstampInvalidArgument(2016, 10, 9, 12, 32, 55, 123, 10);
        testCreateTimstampInvalidArgument(2016, 10, 9, 12, 32, 55, 123, -1);
        testCreateTimstampInvalidArgument(-6384, 12, 31, 23, 59, 59,
                                          999999999, 9);
        testCreateTimstampInvalidArgument(10000, 1, 1, 0, 0, 0, 0, 9);
        testCreateTimstampInvalidArgument(2016, 0, 9, 12, 32, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 13, 9, 12, 32, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 0, 12, 32, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 32, 12, 32, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, -1, 32, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 24, 32, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, -1, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 60, 55, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, -1, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 60, 0, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 1, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 0);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 1);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 10, 1);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 2);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 100, 2);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 3);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 1000, 3);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 4);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 10000, 4);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 5);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 100000, 5);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 6);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 1000000, 6);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 7);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 10000000, 7);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 8);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 100000000, 8);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, -1, 9);
        testCreateTimstampInvalidArgument(2016, 1, 1, 1, 1, 1, 1000000000, 9);
    }

    private void testCreateTimstampInvalidArgument(Timestamp ts, int precision){
        try {
            FieldValueFactory.createTimestamp(ts, precision);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    private void testCreateTimstampInvalidArgument(String str, int precision) {
        try {
            FieldValueFactory.createTimestamp(str, precision);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    private void testCreateTimstampInvalidArgument(int year, int month, int day,
                                                   int hour, int minute,
                                                   int second, int fracSeconds,
                                                   int precision) {
        try {
            FieldValueFactory.createTimestamp(year, month, day, hour, minute,
                                              second, fracSeconds, precision);
            fail("Expected to catch IllegalArgumentException but not");
        } catch (IllegalArgumentException iae) {
        }
    }

    private FieldRange[] createFieldRange(Table table,
                                          Index index,
                                          String fieldName,
                                          Timestamp start, Timestamp end,
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

    private void doTableScan(RecordValue searchKey,
                             FieldRange range,
                             Direction direction,
                             Timestamp baseVal,
                             int expCount,
                             boolean isComplexTable) {

        TableIteratorOptions tio =
            new TableIteratorOptions(direction, null, 0, null);
        MultiRowOptions mro =
            (range == null) ? null : range.createMultiRowOptions();

        TableIterator<Row> itr = null;
        Table table = null;
        if (searchKey.isIndexKey()) {
            itr = tableImpl.tableIterator(searchKey.asIndexKey(), mro, tio);
            table = searchKey.asIndexKey().getIndex().getTable();
        } else {
            itr = tableImpl.tableIterator(searchKey.asPrimaryKey(), mro, tio);
            table = searchKey.asPrimaryKey().getTable();
        }
        RecordValue prev = null;
        int cnt = 0;
        while(itr.hasNext()) {
            Row row = itr.next();
            int id = row.get("id").asInteger().get();
            Row expect = isComplexTable ? createComplexRow(table, id, baseVal) :
                                          createRow(table, id, baseVal);
            assertTrue(row.equals(expect));
            if (direction != Direction.UNORDERED && !isComplexTable) {
                RecordValue current = createKey(searchKey, row);
                if (prev != null) {
                    checkOrdering(prev, current, direction);
                }
                prev = current.clone();
            }
            cnt++;
        }

        assertTrue(cnt == expCount);
        itr.close();
    }

    @SuppressWarnings("deprecation")
    private RecordValue createKey(RecordValue searchKey, Row row) {
        if (searchKey.isIndexKey()) {
            return searchKey.asIndexKey().getIndex().createIndexKey(row);
        }
        return row.createPrimaryKey();
    }

    private void checkOrdering(RecordValue prev, RecordValue current,
                               Direction direction) {

        if (direction == Direction.FORWARD) {
            if (prev.isPrimaryKey()) {
                assertTrue(prev.compareTo(current) < 0);
            } else {
                assertTrue(prev.compareTo(current) <= 0);
            }
        } else if (direction == Direction.REVERSE) {
            if (prev.isPrimaryKey()) {
                assertTrue(prev.compareTo(current) > 0);
            } else {
                assertTrue(prev.compareTo(current) >= 0);
            }
        }
    }

    private Table createSimpleTable(int precision)
            throws Exception {

        final String tableName = "timestampTest_" + precision;

        TableImpl table = getTable(tableName);
        if (table != null) {
            deleteAllRows(table);
            return table;
        }

        table = TableBuilder.createTableBuilder(tableName)
            .addInteger("id")
            .addTimestamp("tsk", precision)
            .addTimestamp("ts", precision)
            .primaryKey("tsk", "id")
            .buildTable();

        addTable(table, true);
        addIndex(table, "idx0", new String[]{"ts"}, true);
        table = getTable(tableName);
        assertTrue(table != null);
        return table;
    }

    private Table createTable()
        throws Exception {

        final String tableName = "timestampTest";
        TableImpl table = getTable(tableName);
        if (table != null) {
            deleteAllRows(table);
            return table;
        }

        table = TableBuilder.createTableBuilder(tableName)
            .addInteger("id")
            .addTimestamp("ts0", 0)
            .addTimestamp("ts1", 1)
            .addTimestamp("ts2", 2)
            .addTimestamp("ts3", 3)
            .addTimestamp("ts4", 4)
            .addTimestamp("ts5", 5)
            .addTimestamp("ts6", 6)
            .addTimestamp("ts7", 7)
            .addTimestamp("ts8", 8)
            .addTimestamp("ts9", 9)
            .shardKey("ts0")
            .primaryKey("ts0", "ts3", "id")
            .buildTable();

       addTable(table, true);
       table = getTable(tableName);
       assertTrue(table != null);
       return table;
    }

    private Table createComplexTable()
            throws Exception {

        final String tableName = "timestampComplex";
        TableImpl table = getTable(tableName);
        if (table != null) {
            deleteAllRows(table);
            return table;
        }

        table = TableBuilder.createTableBuilder(tableName)
            .addInteger("id")
            .addField("arrayF", TableBuilder.createArrayBuilder()
                               .addTimestamp(3).build())
            .addField("mapF", TableBuilder.createMapBuilder()
                             .addTimestamp(6).build())
            .addField("recordF", TableBuilder.createRecordBuilder("recordF")
                                 .addTimestamp("ts0", 0)
                                 .addTimestamp("ts4", 4)
                                 .addTimestamp("ts9", 9).build())
            .primaryKey("id")
            .buildTable();

        addTable(table, true);
        addIndex(table, "idxArray", new String[]{"arrayF[]"}, true);
        addIndex(table, "idxMapValue", new String[]{"mapF.values()"}, true);
        addIndex(table, "idxRecordTs0Ts9",
                 new String[]{"recordF.ts0", "recordF.ts9"}, true);
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

    private void loadRows(Table table, int nRows, Timestamp baseVal) {
        for (int i = 0; i < nRows; i++) {
            Row row = createRow(table, i, baseVal);
            assertTrue(tableImpl.put(row, null, null) != null);
        }
    }

    private Row createRow(Table table, int id, Timestamp baseVal) {
        final Row row = table.createRow();
        for (String fieldName : table.getFields()) {
            if (fieldName.equals("id")) {
                row.put("id", id);
                continue;
            }
            FieldDefImpl def = (FieldDefImpl)table.getField(fieldName);
            assertTrue(def.isTimestamp());
            Timestamp ts = getTimestamp(def.asTimestamp().getPrecision(),
                                        baseVal, id);
            row.put(fieldName, ts);
        }
        return row;
    }

    private void loadRowsToComplexTable(Table table, int nRows,
                                        Timestamp baseVal) {
        for (int i = 0; i < nRows; i++) {
            Row row = createComplexRow(table, i, baseVal);
            assertTrue(tableImpl.put(row, null, null) != null);
        }
    }

    private Row createComplexRow(Table table, int id, Timestamp baseVal) {
        final Row row = table.createRow();
        for (String fieldName : table.getFields()) {
            if (fieldName.equals("id")) {
                row.put("id", id);
                continue;
            }
            FieldDefImpl def = (FieldDefImpl)table.getField(fieldName);
            switch (def.getType()) {
                case ARRAY: {
                    ArrayValue av = row.putArray(fieldName);
                    FieldDef fdef = av.getDefinition().getElement();
                    assertTrue(fdef.isTimestamp());
                    for (int i = 0; i < 3; i++) {
                        Timestamp ts =
                            getTimestamp(fdef.asTimestamp().getPrecision(),
                                         baseVal, (id % 10) * 3 + i);
                        av.add(ts);
                    }
                    break;
                }
                case MAP:{
                    MapValue mv = row.putMap(fieldName);
                    FieldDef fdef = mv.getDefinition().getElement();
                    assertTrue(fdef.isTimestamp());
                    for (int i = 0; i < 3; i++) {
                        Timestamp ts =
                            getTimestamp(fdef.asTimestamp().getPrecision(),
                                         baseVal, (id % 20) * 3 + i);
                        mv.put("key" + i, ts);
                    }
                    break;
                }
                case RECORD: {
                    RecordValue rv = row.putRecord(fieldName);
                    RecordDef rdef = rv.getDefinition();
                    for (String field : rdef.getFieldNames()) {
                        FieldDef fdef = rdef.getFieldDef(field);
                        Timestamp ts =
                            getTimestamp(fdef.asTimestamp().getPrecision(),
                                         baseVal, id % 50);
                        rv.put(field, ts);
                    }
                    break;
                }
                default:
            }
        }
        return row;
    }

    private Timestamp getTimestamp(int precision, Timestamp baseVal, int id) {
        if (precision == 0) {
            return TimestampUtils.plusMillis(baseVal, id * 1000);
        }
        return TimestampUtils.plusNanos
                (baseVal, (long)id * getUnitNanos(precision));
    }

    private int getUnitNanos(int precision) {
        return (int)Math.pow(10, 9 - precision);
    }
}
