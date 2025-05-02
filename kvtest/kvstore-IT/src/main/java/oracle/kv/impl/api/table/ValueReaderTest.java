/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Stack;

import oracle.kv.Version;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.MapValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test suite to use external ValueReader in get/put to deserialze
 * Row/ReturnRow from bytes.
 */
public class ValueReaderTest extends TableTestBase {

    final String createTableDdl =
        "CREATE TABLE IF NOT EXISTS users(" +
            "id INTEGER, " +
            "name STRING, " +
            "age INTEGER, " +
            "addresses MAP(RECORD(city STRING, street STRING)), " +
            "phones ARRAY(STRING), " +
            "primary key(id))";

    private TableImpl usersTable;
    private TestValueReader valReader;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        TableTestBase.staticSetUp(true /* excludeTombstone */);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        executeDdl(createTableDdl);
        usersTable = getTable("users");
        loadRows(10);

        valReader = new TestValueReader(usersTable);
    }

    @Test
    public void testGet() {
        PrimaryKeyImpl pKey = getPrimaryKey(1);

        Row row = tableImpl.get(pKey, null);
        assertEquals(createRow(1), row);

        Result result = tableImpl.getInternal(pKey, null);
        valReader.reset();
        tableImpl.createRowFromGetResult(result, pKey, valReader);
        checkValue(row, valReader);
    }

    @Test
    public void testPut() {
        ReturnRow rr = null;
        Version oldVer, ver;
        Result result;

        /* ReturnRow: null */
        RowImpl oldRow = createRow(1);
        RowImpl newRow = oldRow.clone();
        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        ver = tableImpl.put(newRow, rr, null);
        assertNotNull(ver);

        oldRow = newRow.clone();
        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        result = tableImpl.putInternal(newRow, rr, null);
        ver = result.getNewVersion();
        assertNotNull(ver);
        tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
        assertNull(valReader.getValue());

        for (Choice choice : Choice.values()) {
            /* ReturnRow: ALL */
            oldRow = newRow.clone();
            oldVer = ver;
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            rr = usersTable.createReturnRow(choice);
            ver = tableImpl.put(newRow, rr, null);
            assertNotNull(ver);
            checkReturnRow(oldRow, oldVer, rr);

            oldRow = newRow.clone();
            oldVer = ver;
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            rr = usersTable.createReturnRow(choice);
            result = tableImpl.putInternal(newRow, rr, null);
            ver = result.getNewVersion();
            assertNotNull(ver);
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(oldRow, oldVer, valReader, rr.getReturnChoice());
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPutIfAbsent() {
        final RowImpl emptyRow = usersTable.createRow();
        ReturnRow rr = null;
        Version oldVer, ver;
        Result result;

        /* ReturnRow: null */
        deleteRow(100);
        RowImpl oldRow = createRow(100);
        RowImpl newRow = oldRow.clone();
        ver = tableImpl.putIfAbsent(newRow, rr, null);
        assertNotNull(ver);
        oldVer = ver;

        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        result = tableImpl.putIfAbsentInternal(newRow, rr, null);
        assertNull(result.getNewVersion());
        valReader.reset();
        tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
        assertNull(valReader.getValue());

        /* Key exists, putIfAbsent failed */
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            assertNull(tableImpl.putIfAbsent(newRow, rr, null));
            checkReturnRow(oldRow, oldVer, rr);

            rr = usersTable.createReturnRow(choice);
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            result = tableImpl.putIfAbsentInternal(newRow, rr, null);
            assertNull(result.getNewVersion());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(oldRow, oldVer, valReader, rr.getReturnChoice());
        }

        /* Key doesn't exist, putIfAbsent successfully */
        for (Choice choice : Choice.values()) {
            deleteRow(100);
            rr = usersTable.createReturnRow(choice);
            assertNotNull(tableImpl.putIfAbsent(newRow, rr, null));
            checkReturnRow(emptyRow, null, rr);

            deleteRow(100);
            rr = usersTable.createReturnRow(choice);
            result = tableImpl.putIfAbsentInternal(newRow, rr, null);
            assertNotNull(result.getNewVersion());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPutIfPresent() {
        final RowImpl emptyRow = usersTable.createRow();
        ReturnRow rr = null;
        Version oldVer, ver;
        Result result;

        /* ReturnRow: null */
        putRow(100);
        RowImpl oldRow = createRow(100);
        RowImpl newRow = oldRow.clone();
        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        ver = tableImpl.putIfPresent(newRow, rr, null);
        assertNotNull(ver);
        oldVer = ver;

        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        result = tableImpl.putIfPresentInternal(newRow, rr, null);
        ver = result.getNewVersion();
        assertNotNull(ver);
        valReader.reset();
        tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
        assertNull(valReader.getValue());

        /* Key exists, putIfPresent successfully */
        for (Choice choice : Choice.values()) {
            oldRow = newRow.clone();
            oldVer = ver;
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            rr = usersTable.createReturnRow(choice);
            ver = tableImpl.putIfPresent(newRow, rr, null);
            assertNotNull(ver);
            checkReturnRow(oldRow, oldVer, rr);

            oldRow = newRow.clone();
            oldVer = ver;
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            rr = usersTable.createReturnRow(choice);
            result = tableImpl.putIfPresentInternal(newRow, rr, null);
            ver = result.getNewVersion();
            assertNotNull(ver);
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(oldRow, oldVer, valReader, rr.getReturnChoice());
        }

        /* Key doesn't exist, putIfPresent failed */
        deleteRow(100);
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            assertNull(tableImpl.putIfPresent(newRow, rr, null));
            checkReturnRow(emptyRow, null, rr);

            rr = usersTable.createReturnRow(choice);
            result = tableImpl.putIfPresentInternal(newRow, rr, null);
            assertNull(result.getNewVersion());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPutIfVersion() {
        final RowImpl emptyRow = usersTable.createRow();
        ReturnRow rr = null;
        Version oldVer, ver, orgVer;
        Result result;

        /* ReturnRow: null */
        oldVer = putRow(100);
        orgVer = oldVer;
        RowImpl oldRow = createRow(100);
        RowImpl newRow = oldRow.clone();
        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        ver = tableImpl.putIfVersion(newRow, oldVer, rr, null);
        assertNotNull(ver);
        oldVer = ver;

        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        result = tableImpl.putIfVersionInternal(newRow, oldVer, rr, null);
        ver = result.getNewVersion();
        assertNotNull(ver);
        valReader.reset();
        tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
        assertNull(valReader.getValue());

        /* Key exists matching version, putIfVersion successfully */
        for (Choice choice : Choice.values()) {
            oldRow = newRow.clone();
            oldVer = ver;
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            rr = usersTable.createReturnRow(choice);
            ver = tableImpl.putIfVersion(newRow, oldVer, rr, null);
            assertNotNull(ver);
            checkReturnRow(emptyRow, null, rr);

            oldRow = newRow.clone();
            oldVer = ver;
            newRow.put("age", newRow.get("age").asInteger().get() + 1);
            rr = usersTable.createReturnRow(choice);
            result =
                tableImpl.putIfVersionInternal(newRow, oldVer, rr, null);
            ver = result.getNewVersion();
            assertNotNull(ver);
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }

        /* Key exists but not matching version, putIfVersion failed */
        oldRow = newRow.clone();
        oldVer = ver;
        newRow.put("age", newRow.get("age").asInteger().get() + 1);
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            ver = tableImpl.putIfVersion(newRow, orgVer, rr, null);
            assertNull(ver);
            checkReturnRow(oldRow, oldVer, rr);

            rr = usersTable.createReturnRow(choice);
            result =
                tableImpl.putIfVersionInternal(newRow, orgVer, rr, null);
            ver = result.getNewVersion();
            assertNull(ver);
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(oldRow, oldVer, valReader, rr.getReturnChoice());
        }

        /* Key not exists, putIfVersion failed */
        deleteRow(100);
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            ver = tableImpl.putIfVersion(newRow, oldVer, rr, null);
            assertNull(ver);
            checkReturnRow(emptyRow, null, rr);

            rr = usersTable.createReturnRow(choice);
            result =
                tableImpl.putIfVersionInternal(newRow, oldVer, rr, null);
            ver = result.getNewVersion();
            assertNull(ver);
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, newRow, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }
    }

    @Test
    public void testDelete() {
        final RowImpl emptyRow = usersTable.createRow();
        ReturnRow rr = null;
        Version oldVer;
        Result result;

        /* ReturnRow: null */
        RowImpl oldRow = createRow(100);
        oldVer = putRow(100);
        PrimaryKeyImpl key = getPrimaryKey(100);
        assertTrue(tableImpl.delete(key, rr, null));

        oldVer = putRow(100);
        result = tableImpl.deleteInternal(key, rr, null);
        assertTrue(result.getSuccess());
        valReader.reset();
        tableImpl.initReturnRowFromResult(rr, key, result, valReader);
        assertNull(valReader.getValue());

        /* Key exists, delete successfully */
        for (Choice choice : Choice.values()) {
            oldVer = putRow(100);
            rr = usersTable.createReturnRow(choice);
            assertTrue(tableImpl.delete(key, rr, null));
            checkReturnRow(oldRow, oldVer, rr);

            oldVer = putRow(100);
            rr = usersTable.createReturnRow(choice);
            result = tableImpl.deleteInternal(key, rr, null);
            assertTrue(result.getSuccess());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, key, result, valReader);
            checkValue(oldRow, oldVer, valReader, rr.getReturnChoice());
        }

        /* Key exists, delete successfully */
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            assertFalse(tableImpl.delete(key, rr, null));
            checkReturnRow(emptyRow, null, rr);

            rr = usersTable.createReturnRow(choice);
            result = tableImpl.deleteInternal(key, rr, null);
            assertFalse(result.getSuccess());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, key, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }
    }

    @Test
    public void testDeleteIfVersion() {
        final RowImpl emptyRow = usersTable.createRow();
        ReturnRow rr = null;
        Version oldVer, orgVer;
        Result result;

        /* ReturnRow: null */
        RowImpl oldRow = createRow(100);
        oldVer = putRow(100);
        orgVer = oldVer;
        PrimaryKeyImpl key = getPrimaryKey(100);
        assertTrue(tableImpl.deleteIfVersion(key, oldVer, rr, null));

        oldVer = putRow(100);
        result = tableImpl.deleteIfVersionInternal(key, oldVer, rr, null);
        assertTrue(result.getSuccess());
        valReader.reset();
        tableImpl.initReturnRowFromResult(rr, key, result, valReader);
        assertNull(valReader.getValue());

        /* Key exists and match ifVersion, deleteIfVersion successfully */
        for (Choice choice : Choice.values()) {
            oldVer = putRow(100);
            rr = usersTable.createReturnRow(choice);
            assertTrue(tableImpl.deleteIfVersion(key, oldVer, rr, null));
            checkReturnRow(emptyRow, null, rr);

            oldVer = putRow(100);
            rr = usersTable.createReturnRow(choice);
            result =
                tableImpl.deleteIfVersionInternal(key, oldVer, rr, null);
            assertTrue(result.getSuccess());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, key, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }

        /* Key exists but not matching ifVersion, deleteIfVersion failed */
        oldVer = putRow(100);
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            assertFalse(tableImpl.deleteIfVersion(key, orgVer, rr, null));
            checkReturnRow(oldRow, oldVer, rr);

            rr = usersTable.createReturnRow(choice);
            result =
                tableImpl.deleteIfVersionInternal(key, orgVer, rr, null);
            assertFalse(result.getSuccess());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, key, result, valReader);
            checkValue(oldRow, oldVer, valReader, rr.getReturnChoice());
        }

        /* Key not exists, deleteIfVersion failed */
        deleteRow(100);
        for (Choice choice : Choice.values()) {
            rr = usersTable.createReturnRow(choice);
            assertFalse(tableImpl.deleteIfVersion(key, oldVer, rr, null));
            checkReturnRow(emptyRow, null, rr);

            rr = usersTable.createReturnRow(choice);
            result =
                tableImpl.deleteIfVersionInternal(key, oldVer, rr, null);
            assertFalse(result.getSuccess());
            valReader.reset();
            tableImpl.initReturnRowFromResult(rr, key, result, valReader);
            checkValue(null, null, valReader, rr.getReturnChoice());
        }
    }

    private void checkReturnRow(Row expRow, Version expVer, ReturnRow rr) {
        switch(rr.getReturnChoice()) {
        case ALL:
            assertEquals(expRow, rr);
            assertEquals(expVer, rr.getVersion());
            break;
        case VALUE:
            assertEquals(expRow, rr);
            assertNull(rr.getVersion());
            break;
        case VERSION:
            assertTrue(rr.isEmpty());
            assertEquals(expVer, rr.getVersion());
            break;
        case NONE:
            assertTrue(rr.isEmpty());
            assertNull(rr.getVersion());
            break;
        }
    }

    private void checkValue(Row exp, TestValueReader reader) {
        checkValue(exp, exp.getVersion(), reader, Choice.ALL);
    }

    private void checkValue(Row expVal,
                            Version expVer,
                            TestValueReader reader,
                            Choice choice) {

        switch (choice) {
        case ALL:
            if (expVal == null) {
                assertNull(reader.getValue());
            } else {
                assertEquals(expVal,
                    expVal.getTable().createRowFromJson(reader.getValue(),
                                                        true));
            }
            assertEquals(expVer, reader.getVersion());
            break;
        case VALUE:
            if (expVal == null) {
                assertNull(reader.getValue());
            } else {
                assertEquals(expVal,
                    expVal.getTable().createRowFromJson(reader.getValue(),
                                                        true));
            }
            assertNull(reader.getVersion());
            break;
        case VERSION:
            assertNull(reader.getValue());
            assertEquals(expVer, reader.getVersion());
            break;
        case NONE:
            assertNull(reader.getValue());
            assertNull(reader.getVersion());
            break;
        }
    }

    private void deleteRow(int id) {
        tableImpl.delete(getPrimaryKey(id), null, null);
    }

    private Version putRow(int id) {
        Version ver = tableImpl.put(createRow(id), null, null);
        assertNotNull(ver);
        return ver;
    }

    private void loadRows(int num) {
        for (int i = 0; i < num; i++) {
            RowImpl row = createRow(i);
            tableImpl.put(row, null, null);
        }
    }

    private RowImpl createRow(int id) {
        RowImpl row = usersTable.createRow();
        row.put("id", id);
        row.put("name", "name" + id);
        row.put("age", id + 10);

        MapValueImpl addresses = row.putMap("addresses");
        RecordValueImpl address = addresses.putRecord("work");
        address.put("city", "work_city_" + id);
        address.put("street", "work_street_" + id);
        address = addresses.putRecord("homg");
        address.put("city", "home_city_" + id);
        address.put("street", "home_street_" + id);

        ArrayValueImpl phones = row.putArray("phones");
        for (int i = 0; i < 3; i++) {
            phones.add("phone_" + id + "_" + i);
        }
        return row;
    }

    private PrimaryKeyImpl getPrimaryKey(int id) {
        return (PrimaryKeyImpl)createRow(id).createPrimaryKey();
    }

    static class TestValueReader implements ValueReader<String> {
        private Table table;
        private int tableVersion;
        private Version version;
        private long expirationTime;
        private int regionId;
        private long modificationTime;

        private MapValue root;
        private Stack<FieldValue> nestedNodes;

        TestValueReader(Table table) {
            this.root = null;
            this.table = table;
        }

        @Override
        public void readInteger(String fieldName, int val) {
            writeValue(fieldName,
                       (FieldValueFactory.createInteger(val)));
        }

        @Override
        public void readLong(String fieldName, long val) {
            writeValue(fieldName,
                       (FieldValueFactory.createLong(val)));
        }

        @Override
        public void readFloat(String fieldName, float val) {
            writeValue(fieldName,
                       (FieldValueFactory.createFloat(val)));
        }

        @Override
        public void readDouble(String fieldName, double val) {
            writeValue(fieldName,
                       (FieldValueFactory.createDouble(val)));
        }

        @Override
        public void readNumber(String fieldName, byte[] bytes) {
            writeValue(fieldName,
                       FieldDefImpl.Constants.numberDef.createNumber(bytes));
        }

        @Override
        public void readTimestamp(String fieldName, FieldDef def, byte[] bytes) {
            writeValue(fieldName,
                       ((TimestampDefImpl)def).createTimestamp(bytes));
        }

        @Override
        public void readBinary(String fieldName, byte[] bytes) {
            writeValue(fieldName,
                       (FieldValueFactory.createBinary(bytes)));
        }

        @Override
        public void readFixedBinary(String fieldName, FieldDef def, byte[] bytes) {
            writeValue(fieldName,
                       ((FixedBinaryDefImpl)def).createFixedBinary(bytes));
        }

        @Override
        public void readString(String fieldName, String val) {
            writeValue(fieldName,
                       (FieldValueFactory.createString(val)));
        }

        @Override
        public void readBoolean(String fieldName, boolean val) {
            writeValue(fieldName,
                       (FieldValueFactory.createBoolean(val)));
        }

        @Override
        public void readEnum(String fieldName, FieldDef def, int index) {
            writeValue(fieldName, ((EnumDefImpl)def).createEnum(index));
        }

        @Override
        public void readNull(String fieldName) {
            writeValue(fieldName, NullValueImpl.getInstance());
        }

        @Override
        public void readJsonNull(String fieldName) {
            writeValue(fieldName, NullJsonValueImpl.getInstance());
        }

        @Override
        public void readEmpty(String fieldName) {
            writeValue(fieldName, EmptyValueImpl.getInstance());
        }

        @Override
        public void startRecord(String fieldName, FieldDef def, int size) {
            startMap(fieldName, def, size);
        }

        @Override
        public void endRecord(int size) {
            removeNestedNode();
        }

        @Override
        public void startMap(String fieldName, FieldDef def, int size) {
            MapValue map = FieldValueFactory.createMap();
            writeValue(fieldName, map);
            addNestedNode(map);
        }

        @Override
        public void endMap(int size) {
            removeNestedNode();
        }

        @Override
        public void startArray(String fieldName,
                               FieldDef def,
                               FieldDef elemDef,
                               int size) {
            ArrayValue arNode = FieldValueFactory.createArray();
            writeValue(fieldName, arNode);
            addNestedNode(arNode);
        }

        @Override
        public void endArray(int size) {
            removeNestedNode();
        }

        @Override
        public void setTableVersion(int tableVersion) {
            this.tableVersion = tableVersion;
        }

        public int getTableVersion() {
            return tableVersion;
        }

        @Override
        public void setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        @Override
        public void setVersion(Version version) {
            this.version = version;
        }

        @Override
        public void setRegionId(int regionId) {
            this.regionId = regionId;
        }

        public int getRegionId() {
            return regionId;
        }

        public Version getVersion() {
            return version;
        }

        @Override
        public String getValue() {
            if (root == null) {
                return null;
            }
            return ((FieldValueImpl) root).toJsonString(false);
        }

        @Override
        public Table getTable() {
            return table;
        }

        private void writeValue(String fieldName, FieldValue value) {
            FieldValue curNode;
            if (nestedNodes != null && !nestedNodes.isEmpty()) {
                curNode = nestedNodes.peek();
            } else {
                if (root == null) {
                    root = FieldValueFactory.createMap();
                }
                curNode = root;
            }
            if (curNode.isMap()) {
                curNode.asMap().put(fieldName, value);
            } else {
                assert(curNode.isArray());
                curNode.asArray().add(value);
            }
        }

        private void addNestedNode(FieldValue value) {
            if (nestedNodes == null) {
                nestedNodes = new Stack<FieldValue>();
            }
            nestedNodes.push(value);
        }

        private void removeNestedNode() {
            if (nestedNodes != null && !nestedNodes.isEmpty()) {
                nestedNodes.pop();
            }
        }

        @Override
        public void reset() {
            root = null;
            nestedNodes = null;
        }

        @Override
        public void setValue(String value) {
        }

        @Override
        public void setModificationTime(long modificationTime) {
            this.modificationTime = modificationTime;

        }

        public long getModificationTime() {
            return modificationTime;
        }

        @Override
        public void readCounterCRDT(String fieldName,
                                    FieldValueImpl val) {
            writeValue(fieldName, val);
        }
    }
}
