/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.query.shell;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import oracle.kv.Version;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.OutputMode;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.util.shell.ShellException;

/**
 * Unit tests for 4 different output modes.
 */
public class OutputModeTest extends ShellTestBase {
    private final static String NESTED_RECORD =
        "RECORD(rid INTEGER, " +
        "       rname STRING, " +
        "       rarray ARRAY(STRING), " +
        "       rmap MAP(LONG))";

    /**
     * A table contains fields with simple types.
     */
    @Test
    public void testOutputSimpleFields()
        throws Exception {

        final String TABLE_SIMPLE = "simples";
        final String CREATE_TABLE_SIMPLE =
            "CREATE TABLE IF NOT EXISTS " + TABLE_SIMPLE +
            "(" +
            "  id INTEGER, " +
            "  fint INTEGER, " +
            "  flong LONG, " +
            "  fstring STRING," +
            "  fdouble DOUBLE," +
            "  ffloat FLOAT," +
            "  fboolean BOOLEAN," +
            "  fenum ENUM(value1, value2, value3)," +
            "  fbinary BINARY," +
            "  ffixed BINARY(10)," +
            "  PRIMARY KEY(id)" +
            ")";
        final int numRows = 105;
        final String query = "select * from " + TABLE_SIMPLE;

        Table table = createTable(CREATE_TABLE_SIMPLE, TABLE_SIMPLE);
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        for (OutputMode mode : OutputMode.values()) {
            doExecuteQuery(onqlShell, query, mode,  true, numRows);
        }
    }

    /**
     * A table contains ARRAY fields that contains simple fields.
     */
    @Test
    public void testOutputArrayOfSimples()
        throws Exception {

        final String TABLE_ARRAY = "tarray_simples";
        final String CREATE_TABLE_ARRAY =
            "CREATE TABLE IF NOT EXISTS " + TABLE_ARRAY +
            "(" +
            "  id INTEGER, " +
            "  strings ARRAY(STRING), " +
            "  ints ARRAY(INTEGER), " +
            "  bins ARRAY(BINARY(20)), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_ARRAY, TABLE_ARRAY);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_ARRAY;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains an ARRAY field that contains nested ARRAY(STRING) field.
     */
    @Test
    public void testOutputArrayOfArray()
        throws Exception {

        final String TABLE_ARRAY = "tarray_array";
        final String CREATE_TABLE_ARRAY =
            "CREATE TABLE IF NOT EXISTS " + TABLE_ARRAY +
            "(" +
            "  id INTEGER, " +
            "  farray ARRAY(ARRAY(STRING)), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_ARRAY, TABLE_ARRAY);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_ARRAY;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains an ARRAY field that contains nested MAP(STRING) field.
     */
    @Test
    public void testOutputArrayOfMap()
        throws Exception {

        final String TABLE_ARRAY = "tarray_map";
        final String CREATE_TABLE_ARRAY =
            "CREATE TABLE IF NOT EXISTS " + TABLE_ARRAY +
            "(" +
            "  id INTEGER, " +
            "  farray ARRAY(MAP(STRING)), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_ARRAY, TABLE_ARRAY);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_ARRAY;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains an ARRAY field that contains nested RECORD field.
     */
    @Test
    public void testOutputArrayOfRecord()
        throws Exception {

        final String TABLE_ARRAY = "tarray_record";
        final String CREATE_TABLE_ARRAY =
            "CREATE TABLE IF NOT EXISTS " + TABLE_ARRAY +
            "(" +
            "  id INTEGER, " +
            "  farray ARRAY(" + NESTED_RECORD + "), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_ARRAY, TABLE_ARRAY);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_ARRAY;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains MAP fields that contains simple fields.
     */
    @Test
    public void testOutputMapOfSimples()
        throws Exception {

        final String TABLE_MAP = "tmap_simples";
        final String CREATE_TABLE_MAP =
            "CREATE TABLE IF NOT EXISTS " + TABLE_MAP +
            "(" +
            "  id INTEGER, " +
            "  fmap_str MAP(STRING), " +
            "  fmap_ints MAP(INTEGER), " +
            "  fmap_bins MAP(BINARY), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_MAP, TABLE_MAP);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_MAP;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains a MAP field that contains nested ARRAY(STRING) field.
     */
    @Test
    public void testOutputMapOfArray()
        throws Exception {

        final String TABLE_MAP = "tmap_array";
        final String CREATE_TABLE_MAP =
            "CREATE TABLE IF NOT EXISTS " + TABLE_MAP +
            "(" +
            "  id INTEGER, " +
            "  fmap MAP(ARRAY(STRING)), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_MAP, TABLE_MAP);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_MAP;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains a MAP field that contains nested MAP(STRING) field.
     */
    @Test
    public void testOutputMapOfMap()
        throws Exception {

        final String TABLE_MAP = "tmap_map";
        final String CREATE_TABLE_MAP =
            "CREATE TABLE IF NOT EXISTS " + TABLE_MAP +
            "(" +
            "  id INTEGER, " +
            "  fmap MAP(MAP(STRING)), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_MAP, TABLE_MAP);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_MAP;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains a MAP field that contains nested RECORD field.
     */
    @Test
    public void testOutputMapOfRecord()
        throws Exception {

        final String TABLE_MAP = "tmap_record";
        final String CREATE_TABLE_MAP =
            "CREATE TABLE IF NOT EXISTS " + TABLE_MAP +
            "(" +
            "  id INTEGER, " +
            "  fmap MAP(" + NESTED_RECORD + "), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_MAP, TABLE_MAP);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_MAP;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains a RECORD field that contains simple fields.
     */
    @Test
    public void testOutputRecordOfSimples()
        throws Exception {

        final String TABLE_RECORD = "trec_simples";
        final String CREATE_TABLE_RECORD =
            "CREATE TABLE IF NOT EXISTS " + TABLE_RECORD +
            "(" +
            "  id INTEGER, " +
            "  rec RECORD(rid LONG, rstr STRING, " +
            "             rbinary BINARY(10), " +
            "             renum ENUM(rvalue1, rvalue2, rvalue3)), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_RECORD, TABLE_RECORD);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_RECORD;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * A table contains a RECORD field that contains nested ARRAY, MAP and
     * RECORD fields.
     */
    @Test
    public void testOutputRecordOfComplexes()
        throws Exception {

        final String TABLE_RECORD = "trec_complexes";
        final String CREATE_TABLE_RECORD =
            "CREATE TABLE IF NOT EXISTS " + TABLE_RECORD +
            "(" +
            "  id INTEGER, " +
            "  rec RECORD(frid INTEGER, " +
            "             frarray ARRAY(STRING), " +
            "             frmap MAP(STRING), " +
            "             frrec " + NESTED_RECORD + "), " +
            "  PRIMARY KEY(id) " +
            ")";

        Table table = createTable(CREATE_TABLE_RECORD, TABLE_RECORD);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_RECORD;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    /**
     * The test case of SR #24978
     */
    @Test
    public void testOutputMiscTypes()
        throws Exception {

        final String TABLE_MISC = "tmisc";
        final String CREATE_TABLE_MISC =
            "CREATE TABLE IF NOT EXISTS " + TABLE_MISC +
            "(" +
            "  id integer, " +
            "  firstname string, " +
            "  lastname string, " +
            "  age integer, " +
            "  income integer, " +
            "  address record(street string, " +
            "                 city string, " +
            "                 state string, " +
            "                 phone array(record(type enum(work, home), " +
            "                                    areacode integer, " +
            "                                    number integer " +
            "                             ) " +
            "                 ) " +
            "  ), " +
            "  connections array(integer), " +
            "  properties map(string), " +
            "  primary key (id) " +
            ")";
        Table table = createTable(CREATE_TABLE_MISC, TABLE_MISC);
        int numRows = 105;
        loadRows(table, numRows);

        final OnqlShell onqlShell = getTestShell(null);
        String query = "SELECT * FROM " + TABLE_MISC;
        for (OutputMode mode : OutputMode.values()) {
            boolean shouldSucceed = (mode != OutputMode.CSV);
            doExecuteQuery(onqlShell, query, mode, shouldSucceed, numRows);
        }
    }

    private void doExecuteQuery(OnqlShell onqlShell,
                                String query,
                                OutputMode mode,
                                boolean shouldSucceed,
                                int expNumRows) {

        setOutputMode(onqlShell, mode);
        resetShellOutput(onqlShell);
        executeQuery(onqlShell, query, shouldSucceed, expNumRows);

    }

    private void setOutputMode(final OnqlShell shell,
                               final OutputMode mode) {

        shell.setQueryOutputMode(mode);
        assertTrue(shell.getQueryOutputMode() == mode);
    }

    private void executeQuery(final OnqlShell shell,
                              final String statement,
                              final boolean shouldSucceed,
                              final int expNumRowsReturned) {

        ExecuteCommand cmd = new ExecuteCommand();
        try {
            String ret = cmd.execute(new String[]{statement}, shell);
            if (!shouldSucceed) {
                fail("Execution should be failed but not: " + statement);
            }
            if (expNumRowsReturned >= 0) {
                String expRet = "\n" + expNumRowsReturned +
                                (expNumRowsReturned > 1 ? " rows" : " row") +
                                " returned";
                assertTrue("Expected to get " + expRet + ", but get " + ret,
                           expRet.equals(ret));
            }
        } catch (ShellException e) {
            if (shouldSucceed) {
                fail("Execution should succeed but not: " + statement);
            }
        }
    }

    private static void loadRows(Table table, int numRows) {
        for (int i = 0; i < numRows; i++) {
            Row row = createRow(table, i);
            Version ver = tableAPI.put(row, null, null);
            assertTrue("Put row failed: " + row.toJsonString(false),
                       ver != null);
        }
    }

    private static Row createRow(Table table, int id) {
        Row row = table.createRow();
        for(String field: row.getFieldNames()) {
            if (field.equalsIgnoreCase("id")) {
                row.put("id", id);
            } else {
                if (useNullForValues(id)) {
                    row.putNull(field);
                } else {
                    FieldDef fdef = table.getField(field);
                    FieldValue fval = createFieldValue(fdef, id);
                    row.put(field, fval);
                }
            }
        }
        return row;
    }

    private static FieldValue createFieldValue(final FieldDef def,
                                               final int id) {
        if (def.isComplex()) {
            return createComplexValue(def, id);
        }
        return createSimpleValue(def, id);
    }

    private static FieldValue createSimpleValue(final FieldDef def,
                                                final int id) {

        switch (def.getType()) {
        case INTEGER: {
            final int ival = id + 100;
            return def.createInteger(ival);
        }
        case LONG: {
            final long lval = id + 100000;
            return def.createLong(lval);
        }
        case STRING: {
            final String sval = getString(id % 30 + 1);
            return def.createString(sval);
        }
        case DOUBLE: {
            final double dval = id * 1.00001;
            return def.createDouble(dval);
        }
        case FLOAT: {
            final float fval = id * 1.1f;
            return def.createFloat(fval);
        }
        case BOOLEAN:
            return def.createBoolean((id % 2 == 1));
        case ENUM: {
            final String[] enumValues = def.asEnum().getValues();
            return def.createEnum(enumValues[(id % enumValues.length)]);
        }
        case BINARY: {
            final byte[] buf = getBinaryData(id % 20);
            return def.createBinary(buf);
        }
        case FIXED_BINARY: {
            final int size = def.asFixedBinary().getSize();
            return def.createFixedBinary(getBinaryData(size));
        }
        default:
            throw new IllegalArgumentException(
                "Type not yet implemented: " + def.getType());
        }
    }

    private static FieldValue createComplexValue(FieldDef fdef, int id) {

        assertTrue(fdef.isComplex());
        if (fdef.isArray()) {
            ArrayDef adef = fdef.asArray();
            ArrayValue av = adef.createArray();
            FieldDef edef = adef.getElement();
            int numElements = getNumElements(id);
            for (int i = 0; i < numElements; i++) {
                FieldValue ev = createFieldValue(edef, i);
                av.add(ev);
            }
            return av;
        } else if (fdef.isMap()) {
            MapDef mdef = fdef.asMap();
            MapValue mv = mdef.createMap();
            FieldDef edef = mdef.getElement();
            int numElements = getNumElements(id);
            for (int i = 0; i < numElements; i++) {
                FieldValue ev = createFieldValue(edef, i);
                mv.put("key" + i, ev);
            }
            return mv;
        }
        assertTrue(fdef.isRecord());
        RecordDef rdef = fdef.asRecord();
        RecordValue rv = rdef.createRecord();
        for (String fname : rdef.getFieldNames()) {
            FieldDef edef = rdef.getFieldDef(fname);
            FieldValue ev = createFieldValue(edef, id);
            rv.put(fname, ev);
        }
        return rv;
    }

    private static int getNumElements(int id) {
        if (useEmptyComplexValue(id)) {
            return 0;
        }
        return id % 10 + 1;
    }

    private static boolean useNullForValues(final int id) {
        return (id % 10 == 1);
    }

    private static boolean useEmptyComplexValue(int id) {
        return (id % 10 == 2);
    }

    private static byte[] getBinaryData(int len) {
        final byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte)(i % 0x100);
        }
        return buf;
    }

    private static String getString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + (i % 26)));
        }
        return sb.toString();
    }
}
