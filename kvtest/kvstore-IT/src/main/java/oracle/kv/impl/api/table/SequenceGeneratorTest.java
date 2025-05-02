/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.Version;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.SequenceImpl.SGAttributes;
import oracle.kv.impl.api.table.SequenceImpl.SGAttrsAndValues;
import oracle.kv.impl.api.table.SequenceImpl.SGIntegerValues;
import oracle.kv.impl.api.table.SequenceImpl.SGLongValues;
import oracle.kv.impl.api.table.SequenceImpl.SGNumberValues;
import oracle.kv.impl.query.compiler.Translator;
import oracle.kv.impl.query.compiler.Translator.IdentityDefHelper;
import oracle.kv.impl.systables.SGAttributesTableDesc;
import oracle.kv.impl.systables.SGAttributesTableDesc.SGType;
import oracle.kv.impl.systables.TableMetadataDesc;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ThreadUtils.ThreadPoolExecutorAutoClose;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.WriteOptions;

import org.junit.BeforeClass;
import org.junit.Test;

public class SequenceGeneratorTest extends TableTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        staticSetUp(3, 3, 2);
        waitForTable(tableImpl, SGAttributesTableDesc.TABLE_NAME);
        waitForTable(tableImpl, TableMetadataDesc.TABLE_NAME);
    }

    @Test
    public void testLongIdentity() throws InterruptedException {
        StatementResult sr = store.executeSync("CREATE Table Tid_Long " +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 1 INCREMENT BY 1 MAXVALUE 100 CYCLE CACHE 3)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(150, (long)1, (long)1, (long)100, Type.LONG, true,
                    "Tid_Long");
        sr = store.executeSync("Drop Table Tid_Long");

        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
    }

    @Test
    public void testNumberIdentity() throws InterruptedException {
        StatementResult sr = store.executeSync("CREATE Table Tid_Number " +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 1 INCREMENT BY 1 MAXVALUE 100 CYCLE CACHE 3)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(150, new BigDecimal(1), new BigDecimal(1),
                    new BigDecimal(100), Type.NUMBER, true, "Tid_Number");

        sr = store.executeSync("Drop Table Tid_Number");

        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
    }

    @Test
    public void testIntegerIdentity() throws InterruptedException {
        StatementResult sr = store.executeSync("CREATE Table Tid_Int " +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 1 INCREMENT BY 1 MAXVALUE 100 CYCLE CACHE 3)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(150, 1, 1, 100, Type.INTEGER, true, "Tid_Int");

        sr = store.executeSync("Drop Table Tid_Int");

        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
    }


    @Test
    public void testDefaultValue() throws InterruptedException {
        /*test default values of attributes*/
        StatementResult sr = store.executeSync("CREATE Table " +
            "Test_Default_INT (id INTEGER GENERATED ALWAYS AS " +
            "IDENTITY(MAXVALUE 100), name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(150, 1, 1, 100, Type.INTEGER, false, "Test_Default_INT");
        sr = store.executeSync("Drop Table Test_Default_INT");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Default_LONG " +
            "(id LONG GENERATED ALWAYS AS IDENTITY(MAXVALUE 100)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(150, (long)1, (long)1, (long)100, Type.LONG, false,
                    "Test_Default_LONG");
        sr = store.executeSync("Drop Table Test_Default_LONG");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Default_NUMBER " +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY(MAXVALUE 100)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(150, new BigDecimal(1), new BigDecimal(1),
                    new BigDecimal(100),Type.NUMBER, false,
                    "Test_Default_NUMBER");
        sr = store.executeSync("Drop Table Test_Default_NUMBER");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }

    @Test
    public void testIncrementInt() throws InterruptedException {
        /*test different increment values for INTEGER type*/
        StatementResult sr = store.executeSync("CREATE Table " +
            "Test_Increment_INT1 (id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 0 INCREMENT BY 2 MAXVALUE 100 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, 0, 2, 100,
                    Type.INTEGER, true, "Test_Increment_INT1");
        sr = store.executeSync("Drop Table Test_Increment_INT1");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Increment_INT2" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 0 INCREMENT BY 101 MAXVALUE 100 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, 0, 101, 100,
                    Type.INTEGER, true, "Test_Increment_INT2");
        sr = store.executeSync("Drop Table Test_Increment_INT2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());


        sr = store.executeSync("CREATE Table Test_Increment_INT3" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MINVALUE -10 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, 100, -2, -10,
                    Type.INTEGER, true, "Test_Increment_INT3");
        sr = store.executeSync("Drop Table Test_Increment_INT3");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Increment_INT4" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MINVALUE -10 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, 100, -2, -10,
                    Type.INTEGER, false, "Test_Increment_INT4");
        sr = store.executeSync("Drop Table Test_Increment_INT4");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
    }

    @Test
    public void testIncrementLong() throws InterruptedException {
        /*test different increment values for LONG type*/
        StatementResult sr = store.executeSync(
            "CREATE Table Test_Increment_LONG1" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 0 INCREMENT BY 2 MAXVALUE 100 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, (long)0, (long)2, (long)100,
                    Type.LONG, true, "Test_Increment_LONG1");
        sr = store.executeSync("Drop Table Test_Increment_LONG1");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Increment_LONG2" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 0 INCREMENT BY 101 MAXVALUE 100 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, (long)0, (long)101, (long)100,
                    Type.LONG, true, "Test_Increment_LONG2");
        sr = store.executeSync("Drop Table Test_Increment_LONG2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());


        sr = store.executeSync("CREATE Table Test_Increment_LONG3" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MINVALUE -10 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, (long)100, (long)-2, (long)-10,
                    Type.LONG, true, "Test_Increment_LONG3");
        sr = store.executeSync("Drop Table Test_Increment_LONG3");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Increment_LONG4" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MINVALUE -10 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, (long)100, (long)-2, (long)-10,
                    Type.LONG, false, "Test_Increment_LONG4");
        sr = store.executeSync("Drop Table Test_Increment_LONG4");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }

    @Test
    public void testIncrementNumber() throws InterruptedException {
        /*test different increment values for NUMBER type*/
        StatementResult sr = store.executeSync(
            "CREATE Table Test_Increment_NUM1" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 0 INCREMENT BY 2 MAXVALUE 100 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, new BigDecimal(0), new BigDecimal(2),
            new BigDecimal(100), Type.NUMBER, true, "Test_Increment_NUM1");
        sr = store.executeSync("Drop Table Test_Increment_NUM1");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Increment_NUM2" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 0 INCREMENT BY 101 MAXVALUE 100 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, new BigDecimal(0), new BigDecimal(101),
            new BigDecimal(100), Type.NUMBER, true, "Test_Increment_NUM2");
        sr = store.executeSync("Drop Table Test_Increment_NUM2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());


        sr = store.executeSync("CREATE Table Test_Increment_NUM3" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MINVALUE -10 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, new BigDecimal(100), new BigDecimal(-2),
                    new BigDecimal(-10), Type.NUMBER, true,
                    "Test_Increment_NUM3");
        sr = store.executeSync("Drop Table Test_Increment_NUM3");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Increment_NUM4" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MINVALUE -10 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, new BigDecimal(100), new BigDecimal(-2),
                    new BigDecimal(-10), Type.NUMBER, false,
                    "Test_Increment_NUM4");
        sr = store.executeSync("Drop Table Test_Increment_NUM4");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*increment value is negative but min value is not set.*/
        sr = store.executeSync("CREATE Table Test_Increment_NUM5" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY -2 MAXVALUE 10 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, new BigDecimal(100), new BigDecimal(-2),
                    null, Type.NUMBER, false,
                    "Test_Increment_NUM5");
        sr = store.executeSync("Drop Table Test_Increment_NUM5");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*increment value is positive but max value is not set.*/
        sr = store.executeSync("CREATE Table Test_Increment_NUM5" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH 100 INCREMENT BY 2 MINVALUE -10 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        ddlTestBase(150, new BigDecimal(100), new BigDecimal(2),
                    null, Type.NUMBER, false,
                    "Test_Increment_NUM5");
        sr = store.executeSync("Drop Table Test_Increment_NUM5");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }

    @Test
    public void testBoundsInt() throws InterruptedException {
        /*Test default value of max/min for Integer type and also the cases
         * when the sequence number gets to the max or min value*/
        StatementResult sr = store.executeSync("CREATE Table Test_Bounds_INT1" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Integer.MAX_VALUE - 1) +
            " INCREMENT BY 1 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Integer.MAX_VALUE-1, 1, Integer.MAX_VALUE,
                    Type.INTEGER, false, "Test_Bounds_INT1");
        sr = store.executeSync("Drop Table Test_Bounds_INT1");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_INT2" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Integer.MAX_VALUE - 1) +
            " INCREMENT BY 2 CACHE 3 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Integer.MAX_VALUE-1, 2, Integer.MAX_VALUE,
                    Type.INTEGER, true, "Test_Bounds_INT2");
        sr = store.executeSync("Drop Table Test_Bounds_INT2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_INT3" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Integer.MIN_VALUE + 1) +
            " INCREMENT BY -1 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Integer.MIN_VALUE+1, -1, Integer.MIN_VALUE,
                    Type.INTEGER, false, "Test_Bounds_INT3");
        sr = store.executeSync("Drop Table Test_Bounds_INT3");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_INT4" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Integer.MIN_VALUE + 1) +
            " INCREMENT BY -2 CACHE 3 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Integer.MIN_VALUE+1, -2, Integer.MIN_VALUE,
                    Type.INTEGER, true, "Test_Bounds_INT4");
        sr = store.executeSync("Drop Table Test_Bounds_INT4");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }

    @Test
    public void testBoundsLong() throws InterruptedException {
        /*Test default value of max/min for Long type and also the cases
         * when the sequence number gets to the max or min value*/
        StatementResult sr = store.executeSync("CREATE Table " +
            "Test_Bounds_LONG1 (id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Long.MAX_VALUE - 1) +
            " INCREMENT BY 1 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Long.MAX_VALUE-1, (long)1, Long.MAX_VALUE,
                    Type.LONG, false, "Test_Bounds_LONG1");
        sr = store.executeSync("Drop Table Test_Bounds_LONG1");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_LONG2" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Long.MAX_VALUE - 1) +
            " INCREMENT BY 2 CACHE 3 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Long.MAX_VALUE-1, (long)2, Long.MAX_VALUE,
                    Type.LONG, true, "Test_Bounds_LONG2");
        sr = store.executeSync("Drop Table Test_Bounds_LONG2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_LONG3" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Long.MIN_VALUE + 1) +
            " INCREMENT BY -1 NO CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Long.MIN_VALUE+1, (long)-1, Long.MIN_VALUE,
                    Type.LONG, false, "Test_Bounds_LONG3");
        sr = store.executeSync("Drop Table Test_Bounds_LONG3");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_LONG4" +
            "(id LONG GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Long.MIN_VALUE + 1) +
            " INCREMENT BY -2 CACHE 3 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, Long.MIN_VALUE+1, (long)-2, Long.MIN_VALUE,
                    Type.LONG, true, "Test_Bounds_LONG4");
        sr = store.executeSync("Drop Table Test_Bounds_LONG4");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }

    @Test
    public void testBoundsNumber() throws InterruptedException {
        /*Test the cases when the sequence number gets
         * to the max or min value*/
        StatementResult sr = store.executeSync("CREATE Table Test_Bounds_NUM1" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Long.MAX_VALUE - 1) +
            " INCREMENT BY 2 CACHE 3 MAXVALUE " + Long.MAX_VALUE + " CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, new BigDecimal(Long.MAX_VALUE-1), new BigDecimal(2),
                    new BigDecimal(Long.MAX_VALUE),
                    Type.NUMBER, true, "Test_Bounds_NUM1");
        sr = store.executeSync("Drop Table Test_Bounds_NUM1");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_Bounds_NUM2" +
            "(id NUMBER GENERATED ALWAYS AS IDENTITY " +
            "    (START WITH " + (Long.MIN_VALUE + 1) +
            " INCREMENT BY -2 CACHE 3 MINVALUE " + Long.MIN_VALUE + " CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        ddlTestBase(10, new BigDecimal(Long.MIN_VALUE + 1), new BigDecimal(-2),
                    new BigDecimal(Long.MIN_VALUE),
                    Type.NUMBER, true, "Test_Bounds_NUM2");
        sr = store.executeSync("Drop Table Test_Bounds_NUM2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }
    @Test
    public void testAlterTable() throws InterruptedException {
        StatementResult sr = store.executeSync("CREATE Table Test_alter" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "(START WITH 1 INCREMENT BY 2 MAXVALUE 100 CACHE 10 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        try {
            sr = store.executeSync("ALTER Table Test_alter " +
                "(ADD id2 INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1))");
            assertTrue(sr.isSuccessful());
            assertTrue(sr.isDone());
        } catch (Exception e) {
            assertThat("Alter table. ", e.getMessage(),
                       containsString("Only one identity column is allowed " +
                           "in a table."));
        }

        /*test altering attributes. */
        sr = store.executeSync("ALTER Table Test_alter " +
        "(MODIFY id GENERATED BY DEFAULT AS IDENTITY " +
        "(START WITH 0 INCREMENT BY 3 MAXVALUE 6 CACHE 1 NO CYCLE))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_alter");

        Table sysTable = api.getTable("SYS$SGAttributesTable");
        PrimaryKey sysPk = sysTable.createPrimaryKey();
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                  SGType.INTERNAL.name());
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                  ((TableImpl)table).getId() + "." + "id");
        Row sysRow = api.get(sysPk, null);
        /*check attributes in system table*/
        assertEquals(0, sysRow.get(SGAttributesTableDesc.COL_NAME_STARTWITH).
                                   asNumber().get().intValueExact());
        assertEquals(3, sysRow.get(SGAttributesTableDesc.COL_NAME_INCREMENTBY).
                                   asLong().get());
        assertEquals(6, sysRow.get(SGAttributesTableDesc.COL_NAME_MAXVALUE).
                                    asNumber().get().intValueExact());
        assertEquals(1, sysRow.get(SGAttributesTableDesc.COL_NAME_CACHE).
                                   asLong().get());
        assertEquals(false, sysRow.get(SGAttributesTableDesc.COL_NAME_CYCLE).
                     asBoolean().get());
        assertEquals(1, sysRow.get(SGAttributesTableDesc.COL_NAME_VERSION).
                     asLong().get());

        /*id should start from 0*/
        Row row = table.createRow();
        row.put("name", "joe");
        api.put(row, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 0);
        Row r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("joe", r.get("name").asString().toString());

        row = table.createRow();
        row.put("name", "cezar");
        api.put(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("id", 3);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("cezar", r.get("name").asString().toString());

        /*The sequence generator is BY DEFAULT now, so it is ok
         * to put a value on identity column.*/
        row = table.createRow();
        row.put("name", "paul");
        row.put("id", 5);
        api.put(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("id", 5);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("paul", r.get("name").asString().toString());

        row = table.createRow();
        row.put("name", "hema");
        api.put(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("id", 6);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("hema", r.get("name").asString().toString());
        /*throw error because max is 6 and no cycle*/
        row = table.createRow();
        row.put("name", "tom");
        try {
            api.put(row, null, null);
            fail("Exception Expected.");
        } catch (Exception e) {
            assertThat("Exceed max. ", e.getMessage(),
                       containsString("Current value cannot exceed "
                                      + "max value"));
        }

        /*Test another case: SG type is not altered and other
         * attributes are altered.*/
        sr = store.executeSync("CREATE Table Test_alter2" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
            "(START WITH 1 INCREMENT BY 2 MAXVALUE 100 CACHE 10 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*test altering attributes. */
        sr = store.executeSync("ALTER Table Test_alter2 " +
        "(MODIFY id GENERATED ALWAYS AS IDENTITY " +
        "(START WITH 0 INCREMENT BY 3 MAXVALUE 6 CACHE 1 NO CYCLE))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        api = store.getTableAPI();
        table = api.getTable("Test_alter");

        sysTable = api.getTable("SYS$SGAttributesTable");
        sysPk = sysTable.createPrimaryKey();
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                  SGType.INTERNAL.name());
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                  ((TableImpl)table).getId() + "." + "id");
        sysRow = api.get(sysPk, null);
        /*check attributes in system table*/
        assertEquals(0, sysRow.get(SGAttributesTableDesc.COL_NAME_STARTWITH).
                                   asNumber().get().intValueExact());
        assertEquals(3, sysRow.get(SGAttributesTableDesc.COL_NAME_INCREMENTBY).
                                   asLong().get());
        assertEquals(6, sysRow.get(SGAttributesTableDesc.COL_NAME_MAXVALUE).
                                    asNumber().get().intValueExact());
        assertEquals(1, sysRow.get(SGAttributesTableDesc.COL_NAME_CACHE).
                                   asLong().get());
        assertEquals(false, sysRow.get(SGAttributesTableDesc.COL_NAME_CYCLE).
                     asBoolean().get());
        assertEquals(1, sysRow.get(SGAttributesTableDesc.COL_NAME_VERSION).
                     asLong().get());

    }

    @Test
    public void testApiUpdateGenerateAlwaysPrimaryKey()
        throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_apiUpdateAlwaysPrimary" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY, " +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_apiUpdateAlwaysPrimary");

        Row row = table.createRow();
        row.put("name", "joe");
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "john");
        api.put(row, null, null);

        /*test api.put(2, dave)*/

        try {
            row = table.createRow();
            row.put("id", 2);
            row.put("name", "dave");
            api.put(row, null, null);
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }


        try {
            /*test api.putIfPresent(2, dave)*/
            row = table.createRow();
            row.put("id", 2);
            row.put("name", "dave");
            api.putIfPresent(row, null, null);
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }


        try {
            /*test api.putIfPresent(3, cezar)*/
            row = table.createRow();
            row.put("id", 3);
            row.put("name", "cezar");
            api.putIfPresent(row, null, null);
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }

        /*test api.putIfPresent(hema)*/
        row = table.createRow();
        row.put("name", "hema");
        try {
            api.putIfPresent(row, null, null);
        } catch (Exception e) {
            fail("Exception unexpected.");
        }
        int id = row.get("id").asInteger().get();

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        Row r = api.get(pk,
            new ReadOptions(Consistency.ABSOLUTE, 1000, TimeUnit.MILLISECONDS));
        Thread.sleep(5000);
        assertNull(r);

        /*test api.putIfAbsent(tim)*/
        row = table.createRow();
        row.put("name", "tim");
        try {
            api.putIfAbsent(row, null, null);
        } catch (Exception e) {
            fail("Exception unexpected.");
       }
       id = row.get("id").asInteger().get();

       pk.put("id", id);
       r = getRow(pk, api);
       assertNotNull(r);
       assertEquals("tim", r.get("name").asString().toString());

       /*test putIfVersion*/
       Version version = r.getVersion();
       r.put("name", "Paul");
       ReturnRow rr = table.createReturnRow(ReturnRow.Choice.ALL);
       api.putIfVersion(r, version, rr, null);
       id = r.get("id").asInteger().get();
       /*new sequence number was generated.*/
       assertEquals(id, r.get("id").asInteger().get());
    }

    @Test
    public void testShardPrimaryKey() {
        StatementResult sr =
            store.executeSync("CREATE Table Test_ShardPrimaryKey" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY, " +
            "name STRING, PRIMARY KEY (SHARD(id), name))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_ShardPrimaryKey");

        Row row = table.createRow();
        row.put("name", "joe");
        api.put(row, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);
        pk.put("name", "joe");
        Row r = api.get(pk,
            new ReadOptions(Consistency.ABSOLUTE, 1000, TimeUnit.MILLISECONDS));
        assertNotNull(r);
    }

    @Test
    public void testApiUpdateGenerateDefaultPrimaryKey()
        throws InterruptedException {
        /*Test updating primary keys of rows using api for GENERATED BY DEFAULT
         *and GENERATED BY DEFAULT ON NULL*/
        testApiUpdateGenerateDefaultInternal(true);
        testApiUpdateGenerateDefaultInternal(false);

    }

    private void testApiUpdateGenerateDefaultInternal(boolean onNull)
        throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_apiUpdateDefault" +
            (onNull ? "onNull" : "") + "Primary" +
            "(id INTEGER GENERATED BY DEFAULT " +
            (onNull ? "ON NULL " : "") + "AS IDENTITY, " +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_apiUpdateDefault" +
            (onNull ? "onNull" : "") + "Primary");

        Row row = table.createRow();
        row.put("name", "john");
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "dian");
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "paul");
        row.put("id", 1);
        api.putIfPresent(row, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);
        Row r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("paul", r.get("name").asString().toString());

        /*putIfPresent(tom);*/
        row = table.createRow();
        row.put("name", "tom");
        api.putIfPresent(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("id", 3);
        r = getRow(pk, api);

        assertNull(r);

        /*putIfAbsent(emily)*/
        row = table.createRow();
        row.put("name", "emily");
        api.putIfAbsent(row, null, null);


        pk = table.createPrimaryKey();
        pk.put("id", 4);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("emily", r.get("name").asString().toString());

      /*putIfAbsent(5, yuku)*/
        row = table.createRow();
        row.put("name", "yuki");
        api.putIfAbsent(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("id", 5);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("yuki", r.get("name").asString().toString());

        /*putIfVersion(5, susan)*/
        Version version = r.getVersion();
        r.put("name", "susan");
        ReturnRow rr = table.createReturnRow(ReturnRow.Choice.ALL);
        api.putIfVersion(r, version, rr, null);

        pk = table.createPrimaryKey();
        pk.put("id", 5);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("susan", r.get("name").asString().toString());
    }

    @Test
    public void testApiUpdateGenerateDefaultNotPrimary()
        throws InterruptedException {
        /*Test updating non-primary keys of rows using api for GENERATED BY
         * DEFAULT and GENERATED BY DEFAULT ON NULL*/
        testApiUpdateDefaultNotPrimaryKeyInternal(true);
        testApiUpdateDefaultNotPrimaryKeyInternal(false);

    }

    private void testApiUpdateDefaultNotPrimaryKeyInternal(boolean onNull)
        throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_apiUpdateDefault" +
            (onNull ? "onNull" : "") + "NotPrimary" +
            "(id INTEGER GENERATED BY DEFAULT " +
            (onNull ? "ON NULL " : "") + "AS IDENTITY," +
            " accuNum INTEGER, name STRING, PRIMARY KEY (accuNum))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_apiUpdateDefault" +
            (onNull ? "onNull" : "") + "NotPrimary");

        Row row = table.createRow();
        row.put("name", "tim");
        row.put("accuNum", 100);
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "smith");
        row.put("accuNum", 200);
        api.put(row, null, null);

        /*putIfPresent(100, ,john)*/
        row = table.createRow();
        row.put("name", "john");
        row.put("accuNum", 100);
        api.putIfPresent(row, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("accuNum", 100);
        Row r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("john", r.get("name").asString().toString());
        assertEquals(3, r.get("id").asInteger().get());

        /*putIfPresent(100,2,john)*/
        row = table.createRow();
        row.put("name", "john");
        row.put("accuNum", 100);
        row.put("id", 2);
        api.putIfPresent(row, null, null);

        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals(2, r.get("id").asInteger().get());

        /*putIfAbsent(300, ,yuki)*/
        row = table.createRow();
        row.put("name", "yuki");
        row.put("accuNum", 300);
        api.putIfAbsent(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("accuNum", 300);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("yuki", r.get("name").asString().toString());
        assertEquals(4, r.get("id").asInteger().get());

        /*putIfAbsent(400,7,blair)*/
        row = table.createRow();
        row.put("name", "blair");
        row.put("accuNum", 400);
        row.put("id", 7);
        api.putIfAbsent(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("accuNum", 400);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("blair", r.get("name").asString().toString());
        assertEquals(7, r.get("id").asInteger().get());

        /*putIfVersion(400, ,serena)*/
        Version version = r.getVersion();
        r.put("name", "serena");
        r.remove("id");
        ReturnRow rr = table.createReturnRow(ReturnRow.Choice.ALL);
        api.putIfVersion(r, version, rr, null);

        assertNotNull(r);
        assertEquals("serena", r.get("name").asString().toString());
        assertEquals(5, r.get("id").asInteger().get());

        /*putIfPresent(100,null,john)*/
        row = table.createRow();
        row.put("name", "john");
        row.put("accuNum", 100);
        row.putNull("id");
        api.putIfPresent(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("accuNum", 100);
        r = getRow(pk, api);

        r = getRow(pk, api);

        assertNotNull(r);
        if (onNull) {
            assertEquals(6, r.get("id").asInteger().get());
        } else {
            assertEquals("NULL", r.get("id").toString());
        }

    }

    @Test
    public void testGenerateByDefaultOnNull() throws InterruptedException {
        StatementResult sr = store.
            executeSync("CREATE Table Test_generateByDefaultOnNull" +
            "(id INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_generateByDefaultOnNull");


        Row row = table.createRow();
        row.put("name", "Tim");
        row.put("id", 5);
        try {
            api.put(row, null, null);
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 5);
        Row r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("Tim",r.get("name").asString().toString());

        row = table.createRow();
        row.put("name", "emma");
        try {
            api.put(row, null, null);
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        pk = table.createPrimaryKey();
        pk.put("id", 1);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("emma",r.get("name").asString().toString());


        /*If users set id to null, null will not be taken and a number will
         * be generated.*/
        sr = store.executeSync("CREATE Table Test_generateByDefaultOnNull2" +
            "(id INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY," +
            "accuNum INTEGER, name STRING, PRIMARY KEY (accuNum))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        api = store.getTableAPI();
        table = api.getTable("Test_generateByDefaultOnNull2");

        row = table.createRow();
        row.put("name", "Tim");
        row.put("accuNum", 5);
        row.putNull("id");
        try {
            api.put(row, null, null);
        } catch (Exception e){
            fail("Exception unexpected: " + e.getMessage());
        }
        pk = table.createPrimaryKey();
        pk.put("accuNum", 5);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals(1, r.get("id").asInteger().get());

        /*put duplicate id values*/
        row = table.createRow();
        row.put("name", "helen");
        row.put("accuNum", 2);
        row.put("id", 1);
        api.put(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("accuNum", 2);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals(1, r.get("id").asInteger().get());
    }

    @Test
    public void testGenerateByDefault() throws InterruptedException {
        /*If users set id to null, null will be taken.*/
        StatementResult sr = store.
            executeSync("CREATE Table Test_generateByDefault" +
            "(id INTEGER GENERATED BY DEFAULT AS IDENTITY," +
            "accuNum INTEGER, name STRING, PRIMARY KEY (accuNum))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_generateByDefault");

        Row row = table.createRow();
        row.put("name", "Tim");
        row.put("accuNum", 5);
        row.putNull("id");
        try {
            api.put(row, null, null);
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("accuNum", 5);
        Row r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("NULL", r.get("id").toString());

        /*Generate a value if users do not give a value.*/
        row = table.createRow();
        row.put("name", "Emily");
        row.put("accuNum", 7);
        try {
            api.put(row, null, null);
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        pk = table.createPrimaryKey();
        pk.put("accuNum", 7);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals(1, r.get("id").asInteger().get());

        /*put duplicate id values*/
        row = table.createRow();
        row.put("name", "helen");
        row.put("accuNum", 2);
        row.put("id", 1);
        api.put(row, null, null);

        pk = table.createPrimaryKey();
        pk.put("accuNum", 2);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals(1, r.get("id").asInteger().get());

        /*Generate a value as a primary key*/
        sr = store.executeSync("CREATE Table Test_generateByDefault2" +
            "(id INTEGER GENERATED BY DEFAULT AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        api = store.getTableAPI();
        table = api.getTable("Test_generateByDefault2");

        row = table.createRow();
        row.put("name", "emma");
        try {
            api.put(row, null, null);
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        pk = table.createPrimaryKey();
        pk.put("id", 1);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("emma",r.get("name").asString().toString());

    }

    private static Row getRow(PrimaryKey pk, TableAPI api)
        throws InterruptedException {

        Row r = null;
        int count = 0;
        while (true) {
            r = api.get(pk, new ReadOptions(Consistency.ABSOLUTE, 1000,
                                            TimeUnit.MILLISECONDS));
            if (r != null) {
                break;
            }
            count++;
            if (count > 10) {
                break;
            }
            Thread.sleep(1000);
        }
        return r;
    }

    @Test
    public void testApiUpdateGenerateAlwaysNotPrimary()
        throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_apiUpdateAlwaysNotPrimary" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            " accuNum INTEGER, name STRING, PRIMARY KEY (accuNum))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_apiUpdateAlwaysNotPrimary");

        Row row = table.createRow();
        row.put("name", "joe");
        row.put("accuNum", 100);
        api.put(row, null, null);
        assertEquals(1, row.get("id").asInteger().get());

        row = table.createRow();
        row.put("name", "john");
        row.put("accuNum", 200);
        api.put(row, null, null);
        assertEquals(2, row.get("id").asInteger().get());


        try {
            row = table.createRow();
            row.put("name", "dave");
            row.put("accuNum", 200);
            row.put("id", 2);
            api.put(row, null, null);
            assertEquals(3, row.get("id").asInteger().get());
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }


        try {
            /*putIfPresent(3, 200, cezar)*/
            row = table.createRow();
            row.put("name", "cezar");
            row.put("accuNum", 200);
            row.put("id", 3);
            api.putIfPresent(row, null, null);
            assertEquals(4, row.get("id").asInteger().get());
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }

        /*putIfPresent(400, cezar)*/
        row = table.createRow();
        row.put("name", "cezar");
        row.put("accuNum", 400);
        try {
            api.putIfPresent(row, null, null);
            assertEquals(5, row.get("id").asInteger().get());
        } catch (Exception e){
            fail("Exception unexpected.");
        }

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("accuNum", 400);
        Row r = api.get(pk, new ReadOptions(Consistency.ABSOLUTE,
                                            1000, TimeUnit.MILLISECONDS));
        Thread.sleep(5000);
        assertNull(r);

        /*putIfPresent(200, cezar)*/
        row = table.createRow();
        row.put("name", "cezar");
        row.put("accuNum", 200);
        try {
            api.putIfPresent(row, null, null);
            assertEquals(6, row.get("id").asInteger().get());
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        pk = table.createPrimaryKey();
        pk.put("accuNum", 200);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("cezar",r.get("name").asString().toString());
        assertEquals(6, r.get("id").asInteger().get());

        /*putIfAbsent(300, hema)*/
        row = table.createRow();
        row.put("name", "hema");
        row.put("accuNum", 300);
        try {
            api.putIfAbsent(row, null, null);
            assertEquals(7, row.get("id").asInteger().get());
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        pk = table.createPrimaryKey();
        pk.put("accuNum", 300);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("hema",r.get("name").asString().toString());
        assertEquals(7, r.get("id").asInteger().get());


        try {
            /*putIfAbsent(20, 300, hema)*/
            row = table.createRow();
            row.put("name", "hema");
            row.put("accuNum", 300);
            row.put("id", 20);
            api.putIfAbsent(row, null, null);
            assertEquals(8, row.get("id").asInteger().get());
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }

        /*putIfAbsent(300, Tim)*/
        row = table.createRow();
        row.put("name", "Tim");
        row.put("accuNum", 300);
        try {
            api.putIfAbsent(row, null, null);
            assertEquals(9, row.get("id").asInteger().get());
        } catch (Exception e){
            fail("Exception unexpected.");
        }
        pk = table.createPrimaryKey();
        pk.put("accuNum", 300);
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("hema",r.get("name").asString().toString());
        assertEquals(7, r.get("id").asInteger().get());


        try {
            /*putIfPresent(3, 400, hema)*/
            row = table.createRow();
            row.put("name", "hema");
            row.put("accuNum", 400);
            row.put("id", 3);
            api.putIfAbsent(row, null, null);
            assertEquals(10, row.get("id").asInteger().get());
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }

        /*test putIfVersion*/
        Version version = r.getVersion();
        row = table.createRow();
        row.put("name", "Paul");
        row.put("accuNum", 300);
        ReturnRow rr = table.createReturnRow(ReturnRow.Choice.ALL);
        api.putIfVersion(row, version, rr, null);
        assertEquals(11, row.get("id").asInteger().get());
        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("Paul", r.get("name").asString().toString());
        assertEquals(11, r.get("id").asInteger().get());

        row = table.createRow();
        row.put("name", "Susan");
        row.put("accuNum", 300);
        rr = table.createReturnRow(ReturnRow.Choice.ALL);
        /*This should fail because the version has changed. */
        Version v = api.putIfVersion(row, version, rr, null);
        assertEquals(12, row.get("id").asInteger().get());
        assertNull(v);
        r = getRow(pk, api);
        assertEquals("Paul", r.get("name").asString().toString());
        assertEquals(11, r.get("id").asInteger().get());

        try {
            version = r.getVersion();
            row.put("id", 6);
            rr = table.createReturnRow(ReturnRow.Choice.ALL);
            api.putIfVersion(row, version, rr, null);
            assertEquals(13, row.get("id").asInteger().get());
        } catch (Exception e) {
            fail("Exception no longer expected.");
        }
    }

    @Test
    public void testExecuteListOfOperations() throws TableOpExecutionException,
        InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_executeListOfOperations" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "accuNum INTEGER, name STRING, PRIMARY KEY (SHARD (accuNum), " +
            "name))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_executeListOfOperations");

        List<TableOperation> opList = new ArrayList<TableOperation>();
        for (int i = 1; i <= 100; i++) {
            TableOperationFactory factory = api.getTableOperationFactory();
            Row row = table.createRow();
            row.put("name", "smith" + i);
            row.put("accuNum", 0);
            opList.add(factory.createPut(row, null, false));
        }
        List<TableOperationResult> results = api.execute(opList, null);
        executeCheckResult(results, 1, api, table, 0);

        opList = new ArrayList<TableOperation>();
        for (int i = 101; i <= 200; i++) {
            TableOperationFactory factory = api.getTableOperationFactory();
            Row row = table.createRow();
            row.put("name", "smith" + i);
            row.put("accuNum", 0);
            opList.add(factory.createPutIfAbsent(row, null, false));
        }
        results = api.execute(opList, null);
        executeCheckResult(results, 101, api, table, 0);

        opList = new ArrayList<TableOperation>();
        for (int i = 101; i <= 200; i++) {
            TableOperationFactory factory = api.getTableOperationFactory();
            Row row = table.createRow();
            row.put("name", "smith" + i);
            row.put("accuNum", 0);
            opList.add(factory.createPutIfPresent(row, null, false));
        }
        results = api.execute(opList, null);
        executeCheckResult(results, 101, api, table, 100);
    }

    @Test
    public void testMultiGet() throws InterruptedException {
        /*test multiget and tableiterator for rows with sequence number.*/
        StatementResult sr =
            store.executeSync("CREATE Table Test_testMultiGet" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "accuNum INTEGER, name STRING, PRIMARY KEY (shard(accuNum), id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testMultiGet");

        Row row = table.createRow();
        row.put("name", "joe");
        row.put("accuNum", 100);
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "john");
        row.put("accuNum", 100);
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "lee");
        row.put("accuNum", 100);
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "paul");
        row.put("accuNum", 200);
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "emily");
        row.put("accuNum", 200);
        api.put(row, null, null);

        row = table.createRow();
        row.put("name", "anna");
        row.put("accuNum", 200);
        api.put(row, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("accuNum", 200);
        List<Row> rows = api.multiGet(pk, null, null);
        Thread.sleep(5000);
        assertNotNull(rows);
        TableIterator<Row> iterator = api.tableIterator(pk, null, null);

        int i = 4;
        for (Row r : rows) {
            assertNotNull(r);
            assertEquals(i, r.get("id").asInteger().get());
            assertTrue(iterator.hasNext());
            assertTrue(iterator.next().equals(r));
            i++;
        }
    }

    @Test
    public void testConcurrentPut() throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_testConcurrentPut" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        final TableAPI api = store.getTableAPI();
        final Table table = api.getTable("Test_testConcurrentPut");

        Map<Integer, String> nameMap = new ConcurrentHashMap<Integer, String>();
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, 10, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory("testConcurrentPut", null))) {
            for (int i = 0; i < 10; i++) {
                final int j = i;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        Row row = table.createRow();
                        row.put("name", "smith" + (j+1));
                        api.put(row, null, null);

                        nameMap.put(row.get("id").asInteger().get(),
                                    "smith" + (j+1));
                    }
                });
            }

            Thread.sleep(5000);

            for (int i = 0; i < 10; i++) {
                PrimaryKey pk = table.createPrimaryKey();
                pk.put("id", i+1);

                Row r = getRow(pk, api);
                assertNotNull(r);
                assertEquals(nameMap.get(i+1), r.get("name").asString().get());
            }
        }

    }

    @Test
    public void testSelectSQL() {
        StatementResult sr =
            store.executeSync("CREATE Table Test_testSelectSQL" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testSelectSQL");

        Row row = table.createRow();
        row.put("name", "paul");
        api.put(row, null, null);

        sr = store.executeSync("SELECT * FROM Test_testSelectSQL");
        TableIterator<RecordValue> ti = sr.iterator();
        while (ti.hasNext()) {
            assertThat("SELECT SQL", ti.next().toString(),
                       containsString("{\"id\":1,\"name\":\"paul\"}"));
        }

    }

    @Test
    public void testAddSG() {
        StatementResult sr =
            store.executeSync("CREATE Table Test_testAddSG" +
            "(accNum INTEGER, name STRING, PRIMARY KEY (accNum))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr =
            store.executeSync("CREATE Table Test_testAddSG2" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testAddSG");

        Row row = table.createRow();
        row.put("name", "paul");
        row.put("accNum", 0);
        api.put(row, null, null);

        sr = store.executeSync("ALTER Table Test_testAddSG " +
            "(ADD id INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*check that this identity column has been added to system table*/
        Table sysTable = api.getTable("SYS$SGAttributesTable");
        PrimaryKey sysPk = sysTable.createPrimaryKey();
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                  SGType.INTERNAL.name());
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                  ((TableImpl)table).getId() + "." + "id");
        Row sysRow = api.get(sysPk, null);
        assertNotNull(sysRow);

        api = store.getTableAPI();
        table = api.getTable("Test_testAddSG");

        try {
            row = table.createRow();
            row.put("name", "tom");
            row.put("accNum", 1);
            row.put("id", 0);
            api.put(row, null, null);
        } catch (Exception e) {
            fail("Exception is no longer expected.");
        }
    }

    @Test
    public void testDropSG() {
        StatementResult sr =
            store.executeSync("CREATE Table Test_testDropSG" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1)," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testDropSG");

        Row row = table.createRow();
        row.put("name", "paul");
        api.put(row, null, null);

        sr = store.executeSync("ALTER Table Test_testDropSG " +
            "(MODIFY id DROP IDENTITY)");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        try {
            row = table.createRow();
            row.put("name", "Tom");
            api.put(row, null, null);
            fail("Exception expected.");
        } catch (Exception e) {

        }

        /*check that this identity column has been removed from system table*/
        Table sysTable = api.getTable("SYS$SGAttributesTable");
        PrimaryKey sysPk = sysTable.createPrimaryKey();
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                  SGType.INTERNAL.name());
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                  ((TableImpl)table).getId() + "." + "id");
        Row sysRow = api.get(sysPk, null);
        assertNull(sysRow);

        sr = store.executeSync("DROP TABLE Test_testDropSG");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_testDropSG2" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1)," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        table = api.getTable("Test_testDropSG2");

        sr = store.executeSync("DROP TABLE Test_testDropSG2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*check that this identity column has been removed from system table*/
        sysTable = api.getTable("SYS$SGAttributesTable");
        sysPk = sysTable.createPrimaryKey();
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                  SGType.INTERNAL.name());
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                  ((TableImpl)table).getId() + "." + "id");
        sysRow = api.get(sysPk, null);
        assertNull(sysRow);
    }

    @Test
    public void testFailover() throws InterruptedException {
        /*test the case when the client goes down and then restarts, there will
         *be holes.*/
        StatementResult sr =
            store.executeSync("CREATE Table Test_testFailover" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 5)," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testFailover");

        Row row = table.createRow();
        row.put("name", "paul");
        api.put(row, null, null);

        store.close();

        store = KVStoreFactory.getStore(createKVConfig(createStore));

        api = store.getTableAPI();
        table = api.getTable("Test_testFailover");

        row = table.createRow();
        row.put("name", "smith");
        api.put(row, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);

        Row r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("paul", r.get("name").asString().get());

        pk = table.createPrimaryKey();
        pk.put("id", 6);

        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("smith", r.get("name").asString().get());
    }

    @Test
    public void testCacheValue() {
        /*test default cache size*/
        StatementResult sr =
            store.executeSync("CREATE Table Test_testCacheValue" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testCacheValue");

        CacheTestHook testHook = new CacheTestHook(1000, 1);

        KVStoreImpl.cacheTestHook = testHook;

        Row row = table.createRow();
        row.put("name", "paul");
        api.put(row, null, null);

        assertTrue(testHook.isCorrectCacheSize());

        /*test setting cache size*/
        sr = store.executeSync("CREATE Table Test_testCacheValue2" +
             "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
             "(START WITH 0 CACHE 5 INCREMENT BY 2)," +
             "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        api = store.getTableAPI();
        table = api.getTable("Test_testCacheValue2");

        testHook = new CacheTestHook(5, 2);

        KVStoreImpl.cacheTestHook = testHook;

        for (int i = 0; i < 10; i = i + 2) {
            row = table.createRow();
            row.put("name", "paul" + i);
            api.put(row, null, null);
        }

        row = table.createRow();
        row.put("name", "alan");
        api.put(row, null, null);

        assertTrue(testHook.isCorrectCacheSize());

        /*test setting cache size*/
        sr = store.executeSync("CREATE Table Test_testCacheValue3" +
             "(id INTEGER GENERATED ALWAYS AS IDENTITY " +
             "(START WITH 0 CACHE 5 INCREMENT BY 2)," +
             "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        api = store.getTableAPI();
        table = api.getTable("Test_testCacheValue3");

        testHook = new CacheTestHook(1, 2);

        KVStoreImpl.cacheTestHook = testHook;

        WriteOptions wo = new WriteOptions();
        wo.setIdentityCacheSize(1);
        row = table.createRow();
        row.put("name", "alan");
        api.put(row, null, wo);

        assertTrue(testHook.isCorrectCacheSize());

        testHook = new CacheTestHook(3, 2);

        KVStoreImpl.cacheTestHook = testHook;

        wo = new WriteOptions();
        wo.setIdentityCacheSize(3);
        row = table.createRow();
        row.put("name", "tom");
        api.put(row, null, wo);

        assertTrue(testHook.isCorrectCacheSize());

    }

    @Test
    public void testSqlUpdateNonPrimaryKey()
        throws InterruptedException {
        /*Test update for default on null type*/
        StatementResult sr = store.
            executeSync("CREATE Table Test_sqlUpdateAlwaysNonPrimaryKey3" +
            "(id INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY (CACHE 100)," +
            "accuNum INTEGER, name STRING, PRIMARY KEY (accuNum))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_sqlUpdateAlwaysNonPrimaryKey3");

        Row row = table.createRow();
        row.put("name", "joe");
        row.put("accuNum", 100);
        api.put(row, null, null);

        /*Test updating name*/
        sr = store.executeSync("UPDATE Test_sqlUpdateAlwaysNonPrimaryKey3 " +
                               "SET name = \"dave\" WHERE accuNum = 100");
        Thread.sleep(5000);
        assertTrue(sr.isSuccessful());

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("accuNum", 100);
        Row r = getRow(pk, api);

        assertNotNull(r);
        assertEquals("dave", r.get("name").asString().toString());
        assertEquals(1, r.get("id").asInteger().get());

        /*Test updating id to null*/
        sr = store.executeSync("UPDATE Test_sqlUpdateAlwaysNonPrimaryKey3 SET " +
            "id = NULL WHERE accuNum = 100");
        Thread.sleep(5000);
        assertTrue(sr.isSuccessful());

        pk = table.createPrimaryKey();
        pk.put("accuNum", 100);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals(101, r.get("id").asInteger().get());

        sr = store.executeSync("UPDATE Test_sqlUpdateAlwaysNonPrimaryKey3 SET " +
            "id = NULL WHERE accuNum = 100");
        Thread.sleep(5000);
        assertTrue(sr.isSuccessful());

        pk = table.createPrimaryKey();
        pk.put("accuNum", 100);
        r = getRow(pk, api);

        assertNotNull(r);
        assertEquals(102, r.get("id").asInteger().get());
    }

  @Test
  public void testSGSqlInsert()
      throws Exception {

      /* Identity column is primary key. */
      StatementResult sr =
          store.executeSync("CREATE Table Test_SGSqlInsert" +
              "(id INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1)," +
              "name STRING, PRIMARY KEY (id))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      TableAPI api = store.getTableAPI();
      Table table = api.getTable("Test_SGSqlInsert");

      Row row = table.createRow();
      row.put("name", "paul");
      api.put(row, null, null);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsert " +
          "VALUES (DEFAULT, 'dave')");
      assertTrue(sr.isSuccessful());

      PreparedStatement ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsert VALUES (DEFAULT, 'greg')");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsert " +
          "(name) VALUES ('joe')");
      assertTrue(sr.isSuccessful());

      // get the generated value from a SQL INSERT using RETURNING
      sr = store.executeSync("INSERT INTO Test_SGSqlInsert " +
          "(name) VALUES ('foe') RETURNING id");
      assertTrue(sr.isSuccessful());
      int id = sr.iterator().next().get("id").asInteger().get();
      assertEquals(6, id);

      try {
          store.executeSync("INSERT INTO Test_SGSqlInsert " +
              "VALUES (100, 'foe')");
          fail("Expected error: IllegalArgumentException: Generated always " +
              "identity column must use DEFAULT construct.");
      } catch (IllegalArgumentException iae) {
          assertTrue(iae.getMessage().contains("Generated always " +
              "identity column must use DEFAULT construct."));
      }

      try {
          store.executeSync("INSERT INTO Test_SGSqlInsert " +
              "VALUES (NULL, 'foe')");
          fail("Expected error: IllegalArgumentException: Generated always " +
              "identity column must use DEFAULT construct.");
      } catch (IllegalArgumentException iae) {
          assertTrue(iae.getMessage().contains("Generated always " +
              "identity column must use DEFAULT construct."));
      }

      sr = store.executeSync("SELECT * FROM Test_SGSqlInsert ORDER " +
          "BY id");
      id = 0;
      String name;
      for (RecordValue rec : sr) {
          assertEquals(++id, rec.get("id").asInteger().get());
          switch (id) {
          case 1:
              name = "paul";
              break;
          case 2:
              name = "dave";
              break;
          case 3:
          case 4:
              name = "greg";
              break;
          case 5:
              name = "joe";
              break;
          case 6:
              name = "foe";
              break;
          default:
              fail("Unexpected id: " + id);
              name = "";
          }
          assertEquals(name, rec.get("name").asString().get());
      }

      assertEquals(6, id);

      /* Identity column is not primary key. */
      sr =
          store.executeSync("CREATE Table Test_SGSqlInsert2" +
              "(id INTEGER, name STRING, " +
              "deptId INTEGER GENERATED ALWAYS AS IDENTITY (CACHE 1), " +
              "PRIMARY KEY (id))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsert2 " +
          "VALUES (1, 'dave', DEFAULT)");
      assertTrue(sr.isSuccessful());

      ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsert2 VALUES (2, 'john', DEFAULT)");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsert2 " +
          "(id, name) VALUES (3, 'joe')");
      assertTrue(sr.isSuccessful());

      try {
          store.executeSync("INSERT INTO Test_SGSqlInsert2 " +
              "VALUES (100, 'foe', 1)");
          fail("Expected error: IllegalArgumentException: Generated always " +
              "identity column must use DEFAULT construct.");
      } catch (IllegalArgumentException iae) {
          assertTrue(iae.getMessage().contains("Generated always " +
              "identity column must use DEFAULT construct."));
      }

      /* Identity column is shard key. */
      sr =
          store.executeSync("CREATE TABLE Test_SGSqlInsert3(ID INTEGER, " +
              "NAME STRING, DeptID INTEGER GENERATED ALWAYS AS IDENTITY " +
              "(START WITH 1 INCREMENT BY 1 MAXVALUE 100), " +
              "PRIMARY KEY (SHARD(DeptID),ID))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsert3 " +
          "VALUES (1, 'dave', DEFAULT)");
      assertTrue(sr.isSuccessful());

      ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsert3 VALUES (2, 'john', DEFAULT)");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsert3 " +
          "(id, name) VALUES (3, 'joe')");
      assertTrue(sr.isSuccessful());

      try {
          store.executeSync("INSERT INTO Test_SGSqlInsert3 " +
              "VALUES (100, 'foe', 1)");
          fail("Expected error: IllegalArgumentException: Generated always " +
              "identity column must use DEFAULT construct.");
      } catch (IllegalArgumentException iae) {
          assertTrue(iae.getMessage().contains("Generated always " +
              "identity column must use DEFAULT construct."));
      }

      sr = store.executeSync("DROP TABLE Test_SGSqlInsert");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      sr = store.executeSync("DROP TABLE Test_SGSqlInsert2");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      sr = store.executeSync("DROP TABLE Test_SGSqlInsert3");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());


  }

  @Test
  public void testSGSqlInsertOnNull()
      throws Exception {
      /* Identity column is primary key. */
      StatementResult sr =
          store.executeSync("CREATE Table Test_SGSqlInsertOnNull" +
              "(id INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY " +
              "(CACHE 1), name STRING, PRIMARY KEY (id))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      TableAPI api = store.getTableAPI();
      Table table = api.getTable("Test_SGSqlInsertOnNull");

      Row row = table.createRow();
      row.put("name", "paul");
      api.put(row, null, null);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull " +
          "VALUES (DEFAULT, 'dave')");
      assertTrue(sr.isSuccessful());

      PreparedStatement ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsertOnNull VALUES (NULL, 'greg')");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull " +
          "(name) VALUES ('joe')");
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull " +
          "VALUES (NULL, 'anna')");
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull " +
          "VALUES (100, 'boe')");
      assertTrue(sr.isSuccessful());


      sr = store.executeSync("SELECT * FROM Test_SGSqlInsertOnNull ORDER " +
          "BY id");
      int id = 0;
      String name;
      for (RecordValue rec : sr) {
          if (id == 6) {
              id = 99;
          }
          assertEquals(++id, rec.get("id").asInteger().get());
          switch (id) {
          case 1:
              name = "paul";
              break;
          case 2:
              name = "dave";
              break;
          case 3:
          case 4:
              name = "greg";
              break;
          case 5:
              name = "joe";
              break;
          case 6:
              name = "anna";
              break;
          case 100:
              name = "boe";
              break;
          default:
              fail("Unexpected id: " + id);
              name = "";
          }
          assertEquals(name, rec.get("name").asString().get());
      }

      assertEquals(100, id);

      sr = store.executeSync("DROP TABLE Test_SGSqlInsertOnNull");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());


      /* Identity column is not primary key. */
      sr =
          store.executeSync("CREATE Table Test_SGSqlInsertOnNull2" +
              "(id INTEGER, name STRING, " +
              "deptId INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY (CACHE 1), " +
              "PRIMARY KEY (id))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      table = api.getTable("Test_SGSqlInsertOnNull2");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull2 " +
          "VALUES (1, 'dave', DEFAULT)");
      assertTrue(sr.isSuccessful());

      PrimaryKey pk = table.createPrimaryKey();
      pk.put("id", 1);
      Row r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 1);

      ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsertOnNull2 VALUES (2, 'john', DEFAULT)");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 2);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 2);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull2 " +
          "(id, name) VALUES (3, 'joe')");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 3);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 3);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull2 " +
          "VALUES (4, 'anna', NULL)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 4);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 4);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull2 " +
           "VALUES (100, 'foe', 1)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 100);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 1);

      sr = store.executeSync("DROP TABLE Test_SGSqlInsertOnNull2");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      /* Identity column is shard key. */
      sr =
          store.executeSync("CREATE TABLE Test_SGSqlInsertOnNull3(ID INTEGER, " +
              "NAME STRING, DeptID INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY " +
              "(START WITH 1 INCREMENT BY 1 MAXVALUE 100), " +
              "PRIMARY KEY (SHARD(DeptID),ID))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      table = api.getTable("Test_SGSqlInsertOnNull3");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull3 " +
          "VALUES (1, 'dave', DEFAULT)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 1);
      pk.put("DeptID", 1);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "dave");

      ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsertOnNull3 VALUES (2, 'john', DEFAULT)");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 2);
      pk.put("DeptID", 2);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "john");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull3 " +
          "(id, name) VALUES (3, 'joe')");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 3);
      pk.put("DeptID", 3);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "joe");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull3 " +
          "VALUES (4, 'anna', NULL)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 4);
      pk.put("DeptID", 4);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "anna");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertOnNull3 " +
               "VALUES (100, 'foe', 1)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 100);
      pk.put("DeptID", 1);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "foe");

      sr = store.executeSync("DROP TABLE Test_SGSqlInsertOnNull3");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());
  }

  @Test
  public void testSGSqlInsertByDefault() throws InterruptedException {
      /* Identity column is primary key. */
      StatementResult sr =
          store.executeSync("CREATE Table Test_SGSqlInsertByDefault" +
              "(id INTEGER GENERATED BY DEFAULT AS IDENTITY " +
              "(CACHE 1), name STRING, PRIMARY KEY (id))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      TableAPI api = store.getTableAPI();
      Table table = api.getTable("Test_SGSqlInsertByDefault");

      Row row = table.createRow();
      row.put("name", "paul");
      api.put(row, null, null);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault " +
          "VALUES (DEFAULT, 'dave')");
      assertTrue(sr.isSuccessful());

      PreparedStatement ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsertByDefault VALUES (DEFAULT, 'greg')");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault " +
          "(name) VALUES ('joe')");
      assertTrue(sr.isSuccessful());


      try {
          sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault " +
              "VALUES (NULL, 'anna')");
          fail("Expected error: IllegalArgumentException: Generated always " +
              "identity column must use DEFAULT construct.");
      } catch (IllegalArgumentException iae) {
          assertTrue(iae.getMessage().contains("Field \"id\" is not nullable"));
      }

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault " +
          "VALUES (100, 'boe')");
      assertTrue(sr.isSuccessful());


      sr = store.executeSync("SELECT * FROM Test_SGSqlInsertByDefault ORDER " +
          "BY id");
      int id = 0;
      String name;
      for (RecordValue rec : sr) {
          if (id == 5) {
              id = 99;
          }
          assertEquals(++id, rec.get("id").asInteger().get());
          switch (id) {
          case 1:
              name = "paul";
              break;
          case 2:
              name = "dave";
              break;
          case 3:
          case 4:
              name = "greg";
              break;
          case 5:
              name = "joe";
              break;
          case 100:
              name = "boe";
              break;
          default:
              fail("Unexpected id: " + id);
              name = "";
          }
          assertEquals(name, rec.get("name").asString().get());
      }

      assertEquals(100, id);

      sr = store.executeSync("DROP TABLE Test_SGSqlInsertByDefault");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());


      /* Identity column is not primary key. */
      sr =
          store.executeSync("CREATE Table Test_SGSqlInsertByDefault2" +
              "(id INTEGER, name STRING, " +
              "deptId INTEGER GENERATED BY DEFAULT AS IDENTITY (CACHE 1), " +
              "PRIMARY KEY (id))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      table = api.getTable("Test_SGSqlInsertByDefault2");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault2 " +
          "VALUES (1, 'dave', DEFAULT)");
      assertTrue(sr.isSuccessful());

      PrimaryKey pk = table.createPrimaryKey();
      pk.put("id", 1);
      Row r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 1);

      ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsertByDefault2 VALUES (2, 'john', DEFAULT)");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 2);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 2);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault2 " +
          "(id, name) VALUES (3, 'joe')");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 3);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 3);

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault2 " +
          "VALUES (4, 'anna', NULL)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 4);
      r = getRow(pk, api);
      assertEquals("NULL", r.get("deptId").toString());


      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault2 " +
           "VALUES (100, 'foe', 1)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 100);
      r = getRow(pk, api);
      assertEquals(r.get("deptId").asInteger().get(), 1);

      sr = store.executeSync("DROP TABLE Test_SGSqlInsertByDefault2");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      /* Identity column is shard key. */
      sr =
          store.executeSync("CREATE TABLE Test_SGSqlInsertByDefault3(ID INTEGER, " +
              "NAME STRING, DeptID INTEGER GENERATED BY DEFAULT AS IDENTITY " +
              "(START WITH 1 INCREMENT BY 1 MAXVALUE 100), " +
              "PRIMARY KEY (SHARD(DeptID),ID))");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());

      table = api.getTable("Test_SGSqlInsertByDefault3");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault3 " +
          "VALUES (1, 'dave', DEFAULT)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 1);
      pk.put("DeptID", 1);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "dave");

      ps = store.prepare("INSERT INTO " +
          "Test_SGSqlInsertByDefault3 VALUES (2, 'john', DEFAULT)");

      sr = store.executeSync(ps);
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 2);
      pk.put("DeptID", 2);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "john");

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault3 " +
          "(id, name) VALUES (3, 'joe')");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 3);
      pk.put("DeptID", 3);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "joe");

      try {
          sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault3 " +
              "VALUES (4, 'anna', NULL)");
          fail("Expected error: IllegalArgumentException: Generated always " +
              "identity column must use DEFAULT construct.");
      } catch (IllegalArgumentException iae) {
          assertTrue(iae.getMessage().contains("Field \"DeptID\" is not nullable"));
      }

      sr = store.executeSync("INSERT INTO Test_SGSqlInsertByDefault3 " +
               "VALUES (100, 'foe', 1)");
      assertTrue(sr.isSuccessful());

      pk = table.createPrimaryKey();
      pk.put("id", 100);
      pk.put("DeptID", 1);
      r = getRow(pk, api);
      assertEquals(r.get("NAME").asString().get(), "foe");

      sr = store.executeSync("DROP TABLE Test_SGSqlInsertByDefault3");
      assertTrue(sr.isSuccessful());
      assertTrue(sr.isDone());
  }

    @Test
    public void testAlterSG()
        throws Exception {

        StatementResult res;
        String q = "DROP TABLE IF EXISTS t ";
        store.executeSync(q);

        q = "create table if not exists T " +
            "(c1 integer generated by default as identity (start with 1 increment by 1 maxvalue 2147483647 cycle)," +
            "c2 long," +
            "c3 float," +
            "c4 double," +
            "c5 number," +
            "c6 string," +
            "c7 boolean," +
            "primary key(c1))";
        store.executeSync(q);

        TableImpl t = (TableImpl) (store.getTableAPI().getTable("T"));
        assertTrue(t.hasIdentityColumn());
        assertEquals(0, t.getIdentityColumn());
        assertNotNull(t.getIdentityColumnInfo());
        assertEquals(0, t.getIdentityColumnInfo().getIdentityColumn());
        assertFalse(t.getIdentityColumnInfo().isIdentityGeneratedAlways());
        assertFalse(t.getIdentityColumnInfo().isIdentityOnNull());
        assertNotNull(t.getIdentitySequenceDef());

        q = "insert into T values (DEFAULT, 1, 1, 1," +
            " 1, 'str1', true " + ")";
        res = store.executeSync(q);
        assertTrue(res.isSuccessful());

        q = "select * from T";
        res = store.executeSync(q);
        for (RecordValue row : res) {
            assertEquals(1, row.get("c1").asInteger().get());
            assertEquals(1, row.get("c2").asLong().get());
            assertEquals(1, row.get("c3").asFloat().get(), 0.1);
        }

        t = (TableImpl) (store.getTableAPI().getTable("T"));
        assertTrue(t.hasIdentityColumn());
        assertEquals(0, t.getIdentityColumn());
        assertNotNull(t.getIdentityColumnInfo());
        assertEquals(0, t.getIdentityColumnInfo().getIdentityColumn());
        assertFalse(t.getIdentityColumnInfo().isIdentityGeneratedAlways());
        assertFalse(t.getIdentityColumnInfo().isIdentityOnNull());
        assertNotNull(t.getIdentitySequenceDef());

        q = "alter table T (drop c2)";
        res = store.executeSync(q);
        assertTrue(res.isSuccessful());

        t = (TableImpl) (store.getTableAPI().getTable("T"));
        assertTrue(t.hasIdentityColumn());
        assertEquals(0, t.getIdentityColumn());
        assertNotNull(t.getIdentityColumnInfo());
        assertEquals(0, t.getIdentityColumnInfo().getIdentityColumn());
        assertFalse(t.getIdentityColumnInfo().isIdentityGeneratedAlways());
        assertFalse(t.getIdentityColumnInfo().isIdentityOnNull());
        assertNotNull(t.getIdentitySequenceDef());

        q = "insert into T values (DEFAULT, 2, 2," +
            " 2, 'str2', true " + ")";
        res = store.executeSync(q);
        assertTrue(res.isSuccessful());

        q = "select * from T order by c1";
        res = store.executeSync(q);
        int i = 0;
        for (RecordValue row : res) {
            i++;
            assertEquals(i, row.get("c1").asInteger().get());
            assertEquals(i, row.get("c3").asFloat().get(), 0.1);
        }

        q = "select * from SYS$SGAttributesTable";
        res = store.executeSync(q);
        for (RecordValue row : res) {
            assertEquals("INTERNAL", row.get("SGType").asString().get());
            assertTrue(row.get("SGName").asString().get().contains(".c1"));
            assertEquals("INTEGER", row.get("DataType").asString().get());
        }
    }

    @Test
    public void testAlterDropCol() {
        String q = "CREATE TABLE DropIdentity(ID INTEGER, NAME STRING, " +
            "DeptID INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 " +
            "INCREMENT BY 10 MAXVALUE 100), PRIMARY KEY (ID))";
        executeDdl(q);

        assertSgSysTableContains("DeptID", true, 1, 10);

        q = "ALTER TABLE DropIdentity (DROP DeptID)";
        executeDdl(q);

        assertSgSysTableContains("DeptID", false, 0, 0);

        q = "ALTER TABLE DropIdentity (ADD DeptID INTEGER GENERATED ALWAYS " +
            "AS IDENTITY (START WITH 2 INCREMENT BY 20 MAXVALUE 200))";
        executeDdl(q);

        assertSgSysTableContains("DeptID", true, 2, 20);

        q = "ALTER TABLE DropIdentity (MODIFY DeptID DROP IDENTITY)";
        executeDdl(q);

        assertSgSysTableContains("DeptID", false, 0, 0);

        q = "ALTER TABLE DropIdentity (MODIFY DeptID GENERATED ALWAYS" +
            " AS IDENTITY (START WITH 3 INCREMENT BY 30 MAXVALUE 300))";
        executeDdl(q);

        assertSgSysTableContains("DeptID", true, 3, 30);

        q = "ALTER TABLE DropIdentity (DROP name)";
        executeDdl(q);

        assertSgSysTableContains("DeptID", true, 3, 30);

        q = "ALTER TABLE DropIdentity (ADD name STRING)";
        executeDdl(q);

        assertSgSysTableContains("DeptID", true, 3, 30);
    }

    private void assertSgSysTableContains(String val, boolean contains,
        int startWith, int incrementBy) {
        String q;
        q = "select * from SYS$SGAttributesTable";
        boolean exists = false;
        StatementResult sr = store.executeSync(q);
        for (RecordValue r : sr) {
            if (r.get("SGType").asString().get().equals("INTERNAL") &&
                r.get("SGName").asString().get().contains(val))
            {
                if (!contains) {
                    exists = true;
                    break;
                }
                if (contains && startWith ==
                    r.get("StartWith").asNumber().get().intValue() &&
                    incrementBy == r.get("IncrementBy").asLong().get()) {
                    exists = true;
                    break;
                }
            }
        }
        assertEquals("SYS$SGAttributesTable should " + (contains ? "" :"not " ) +
            "contain identity column: " + val, contains, exists);
    }

    private class CacheTestHook implements TestHook<Integer> {

        private int cacheSize;
        private int increment;
        private Boolean correctCacheSize = null;

        public CacheTestHook(int cacheSize, int increment) {
            this.cacheSize = cacheSize;
            this.increment = increment;
        }

        @Override
        public void doHook(Integer cacheTotal) {
            correctCacheSize = cacheTotal == (cacheSize-1) * increment;
        }

        public Boolean isCorrectCacheSize() {
            return correctCacheSize;

        }

    }



    private void executeCheckResult(List<TableOperationResult> results,
                                       int startIndex,
                                       TableAPI api,
                                       Table table,
                                       int diff)
        throws InterruptedException {
        assertEquals("Unexpected result size", 100, results.size());
        for (TableOperationResult res : results) {
            assertNotNull(res.getNewVersion());
            assertTrue(res.getSuccess());
        }

        for (int i = startIndex; i <= (startIndex + 99); i++) {
            PrimaryKey pk = table.createPrimaryKey();
            pk.put("accuNum", 0);
            pk.put("name", "smith" + i);
            Row r = getRow(pk, api);
            assertNotNull(r);
            assertEquals(i + diff, r.get("id").asInteger().get());
        }
    }


    private void ddlTestBase(int putTotal,
                             Object start,
                             Object increment,
                             Object maxMin,
                             Type type,
                             boolean cycle,
                             String tableName)
        throws InterruptedException {
        TableAPI api = store.getTableAPI();
        Table table = api.getTable(tableName);

        Object current = start;

        boolean overflow = false;
        Row row = table.createRow();
        for (int i = 0; i < putTotal; i++) {
            row.put("name", "smith" + current.toString());

            PrimaryKey pk = table.createPrimaryKey();

            boolean expectException = false;
            Object last = current;
            boolean positiveIncrement = true;

            switch(type) {
            case INTEGER:
                pk.put("id", (int)current);
                positiveIncrement = (Integer)increment > 0;
                if (maxMin != null && (((Integer)increment > 0 &&
                    ((Integer)current > (Integer)maxMin || overflow) ||
                    ((Integer)increment < 0 &&
                    ((Integer)current < (Integer)maxMin || overflow))))) {
                    overflow = false;
                    if (cycle) {
                        current = start;
                        continue;
                    }
                    expectException = true;
                } else {
                    int next = (Integer)current + (Integer)increment;
                    if (((Integer)increment > 0 && next < (Integer)current) ||
                        ((Integer)increment < 0 && next > (Integer)current)) {
                        overflow = true;
                    } else {
                        current = next;
                    }
                }
                break;
            case LONG:
                pk.put("id", (long)current);
                positiveIncrement = (Long)increment > 0;
                if (maxMin != null && (((Long)increment > 0 &&
                    ((Long)current > (Long)maxMin ||
                    overflow) || ((Long)increment < 0 &&
                    ((Long)current < (Long)maxMin || overflow))))) {
                    overflow = false;
                    if (cycle) {
                        current = start;
                        continue;
                    }
                    expectException = true;
                } else {
                    long next = (Long)current + (Long)increment;
                    if (((Long)increment > 0 && next < (Long)current) ||
                        ((Long)increment < 0 && next > (Long)current)) {
                        overflow = true;
                    } else {
                        current = next;
                    }
                }
                break;
            case NUMBER:
                pk.put("id", FieldValueFactory.
                       createNumber((BigDecimal)current));
                positiveIncrement = ((BigDecimal)increment).
                    compareTo(new BigDecimal(0)) == 1;
                if (maxMin != null && ((positiveIncrement &&
                    (((BigDecimal)current).compareTo((BigDecimal)maxMin) > 0 ||
                    overflow)) || (!positiveIncrement &&
                    (((BigDecimal)current).compareTo((BigDecimal)maxMin) < 0 ||
                    overflow)))) {
                    overflow = false;
                    if (cycle) {
                        current = start;
                        continue;
                    }
                    expectException = true;
                } else {
                    BigDecimal next = ((BigDecimal)current).
                        add((BigDecimal)increment);
                    if ((positiveIncrement &&
                        next.compareTo((BigDecimal)current) < 0) ||
                        (!positiveIncrement &&
                        next.compareTo((BigDecimal)current) > 0)) {
                        overflow = true;
                    } else {
                        current = next;
                    }
                }
                break;
            default:
                fail("Unknown value type.");
            }
            try {
                api.put(row, null, null);
                if (expectException) {
                    fail("Exception expected.");
                }
            } catch (Exception e) {
                if (expectException) {
                    assertTrue(e instanceof IllegalArgumentException);
                    if (positiveIncrement) {
                        assertThat("Exceed max. ", e.getMessage(),
                                   containsString("Current value cannot exceed "
                                                  + "max value"));
                    } else{
                        assertThat("Exceed max. ", e.getMessage(),
                                   containsString("Current value cannot exceed "
                                                  + "min value"));
                    }
                    break;
                }
                e.printStackTrace();
                fail("Exception is not expected");
            }
            Row r = getRow(pk, api);
            if (r == null) {
                fail("Time out.");
            } else {
                assertEquals(r.get("name").toString(), "smith" +
                             last.toString());
            }
        }

    }

    @Test
    public void testNoCache() {
        StatementResult resultTable;
        String query;

        query = "CREATE TABLE IF NOT EXISTS TEST (id INTEGER " +
            "GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 " +
            "NO CYCLE NO CACHE), pk INTEGER, name STRING, PRIMARY KEY(pk))";
        resultTable = store.executeSync(query);

        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("TEST");

        // Test if sequence number is generated when coll not part of key with
        // all put flavors: put, putIfAbsent, putIfPresent, putIfVersion

        // put
        Row row = table.createRow();
        row.put("pk", 1);
        row.put("name", "j1");
        // row[id] will be set inside api.put() call
        Version res = api.put(row, null, null);
        assertNotNull(res);
        assertEquals(1, row.get("id").asInteger().get());

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("pk", 1);
        Row dbRow = api.get(pk, null);
        assertEquals(1, dbRow.get("id").asInteger().get());


        // putIfAbsent without a row available
        row = table.createRow();
        row.put("pk", 2);
        row.put("name", "j2-1");
        // row[id] will be set inside api.putIfAbsent() call
        res = api.putIfAbsent(row, null, null);
        assertNotNull(res);
        assertEquals(2, row.get("id").asInteger().get());

        pk.put("pk", 2);
        dbRow = api.get(pk, null);
        assertEquals(2, dbRow.get("id").asInteger().get());


        // putIfPresent with a row present
        row = table.createRow();
        row.put("pk", 2);
        row.put("name", "j2-2");
        res = api.putIfPresent(row,null,null);
        assertNotNull(res);
        assertEquals(3, row.get("id").asInteger().get());

        pk.put("pk",2);
        dbRow = api.get(pk,null);
        assertEquals(3, dbRow.get("id").asInteger().get());


        // putIfVersion with a version available
        row = table.createRow();
        row.put("pk",2);
        row.put("name","j2-3");
        res = api.putIfVersion(row, res,null,null);
        assertNotNull(res);
        assertEquals(4, row.get("id").asInteger().get());

        pk.put("pk", 2);
        dbRow = api.get(pk, null);
        assertEquals(4, dbRow.get("id").asInteger().get());


        // putIfPresent without a row available
        row = table.createRow();
        row.put("pk", 3);
        row.put("name", "j3");
        res = api.putIfPresent(row, null, null);
        assertNull(res);
        assertEquals(5, row.get("id").asInteger().get());

        pk.put("pk", 3);
        dbRow = api.get(pk, null);
        assertNull(dbRow);
    }

    @Test
    public void testPutFlavorsGenAlways() {
        StatementResult resultTable;
        String query;

        query = "CREATE TABLE IF NOT EXISTS TEST (id INTEGER " +
            "GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 " +
            "NO CYCLE NO CACHE), name STRING, PRIMARY KEY(id))";
        resultTable = store.executeSync(query);

        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("TEST");

        // Test if sequence number is generated when coll is part of key with
        // all put flavors: put, putIfAbsent, putIfPresent, putIfVersion

        // put
        Row row = table.createRow();
        row.put("name", "j1");
        Version version = api.put(row, null, null);
        assertNotNull(version);
        int id = row.get("id").asInteger().get();
        assertEquals(1, id);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        Row dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j1", dbRow.get("name").asString().get());
        Version versionR1 = version;


        // putIfAbsent without a row
        row = table.createRow();
        row.put("name", "j2");
        version = api.putIfAbsent(row, null, null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2", dbRow.get("name").asString().get());


        // putIfPresent without a row
        row = table.createRow();
        row.put("name", "j3");
        version = api.putIfPresent(row,null,null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(3, id);

        pk.put("id", id);
        dbRow = api.get(pk,null);
        assertNull(dbRow);

        // putIfPresent with a row in db and GENERATED ALWAYS,
        // SPECIAL CASE: identity value must be included in the primary key!
        // This is the only way other than SQL update to update a row when GENERATED ALWAYS is used
        row = table.createRow();
        row.put("name", "j4");
        version = api.put(row,null,null);
        assertNotNull(version);
        assertNotNull(row.get("id"));
        id = row.get("id").asInteger().get();
        pk.put("id", id);
        dbRow = api.get(pk,null);
        assertNotNull(dbRow);
        // the row exists now, putIfPresent with key will update the row
        Row updatedRow = table.createRow();
        updatedRow.put("id", id);
        assertEquals(id, updatedRow.get("id").asInteger().get());
        updatedRow.put("name", "j4 - updated");
        version = api.putIfPresent(updatedRow, null, null);
        assertNotNull(version);
        dbRow = api.get(pk,null);
        assertNotNull(dbRow);
        assertEquals(id, updatedRow.get("id").asInteger().get());
        assertEquals(updatedRow.get("id").asInteger().get(), dbRow.get("id").asInteger().get());
        assertEquals(updatedRow.get("name").asString().get(), dbRow.get("name").asString().get());


        // putIfVersion without a row
        row = table.createRow();
        row.put("name","j5");
        version = api.putIfVersion(row, versionR1,null,null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(5, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertNull(dbRow);


        // row.put on col with generated always throws exception
        row = table.createRow();
        try {
            row.put("id", 1);
        } catch (IllegalArgumentException e) {
            fail("Row.put() should work even for GENERATED ALWAYS id col.");
        }
    }

    @Test
    public void testPutFlavorsGenByDefault() {
        StatementResult resultTable;
        String query;

        query = "CREATE TABLE IF NOT EXISTS TEST (id INTEGER " +
            "GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1 " +
            "NO CYCLE NO CACHE), name STRING, PRIMARY KEY(id))";
        resultTable = store.executeSync(query);

        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("TEST");

        // Test if sequence number is generated when coll is part of key with
        // all put flavors: put, putIfAbsent, putIfPresent, putIfVersion

        // put without a row
        Row row = table.createRow();
        row.put("name", "j1");
        Version version = api.put(row, null, null);
        assertNotNull(version);
        int id = row.get("id").asInteger().get();
        assertEquals(1, id);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        Row dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j1", dbRow.get("name").asString().get());
        Version versionR1 = version;


        // putIfAbsent with absent row
        row = table.createRow();
        row.put("name", "j2-1");
        version = api.putIfAbsent(row, null, null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2-1", dbRow.get("name").asString().get());


        // putIfAbsent with row already available
        row = table.createRow();
        row.put("id", 2);
        row.put("name", "j2-3");
        version = api.putIfAbsent(row, null, null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2-1", dbRow.get("name").asString().get());


        // putIfPresent with row already present
        row = table.createRow();
        row.put("id", 2);
        row.put("name", "j2-2");
        version = api.putIfPresent(row, null, null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2-2", dbRow.get("name").asString().get());


        // putIfPresent with row missing
        row = table.createRow();
        row.put("name", "j3");
        version = api.putIfPresent(row,null,null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(3, id);

        pk.put("id", id);
        dbRow = api.get(pk,null);
        assertNull(dbRow);


        // putIfVersion with missing row
        row = table.createRow();
        row.put("name","j4");
        version = api.putIfVersion(row, versionR1,null,null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(4, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertNull(dbRow);


        // putIfVersion with version available
        row = table.createRow();
        row.put("id", 1);
        row.put("name","j1-2");
        version = api.putIfVersion(row, versionR1,null,null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(1, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertNotNull(dbRow);
        assertEquals("j1-2", dbRow.get("name").asString().get());
    }

    @Test
    public void testPutFlavorsGenByDefaultOnNull() {
        StatementResult resultTable;
        String query;

        query = "CREATE TABLE IF NOT EXISTS TEST (id INTEGER " +
            "GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 1 INCREMENT" +
            " BY 1 NO CYCLE NO CACHE), name STRING, PRIMARY KEY(id))";
        resultTable = store.executeSync(query);

        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("TEST");

        // Test if sequence number is generated when coll is part of key with
        // all put flavors: put, putIfAbsent, putIfPresent, putIfVersion

        // put without a row
        Row row = table.createRow();
        try {
            // this is not allowed because id coll is part of key and those
            // columns are not nullable
            row.putNull("id");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertEquals("Field \"id\" is not nullable", e.getMessage());
        }
        // row[id] not set
        row.put("name", "j1");
        Version version = api.put(row, null, null);
        assertNotNull(version);
        int id = row.get("id").asInteger().get();
        assertEquals(1, id);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", id);
        Row dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j1", dbRow.get("name").asString().get());
        Version versionR1 = version;


        // putIfAbsent with absent row
        row = table.createRow();
        // row[id] not set
        row.put("name", "j2-1");
        version = api.putIfAbsent(row, null, null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2-1", dbRow.get("name").asString().get());


        // putIfAbsent with row already available
        row = table.createRow();
        row.put("id", 2);
        row.put("name", "j2-3");
        version = api.putIfAbsent(row, null, null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2-1", dbRow.get("name").asString().get());


        // putIfPresent with row already present
        row = table.createRow();
        row.put("id", 2);
        row.put("name", "j2-2");
        version = api.putIfPresent(row, null, null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(2, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertEquals(id, dbRow.get("id").asInteger().get());
        assertEquals("j2-2", dbRow.get("name").asString().get());


        // putIfPresent with row missing
        row = table.createRow();
        // row[id] not set
        row.put("name", "j3");
        version = api.putIfPresent(row,null,null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(3, id);

        pk.put("id", id);
        dbRow = api.get(pk,null);
        assertNull(dbRow);


        // putIfVersion with missing row
        row = table.createRow();
        // row[id] not set
        row.put("name","j4");
        version = api.putIfVersion(row, versionR1,null,null);
        assertNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(4, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertNull(dbRow);


        // putIfVersion with version available
        row = table.createRow();
        row.put("id", 1);
        row.put("name","j1-2");
        version = api.putIfVersion(row, versionR1,null,null);
        assertNotNull(version);
        id = row.get("id").asInteger().get();
        assertEquals(1, id);

        pk.put("id", id);
        dbRow = api.get(pk, null);
        assertNotNull(dbRow);
        assertEquals("j1-2", dbRow.get("name").asString().get());
    }

    /*
     * Tests the path where there is no actual payload in the row. In this
     * case the key is generated and everything else is defaulted.
     */
    @Test
    public void testNoPayload() throws Exception {
        final String createTable = "create table idIsKey(" +
            "id integer generated always as identity," +
            "document json, primary key(id))";
        final String createTable1 = "create table idIsNotKey(" +
            "id integer, notkey integer generated always as identity," +
            "document json, primary key(id))";
        final String createTable2 = "create table idIsKey1(" +
            "id integer generated always as identity, id1 integer," +
            "document json, primary key(id, id1))";
        StatementResult resultTable = store.executeSync(createTable);
        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        resultTable = store.executeSync(createTable1);
        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        resultTable = store.executeSync(createTable2);
        assertNotNull(resultTable);
        assertTrue(resultTable.isSuccessful());

        /* will work because key is identity column */
        Table table = tableImpl.getTable("idIsKey");
        Row row = table.createRow();
        tableImpl.put(row, null, null);
        assertEquals(row.get("id").asInteger().get(), 1);

        /* will fail -- identity column is not the only key field */
        table = tableImpl.getTable("idIsKey1");
        row = table.createRow();
        try {
            tableImpl.put(row, null, null);
            fail("Put should have failed due to empty key");
        } catch (Exception e) {
            // success
        }

        /* will fail -- identity column is not a key */
        table = tableImpl.getTable("idIsNotKey");
        row = table.createRow();
        try {
            tableImpl.put(row, null, null);
            fail("Put should have failed due to empty key");
        } catch (Exception e) {
            // success
        }
    }

    @Test
    public void testSequenceImpl() throws Exception {
        StatementResult sr = store.executeSync("CREATE Table TestSGTable"
                + "(id INTEGER GENERATED ALWAYS AS IDENTITY,"
                + "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        TableAPI api = store.getTableAPI();
        TableImpl table = (TableImpl) api.getTable("TestSGTable");
        try {
            SequenceImpl.getSgName(table, 1);
            fail("Exception throw for wrong identity column");
        } catch (IllegalStateException e) {
            /* success */
        }

        SequenceImpl.SGKey sgkey = new SequenceImpl.SGKey(SGType.INTERNAL,
                SequenceImpl.getSgName(table), table.getId());
        SequenceImpl.SGKey sgkeyExt = new SequenceImpl.SGKey(SGType.EXTERNAL,
                SequenceImpl.getSgName(table), table.getId());
        SequenceImpl.SGKey sgSame = sgkey;
        assertFalse(sgkey.equals(sgkeyExt));
        assertTrue(sgkey.equals(sgSame));
        assertEquals(sgkey.toString(), sgSame.toString());

        /* No SG */
        StatementResult sr1 = store.executeSync("CREATE Table TestTable"
                + "(id INTEGER, name STRING, PRIMARY KEY (id))");
        assertTrue(sr1.isSuccessful());
        assertTrue(sr1.isDone());
        table = (TableImpl) api.getTable("TestTable");
        assertEquals(null, SequenceImpl.getSgName(table));

        SequenceImpl.SGKey sgkeyOther = new SequenceImpl.SGKey(SGType.INTERNAL,
                SequenceImpl.getSgName(table), table.getId());
        assertFalse(sgkey.equals(sgkeyOther));

        /* SGIntegerValues.writeFastExternal */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        SequenceImpl.SGValues<?> sgInt = SequenceImpl.SGValues
                .newInstance(Type.INTEGER, 2);
        sgInt.writeFastExternal(dos, SerialVersion.CURRENT);

        SGIntegerValues sgRet = (SGIntegerValues) sgInt
                .deserializedForm(SerialVersion.CURRENT);
        assertEquals(sgInt.toString(), sgRet.toString());

        /* Update */
        BigDecimal bdCurr = new BigDecimal(10);
        BigDecimal bdLast = new BigDecimal(1);
        sgInt.update(bdCurr, bdLast);
        sgInt.writeFastExternal(dos, SerialVersion.CURRENT);
        SGIntegerValues sgRetNew = (SGIntegerValues) sgInt
                .deserializedForm(SerialVersion.CURRENT);
        assertEquals(sgInt.toString(), sgRetNew.toString());

        /* SGValues.equal */
        SequenceImpl.SGValues<?> sgIntOther = SequenceImpl.SGValues
                .newInstance(Type.INTEGER, 5);
        SequenceImpl.SGValues<?> sgLong = SequenceImpl.SGValues
                .newInstance(Type.LONG, 5);
        SequenceImpl.SGValues<?> sgIntSame = SequenceImpl.SGValues
                .newInstance(Type.INTEGER, 2);
        assertFalse(sgInt.equals(sgLong));
        assertFalse(sgInt.equals(sgIntSame));
        SequenceImpl.SGValues<?> sgIntComp = sgIntOther;
        assertTrue(sgIntOther.equals(sgIntComp));
        assertFalse(sgInt.equals(sgIntOther));
        sgIntSame.update(bdCurr, bdLast);
        assertTrue(sgInt.equals(sgIntSame));
        assertEquals(sgInt.toString(), sgIntSame.toString());
        BigDecimal bdCurrUpdate = new BigDecimal(11);
        sgIntSame.update(bdCurrUpdate, bdLast);
        assertFalse(sgInt.equals(sgIntSame));
        BigDecimal bdLastUpdate = new BigDecimal(2);
        sgIntSame.update(bdCurrUpdate, bdLastUpdate);
        assertFalse(sgInt.equals(sgIntSame));
        sgIntSame.update(bdCurr, bdLastUpdate);
        assertFalse(sgInt.equals(sgIntSame));

        try {
            @SuppressWarnings("unused")
            SequenceImpl.SGValues<?> sgVal = SequenceImpl.SGValues
                    .newInstance(null, 1);
            fail("Not a valid type throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        try {
            @SuppressWarnings("unused")
            SequenceImpl.SGValues<?> sgVal = SequenceImpl.SGValues
                    .newInstance(Type.DOUBLE, 1);
            fail("Not a valid type throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* SGLongValues.writeFastExternal */
        sgLong.writeFastExternal(dos, SerialVersion.CURRENT);
        SGLongValues sgRetLong = (SGLongValues) sgLong
                .deserializedForm(SerialVersion.CURRENT);
        assertEquals(sgLong.toString(), sgRetLong.toString());

        /* SGNumberValues.writeFastExternal */
        SequenceImpl.SGValues<?> sgNumber = SequenceImpl.SGValues
                .newInstance(Type.NUMBER, 2);
        sgNumber.writeFastExternal(dos, SerialVersion.CURRENT);
        SGNumberValues sgRetNumber = (SGNumberValues) sgNumber
                .deserializedForm(SerialVersion.CURRENT);
        assertEquals(sgNumber.toString(), sgRetNumber.toString());
    }

    @Test
    public void testSGAttributes() throws Exception {
        StatementResult sr = store.executeSync("CREATE Table TestSGTable1"
                + "(id INTEGER GENERATED ALWAYS AS IDENTITY,"
                + "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        TableAPI api = store.getTableAPI();
        TableImpl table = (TableImpl) api.getTable("TestSGTable1");

        /* SG Attributes 1 */
        Table sysTable = api.getTable("SYS$SGAttributesTable");
        PrimaryKey sysPk = sysTable.createPrimaryKey();
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                SGType.INTERNAL.name());
        sysPk.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                table.getId() + "." + "id");
        Row sysRow = api.get(sysPk, null);
        SGAttributes sgAttr = new SGAttributes(sysRow);

        StatementResult srOther = store.executeSync("CREATE Table TestSGTable2"
                + "(id INTEGER GENERATED ALWAYS AS IDENTITY,"
                + "name STRING, PRIMARY KEY (id))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        TableImpl tableOther = (TableImpl) api.getTable("TestSGTable2");

        /* SG Attributes 2 */
        Table sysTableOther = api.getTable("SYS$SGAttributesTable");
        PrimaryKey sysPkOther = sysTableOther.createPrimaryKey();
        sysPkOther.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                SGType.INTERNAL.name());
        sysPkOther.put(SGAttributesTableDesc.COL_NAME_SGNAME,
                tableOther.getId() + "." + "id");
        Row sysRowOther = api.get(sysPkOther, null);
        SGAttributes sgAttrOther = new SGAttributes(sysRowOther);

        assertTrue(sgAttr.equals(sgAttrOther));
        assertEquals(sgAttr.toString(), sgAttrOther.toString());
        assertEquals(sgAttr.hashCode(), sgAttrOther.hashCode());

        srOther = store.executeSync("ALTER Table TestSGTable2 "
                + "(MODIFY id GENERATED ALWAYS AS IDENTITY "
                + "(START WITH 2))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        sysRowOther = api.get(sysPkOther, null);
        sgAttrOther = new SGAttributes(sysRowOther);
        assertFalse(sgAttr.equals(sgAttrOther));

        srOther = store.executeSync("ALTER Table TestSGTable2 "
                + "(MODIFY id GENERATED ALWAYS AS IDENTITY "
                + "(START WITH 1 INCREMENT BY 2))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        sysRowOther = api.get(sysPkOther, null);
        sgAttrOther = new SGAttributes(sysRowOther);
        assertFalse(sgAttr.equals(sgAttrOther));

        srOther = store.executeSync("ALTER Table TestSGTable2 "
                + "(MODIFY id GENERATED ALWAYS AS IDENTITY "
                + "(START WITH 1 INCREMENT BY 1 MAXVALUE 1000000))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        sysRowOther = api.get(sysPkOther, null);
        sgAttrOther = new SGAttributes(sysRowOther);
        assertFalse(sgAttr.equals(sgAttrOther));

        srOther = store.executeSync("ALTER Table TestSGTable2 "
                + "(MODIFY id GENERATED ALWAYS AS IDENTITY "
                + "(START WITH 1 INCREMENT BY 1 MAXVALUE 2147483647 "
                + "MINVALUE 0))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        sysRowOther = api.get(sysPkOther, null);
        sgAttrOther = new SGAttributes(sysRowOther);
        assertFalse(sgAttr.equals(sgAttrOther));

        srOther = store.executeSync("ALTER Table TestSGTable2 "
                + "(MODIFY id GENERATED ALWAYS AS IDENTITY "
                + "(START WITH 1 INCREMENT BY 1 MAXVALUE 2147483647 MINVALUE "
                + "-2147483648 CACHE 2500))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        sysRowOther = api.get(sysPkOther, null);
        sgAttrOther = new SGAttributes(sysRowOther);
        assertFalse(sgAttr.equals(sgAttrOther));

        srOther = store.executeSync("ALTER Table TestSGTable2 "
                + "(MODIFY id GENERATED ALWAYS AS IDENTITY "
                + "(START WITH 1 INCREMENT BY 1 MAXVALUE 2147483647 MINVALUE "
                + "-2147483648 CYCLE CACHE 1000))");
        assertTrue(srOther.isSuccessful());
        assertTrue(srOther.isDone());
        sysRowOther = api.get(sysPkOther, null);
        sgAttrOther = new SGAttributes(sysRowOther);
        assertFalse(sgAttr.equals(sgAttrOther));

        SGAttributes sgAttrSame = sgAttrOther;
        assertTrue(sgAttrOther.equals(sgAttrSame));

        try {
            @SuppressWarnings("unused")
            SGAttributes sgAttrNullRow = new SGAttributes(null);
            fail("Specifying null Row should throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }

        /* SG Attributes and Values */
        SGAttrsAndValues sgAttrAndVal = new SGAttrsAndValues();
        SGAttrsAndValues sgAttrAndValOther = new SGAttrsAndValues();
        SequenceImpl.SGValues<?> sgVal = SequenceImpl.SGValues
                .newInstance(Type.INTEGER, 1);
        sgAttrAndVal.setAttributes(sgAttr);
        sgAttrAndVal.setValueCache(sgVal);
        sgAttrAndValOther.setAttributes(sgAttr);
        sgAttrAndValOther.setValueCache(sgVal);
        /* Both Same */
        assertTrue(sgAttrAndVal.equals(sgAttrAndValOther));
        assertEquals(sgAttrAndVal.hashCode(), sgAttrAndValOther.hashCode());
        assertEquals(sgAttrAndVal.toString(), sgAttrAndValOther.toString());

        sgAttrAndValOther.setAttributes(sgAttrOther);
        sgAttrAndValOther.setValueCache(sgVal);
        /* Attribute different and value same */
        assertFalse(sgAttrAndVal.equals(sgAttrAndValOther));

        SequenceImpl.SGValues<?> sgValUpdate = SequenceImpl.SGValues
                .newInstance(Type.INTEGER, 1);
        BigDecimal bdCurr = new BigDecimal(10);
        BigDecimal bdLast = new BigDecimal(1);
        sgValUpdate.update(bdCurr, bdLast);
        sgAttrAndValOther.setAttributes(sgAttr);
        sgAttrAndValOther.setValueCache(sgValUpdate);
        /* Attribute same and value different */
        assertFalse(sgAttrAndVal.equals(sgAttrAndValOther));

        SGAttrsAndValues sgAttrAndValSame = sgAttrAndValOther;
        assertTrue(sgAttrAndValOther.equals(sgAttrAndValSame));

        TableLimits sysTableLimit = new TableLimits(100, 100, 1);
        try {
            ((TableImpl) sysTable).setTableLimits(sysTableLimit);
            fail("Setting limits for system table throw exception");
        } catch (IllegalCommandException e) {
            /* success */
        }
    }

    @Test
    public void testSGDef() throws Exception {
        Translator.IdentityDefHelper identityHelper = new IdentityDefHelper();
        identityHelper.setStart("min");

        FieldDefImpl fdi = new IntegerDefImpl();
        SequenceDefImpl sdi = new SequenceDefImpl(fdi, identityHelper);
        FieldValueImpl fvi = null;
        try {
            sdi.setStartValue(fvi);
            fail("Specifying null value for SG attributes throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        try {
            sdi.setMaxValue(fvi);
            fail("Specifying null value for SG attributes throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        try {
            sdi.setMinValue(fvi);
            fail("Specifying null value for SG attributes throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        try {
            sdi.setCacheValue(fvi);
            fail("Specifying null value for SG attributes throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        try {
            sdi.setIncrementValue(fvi);
            fail("Specifying null value for SG attributes throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        FieldValueImpl fviMin = new IntegerValueImpl(1);
        FieldValueImpl fviMax = new IntegerValueImpl(100);
        FieldValueImpl fviStart = new IntegerValueImpl(0);
        FieldValueImpl fviIncr = new IntegerValueImpl(1);
        FieldValueImpl fviCache = new IntegerValueImpl(5000);
        sdi.setMinValue(fviMin);
        sdi.setMaxValue(fviMax);
        sdi.setStartValue(fviStart);
        sdi.setIncrementValue(fviIncr);
        sdi.setCacheValue(fviCache);

        Translator.IdentityDefHelper identityHelperOther =
                new IdentityDefHelper();
        identityHelperOther.setStart("min");

        FieldDefImpl fdiOther = new IntegerDefImpl();
        SequenceDefImpl sdiOther = new SequenceDefImpl(fdiOther,
                identityHelperOther);
        assertFalse(sdi.equals(sdiOther));
        sdiOther.setStartValue(fviStart);
        fviIncr.setInt(2);
        sdi.setIncrementValue(fviIncr);
        assertFalse(sdi.equals(sdiOther));
        sdiOther.setIncrementValue(fviIncr);
        fviMin.setInt(2);
        sdi.setMinValue(fviMin);
        assertFalse(sdi.equals(sdiOther));
        sdiOther.setMinValue(fviMin);
        fviMax.setInt(101);
        sdi.setMaxValue(fviMax);
        assertFalse(sdi.equals(sdiOther));
        sdiOther.setMaxValue(fviMax);
        fviCache.setInt(6000);
        sdi.setCacheValue(fviCache);
        assertFalse(sdi.equals(sdiOther));
        sdiOther.setCacheValue(fviCache);
        sdi.setCycle(true);
        assertFalse(sdi.equals(sdiOther));
        sdiOther.setCycle(true);
        assertEquals(sdi.hashCode(), sdiOther.hashCode());
    }
}
