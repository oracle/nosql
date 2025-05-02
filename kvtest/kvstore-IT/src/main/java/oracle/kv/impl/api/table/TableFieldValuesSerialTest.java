/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;
import static oracle.kv.impl.util.SerialVersion.JSON_COLLECTION_VERSION;
import static oracle.kv.impl.util.SerialVersion.SCHEMALESS_TABLE_VERSION;
import static oracle.kv.util.TestUtils.checkAll;
import static oracle.kv.util.TestUtils.checkException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Stream;

import oracle.kv.impl.api.ops.BasicClientTestBase;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialTestUtils.SerialVersionChecker;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializeExceptionUtilTest;
import oracle.kv.Direction;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.TimeToLive;

import org.junit.Test;

/**
 * Test serial version compatibility for the table FieldValue and related
 * classes.
 */
public class TableFieldValuesSerialTest extends BasicClientTestBase {

    /* Tests for FieldValueImpl subclasses */

    @Test
    public void testArrayValueImpl() {
        final ArrayValueImpl arrayValueImpl =
            new ArrayValueImpl(new ArrayDefImpl(new AnyDefImpl()));
        checkNoWriteFastExternal(arrayValueImpl);
    }

    @Test
    public void testBinaryValueImpl() {
        checkValues(new BinaryDefImpl(),
                    serialVersionChecker(
                        new BinaryValueImpl(new byte[] { 1, 2 }),
                        0xeff6f27cbbda22adL));
    }

    @Test
    public void testBooleanValueImpl() {
        checkValues(new BooleanDefImpl(),
                    serialVersionChecker(
                        BooleanValueImpl.trueValue,
                        0xc92920944247d80cL));
    }

    @Test
    public void testDoubleRangeValue() {
        final DoubleDefImpl def = new DoubleDefImpl("My description");
        checkValues(def,
                    serialVersionChecker(
                        new DoubleRangeValue(2.0, def),
                        0x9d7649010c891529L),
                    serialVersionChecker(
                        new DoubleRangeValue("myKeyValue", def),
                        0x6c1cf382778d14d0L));
    }

    @Test
    public void testDoubleValueImpl() {
        checkValues(new DoubleDefImpl("My description"),
                    serialVersionChecker(
                        new DoubleValueImpl(3.0),
                        0x5d6b4ee4aafbdeddL),
                    serialVersionChecker(
                        new DoubleValueImpl("myKeyValue"),
                        0x662860b03575aacbL));
    }

    @Test
    public void testEmptyValueImpl() {
        checkValues(new EmptyDefImpl(),
                    serialVersionChecker(
                        EmptyValueImpl.getInstance(),
                        0x2d0134ed3b9de132L));
    }

    @Test
    public void testEnumValueImpl() {
        final EnumDefImpl def =
            new EnumDefImpl("Fruits",
                            new String[] { "APPLE", "ORANGE" },
                            "My description");
        checkValues(def,
                    serialVersionChecker(
                        new EnumValueImpl(def, "APPLE"),
                        0x3dd45990ec6b0716L));
    }

    @Test
    public void testFixedBinaryValueImpl() {
        final FixedBinaryDefImpl def = new FixedBinaryDefImpl("myName", 2);
        checkValues(def,
                    serialVersionChecker(
                        new FixedBinaryValueImpl(new byte[] { 3, 5 }, def),
                        0x5b881b1621b0fe7L),
                    serialVersionChecker(
                        new FixedBinaryValueImpl((byte[]) null, /* value */
                                                 def),
                        0xe26c10bb5c47f19aL));
    }

    @Test
    public void testFloatRangeValue() {
        final FloatDefImpl def = new FloatDefImpl("My description");
        checkValues(def,
                    serialVersionChecker(
                        new FloatRangeValue(3.0F, def),
                        0x59a56a60db120ab6L),
                    serialVersionChecker(
                        new FloatRangeValue("myKey", def),
                        0x231943013be64ec3L));
    }

    @Test
    public void testFloatValueImpl() {
        final FloatDefImpl def = new FloatDefImpl("My description");
        checkValues(def,
                    serialVersionChecker(
                        new FloatValueImpl(3.0F),
                        0xdecf1b2e93680b71L),
                    serialVersionChecker(
                        new FloatValueImpl("myKey"),
                        0xbeb67ee8dbad2056L));
    }

    @Test
    public void testIntegerCRDTValueImpl() {
        checkNoWriteFastExternal(new IntegerCRDTValueImpl());
    }

    @Test
    public void testIntegerRangeValue() {
        final IntegerDefImpl def = new IntegerDefImpl();
        checkValues(def,
                    serialVersionChecker(
                        new IntegerRangeValue(4, def),
                        0x82d459bb260f77b8L),
                    serialVersionChecker(
                        new IntegerRangeValue("myKey", def),
                        0x5258220329c92cdbL));
    }

    @Test
    public void testIntegerValueImpl() {
        checkValues(new IntegerDefImpl(),
                    serialVersionChecker(
                        new IntegerValueImpl(4),
                        0x5f18dd00a7b2f0f5L),
                    serialVersionChecker(
                        new IntegerValueImpl("myKey"),
                        0xa89c2289f3db9c62L));
    }

    @Test
    public void testLongCRDTValueImpl() {
        checkNoWriteFastExternal(new LongCRDTValueImpl());
    }

    @Test
    public void testLongRangeValue() {
        final LongDefImpl def = new LongDefImpl();
        checkValues(def,
                    serialVersionChecker(
                        new LongRangeValue(4, def),
                        0x4a0a739f89cfc3a2L),
                    serialVersionChecker(
                        new LongRangeValue("myKey", def),
                        0xb8411fe78ce00c02L));
    }

    @Test
    public void testLongValueImpl() {
        checkValues(new LongDefImpl(),
                    serialVersionChecker(
                        new LongValueImpl(4),
                        0x18b5e2e602f84732L),
                    serialVersionChecker(
                        new LongValueImpl("myKey"),
                        0xaa5c3132d9aac6b0L));
    }

    @Test
    public void testMapValueImpl() {
        checkNoWriteFastExternal(
            new MapValueImpl(new MapDefImpl(new AnyDefImpl())));
    }

    @Test
    public void testNullJsonValueImpl() {
        checkNoWriteFastExternal(
            NullJsonValueImpl.getInstance());
    }

    @Test
    public void testNullValueImpl() {
        checkNoWriteFastExternal(
            NullValueImpl.getInstance());
    }

    @Test
    public void testNumberCRDTValueImpl() {
        checkNoWriteFastExternal(
            new NumberCRDTValueImpl());
    }

    @Test
    public void testNumberValueImpl() {
        checkValues(new NumberDefImpl(),
                    serialVersionChecker(
                        new NumberValueImpl(20),
                        0x2bcc29cfc8fc9194L));
    }

    @Test
    public void testPrimaryKeyImpl() {
        final FieldMap map = new FieldMap();
        map.put("intField", new IntegerDefImpl(),
                false, /* nullable */
                null);
        final RecordDefImpl recordDef =
            new RecordDefImpl(map, "My description");
        final TableImpl table = TableBuilder.createTableBuilder("myName")
            .addInteger("id")
            .addString("val")
            .primaryKey("id")
            .buildTable();
        final PrimaryKeyImpl primaryKeyImpl =
            new PrimaryKeyImpl(recordDef, table);
        checkNoWriteFastExternal(primaryKeyImpl);
    }

    @Test
    public void testRecordValueImpl() {
        checkNoWriteFastExternal(new RecordValueImpl());
    }

    @Test
    public void testReturnRowImpl() {
        final FieldMap map = new FieldMap();
        final TableImpl table = TableBuilder.createTableBuilder("myName")
            .addInteger("id")
            .addString("val")
            .primaryKey("id")
            .buildTable();
        map.put("intField", new IntegerDefImpl(),
                false, /* nullable */
                null);
        checkNoWriteFastExternal(
            new ReturnRowImpl(new RecordDefImpl(map, "My description"),
                              table, ReturnRow.Choice.ALL));
    }

    @Test
    public void testStringRangeValue() {
        final StringDefImpl def = new StringDefImpl();
        checkValues(def,
                    serialVersionChecker(
                        new StringRangeValue("myString", def),
                        0xdbc4814875cb805dL));
    }

    @Test
    public void testStringValueImpl() {
        final StringDefImpl def = new StringDefImpl();
        checkValues(def,
                    serialVersionChecker(
                        new StringValueImpl("My string value"),
                        0x3077ba5ec453ef4cL),
                    serialVersionChecker(
                        new StringValueImpl(null /* value */),
                        0xa95f9a0a4b8ddfa1L));
    }

    @Test
    public void testTimestampValueImpl() {
        final TimestampDefImpl def = new TimestampDefImpl(3);
        checkValues(def,
                    serialVersionChecker(
                        new TimestampValueImpl(def, 42L),
                        0x77bd297dfc7d975cL));
    }

    @Test
    public void testTupleValue() {
        final FieldMap fieldMap = new FieldMap();
        fieldMap.put("val", new IntegerDefImpl(), false /* nullable */, null);
        checkNoWriteFastExternal(
            new TupleValue(new RecordDefImpl(fieldMap, "myDescription"),
                           new FieldValueImpl[] { new IntegerValueImpl(3) },
                           new int[] { 1 }));
    }

    /* Tests for related classes */

    @Test
    public void testFieldMap() {
        final FieldMap map2 = new FieldMap();
        map2.put("intField", new IntegerDefImpl(),
                 true, /* nullable */
                 new IntegerValueImpl(3));
        checkAll(serialVersionChecker(
                     new FieldMap(),
                     0x5ba93c9db0cff93fL),
                 serialVersionChecker(
                     map2,
                     SerialVersion.MINIMUM, 0x7720865b5ee64371L));
    }

    @Test
    public void testFieldMapEntry() {
        checkAll(serialVersionChecker(
                     new FieldMapEntry("myName", new IntegerDefImpl(),
                                       true, /* nullable */
                                       new IntegerValueImpl(3)),
                     SerialVersion.MINIMUM, 0x6d3f3ed296c8dab2L),
                 serialVersionChecker(
                     new FieldMapEntry("myName", new IntegerDefImpl(),
                                       false, /* nullable */
                                       null /* defaultValue */),
                     SerialVersion.MINIMUM, 0x8a1bcfbd9bf0cb19L),
                 serialVersionChecker(
                     new FieldMapEntry(
                         "myName",
                         new IntegerDefImpl(true /* isMRCounter */),
                         false, /* nullable */
                         null /* defaultValue */),
                     SerialVersion.MINIMUM, 0xceed54a22c0f5acfL));

    }

    @Test
    public void testFieldRange() {
        checkAll(serialVersionChecker(
                     new FieldRange("myFieldPath",
                                    new AnyAtomicDefImpl(),
                                    3),
                     0x7e2034b83ef96084L));
    }

    @Test
    public void testIdentityColumnInfo() {
        checkAll(serialVersionChecker(
                     new IdentityColumnInfo(4, true, false),
                     0x28622413e90bc025L));
    }

    @Test
    public void testIndexImpl() {
        final TableImpl table = TableBuilder.createTableBuilder("myName")
            .addInteger("id")
            .addString("val")
            .addJson("json", "description")
            .primaryKey("id")
            .buildTable();
        checkAll(
            Stream.of(
                serialVersionChecker(
                    new IndexImpl("myName",
                                  table,
                                  singletonList("json"),
                                  singletonList(FieldDef.Type.STRING),
                                  true, /* indexNulls */
                                  false, /* isUnique */
                                  singletonMap("val", "annotation"),
                                  singletonMap("val", "prop"),
                                  "My description"),
                    SerialVersion.MINIMUM, 0xc8194bc851fef6b8L,
                    QUERY_VERSION_16, 0x7526344d791d90efL),
                serialVersionChecker(
                    new IndexImpl("myName",
                                  table,
                                  singletonList("val"),
                                  null, /* types */
                                  false, /* indexNulls */
                                  true, /* isUnique */
                                  null, /* annotations */
                                  null, /* properties */
                                  null /* description */),
                    SerialVersion.MINIMUM, 0x77cf474194defdaeL,
                    QUERY_VERSION_16, 0xe6e848d098fd86a2L))
            .map(svc -> svc.reader(
                     (in, sv) -> new IndexImpl(in, sv, table))));
    }

    @Test
    public void testIndexKeyImpl() {
        final TableImpl table = TableBuilder.createTableBuilder("myName")
            .addInteger("id")
            .addString("val")
            .primaryKey("id")
            .buildTable();
        final IndexImpl index = new IndexImpl("indexName", table,
                                              singletonList("val"),
                                              singletonList(null),
                                              "description");
        final FieldMap fieldMap = new FieldMap();
        fieldMap.put("val", new IntegerDefImpl(), false /* nullable */, null);
        final IndexKeyImpl indexKey =
            new IndexKeyImpl(index,
                             new RecordDefImpl(fieldMap, "myDescription"));
        checkNoWriteFastExternal(indexKey);
    }

    @Test
    public void testIndexRange() {
        final TableImpl table = TableBuilder.createTableBuilder("myName")
            .addInteger("id")
            .addString("val")
            .primaryKey("id")
            .buildTable();
        final IndexImpl index = new IndexImpl("indexName", table,
                                              singletonList("val"),
                                              singletonList(null),
                                              "description");
        final FieldMap fieldMap = new FieldMap();
        fieldMap.put("val", new IntegerDefImpl(), false /* nullable */, null);
        final IndexKeyImpl indexKey =
            new IndexKeyImpl(index,
                             new RecordDefImpl(fieldMap, "myDescription"));
        checkAll(serialVersionChecker(
                     new IndexRange(indexKey,
                                    new FieldRange("myFieldPath",
                                                   new AnyAtomicDefImpl(),
                                                   3),
                                    Direction.FORWARD),
                     0x2fa2abc173c09209L),
                 serialVersionChecker(
                     new IndexRange(indexKey,
                                    null, /* range */
                                    Direction.UNORDERED),
                     0x9aeb87a621970708L));
    }

    @Test
    public void testTableImpl() {
        final TableImpl table = TableBuilder.createTableBuilder("myName")
            .addInteger("id")
            .addString("val")
            .primaryKey("id")
            .buildTable();
        checkAll(serialVersionChecker(
                     table,
                     SerialVersion.MINIMUM, 0x9f996711f7e27749L,
                     SCHEMALESS_TABLE_VERSION, 0x757204f05b7c954fL,
                     JSON_COLLECTION_VERSION, 0xd4cd21e87d4b06a2L)
                 .reader((in, sv) ->
                         new TableImpl(in, sv, null /* parent */)));
    }

    @Test
    public void testTableLimits() {
        checkAll(serialVersionChecker(
                     new TableLimits(1, 2, 3, 4, 5, 6, 7),
                     0x2d237ebcd14ef10L));
    }

    @Test
    public void testTableMetadata() {
        final FieldMap fieldMap = new FieldMap();
        fieldMap.put("id", new IntegerDefImpl(), false, /* nullable */ null);
        fieldMap.put("val", new StringDefImpl(), false, /* nullable */ null);

        final TableMetadata tableMetadata1 =
            new TableMetadata(true /* keepChanges */);
        tableMetadata1.setLocalRegionName("boston");
        tableMetadata1.addTable("myNamespace", "myTable",
                                null /* parent */,
                                singletonList("id"),
                                singletonList(2),
                                singletonList("id"),
                                fieldMap,
                                TimeToLive.ofHours(5),
                                new TableLimits(1, 2, 3, 4, 5, 6, 7),
                                false, /* r2compat */
                                4, /* schemaId */
                                "My description",
                                new ResourceOwner("myId", "myName"),
                                false, /* sysTable */
                                null, /* identityColumnInfo */
                                singleton(1), /* regionIds */
                                false, /* json collection */
                                null   /* mr counters for json collection */);
        tableMetadata1.addIndex("myNamespace", "myIndex", "myTable",
                                singletonList("val"),
                                null, /* types */
                                true, /* indexNulls */
                                true, /* isUnique */
                                "My description");
        tableMetadata1.addTextIndex("myNamespace", "myTextIndex", "myTable",
                                    singletonList(
                                        new IndexImpl.AnnotatedField(
                                            "val", "annotation")),
                                    singletonMap("propKey", "propVal"),
                                    "My description");

        checkAll(serialVersionChecker(
                     tableMetadata1,
                     SerialVersion.MINIMUM, 0x2ce5a1bd36005028L,
                     SCHEMALESS_TABLE_VERSION, 0xdd84145d183a8efdL,
                     JSON_COLLECTION_VERSION, 0x85a8ea004f0a6fcaL,
                     QUERY_VERSION_16, 0x221258a889bcf7d9L),
                 serialVersionChecker(
                     new TableMetadata(false /* keepChanges */),
                     SerialVersion.MINIMUM, 0x237c6f38ef9b06a3L,
                     QUERY_VERSION_16, 0x60ca111cb73e9593L));
    }

    @Test
    public void testTableMetadataJsonCollection() {
        final FieldMap jsonCollectionFieldMap = new FieldMap();
        jsonCollectionFieldMap.put("id", new IntegerDefImpl(),
                                   false, /* nullable */ null);
        HashMap<String, FieldDef.Type> jsonCollectionMRCounters =
            new HashMap<String, FieldDef.Type>();
        jsonCollectionMRCounters.put("counter", FieldDef.Type.LONG);

        final TableMetadata tableMetadata1 =
            new TableMetadata(true /* keepChanges */);
        tableMetadata1.setLocalRegionName("boston");
        tableMetadata1.addTable("myNamespace", "myJsonCollection",
                                null /* parent */,
                                singletonList("id"),
                                singletonList(1),
                                singletonList("id"),
                                jsonCollectionFieldMap,
                                TimeToLive.ofHours(5),
                                new TableLimits(1, 2, 3, 4, 5, 6, 7),
                                false, /* r2compat */
                                0, /* schemaId */
                                "JSON collection",
                                null, /* ResourceOwner */
                                false, /* sysTable */
                                null, /* identityColumnInfo */
                                singleton(1), /* regionIds */
                                true, /* json collection */
                                jsonCollectionMRCounters);

        checkAll(serialVersionChecker(
                     tableMetadata1,
                     JSON_COLLECTION_VERSION, 0x6e1320898e122560L,
                     QUERY_VERSION_16, 0x9e9c33072fe00095L));
    }

    @Test
    public void testTimeToLive() {
        checkAll(
            Stream.of(serialVersionChecker(
                          TimeToLive.ofHours(5),
                          0xfb0e1608e69fdec9L),
                      serialVersionChecker(
                          TimeToLive.ofDays(10),
                          0x2168966db715b4b6L),
                      serialVersionChecker(
                          TimeToLive.DO_NOT_EXPIRE,
                          0x9069ca78e7450a28L))
            .map(svc -> svc.reader(TimeToLive::readFastExternal)));
    }

    @Test
    public void testTableVersionException() {
        final TableVersionException exception =
            new TableVersionException(1);
        /* Use a fixed stack trace so we get the same hash */
        final StackTraceElement[] stack = {
            new StackTraceElement("cl", "meth", "file", 22)
        };
        exception.setStackTrace(stack);
        checkAll(serialVersionChecker(
                     exception,
                     0x7fbefc703ec12ff4L)
                 .equalsChecker(SerializeExceptionUtilTest::equalExceptions));
    }

    @Test
    public void testTargetTables() {
        final TableImpl parent = TableBuilder.createTableBuilder("parent")
            .addInteger("id")
            .primaryKey("id")
            .buildTable();
        final TableImpl child =
            TableBuilder.createTableBuilder("child", "My description", parent)
            .addString("childVal")
            .primaryKey("childVal")
            .buildTable();
        final TableImpl grandchild =
            TableBuilder.createTableBuilder("grandchild", "My description",
                                            child)
            .addString("grandchildVal")
            .primaryKey("grandchildVal")
            .buildTable();
        checkAll(serialVersionChecker(
                     new TargetTables(child,
                                      singletonList(grandchild),
                                      singletonList(parent)),
                     0x1a26374e5270c22eL),
                 serialVersionChecker(
                     new TargetTables(parent,
                                      null, /* checkTables */
                                      null /* ancestorTables */),
                     0xbd1c92bccdeb1db6L));
    }

    /* Other methods */

    /**
     * Check field values, using FieldValueImpl.readFastExternalOrNull as the
     * reader with the specified field definition.
     */
    @SafeVarargs
    @SuppressWarnings({"all","varargs"})
    private static <T extends FieldValueImpl>
        void checkValues(FieldDefImpl definition,
                         SerialVersionChecker<T>... checkers)
    {
        checkValues(definition, Stream.of(checkers));
    }

    /**
     * Check a stream of field defs, using
     * FieldValueImpl.readFastExternalOrNull as the reader with the specified
     * field definition.
     */
    private static <T extends FieldValueImpl>
        void checkValues(FieldDefImpl definition,
                         Stream<SerialVersionChecker<T>> checkers)
    {
        checkAll(
            checkers.map(
                svc -> svc.reader((in, sv) ->
                                  FieldValueImpl.readFastExternalOrNull(
                                      in, sv, definition))));
    }

    private static void checkNoWriteFastExternal(FastExternalizable obj) {
        try (final DataOutputStream out =
             new DataOutputStream(new ByteArrayOutputStream())) {
            checkException(
                () -> obj.writeFastExternal(out, SerialVersion.CURRENT),
                IllegalStateException.class,
                "FastExternal serialization not supported");
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
    }
}
