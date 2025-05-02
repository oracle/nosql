/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonMap;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import java.util.stream.Stream;

import oracle.kv.impl.api.ops.BasicClientTestBase;
import oracle.kv.impl.util.SerialTestUtils.SerialVersionChecker;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.FieldDef;

import org.junit.Test;

/** Test serial version compatibility for the table FieldDef classes. */
public class TableFieldDefsSerialTest extends BasicClientTestBase {

    /* Tests */

    @Test
    public void testAnyAtomicDefImpl() {
        checkDefs(serialVersionChecker(
                      new AnyAtomicDefImpl(),
                      0xadf2661d275d11e8L));
    }

    @Test
    public void testAnyDefImpl() {
        checkDefs(serialVersionChecker(
                      new AnyDefImpl(),
                      0xde28f98354f48e7cL));
    }

    @Test
    public void testAnyJsonAtomicDefImpl() {
        checkDefs(serialVersionChecker(
                      new AnyJsonAtomicDefImpl(),
                      0x300268d606563a63L));
    }

    @Test
    public void testAnyRecordDefImpl() {
        checkDefs(serialVersionChecker(
                      new AnyRecordDefImpl(),
                      0xdb5b55a9b215f744L));
    }

    @Test
    public void testArrayDefImpl() {
        checkDefs(serialVersionChecker(
                      new ArrayDefImpl(new AnyDefImpl(), "My description"),
                      0x64bd90d4ce8d76afL),
                  serialVersionChecker(
                      new ArrayDefImpl(new AnyDefImpl(),
                                       null /* description */),
                      0x2076ffa8ffcdc9b8L));
    }

    @Test
    public void testBinaryDefImpl() {
        checkDefs(serialVersionChecker(
                      new BinaryDefImpl("My description"),
                      0xecaa0f6c7f208503L),
                  serialVersionChecker(
                      new BinaryDefImpl(null /* description */),
                      0x289fd1f8a68036b7L));
    }

    @Test
    public void testBooleanDefImpl() {
        checkDefs(serialVersionChecker(
                      new BooleanDefImpl("My description"),
                      0xb6643d8eaeeec1b7L),
                  serialVersionChecker(
                      new BooleanDefImpl(null /* description */),
                      0x7d4aed081892510aL));
    }

    @Test
    public void testDoubleDefImpl() {
        checkDefs(serialVersionChecker(
                      new DoubleDefImpl("My description", 10.0, 20.0),
                      0xe6c8c6d446426cb1L),
                  serialVersionChecker(
                      new DoubleDefImpl(null /* description */,
                                        null /* min */,
                                        null /* max */),
                      0xcb46305ee4bf7ab5L));
    }

    @Test
    public void testEmptyDefImpl() {
        checkDefs(serialVersionChecker(
                      new EmptyDefImpl(),
                      0x1170c7bc5a746d6aL));
    }

    @Test
    public void testEnumDefImpl() {
        checkDefs(serialVersionChecker(
                      new EnumDefImpl(new String[] { "APPLE", "ORANGE" },
                                      "My description"),
                      0x919ce616e78a992eL),
                  serialVersionChecker(
                      new EnumDefImpl("Fruits",
                                      new String[] { "APPLE", "ORANGE" },
                                      "My description"),
                      0xc9c586a20cf393d9L),
                  serialVersionChecker(
                      new EnumDefImpl(new String[] { "APPLE", "ORANGE" },
                                      null /* description */),
                      0x4ce63b079438b6d8L));
    }

    @Test
    public void testFixedBinaryDefImpl() {
        checkDefs(serialVersionChecker(
                      new FixedBinaryDefImpl(34, "My description"),
                      0x1599707bd9584c23L),
                  serialVersionChecker(
                      new FixedBinaryDefImpl(35, null /* description */),
                      0x1f7ff9ebd8d31c0bL),
                  serialVersionChecker(
                      new FixedBinaryDefImpl("myName", 36, "My description"),
                      0xf2a7312966e5286cL));
    }

    @Test
    public void testFloatDefImpl() {
        checkDefs(serialVersionChecker(
                      new FloatDefImpl("My description", 10.0F, 20.0F),
                      0xbbc48f3aae5ee273L),
                  serialVersionChecker(
                      new FloatDefImpl("My description"),
                      0x9d1ed05effd769c6L),
                  serialVersionChecker(
                      new FloatDefImpl(null /* description */),
                      0x85c1a9bdfb6bb0f1L));
    }

    @Test
    public void testIntegerDefImpl() {
        checkDefs(serialVersionChecker(
                      new IntegerDefImpl(),
                      SerialVersion.MINIMUM, 0x9e19a574a40ab0bL),
                  serialVersionChecker(
                      new IntegerDefImpl("myDescription"),
                      SerialVersion.MINIMUM, 0x7b7accfb6a6eb7b4L));
    }

    @Test
    public void testJsonDefImpl() {
        checkDefs(serialVersionChecker(
                      new JsonDefImpl(
                          singletonMap("myField", FieldDef.Type.ANY),
                          "My description"),
                      SerialVersion.MINIMUM, 0x5fa10dd6dacf0d25L),
                  serialVersionChecker(
                      new JsonDefImpl("My description"),
                      SerialVersion.MINIMUM, 0xb78f00d741a78771L),
                  serialVersionChecker(
                      new JsonDefImpl(null /* description */),
                      SerialVersion.MINIMUM, 0xa408321593adc72aL));
    }

    @Test
    public void testLongDefImpl() {
        checkDefs(serialVersionChecker(
                      new LongDefImpl("My description", 10L, 20L),
                      SerialVersion.MINIMUM, 0x5d2a2e76c447135cL),
                  serialVersionChecker(
                      new LongDefImpl("My description"),
                      SerialVersion.MINIMUM, 0x77ab5df2e6c30604L),
                  serialVersionChecker(
                      new LongDefImpl(),
                      SerialVersion.MINIMUM, 0x4b1138bccc9fd70eL));
    }

    @Test
    public void testMapDefImpl() {
        checkDefs(serialVersionChecker(
                      new MapDefImpl(new AnyDefImpl(), "My description"),
                      0x89667d1ad2282d1aL),
                  serialVersionChecker(
                      new MapDefImpl(new AnyDefImpl(), null /* description */),
                      0x49c7ca7c0a06f02eL));
    }

    @Test
    public void testNumberDefImpl() {
        checkDefs(serialVersionChecker(
                      new NumberDefImpl(true /* isMRCounter */,
                                        "My description"),
                      SerialVersion.MINIMUM, 0x35a9c9a200011562L),
                  serialVersionChecker(
                      new NumberDefImpl("My description"),
                      SerialVersion.MINIMUM, 0x4a1e5261038aa295L),
                  serialVersionChecker(
                      new NumberDefImpl(),
                      SerialVersion.MINIMUM, 0xaf71536b2c4bc06L));
    }

    @Test
    public void testRecordDefImpl() {
        final FieldMap fieldMap = new FieldMap();
        fieldMap.put("num", new IntegerDefImpl(),
                     false, /* nullable */
                     null /* defaultValue */);
        checkDefs(serialVersionChecker(
                      new RecordDefImpl("myName", fieldMap, "My description"),
                      SerialVersion.MINIMUM, 0x8297cfddc6418280L),
                  serialVersionChecker(
                      new RecordDefImpl(fieldMap, null /* description */),
                      SerialVersion.MINIMUM, 0x6afb6a10af6c4015L));
    }

    @Test
    public void testStringDefImpl() {
        checkDefs(serialVersionChecker(
                      new StringDefImpl("My description",
                                        "aaa-min",
                                        "zzz-max",
                                        true, /* minInclusive */
                                        false, /* maxInclusive */
                                        true, /* isUUID */
                                        true /* generatedByDefault */),
                      SerialVersion.MINIMUM, 0x6103b550367d5effL),
                  serialVersionChecker(
                      new StringDefImpl("My description",
                                        "aaa-min",
                                        "zzz-max",
                                        true, /* minInclusive */
                                        false, /* maxInclusive */
                                        true, /* isUUID */
                                        false /* generatedByDefault */),
                      SerialVersion.MINIMUM, 0xa903e0b343cc319bL),
                  serialVersionChecker(
                      new StringDefImpl("My description",
                                        "aaa-min",
                                        "zzz-max",
                                        true, /* minInclusive */
                                        false /* maxInclusive */),
                      SerialVersion.MINIMUM, 0xc7914260c8fc7d57L),
                  serialVersionChecker(
                      new StringDefImpl(),
                      SerialVersion.MINIMUM, 0x3854335c464016c3L));
    }

    @Test
    public void testTimestampDefImpl() {
        checkDefs(serialVersionChecker(
                      new TimestampDefImpl(3, "My description"),
                      0x25e28b74b4a4b70eL),
                  serialVersionChecker(
                      new TimestampDefImpl(4),
                      0x55340ad45e832bc3L));
    }

    /* Other methods */

    /**
     * Check field defs, using FieldDefImpl.readFastExternal as the reader.
     */
    @SafeVarargs
    @SuppressWarnings({"all","varargs"})
    private static <T extends FieldDefImpl>
        void checkDefs(SerialVersionChecker<T>... checkers)
    {
        checkDefs(Stream.of(checkers));
    }

    /**
     * Check a stream of field defs, using FieldDefImpl.readFastExternal as the
     * reader.
     */
    private static <T extends FieldDefImpl>
        void checkDefs(Stream<SerialVersionChecker<T>> checkers)
    {
        checkAll(
            checkers.map(
                svc -> svc.reader(FieldDefImpl::readFastExternal)));
    }
}
