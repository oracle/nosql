/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readCharArray;
import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readJavaSerial;
import static oracle.kv.impl.util.SerializationUtil.readMap;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readProperties;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readTimeUnit;
import static oracle.kv.impl.util.SerializationUtil.writeArrayLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeCharArray;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeCollectionLength;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeJavaSerial;
import static oracle.kv.impl.util.SerializationUtil.writeMap;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullCollection;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeProperties;
import static oracle.kv.impl.util.SerializationUtil.writeSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeTimeUnit;
import static oracle.kv.util.TestUtils.checkException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.TestBase;
import oracle.kv.Value;

import org.junit.Before;
import org.junit.Test;

/** Tests for the {@link SerializationUtil} class. */
public class SerializationUtilTest extends TestBase {
    private static final short STD_UTF8 = SerialVersion.MINIMUM;

    private static final char A_UMLAUT = '\u00e4';
    private static final String MADCHEN = "M\u00e4dchen"; /* a with umlaut */

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final DataOutput out = new DataOutputStream(baos);

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        baos.reset();
    }

    /*
     * Tests
     */

    @Test
    public void testReadWriteNullString() throws Exception {
        try {
            SerializationUtil.writeNonNullString(out, STD_UTF8, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        SerializationUtil.writeString(out, STD_UTF8, null);
        try {
            readNonNullString(baos, STD_UTF8);
            fail("Expected IOException");
        } catch (IOException e) {
        }
        assertEquals(null, readString(baos, STD_UTF8));

        baos.reset();
        SerializationUtil.writePackedInt(out, -42);

        try {
            readNonNullString(baos, STD_UTF8);
            fail("Expected IOException");
        } catch (IOException e) {
        }
        assertEquals(null, readString(baos, STD_UTF8));
    }

    @Test
    public void testReadWriteEmptyString() throws Exception {
        final String s = "";

        SerializationUtil.writeNonNullString(out, STD_UTF8, s);
        assertEquals(s, readNonNullString(baos, STD_UTF8));
        assertEquals(s, readString(baos, STD_UTF8));

        baos.reset();
        SerializationUtil.writeString(out, STD_UTF8, s);
        assertEquals(s, readNonNullString(baos, STD_UTF8));
        assertEquals(s, readString(baos, STD_UTF8));
    }

    @Test
    public void testReadWriteNonEmptyString() throws Exception {
        final String s =
            "Hello, my name is Joe, and I work in a button factory.";

        SerializationUtil.writeNonNullString(out, STD_UTF8, s);
        assertEquals(s, readNonNullString(baos, STD_UTF8));
        assertEquals(s, readString(baos, STD_UTF8));

        baos.reset();
        SerializationUtil.writeString(out, STD_UTF8, s);
        assertEquals(s, readNonNullString(baos, STD_UTF8));
        assertEquals(s, readString(baos, STD_UTF8));

        final int originalMaxSeqLen = SerializationUtil.maxSequenceLength;
        SerializationUtil.maxSequenceLength = 10;
        try {
            baos.reset();
            SerializationUtil.writeString(out, STD_UTF8, s);

            try {
                readNonNullString(baos, STD_UTF8);
                fail("Expected IOException");
            } catch (IOException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("maximum sequence length"));
            }

            try {
                readString(baos, STD_UTF8);
                fail("Expected IOException");
            } catch (IOException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("maximum sequence length"));
            }
        } finally {
            SerializationUtil.maxSequenceLength = originalMaxSeqLen;
        }
    }

    @Test
    public void testReadBadString() throws Exception {
        testReadIOException(new byte[0]);

        SerializationUtil.writePackedLong(out, Long.MAX_VALUE);
        testReadIOException(baos.toByteArray());

        baos.reset();
        SerializationUtil.writePackedLong(out, Long.MIN_VALUE);
        testReadIOException(baos.toByteArray());

        baos.reset();
        SerializationUtil.writeString(out, STD_UTF8, "Hello");
        byte[] bytes = baos.toByteArray();
        bytes = Arrays.copyOfRange(bytes, 0, bytes.length - 1);
        testReadIOException(bytes);
    }

    @Test
    public void testCollectionLength()
        throws IOException {

        Set<Integer> set = null;

        writeCollectionLength(out, set);
        DataInput in = input(baos);
        try {
            readNonNullSequenceLength(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }
        assertEquals(-1, readSequenceLength(input(baos)));

        set = new HashSet<>();

        baos.reset();
        writeCollectionLength(out, set);
        assertEquals(0, readSequenceLength(input(baos)));
        assertEquals(0, readNonNullSequenceLength(input(baos)));

        set.add(1);
        set.add(2);

        baos.reset();
        writeCollectionLength(out, set);
        assertEquals(2, readSequenceLength(input(baos)));
        assertEquals(2, readNonNullSequenceLength(input(baos)));

        final int originalMaxSeqLen = SerializationUtil.maxSequenceLength;
        SerializationUtil.maxSequenceLength = 10;
        try {
            set.clear();
            Collections.addAll(set, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

            baos.reset();
            SerializationUtil.writeCollectionLength(out, set);

            try {
                readSequenceLength(input(baos));
                fail("Expected IOException");
            } catch (IOException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("maximum sequence length"));
            }

            try {
                readNonNullSequenceLength(input(baos));
                fail("Expected IOException");
            } catch (IOException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("maximum sequence length"));
            }
        } finally {
            SerializationUtil.maxSequenceLength = originalMaxSeqLen;
        }
    }

    @Test
    public void testArrayLength()
        throws IOException {

        try {
            writeArrayLength(out, "hi");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        int[] array = null;

        baos.reset();
        writeArrayLength(out, array);
        DataInput in = input(baos);
        try {
            readNonNullSequenceLength(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }
        assertEquals(-1, readSequenceLength(input(baos)));

        array = new int[0];

        baos.reset();
        writeArrayLength(out, array);
        assertEquals(0, readSequenceLength(input(baos)));
        assertEquals(0, readNonNullSequenceLength(input(baos)));

        array = new int[] { 1, 2, 3 };

        baos.reset();
        writeArrayLength(out, array);
        assertEquals(3, readSequenceLength(input(baos)));
        assertEquals(3, readNonNullSequenceLength(input(baos)));
    }

    @Test
    public void testSequenceLength()
        throws IOException {

        try {
            writeNonNullSequenceLength(out, -2);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        try {
            writeSequenceLength(out, -2);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        try {
            writeNonNullSequenceLength(out, -1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        writeSequenceLength(out, -1);
        assertEquals(-1, readSequenceLength(input(baos)));

        baos.reset();
        writeSequenceLength(out, 0);
        assertEquals(0, readSequenceLength(input(baos)));
        assertEquals(0, readNonNullSequenceLength(input(baos)));

        baos.reset();
        writeSequenceLength(out, Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, readSequenceLength(input(baos)));
        assertEquals(Integer.MAX_VALUE, readNonNullSequenceLength(input(baos)));

        baos.reset();
        writePackedLong(out, Long.MAX_VALUE);
        DataInput in = input(baos);
        try {
            readNonNullSequenceLength(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }

        in = input(baos);
        try {
            readSequenceLength(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }

        baos.reset();
        writePackedInt(out, -2);
        in = input(baos);
        try {
            readSequenceLength(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }

        in = input(baos);
        try {
            readNonNullSequenceLength(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }
    }

    @Test
    public void testByteArray()
        throws IOException {

        byte[] array = null;

        try {
            writeNonNullByteArray(out, array);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        baos.reset();
        writeByteArray(out, array);
        DataInput in = input(baos);
        try {
            readNonNullByteArray(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }
        assertEquals(null, readByteArray(input(baos)));

        array = new byte[0];

        baos.reset();
        writeNonNullByteArray(out, array);
        assertArrayEquals(array, readNonNullByteArray(input(baos)));
        assertArrayEquals(array, readByteArray(input(baos)));

        baos.reset();
        writeByteArray(out, array);
        assertArrayEquals(array, readNonNullByteArray(input(baos)));
        assertArrayEquals(array, readByteArray(input(baos)));

        array = new byte[] { 1, 2, -1 };

        baos.reset();
        writeNonNullByteArray(out, array);
        assertArrayEquals(array, readNonNullByteArray(input(baos)));
        assertArrayEquals(array, readByteArray(input(baos)));

        baos.reset();
        writeByteArray(out, array);
        assertArrayEquals(array, readNonNullByteArray(input(baos)));
        assertArrayEquals(array, readByteArray(input(baos)));

        final int originalMaxSeqLen = SerializationUtil.maxSequenceLength;
        SerializationUtil.maxSequenceLength = 10;
        try {
            array = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };

            baos.reset();
            writeByteArray(out, array);

            try {
                readByteArray(input(baos));
                fail("Expected IOException");
            } catch (IOException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("maximum sequence length"));
            }

            try {
                readNonNullByteArray(input(baos));
                fail("Expected IOException");
            } catch (IOException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("maximum sequence length"));
            }
        } finally {
            SerializationUtil.maxSequenceLength = originalMaxSeqLen;
        }
    }

    @Test
    public void testWriteNonNullCollection()
        throws IOException {

        List<Value> list = null;

        try {
            writeNonNullCollection(out, STD_UTF8, list);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        list = new ArrayList<>();
        list.add(null);
        try {
            writeNonNullCollection(out, STD_UTF8, list);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        list.clear();
        baos.reset();
        writeNonNullCollection(out, STD_UTF8, list);
        DataInput in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        Value v1 = Value.createValue(new byte[] { 1, 2 });
        Value v2 = Value.createValue(new byte[] { 3, 4 });
        list.add(v1);
        list.add(v2);

        baos.reset();
        writeNonNullCollection(out, STD_UTF8, list);
        in = input(baos);
        assertEquals(2, readSequenceLength(in));
        assertEquals(v1, new Value(in, STD_UTF8));
        assertEquals(v2, new Value(in, STD_UTF8));
        assertEOF(in);
    }

    @Test
    public void testCollection()
        throws IOException {

        List<Value> list = null;
        writeCollection(out, STD_UTF8, list);
        DataInput in = input(baos);
        assertEquals(-1, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(null,
                     readCollection(in, STD_UTF8, LinkedList::new,
                                    Value::new));

        list = new ArrayList<>();

        baos.reset();
        writeCollection(out, STD_UTF8, list);
        in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        List<Value> result = readCollection(in, STD_UTF8, LinkedList::new,
                                            Value::new);
        assertEquals(list, result);
        assertEquals(LinkedList.class, result.getClass());

        list.add(Value.createValue(new byte[] { 1, 2 }));
        list.add(Value.createValue(new byte[] { 3, 4 }));
        list.add(null);

        baos.reset();
        writeCollection(out, STD_UTF8, list);
        in = input(baos);
        assertEquals(list.size(), readSequenceLength(in));
        for (final Value v : list) {
            assertEquals(v, readFastExternalOrNull(in, STD_UTF8, Value::new));
        }
        assertEOF(in);

        in = input(baos);
        result = readCollection(in, STD_UTF8, LinkedList::new, Value::new);
        assertEquals(list, result);
        assertEquals(LinkedList.class, result.getClass());
    }

    @Test
    public void testCollectionElementWriter()
        throws IOException {

        List<String> list = null;
        writeCollection(out, STD_UTF8, list, WriteFastExternal::writeString);
        DataInput in = input(baos);
        assertEquals(-1, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(null,
                     readCollection(in, STD_UTF8, LinkedList::new,
                                    SerializationUtil::readString));

        list = new ArrayList<>();

        baos.reset();
        writeCollection(out, STD_UTF8, list, WriteFastExternal::writeString);
        in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        List<String> result = readCollection(in, STD_UTF8, LinkedList::new,
                                             SerializationUtil::readString);
        assertEquals(list, result);
        assertEquals(LinkedList.class, result.getClass());

        list.add("Hello");
        list.add(MADCHEN);
        list.add(null);

        baos.reset();
        writeCollection(out, STD_UTF8, list, WriteFastExternal::writeString);
        in = input(baos);
        assertEquals(list.size(), readSequenceLength(in));
        for (String v : list) {
            assertEquals(v,
                         readFastExternalOrNull(
                             in, STD_UTF8, SerializationUtil::readString));
        }
        assertEOF(in);

        in = input(baos);
        result = readCollection(in, STD_UTF8, LinkedList::new,
                                SerializationUtil::readString);
        assertEquals(list, result);
        assertEquals(LinkedList.class, result.getClass());
    }

    @Test
    public void testMap()
        throws IOException {

        Map<String, Consistency> map = null;
        writeMap(out, STD_UTF8, map, WriteFastExternal::writeString);
        DataInput in = input(baos);
        assertEquals(-1, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(null,
                     readMap(in, STD_UTF8, TreeMap::new,
                             SerializationUtil::readString,
                             Consistency::readFastExternal));

        map = new HashMap<>();

        baos.reset();
        writeMap(out, STD_UTF8, map, WriteFastExternal::writeString);
        in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        Map<String, Consistency> result =
            readMap(in, STD_UTF8, TreeMap::new,
                    SerializationUtil::readString,
                    Consistency::readFastExternal);
        assertEquals(map, result);
        assertEquals(TreeMap.class, result.getClass());

        map.put("k1", new Consistency.Time(5, SECONDS, 6, SECONDS));
        map.put("k2", Consistency.NONE_REQUIRED);
        map.put("k3", null);

        baos.reset();
        writeMap(out, STD_UTF8, map, WriteFastExternal::writeString);
        in = input(baos);
        assertEquals(map.size(), readSequenceLength(in));
        for (final Entry<String, Consistency> entry : map.entrySet()) {
            assertEquals(entry.getKey(),
                         SerializationUtil.readString(in, STD_UTF8));
            assertEquals(entry.getValue(),
                         readFastExternalOrNull(
                             in, STD_UTF8, Consistency::readFastExternal));
        }
        assertEOF(in);

        in = input(baos);
        result = readMap(in, STD_UTF8, TreeMap::new,
                         SerializationUtil::readString,
                         Consistency::readFastExternal);
        assertEquals(map, result);
        assertEquals(TreeMap.class, result.getClass());
    }

    @Test
    public void testMapValueWriter()
        throws IOException {

        Map<String, Long> map = null;
        WriteFastExternal<Long> writeValue =
            (val, outp, sv) -> outp.writeLong(val);
        ReadFastExternal<Long> readValue = (in, sv) -> in.readLong();
        writeMap(out, STD_UTF8, map, WriteFastExternal::writeString,
                 writeValue);
        DataInput in = input(baos);
        assertEquals(-1, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(null,
                     readMap(in, STD_UTF8, TreeMap::new,
                             SerializationUtil::readString,
                             readValue));

        map = new HashMap<>();

        baos.reset();
        writeMap(out, STD_UTF8, map, WriteFastExternal::writeString,
                 writeValue);
        in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        Map<String, Long> result =
            readMap(in, STD_UTF8, TreeMap::new,
                    SerializationUtil::readString, readValue);
        assertEquals(map, result);
        assertEquals(TreeMap.class, result.getClass());

        map.put("k1", 44L);
        map.put("k2", 99L);
        map.put("k3", null);

        baos.reset();
        writeMap(out, STD_UTF8, map, WriteFastExternal::writeString,
                 writeValue);
        in = input(baos);
        assertEquals(map.size(), readSequenceLength(in));
        for (final Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getKey(),
                         SerializationUtil.readString(in, STD_UTF8));
            assertEquals(entry.getValue(),
                         readFastExternalOrNull(
                             in, STD_UTF8,
                             (inp, sv) -> Long.valueOf(inp.readLong())));
        }
        assertEOF(in);

        in = input(baos);
        result = readMap(in, STD_UTF8, TreeMap::new,
                         SerializationUtil::readString, readValue);
        assertEquals(map, result);
        assertEquals(TreeMap.class, result.getClass());
    }

    @Test
    public void testWriteProperties()
        throws IOException {

        writeProperties(out, STD_UTF8, null);
        DataInput in = input(baos);
        assertEquals(-1, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(null, readProperties(in, STD_UTF8));

        final Properties props = new Properties();

        baos.reset();
        writeProperties(out, STD_UTF8, props);
        in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(props, readProperties(in, STD_UTF8));

        props.setProperty("girl", MADCHEN);
        props.setProperty("boy", "lad");

        baos.reset();
        writeProperties(out, STD_UTF8, props);
        in = input(baos);
        assertEquals(props, readProperties(in, STD_UTF8));

        props.put("42", 42);

        baos.reset();
        checkException(() -> writeProperties(out, STD_UTF8, props),
                       ClassCastException.class, "Integer");

        props.remove("42");
        props.put(99L, "99");

        baos.reset();
        checkException(() -> writeProperties(out, STD_UTF8, props),
                       ClassCastException.class, "Long");
    }

    @Test
    public void testWriteNonNullArray()
        throws IOException {

        Value[] array = null;

        try {
            writeNonNullArray(out, STD_UTF8, array);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        array = new Value[0];

        baos.reset();
        writeNonNullArray(out, STD_UTF8, array);
        DataInput in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        Value v1 = Value.createValue(new byte[] { 1, 2 });
        Value v2 = Value.createValue(new byte[] { 3, 4 });
        array = new Value[] { v1, v2 };

        baos.reset();
        writeNonNullArray(out, STD_UTF8, array);
        in = input(baos);
        assertEquals(2, readSequenceLength(in));
        assertEquals(v1, new Value(in, STD_UTF8));
        assertEquals(v2, new Value(in, STD_UTF8));
        assertEOF(in);
    }

    @Test
    public void testFastExternalOrNull()
        throws IOException {

        Value value = null;
        writeFastExternalOrNull(out, STD_UTF8, value);
        DataInput in = input(baos);
        assertFalse(in.readBoolean());
        assertEOF(in);

        in = input(baos);
        assertEquals(null, readFastExternalOrNull(in, STD_UTF8, Value::new));

        value = Value.createValue(new byte[] { 1, 2 });

        baos.reset();
        writeFastExternalOrNull(out, STD_UTF8, value);
        in = input(baos);
        assertTrue(in.readBoolean());
        assertEquals(value, new Value(in, STD_UTF8));
        assertEOF(in);

        in = input(baos);
        assertEquals(value, readFastExternalOrNull(in, STD_UTF8, Value::new));
    }

    @Test
    public void testFastExternalOrNullWriter()
        throws IOException {

        String value = null;
        writeFastExternalOrNull(out, STD_UTF8, value,
                                (outp, sv, item) -> fail("Not expected"));
        DataInput in = input(baos);
        assertFalse(in.readBoolean());
        assertEOF(in);

        in = input(baos);
        assertEquals(null,
                     readFastExternalOrNull(in, STD_UTF8,
                                            SerializationUtil::readString));

        value = MADCHEN;

        baos.reset();
        writeFastExternalOrNull(out, STD_UTF8, value,
                                WriteFastExternal::writeString);
        in = input(baos);
        assertTrue(in.readBoolean());
        assertEquals(value, SerializationUtil.readString(in, STD_UTF8));
        assertEOF(in);

        in = input(baos);
        assertEquals(value,
                     readFastExternalOrNull(in, STD_UTF8,
                                            SerializationUtil::readString));
    }

    @Test
    public void testCharArray()
        throws IOException {

        char[] array = null;
        writeCharArray(out, array);
        DataInput in = input(baos);
        assertEquals(-1, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertEquals(null, readCharArray(in));

        array = new char[0];

        baos.reset();
        writeCharArray(out, array);
        in = input(baos);
        assertEquals(0, readSequenceLength(in));
        assertEOF(in);

        in = input(baos);
        assertArrayEquals(array, readCharArray(in));

        array = new char[] { 'a', A_UMLAUT };
        baos.reset();
        writeCharArray(out, array);
        in = input(baos);
        assertEquals(2, readSequenceLength(in));
        assertEquals(array[0], in.readChar());
        assertEquals(array[1], in.readChar());
        assertEOF(in);

        in = input(baos);
        assertArrayEquals(array, readCharArray(in));
    }

    @Test
    public void testTimeUnit()
        throws IOException {

        try {
            writeTimeUnit(out, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        for (TimeUnit t : TimeUnit.values()) {
            baos.reset();
            writeTimeUnit(out, t);
            final DataInput in = input(baos);
            assertEquals(t, readTimeUnit(in));
            assertEOF(in);
        }

        baos.reset();
        writePackedInt(out, -2);
        DataInput in = input(baos);
        try {
            readTimeUnit(in);
            fail("Expected IOException");
        } catch (IOException e) {
        }
    }

    @Test
    public void testJavaSerial()
        throws IOException {

        writeJavaSerial(out, null);
        DataInput in = input(baos);
        assertEquals(null, readJavaSerial(in, String.class));

        baos.reset();
        writeJavaSerial(out, "hi");
        in = input(baos);
        String s = readJavaSerial(in, String.class);
        assertEquals("hi", s);

        baos.reset();
        writeJavaSerial(out, new int[] { 1, 2, 3 });
        in = input(baos);
        assertArrayEquals(new int[] { 1, 2, 3 },
                          (int[]) readJavaSerial(in, Object.class));

        baos.reset();
        Set<String> set = new HashSet<>();
        set.add("Apple");
        set.add("Lime");
        writeJavaSerial(out, set);
        in = input(baos);
        Set<?> set2 = readJavaSerial(in, Set.class);
        assertEquals(set, set2);

        baos.reset();
        checkException(() -> writeJavaSerial(out, new Thread()),
                       IllegalStateException.class,
                       "Exception serializing object");

        final DataOutput failingOut =
            new DataOutputStream(
                new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        throw new IOException("Write failed");
                    }
                });
        checkException(() -> writeJavaSerial(failingOut, "Hi"),
                       IOException.class,
                       "Write failed");

        baos.reset();
        writeJavaSerial(out, "Hi");
        {
            final DataInput inFinal = input(baos);
            checkException(() -> readJavaSerial(inFinal, Integer.class),
                           ClassCastException.class);
        }

        baos.reset();
        writeJavaSerial(out, new FailOnDeserialize());
        {
            final DataInput inFinal = input(baos);
            checkException(() -> readJavaSerial(inFinal, Object.class),
                           IllegalStateException.class,
                           "Exception deserializing object");
        }

        final DataInput failingIn =
            new DataInputStream(
                new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException("Read failed");
                    }
                });
        checkException(() -> readJavaSerial(failingIn, String.class),
                       IOException.class,
                       "Read failed");
    }

    /*
     * Utilities
     */

    private void testReadIOException(byte[] bytes) {
        try {
            readString(bytes, STD_UTF8);
            fail("Expected IOException");
        } catch (IOException e) {
        }
        try {
            readNonNullString(bytes, STD_UTF8);
            fail("Expected IOException");
        } catch (IOException e) {
        }
    }

    private static String readNonNullString(ByteArrayOutputStream baos,
                                            short serialVersion)
        throws IOException {

        return readNonNullString(baos.toByteArray(), serialVersion);
    }

    private static String readNonNullString(byte[] bytes, short serialVersion)
        throws IOException {

        final DataInput in = input(bytes);
        final String s =
            SerializationUtil.readNonNullString(in, serialVersion);
        assertEOF(in);
        return s;
    }

    private static String readString(ByteArrayOutputStream baos,
                                     short serialVersion)
        throws IOException {

        return readString(baos.toByteArray(), serialVersion);
    }

    private static String readString(byte[] bytes, short serialVersion)
        throws IOException {

        final DataInput in = input(bytes);
        final String s = SerializationUtil.readString(in, serialVersion);
        assertEOF(in);
        return s;
    }

    private static DataInput input(ByteArrayOutputStream baos) {
        return input(baos.toByteArray());
    }

    private static DataInput input(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static void assertEOF(DataInput in) throws IOException {
        try {
            in.readByte();
            fail("Expected EOFException");
        } catch (EOFException e) {
        }
    }

    private static class FailOnDeserialize implements Serializable {
        private static final long serialVersionUID = 1;
        private void readObject(@SuppressWarnings("unused")
                                ObjectInputStream in)
            throws ClassNotFoundException
        {
            /* Simulate a class not found */
            throw new ClassNotFoundException("Class not found");
        }
    }
}
