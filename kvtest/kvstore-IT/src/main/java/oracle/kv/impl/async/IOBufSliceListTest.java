/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.Test;

/** Test the {@link IOBufSliceList} class. */
public class IOBufSliceListTest extends TestBase {
    @Test
    public void testAdd() {
        int size = 8;
        IOBufSliceList.Entry[] expected = createArray(size);
        IOBufSliceList list = new IOBufSliceList();
        check(list, null, null, 0, "after initialization");
        for (int i = 0; i < size; ++i) {
            list.add(expected[i]);
            check(list, expected[0], expected[i], i + 1,
                  String.format("after adding %dth slice", i));
        }
    }

    @Test
    public void testPoll() {
        int size = 8;
        IOBufSliceList.Entry[] expected = createArray(size);
        IOBufSliceList list = new IOBufSliceList();
        for (int i = 0; i < size; ++i) {
            list.add(expected[i]);
        }
        for (int i = 0; i < size; ++i) {
            check(list, expected[i], expected[size - 1], size - i,
                  String.format("before polling %dth slice", i));
            IOBufSlice slice = list.poll();
            assertEquals(expected[i], slice);
        }
        check(list, null, null, 0, "after polling all slices");
    }

    @Test
    public void testExtend() {
        int size = 8;
        int size1 = 3;
        IOBufSliceList.Entry[] expected = createArray(size);
        IOBufSliceList list1 = new IOBufSliceList();
        IOBufSliceList list2 = new IOBufSliceList();
        for (int iter = 0; iter < 8; ++iter) {
            for (int i = 0; i < size1; ++i) {
                list1.add(expected[i]);
            }
            for (int i = size1; i < size; ++i) {
                list2.add(expected[i]);
            }
            list1.extend(list2, null);
            IOBufSliceList.Entry curr = list1.head();
            for (int i = 0; i < size; ++i) {
                assertEquals(expected[i], curr);
                if (curr == null) {
                    throw new IllegalStateException("Curr is null");
                }
                curr = curr.next();
            }
            check(list1, expected[0], expected[size - 1], size,
                  "after list1 extended");
            check(list2, null, null, 0, "after list2 used to extend");
            clear(list1);
            clear(list2);
        }
    }

    @Test
    public void testBufArray() {
        int size = 8;
        IOBufSliceList.Entry[] expected = createArray(size);
        IOBufSliceList list = new IOBufSliceList();
        for (int i = 0; i < size; ++i) {
            list.add(expected[i]);
        }
        assertArrayEquals(
            Arrays.stream(expected).
            map(s -> s.buf()).toArray(ByteBuffer[]::new),
            list.toBufArray());
    }

    @Test
    public void testEmptyList() {
        IOBufSliceList list = new IOBufSliceList();
        IOBufSliceList.Entry slice =
            new IOBufSliceImpl.HeapBufSlice(ByteBuffer.allocate(0));
        list.add(slice);
        check(list, slice, slice, 1, "after add from empty");

        list = new IOBufSliceList();
        slice = list.poll();
        check(list, null, null, 0, "after poll from empty");
        assertEquals(slice, null);

        int size = 8;
        list = new IOBufSliceList();
        IOBufSliceList.Entry[] expected = createArray(size);
        IOBufSliceList list1 = new IOBufSliceList();
        for (int i = 0; i < size; ++i) {
            list1.add(expected[i]);
        }
        list.extend(list1, null);
        check(list, expected[0], expected[size - 1], size,
              "after list extended");
        check(list1, null, null, 0, "after list1 used to extend");

        list = new IOBufSliceList();
        ByteBuffer[] bufs = list.toBufArray();
        assertEquals(0, bufs.length);
    }

    private IOBufSliceList.Entry[] createArray(int size) {
        IOBufSliceList.Entry[] result = new IOBufSliceList.Entry[8];
        for (int i = 0; i < size; ++i) {
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put(0, (byte) i);
            result[i] = new IOBufSliceImpl.HeapBufSlice(buf);
        }
        return result;
    }

    private void check(IOBufSliceList list,
                       @Nullable IOBufSlice expectedHead,
                       @Nullable IOBufSlice expectedTail,
                       int expectedSize,
                       String mesg) {

        assertEquals(String.format("Head incorrect %s,", mesg),
                     expectedHead, list.head());
        assertEquals(String.format("Tail incorrect %s", mesg),
                     expectedTail, list.tail());
        assertEquals(String.format("Size incorrect %s", mesg),
                     expectedSize, list.size());

        int size = 0;
        IOBufSliceList.Entry curr = list.head();
        while (true) {
            if (curr == null) {
                break;
            }
            curr = curr.next();
            size ++;
        }
        assertEquals(String.format("Size incorrect %s", mesg),
                     expectedSize, size);
    }

    private void clear(IOBufSliceList list) {
        while (true) {
            IOBufSlice curr = list.poll();
            if (curr == null) {
                break;
            }
        }
    }
}
