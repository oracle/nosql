/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Queue;
import java.util.Random;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

public class MessageInputOutputTest extends TestBase {

    private final static Random rand = new Random();

    private final static boolean TRUE_VAL = true;
    private final static boolean FALSE_VAL = false;
    private final static int INT_VAL = 0xABACADAE;
    private final static long LONG_VAL = 0xA1A2A3A4ABACADAEL;
    private final static float FLOAT_VAL = 0.1234f;
    private final static double DOUBLE_VAL = 0.1234567890123d;
    private final static byte NEG_VAL = -((byte) 42);
    private final static byte[] BYTE_ARRAY = new byte[] {
        (byte) 0, (byte) 1, (byte) 2, (byte) 3 };
    private final static char[] UTF_CHARS = new char[] {
        (char) 0x0000, (char) 0x007F, (char) 0x0080, (char) 0x07FF,
        (char) 0x0800, (char) 0xFFFF,
    };

    /**
     * Test input and output for primitive data types and UTF string.
     *
     * The test varies the frame size of message output. It also adds a varied
     * number of bytes at the beginning of the message output to cover corner
     * cases.
     */
    @Test
    public void testInputOutputBasic() {
        final int maxFrameSize = 64;
        final int bufferSize = 16;
        final int maxPadding = maxFrameSize;
        for (int fs = 1; fs <= maxFrameSize; fs ++) {
            for (int p = 0; p <= maxPadding; p ++) {
                MessageOutput output =
                    new MessageOutput(new HeapIOBufferPool(bufferSize));
                writePrimitiveOutput(output, p);
                MessageInput input = new MessageInput();
                String info = feedOutputToInput(output, input, fs);
                checkPrimitiveInput(input, p, info);
            }
        }
    }

    /**
     * Test for more arbitrary patterns of byte arrays interspersed with
     * primitives.
     */
    @Test
    public void testInputOutputRandom() {
        for (int i = 0; i < 1024; ++i) {
            final int frameSize = rand.nextInt(64 - 1) + 1;
            final int bufferSize = rand.nextInt(64 - 16) + 16;
            MessageOutput output =
                new MessageOutput(new HeapIOBufferPool(bufferSize));
            byte[] starts = writeRandomOutput(output);
            MessageInput input = new MessageInput();
            String info = feedOutputToInput(output, input, frameSize);
            checkRandomInput(input, starts, info);
        }
    }


    /**
     * Test skip bytes.
     *
     * Write some integers and skip some.
     */
    @Test
    public void testSkipBytes() {
        try {
            int nskip = 64;
            int maxFrameSize = 64;
            for (int i = 0; i < nskip; ++i) {
                for (int fs = 1; fs < maxFrameSize; fs ++) {
                    MessageOutput output =
                        new MessageOutput(new HeapIOBufferPool(8));
                    for (int j = 0; j < nskip + 1; ++j) {
                        output.writeInt(j);
                    }
                    MessageInput input = new MessageInput();
                    feedOutputToInput(output, input, fs);
                    int n = input.skipBytes(i * 4);
                    assertEquals(i * 4, n);
                    assertEquals(i, input.readInt());
                }
            }
        } catch (Throwable e) {
            fail(LoggerUtils.getStackTrace(e));
        }
    }


    private void writePrimitiveOutput(MessageOutput output, int nprefix) {
        try {
            for (int i = 0; i < nprefix; ++i) {
                output.write(i);
            }
            /*
             * Write twice so that we cover the case of writing non-buffered
             * types before and after buffered types.
             */
            for (int i = 0; i < 2; ++i) {
                output.writeBoolean(TRUE_VAL);
                output.writeBoolean(FALSE_VAL);
                output.writeByte(INT_VAL);
                output.writeShort(INT_VAL);
                output.writeChar(INT_VAL);
                output.writeInt(INT_VAL);
                output.writeLong(LONG_VAL);
                output.writeFloat(FLOAT_VAL);
                output.writeDouble(DOUBLE_VAL);
                output.writeByte(NEG_VAL);
                output.writeShort(NEG_VAL);
                output.writeByte(NEG_VAL);
                output.writeShort(NEG_VAL);
                output.write(BYTE_ARRAY);
                output.write(BYTE_ARRAY,
                        BYTE_ARRAY.length / 2, BYTE_ARRAY.length / 2);
                SerializationUtil.writeString(
                        output, SerialVersion.MINIMUM,
                        new String(UTF_CHARS));
            }
        } catch (Throwable e) {
            fail(LoggerUtils.getStackTrace(e));
        }
    }

    private String feedOutputToInput(MessageOutput output,
                                     MessageInput input,
                                     int frameSize) {
        int size = 0;
        Queue<IOBufSliceList> frames = output.pollFrames(frameSize, null);
        if (frames == null) {
            throw new AssertionError("frames should not be null");
        }
        StringBuilder sb = new StringBuilder("Frames: ");
        for (IOBufSliceList frame : frames) {
            CopyingBytesInput bytesInput =
                new CopyingBytesInput(frame.toBufArray());
            assertTrue(String.format("Size incorrect, " +
                                     "frame size=%s, got=%s",
                                     frameSize, bytesInput.remaining()),
                       bytesInput.remaining() <= frameSize);
            size += bytesInput.remaining();
            input.add(bytesInput);
            sb.append(frame);
            sb.append("; ");
        }
        /* Check the size here. */
        assertEquals(size, output.size());
        return sb.toString();
    }

    private void checkPrimitiveInput(MessageInput input,
                                     int nprefix,
                                     String info) {
        try {
            for (int i = 0; i < nprefix; ++i) {
                assertEquals(info, (byte) i, input.readByte());
            }
            for (int i = 0; i < 2; ++i) {
                assertEquals(info, TRUE_VAL, input.readBoolean());
                assertEquals(info, FALSE_VAL, input.readBoolean());
                assertEquals(info, (byte) INT_VAL, input.readByte());
                assertEquals(info, (short) INT_VAL, input.readShort());
                assertEquals(info, (char) INT_VAL, input.readChar());
                assertEquals(info, INT_VAL, input.readInt());
                assertEquals(info, LONG_VAL, input.readLong());
                assertEquals(info, FLOAT_VAL, input.readFloat(), 0.0f);
                assertEquals(info, DOUBLE_VAL, input.readDouble(), 0.0f);
                assertEquals(info, 0xff & NEG_VAL,
                        input.readUnsignedByte());
                assertEquals(info, 0xffff & NEG_VAL,
                        input.readUnsignedShort());
                assertEquals(info, NEG_VAL, input.readByte());
                assertEquals(info, NEG_VAL, input.readShort());
                byte[] actual = new byte[BYTE_ARRAY.length];
                input.readFully(actual);
                assertArrayEquals(info, BYTE_ARRAY, actual);
                byte[] expected = new byte[BYTE_ARRAY.length / 2];
                System.arraycopy(BYTE_ARRAY, BYTE_ARRAY.length / 2,
                        expected, 0, BYTE_ARRAY.length / 2);
                actual = new byte[BYTE_ARRAY.length / 2];
                input.readFully(actual);
                assertArrayEquals(info, expected, actual);
                assertEquals(info, new String(UTF_CHARS),
                        SerializationUtil.readString(
                            input, SerialVersion.MINIMUM));
            }
        } catch (Throwable e) {
            fail(LoggerUtils.getStackTrace(e) + info);
        }
    }

    private byte[] writeRandomOutput(MessageOutput output) {
        int n = rand.nextInt(16);
        output.writeInt(n);
        byte[] starts = new byte[n];
        for (int i = 0; i < n; ++i) {
            int size = rand.nextInt(16);
            output.writeInt(size);
            byte[] bytes = new byte[size];
            byte start = (byte) rand.nextInt();
            starts[i] = start;
            for (int j = 0; j < size; ++j) {
                bytes[j] = (byte) (start + j);
            }
            output.write(bytes);

            size = 1 << rand.nextInt(4);
            output.writeInt(size);
            switch(size) {
            case 1:
                output.write((byte) INT_VAL);
                break;
            case 2:
                output.writeShort((short) INT_VAL);
                break;
            case 4:
                output.writeInt(INT_VAL);
                break;
            case 8:
                output.writeLong(LONG_VAL);
                break;
            default:
                throw new IllegalArgumentException();
            }
        }
        return starts;
    }

    private void checkRandomInput(MessageInput input,
                                  byte[] starts,
                                  String info) {
        try {
            int n = input.readInt();
            for (int i = 0; i < n; ++i) {
                int size = input.readInt();
                byte[] bytes = new byte[size];
                input.readFully(bytes);
                for (int j = 0; j < size; ++j) {
                    assertEquals((byte) (starts[i] + j), bytes[j]);
                }

                size = input.readInt();
                switch(size) {
                    case 1:
                        assertEquals((byte) INT_VAL, input.readByte());
                        break;
                    case 2:
                        assertEquals((short) INT_VAL, input.readShort());
                        break;
                    case 4:
                        assertEquals(INT_VAL, input.readInt());
                        break;
                    case 8:
                        assertEquals(LONG_VAL, input.readLong());
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            }
        } catch (Throwable e) {
            fail(LoggerUtils.getStackTrace(e) + info);
        }
    }
}
