/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog.nio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Random;

import oracle.kv.TestBase;
import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.HeapIOBufferPool;
import oracle.kv.impl.async.dialog.ChannelOutput;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.util.PackedInteger;

import org.junit.Test;

public class NioChannelInputOutputTest extends TestBase {

    private static final Random rand = new Random();
    private static final byte BYTE_VAL = (byte) 0;
    private static final long LONG_VAL = 42;
    private static final String STRING_VAL = "String";
    private static final Charset utf8 = Charset.forName("UTF-8");

    /**
     * Test basic input data read.
     */
    @Test
    public void testInputDataBasic() {
        ByteBuffer buf = ByteBuffer.allocate(128);
        NioChannelInput input =
            new NioChannelInput(new HeapIOBufferPool(128));
        writeByte(buf);
        writeBytes(buf, 16);
        writePackedLong(buf);
        writeUTF8(buf);
        buf.flip();
        inputReadAndCheck(input, buf, 4);
    }

    /**
     * Test basic input data read with multilple channel buffers.
     */
    @Test
    public void testInputDataMultiBuffers() {
        for (int i = 1; i <= 16; ++i) {
            ByteBuffer buf = ByteBuffer.allocate(128);
            NioChannelInput input =
                new NioChannelInput(new HeapIOBufferPool(i));
            writeByte(buf);
            writeBytes(buf, 16);
            writePackedLong(buf);
            writeUTF8(buf);
            buf.flip();
            inputReadAndCheck(input, buf, 4);
        }
    }

    /**
     * Test input mark and reset.
     */
    @Test
    public void testInputMarkAndReset() {
        final int nbytes = 128;
        final int maxInputBufSize = 16;
        for (int i = 0; i < 1024; ++i) {
            ByteBuffer buf = ByteBuffer.allocate(nbytes);
            NioChannelInput input =
                new NioChannelInput(
                        new HeapIOBufferPool(
                                rand.nextInt(maxInputBufSize - 1) + 1));
            for (int j = 0; j < nbytes; ++j) {
                buf.put((byte) j);
            }
            buf.flip();
            inputMarkAndReset(input, buf, nbytes);
        }
    }


    /**
     * Test input with random data.
     */
    @Test
    public void testInputRandom() {
        int maxInputBufSize = 16;
        int numItem = 128;
        for (int i = 0; i < 128; ++i) {
            ByteBuffer buf = generateInputData(numItem);
            NioChannelInput input =
                new NioChannelInput(
                        new HeapIOBufferPool(
                                rand.nextInt(maxInputBufSize - 1) + 1));
            inputReadAndCheck(input, buf, numItem);
        }
    }

    /**
     * Test output with random flush.
     */
    @Test
    public void testOutputRandom() {
        final int arrayLength = 16;
        final int niter = 128;
        final int nbytes = 1024;
        for (int i = 0; i < niter; ++i) {
            byte[] bytes = new byte[nbytes];
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            NioChannelOutput output = new NioChannelOutput(arrayLength);
            int cnt = 0;
            while (true) {
                /* Writes random number of bytes to the output. */
                int nb = rand.nextInt(arrayLength * 2);
                nb = Math.min(nb, nbytes - cnt);
                for (int j = 0; j < nb; ++j) {
                    ChannelOutput.Chunk chunk = output.beginChunk(1, false);
                    chunk.writeByte((byte) cnt);
                    chunk.done();
                    cnt ++;
                }
                /* Get the array from output. */
                NioChannelOutput.Bufs outBufs = output.getBufs();
                if (outBufs.length() == 0) {
                    if (cnt == nbytes) {
                        break;
                    }
                    continue;
                }
                /* Flushes a subset of buffers in the array. */
                int len = rand.nextInt(outBufs.length() + 1);
                ByteBuffer[] arr = outBufs.array();
                int off = outBufs.offset();
                for (int j = 0; j < len; ++j) {
                    buf.put(arr[j + off]);
                }
            }
            for (int j = 0; j < bytes.length; ++j) {
                assertEquals((byte) j, bytes[j]);
            }
        }
    }

    private ByteBuffer generateInputData(int numItem) {
        int maxItemSize = 64;
        byte[] array = new byte[maxItemSize * numItem];
        ByteBuffer buf = ByteBuffer.wrap(array);
        for (int i = 0; i < numItem; ++i) {
            byte type = (byte) rand.nextInt(3);
            switch (type) {
            case 0:
                writeByte(buf);
                break;
            case 1:
                writeBytes(buf, maxItemSize);
                break;
            case 2:
                writePackedLong(buf);
                break;
            case 3:
                writeUTF8(buf);
                break;
            default:
                throw new AssertionError();
            }
        }
        buf.flip();
        return buf;
    }

    private void writeByte(ByteBuffer buf) {
        buf.put((byte) 0);
        buf.put(BYTE_VAL);
    }

    private void writeBytes(ByteBuffer buf, int maxSize) {
        buf.put((byte) 1);
        byte nbytes = (byte) rand.nextInt(maxSize - 2);
        buf.put(nbytes);
        for (byte j = 0; j < nbytes; ++j) {
            buf.put(j);
        }

    }

    private void writePackedLong(ByteBuffer buf) {
        writePackedLong(buf, LONG_VAL);
    }

    private void writePackedLong(ByteBuffer buf, long val) {
        buf.put((byte) 2);
        writePackedLongNoType(buf, val);
    }

    private void writePackedLongNoType(ByteBuffer buf, long val) {
        int newPos = PackedInteger.writeLong(
                buf.array(), buf.position(), val);
        buf.position(newPos);
    }

    private void writeUTF8(ByteBuffer buf) {
        buf.put((byte) 3);
        ByteBuffer encoded = utf8.encode(STRING_VAL);
        writePackedLongNoType(buf, encoded.limit());
        buf.put(encoded);
    }

    private void inputReadAndCheck(NioChannelInput input,
                                   ByteBuffer buf,
                                   int numItem) {
        final int lim = buf.limit();
        try {
            buf.limit(0);
            int n = 0;
            while (true) {
                int currLim = buf.limit();
                int nextLim = Math.min(lim, currLim + rand.nextInt(3) + 1);
                buf.limit(nextLim);
                fillInputBufs(input, buf);
                input.flipToProtocolRead();
                while (tryReadOneItem(input)) {
                    n ++;
                }
                if (buf.position() == lim) {
                    break;
                }
            }
            assertEquals(String.format("Num of items not equal: " +
                        "lim=%s", lim),
                    numItem, n);
        } catch (Throwable t) {
            fail(String.format("bytes=%s, input=%s, error=%s",
                        BytesUtil.toString(buf, lim),
                        input,
                        LoggerUtils.getStackTrace(t)));
        }
    }

    private void fillInputBufs(NioChannelInput input, ByteBuffer buf) {
        ByteBuffer[] dsts = input.flipToChannelRead();
        for (ByteBuffer dst : dsts) {
            if (buf.remaining() == 0) {
                break;
            }
            if (dst.remaining() >= buf.remaining()) {
                dst.put(buf);
                break;
            }
            int lim = buf.limit();
            buf.limit(buf.position() + dst.remaining());
            dst.put(buf);
            buf.limit(lim);
        }
    }

    private boolean tryReadOneItem(NioChannelInput input) throws Exception {
        if (input.readableBytes() == 0) {
            return false;
        }
        input.mark();
        byte type = input.readByte();
        switch (type) {
        case 0:
            return readByte(input);
        case 1:
            return readBytes(input);
        case 2:
            return readPackedLong(input);
        case 3:
            return readUTF8(input);
        default:
            throw new AssertionError();
        }
    }

    private boolean readByte(NioChannelInput input) throws Exception {
        if (input.readableBytes() < 1) {
            input.reset();
            return false;
        }
        byte b = input.readByte();
        assertEquals(BYTE_VAL, b);
        return true;
    }

    private boolean readBytes(NioChannelInput input) throws Exception {
        if (input.readableBytes() < 1) {
            input.reset();
            return false;
        }
        byte size = input.readByte();
        if (input.readableBytes() < size) {
            input.reset();
            return false;
        }
        BytesInput bytes = input.readBytes(size);
        for (byte i = 0; i < size; ++i) {
            assertEquals(i, bytes.readByte());
        }
        return true;
    }

    private boolean readPackedLong(NioChannelInput input) throws Exception {
        if (!input.canReadPackedLong()) {
            input.reset();
            return false;
        }
        assertEquals(LONG_VAL, input.readPackedLong());
        return true;
    }

    private boolean readUTF8(NioChannelInput input) throws Exception {
        if (!input.canReadPackedLong()) {
            input.reset();
            return false;
        }
        long stringSize = input.readPackedLong();
        String string = input.readUTF8((int) stringSize);
        if (string == null) {
            input.reset();
            return false;
        }
        assertEquals(STRING_VAL, string);
        return true;
    }

    private void inputMarkAndReset(NioChannelInput input,
                                   ByteBuffer buf,
                                   int nbytes) {
        final int markPos = rand.nextInt(nbytes);
        final int resetPos = markPos + rand.nextInt(nbytes - markPos);
        int lim = buf.limit();
        buf.limit(0);
        while (true) {
            int currLim = buf.limit();
            int nextLim = Math.min(lim, currLim + rand.nextInt(3) + 1);
            buf.limit(nextLim);
            fillInputBufs(input, buf);
            input.flipToProtocolRead();
            final int readableBytes = input.readableBytes();
            if (input.readableBytes() <= resetPos) {
                continue;
            }
            for (int i = 0; i < markPos; ++i) {
                input.readByte();
            }
            input.mark();
            for (int i = 0; i < resetPos - markPos; ++i) {
                input.readByte();
            }
            input.reset();
            String info = String.format(
                    "markPos=%s, resetPos=%s", markPos, resetPos);
            assertEquals(info, readableBytes - markPos, input.readableBytes());
            assertEquals(info, (byte) markPos, input.readByte());
            break;
        }
    }
}
