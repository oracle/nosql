/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.CopyingBytesInput;
import oracle.kv.impl.async.IOBufSliceImpl;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.dialog.exception.ProtocolViolationException;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

public class ProtocolTest extends TestBase {

    private static final Random rand = new Random();

    /**
     * Test message read write
     */
    @Test
    public void testMesgReadWrite() {
        int bufferSize = 4096;
        int maxLengthInMesg = 128;
        for (int i = 0; i < 1024; ++i) {
            Tester tester = new Tester(
                    bufferSize, maxLengthInMesg, maxLengthInMesg);
            try {
                writeNonConnAbortMesgs(tester);
                tester.writeConnectionAbort();
                writeNonConnAbortMesgs(tester);
                tester.readAndCheck();
            } catch (Exception e) {
                fail(LoggerUtils.getStackTrace(e) + "\n" +
                        tester.toString());
            }
        }
    }

    /**
     * Test write large string.
     */
    @Test
    public void testWriteLargeString() {
        int maxLen = 8;
        Tester tester = new Tester(1024, maxLen, maxLen);
        tester.writeDialogAbort(1, "Detail that is larger than allowed.");
        tester.writeConnectionAbort("Detail that is larger than allowed.");
        for (ProtocolMesg mesg : tester.read(2)) {
            if (mesg.type() == ProtocolMesg.DIALOG_ABORT_MESG) {
                ProtocolMesg.DialogAbort actual =
                    (ProtocolMesg.DialogAbort) mesg;
                assertTrue(actual.detail.contains("Detail"));
            } else if (mesg.type() == ProtocolMesg.CONNECTION_ABORT_MESG) {
                ProtocolMesg.ConnectionAbort actual =
                    (ProtocolMesg.ConnectionAbort) mesg;
                assertTrue(actual.detail.contains("Detail"));
            } else {
                throw new AssertionError();
            }
        }
    }

    /**
     * Test the behavior of reading an invalid message identifier.
     */
    @Test
    public void testInvalidMesgIdent() {
        int bufferSize = 256;
        int maxLength = 16;
        Set<Byte> validTypes = new HashSet<Byte>();
        validTypes.add(ProtocolMesg.PROTOCOL_VERSION_MESG);
        validTypes.add(ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG);
        validTypes.add(ProtocolMesg.CONNECTION_CONFIG_MESG);
        validTypes.add(ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG);
        validTypes.add(ProtocolMesg.NO_OPERATION_MESG);
        validTypes.add(ProtocolMesg.CONNECTION_ABORT_MESG);
        validTypes.add(ProtocolMesg.PING_MESG);
        validTypes.add(ProtocolMesg.PINGACK_MESG);
        validTypes.add(ProtocolMesg.DIALOG_START_MESG);
        validTypes.add(ProtocolMesg.DIALOG_FRAME_MESG);
        validTypes.add(ProtocolMesg.DIALOG_ABORT_MESG);
        Tester tester = null;
        try {
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
                tester = new Tester(bufferSize, maxLength, maxLength);
                writeNonConnAbortMesgs(tester);
                ChannelOutput output = tester.output;
                ChannelOutput.Chunk chunk = output.beginChunk(1, false);
                byte ident = (byte) i;
                chunk.writeByte(ident);
                chunk.done();
                if (!validTypes.contains(ProtocolMesg.getType(ident))) {
                    checkReadProtocolViolation(tester,
                            ProtocolViolationException.
                            ERROR_UNKNOWN_IDENTIFIER);
                }
            }
        } catch (Exception e) {
            fail(LoggerUtils.getStackTrace(e) + "\n" +
                    ((tester == null) ? null : tester.toString()));
        }
    }

    /**
     * Test the behavior of receiving invalid messages.
     */
    @Test
    public void testInvalidMesg() {
        testInvalidProtocolVersion();
        testInvalidProtocolVersionResponse();
        testInvalidConnectionConfig();
        testInvalidConnectionConfigResponse();
        testInvalidConnectionAbort();
        testInvalidDialogStart();
        testInvalidDialogFrame();
        testInvalidDialogAbort();
    }

    /**
     * Helper class for testing.
     */
    private class Tester {

        private final byte[] buffer;
        private final ByteArrayChannelInput input;
        private final ByteArrayChannelOutput output;
        private final int writeMaxLen;
        private final ProtocolReader reader;
        private final ProtocolWriter writer;
        private final List<ProtocolMesg> writtenMesgs;
        Tester(int bufferSize, int readMaxLen, int writeMaxLen) {
            this.buffer = new byte[bufferSize];
            this.input = new ByteArrayChannelInput(buffer);
            this.output = new ByteArrayChannelOutput(buffer);
            this.writeMaxLen = writeMaxLen;
            this.reader = new ProtocolReader(input, readMaxLen);
            this.writer = new ProtocolWriter(output, writeMaxLen);
            this.writtenMesgs = new ArrayList<ProtocolMesg>();
        }
        @Override
        public String toString() {
            output.flush();
            StringBuilder sb = new StringBuilder();
            sb.append(BytesUtil.toString(buffer, 0, output.position()));
            sb.append(", reading position=" + input.position());
            sb.append(", reading limit=" + input.limit());
            sb.append(", writing position=" + output.position());
            sb.append(".  ");
            return sb.toString();
        }
        void writeProtocolVersion() {
            int version = rand.nextInt();
            writtenMesgs.add(new ProtocolMesg.ProtocolVersion(version));
            writer.writeProtocolVersion(version);
        }
        void writeProtocolVersionResponse() {
            int version = rand.nextInt();
            writtenMesgs.add(
                    new ProtocolMesg.ProtocolVersionResponse(version));
            writer.writeProtocolVersionResponse(version);
        }
        void writeConnectionConfig() {
            int[] v = new int[5];
            for (int i = 0; i < v.length; ++i) {
                v[i] = rand.nextInt(Integer.MAX_VALUE - 1) + 1;
            }
            writeConnectionConfig(v);
        }
        void writeConnectionConfig(int[] v) {
            writtenMesgs.add(new ProtocolMesg.ConnectionConfig(
                        v[0], v[1], v[2], v[3], v[4]));
            writer.writeConnectionConfig(v[0], v[1], v[2], v[3], v[4]);
        }
        void writeConnectionConfigResponse() {
            int[] v = new int[4];
            for (int i = 0; i < v.length; ++i) {
                v[i] = rand.nextInt(Integer.MAX_VALUE);
            }
            writeConnectionConfigResponse(v);
        }
        void writeConnectionConfigResponse(int[] v) {
            writtenMesgs.add(new ProtocolMesg.ConnectionConfigResponse(
                        v[0], v[1], v[2], v[3]));
            writer.writeConnectionConfigResponse(v[0], v[1], v[2], v[3]);
        }
        void writeNoOperation() {
            writtenMesgs.add(new ProtocolMesg.NoOperation());
            writer.writeNoOperation();
        }
        void writeConnectionAbort() {
            int detailLen = rand.nextInt(writeMaxLen);
            writeConnectionAbort(detailLen);
        }
        void writeConnectionAbort(int detailLen) {
            int ncauses = ProtocolMesg.ConnectionAbort.CAUSES.length;
            ProtocolMesg.ConnectionAbort.Cause cause =
                ProtocolMesg.ConnectionAbort.CAUSES[rand.nextInt(ncauses)];
            char[] chars = new char[detailLen];
            Arrays.fill(chars, '\u0042');
            String detail = new String(chars);
            writtenMesgs.add(new ProtocolMesg.ConnectionAbort(cause, detail));
            writer.writeConnectionAbort(cause, detail);
        }
        void writeConnectionAbort(String detail) {
            int ncauses = ProtocolMesg.ConnectionAbort.CAUSES.length;
            ProtocolMesg.ConnectionAbort.Cause cause =
                ProtocolMesg.ConnectionAbort.CAUSES[rand.nextInt(ncauses)];
            writtenMesgs.add(new ProtocolMesg.ConnectionAbort(cause, detail));
            writer.writeConnectionAbort(cause, detail);
        }
        void writePing() {
            int cookie = rand.nextInt();
            writtenMesgs.add(new ProtocolMesg.Ping(cookie));
            writer.writePing(cookie);
        }
        void writePingAck() {
            int cookie = rand.nextInt();
            writtenMesgs.add(new ProtocolMesg.PingAck(cookie));
            writer.writePingAck(cookie);
        }
        void writeDialogStart() {
            boolean finish = rand.nextBoolean();
            boolean cont = finish ? false : rand.nextBoolean();
            int typeno = rand.nextInt();
            int dialogId = rand.nextInt();
            dialogId = (dialogId == 0) ? 1 : dialogId;
            int timeout = rand.nextInt(Integer.MAX_VALUE - 1) + 1;
            int bytesLen = rand.nextBoolean() ? 0 : writeMaxLen;
            writeDialogStart(finish, cont, typeno, dialogId, timeout, bytesLen);
        }
        void writeDialogStart(boolean finish,
                              boolean cont,
                              int typeno,
                              int dialogId,
                              int timeout,
                              int bytesLen) {
            IOBufSliceList bytes = generateBytesList(bytesLen);
            writtenMesgs.add(new ProtocolMesg.DialogStart(
                        false, finish, cont, typeno, dialogId, timeout,
                        new CopyingBytesInput(bytes.toBufArray())));
            writer.writeDialogStart(
                    false, finish, cont, typeno, dialogId, timeout, bytes);
        }
        void writeWrongFlagDialogStart(int typeno,
                                       int dialogId,
                                       int timeout,
                                       int bytesLen) {
            IOBufSliceList bytes = generateBytesList(bytesLen);
            writtenMesgs.add(new ProtocolMesg.DialogStart(
                        false, true, true, typeno, dialogId, timeout,
                        new CopyingBytesInput(bytes.toBufArray())));
            writer.writeDialogStartWithoutFlagCheck(
                    false, true, true, typeno, dialogId, timeout, bytes);
        }
        void writeDialogFrame() {
            boolean finish = rand.nextBoolean();
            boolean cont = finish ? false : rand.nextBoolean();
            int dialogId = rand.nextInt();
            dialogId = (dialogId == 0) ? 1 : dialogId;
            int bytesLen = rand.nextBoolean() ? 0 : writeMaxLen;
            writeDialogFrame(finish, cont, dialogId, bytesLen);
        }
        void writeDialogFrame(boolean finish,
                              boolean cont,
                              int dialogId,
                              int bytesLen) {
            IOBufSliceList bytes = generateBytesList(bytesLen);
            writtenMesgs.add(
                new ProtocolMesg.DialogFrame(
                        finish, cont, dialogId,
                        new CopyingBytesInput(bytes.toBufArray())));
            writer.writeDialogFrame(
                    finish, cont, dialogId, bytes);
        }
        void writeWrongFlagDialogFrame(int dialogId,
                                       int bytesLen) {
            IOBufSliceList bytes = generateBytesList(bytesLen);
            writtenMesgs.add(
                new ProtocolMesg.DialogFrame(
                        true, true, dialogId,
                        new CopyingBytesInput(bytes.toBufArray())));
            writer.writeDialogFrameWithoutFlagCheck(
                    true, true, dialogId, bytes);
        }
        void writeDialogAbort() {
            int dialogId = rand.nextInt();
            int detailLen = rand.nextInt(writeMaxLen);
            writeDialogAbort(dialogId, detailLen);
        }
        void writeDialogAbort(int dialogId, int detailLen) {
            int ncauses = ProtocolMesg.DialogAbort.CAUSES.length;
            ProtocolMesg.DialogAbort.Cause cause =
                ProtocolMesg.DialogAbort.CAUSES[rand.nextInt(ncauses)];
            char[] chars = new char[detailLen];
            Arrays.fill(chars, '\u0042');
            String detail = new String(chars);
            writtenMesgs.add(
                    new ProtocolMesg.DialogAbort(cause, dialogId, detail));
            writer.writeDialogAbort(cause, dialogId, detail);
        }
        void writeDialogAbort(int dialogId, String detail) {
            int ncauses = ProtocolMesg.DialogAbort.CAUSES.length;
            ProtocolMesg.DialogAbort.Cause cause =
                ProtocolMesg.DialogAbort.CAUSES[rand.nextInt(ncauses)];
            writtenMesgs.add(
                    new ProtocolMesg.DialogAbort(cause, dialogId, detail));
            writer.writeDialogAbort(cause, dialogId, detail);
        }
        void readAndCheck() {
            List<ProtocolMesg> actual = read(Integer.MAX_VALUE);
            List<ProtocolMesg> expected = getExpectedMesgs();
            assertEquals(toString(), expected, actual);
        }
        List<ProtocolMesg> read(int mesgCnt) {
            output.flush();
            List<ProtocolMesg> actual = new ArrayList<ProtocolMesg>();
            /*
             * Gradually increase the readable part of the input to test the
             * cases of partial data.
             */
            int opos = output.position();
            while (mesgCnt > 0) {
                int ilimit = input.limit();
                if (ilimit == opos) {
                    break;
                }
                int inc = (opos - ilimit == 1) ?
                    1 : rand.nextInt(Math.min(16, opos - ilimit - 1)) + 1;
                input.limit(ilimit + inc);
                while (true) {
                    ProtocolMesg mesg = reader.read((id) -> true);
                    if (mesg == null) {
                        break;
                    }
                    actual.add(mesg);
                    mesgCnt --;
                    if (mesgCnt <= 0) {
                        break;
                    }
                }
            }
            return actual;
        }
        private List<ProtocolMesg> getExpectedMesgs() {
            /* Ignore messages after the first ConnectionAbort. */
            List<ProtocolMesg> expected = new ArrayList<ProtocolMesg>();
            for (ProtocolMesg mesg : writtenMesgs) {
                expected.add(mesg);
                if (mesg.type() == ProtocolMesg.CONNECTION_ABORT_MESG) {
                    break;
                }
            }
            return expected;
        }
    }

    private IOBufSliceList generateBytesList(int bytesLen) {
        IOBufSliceList bufs = new IOBufSliceList();
        if (bytesLen <= 0) {
            return bufs;
        }
        int n = (bytesLen == 1) ?
            1 : rand.nextInt(Math.min(8, bytesLen - 1)) + 1;
        int len = bytesLen / n;
        int cnt = 0;
        for (int i = 0; i < n; ++i) {
            int s = (i != n - 1) ? len : bytesLen - len * (n - 1);
            byte[] b = new byte[s];
            for (int j = 0; j < b.length; ++j) {
                b[j] = (byte) cnt++;
            }
            bufs.add(new IOBufSliceImpl.HeapBufSlice(ByteBuffer.wrap(b)));
        }
        return bufs;
    }

    private void writeNonConnAbortMesgs(Tester tester) {
        tester.writeProtocolVersion();
        tester.writeProtocolVersionResponse();
        tester.writeConnectionConfig();
        tester.writeConnectionConfigResponse();
        tester.writeNoOperation();
        tester.writePing();
        tester.writePingAck();
        tester.writeDialogStart();
        tester.writeDialogFrame();
        tester.writeDialogAbort();
    }

    private void checkReadProtocolViolation(Tester tester, String detail) {
        try {
            tester.readAndCheck();
            fail("Should throw ProtocolViolationException with " +
                    detail + ".\n" + tester.toString());
        } catch (IllegalStateException e) {
            if (!e.getMessage().contains(detail)) {
                throw e;
            }
        }
    }


    private void testInvalidProtocolVersion() {
        testInvalidProtocolHandshakeMesg(ProtocolMesg.PROTOCOL_VERSION_MESG);
    }

    private void testInvalidProtocolVersionResponse() {
        testInvalidProtocolHandshakeMesg(
                ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG);
    }

    private void testInvalidProtocolHandshakeMesg(byte ident) {
        Tester tester = new Tester(16, 0, 0);
        try {
            /* Test invalid magic number. */
            ChannelOutput output = tester.output;
            ChannelOutput.Chunk chunk = output.beginChunk(16, false);
            chunk.writeByte(ident);
            IOBufSliceList bytes = new IOBufSliceList();
            bytes.add(new IOBufSliceImpl.HeapBufSlice(ByteBuffer.allocate(3)));
            chunk.writeBytes(bytes);
            chunk.writePackedLong(42);
            chunk.done();
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_MAGIC_NUMBER);
        } catch (Exception e) {
            fail(LoggerUtils.getStackTrace(e) + "\n" +
                    tester.toString());
        }
    }

    private void testInvalidConnectionConfig() {
        testInvalidConfigHandshakeMesg(false);
    }

    private void testInvalidConnectionConfigResponse() {
        testInvalidConfigHandshakeMesg(true);
    }

    private void testInvalidConfigHandshakeMesg(boolean isResponse) {
        for (int i = 0; i < 1024; ++i) {
            Tester tester = new Tester(64, 0, 0);
            try {
                /* Test invalid fields. */
                int[] v = isResponse ? new int[4] : new int[5];
                for (int n = 0; n < v.length; ++n) {
                    v[n] = rand.nextInt();
                }
                if (!isResponse) {
                    tester.writeConnectionConfig(v);
                } else {
                    tester.writeConnectionConfigResponse(v);
                }
                for (int n = 0; n < 4; ++n) {
                    if (v[v.length - n - 1] <= 0) {
                        checkReadProtocolViolation(tester,
                                ProtocolViolationException.
                                ERROR_INVALID_FIELD);
                        break;
                    }
                }
            } catch (Exception e) {
                fail(LoggerUtils.getStackTrace(e) + "\n" +
                        tester.toString());
            }
        }
    }

    private void testInvalidConnectionAbort() {
        int bufferSize = 128;
        int writeLen = 64;
        int readLen = 8;
        Tester tester = null;
        try {
            /* Test invalid errno. */
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
                tester = new Tester(bufferSize, readLen, writeLen);
                ChannelOutput output = tester.output;
                ChannelOutput.Chunk chunk = output.beginChunk(16, false);
                chunk.writeByte(ProtocolMesg.CONNECTION_ABORT_MESG);
                chunk.writeByte((byte) i);
                chunk.writeUTF8("42", writeLen);
                chunk.done();
                if ((i < 0) ||
                    (i >= ProtocolMesg.ConnectionAbort.CAUSES.length)) {
                    ProtocolMesg.ConnectionAbort.Cause expected =
                        ProtocolMesg.ConnectionAbort.Cause.UNKNOWN_REASON;
                    ProtocolMesg.ConnectionAbort mesg =
                        (ProtocolMesg.ConnectionAbort) tester.read(1).get(0);
                    assertEquals(
                            "The errno of ConnectionAbort " +
                            "that has an invalid number should be: " +
                            expected + ", but got" + mesg.cause,
                            mesg.cause, expected);
                }
            }
            /* Test invalid length. */
            tester = new Tester(bufferSize, readLen, writeLen);
            ChannelOutput output = tester.output;
            ChannelOutput.Chunk chunk = output.beginChunk(16, false);
            chunk.writeByte(ProtocolMesg.CONNECTION_ABORT_MESG);
            chunk.writeByte((byte) 0);
            chunk.writePackedLong(-1);
            chunk.done();
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeConnectionAbort(writeLen);
            ProtocolMesg.ConnectionAbort mesg =
                (ProtocolMesg.ConnectionAbort) tester.read(1).get(0);
            assertEquals("The detail of ConnectionAbort " +
                    "that has a length too large should be ignored",
                    mesg.detail.length(), 0);
        } catch (Exception e) {
            fail(LoggerUtils.getStackTrace(e) + "\n" +
                    ((tester == null) ? null : tester.toString()));
        }
    }

    private void testInvalidDialogStart() {
        int bufferSize = 128;
        int writeLen = 64;
        int readLen = 8;
        Tester tester = null;
        try {
            /* Test invalid finish and cont combination. */
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeWrongFlagDialogStart(42, 42, 42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            /* Test invalid dialogId and timeoutMillis */
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogStart(true, false, 42, 0, 42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogStart(true, false, 42, 42, 0, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogStart(true, false, 42, 42, -42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            /* Test invalid length */
            tester = new Tester(bufferSize, readLen, writeLen);
            ChannelOutput output = tester.output;
            ChannelOutput.Chunk chunk = output.beginChunk(16, false);
            chunk.writeByte(ProtocolMesg.DIALOG_START_MESG);
            chunk.writePackedLong(42);
            chunk.writePackedLong(42);
            chunk.writePackedLong(42);
            chunk.writePackedLong(-1);
            chunk.done();
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogStart(true, false, 42, 42, 42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_MAX_LENGTH_EXCEEDED);
        } catch (Exception e) {
            fail(LoggerUtils.getStackTrace(e) + "\n" +
                    ((tester == null) ? null : tester.toString()));
        }
    }

    private void testInvalidDialogFrame() {
        int bufferSize = 128;
        int writeLen = 64;
        int readLen = 8;
        Tester tester = null;
        try {
            /* Test invalid finish and cont combination. */
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeWrongFlagDialogFrame(42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            /* Test invalid dialogId */
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogFrame(true, false, 0, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            /* Test invalid length */
            tester = new Tester(bufferSize, readLen, writeLen);
            ChannelOutput output = tester.output;
            ChannelOutput.Chunk chunk = output.beginChunk(16, false);
            chunk.writeByte(ProtocolMesg.DIALOG_FRAME_MESG);
            chunk.writePackedLong(42);
            chunk.writePackedLong(-1);
            chunk.done();
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogFrame(true, false, 42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_MAX_LENGTH_EXCEEDED);
        } catch (Exception e) {
            fail(LoggerUtils.getStackTrace(e) + "\n" +
                    ((tester == null) ? null : tester.toString()));
        }
    }

    private void testInvalidDialogAbort() {
        int bufferSize = 128;
        int writeLen = 64;
        int readLen = 8;
        Tester tester = null;
        try {
            /* Test invalid errno */
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
                tester = new Tester(bufferSize, readLen, writeLen);
                ChannelOutput output = tester.output;
                ChannelOutput.Chunk chunk = output.beginChunk(16, false);
                chunk.writeByte(ProtocolMesg.DIALOG_ABORT_MESG);
                chunk.writeByte((byte) i);
                chunk.writePackedLong(42);
                chunk.writeUTF8("42", writeLen);
                chunk.done();
                if ((i < 0) ||
                    (i >= ProtocolMesg.DialogAbort.CAUSES.length)) {
                    ProtocolMesg.DialogAbort.Cause expected =
                        ProtocolMesg.DialogAbort.Cause.UNKNOWN_REASON;
                    ProtocolMesg.DialogAbort mesg =
                        (ProtocolMesg.DialogAbort) tester.read(1).get(0);
                    assertEquals(
                            "The errno of DialogAbort " +
                            "that has an invalid number should be: " +
                            expected + ", but got" + mesg.cause,
                            mesg.cause, expected);
                }
            }
            /* Test invalid dialogId */
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogAbort(0, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            /* Test invalid length */
            tester = new Tester(bufferSize, readLen, writeLen);
            ChannelOutput output = tester.output;
            ChannelOutput.Chunk chunk = output.beginChunk(16, false);
            chunk.writeByte(ProtocolMesg.DIALOG_ABORT_MESG);
            chunk.writeByte((byte) 0);
            chunk.writePackedLong(42);
            chunk.writePackedLong(-1);
            chunk.done();
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD);
            tester = new Tester(bufferSize, readLen, writeLen);
            tester.writeDialogAbort(42, writeLen);
            checkReadProtocolViolation(tester,
                    ProtocolViolationException.
                    ERROR_MAX_LENGTH_EXCEEDED);
        } catch (Exception e) {
            fail(LoggerUtils.getStackTrace(e) + "\n" +
                    ((tester == null) ? null : tester.toString()));
        }
    }
}
