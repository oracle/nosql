/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.async.dialog;

import com.sleepycat.util.PackedInteger;

import oracle.kv.impl.async.IOBufSliceList;

public class ProtocolWriter {

    private static final int SZ_HEADER = 1;
    private static final int SZ_MAGIC = 3;
    private static final int SZ_LONG = PackedInteger.MAX_LONG_LENGTH;

    private final ChannelOutput output;
    private volatile int maxMesgLen;

    public ProtocolWriter(ChannelOutput output, int maxLength) {
        this.maxMesgLen = maxLength;
        this.output = output;
    }

    public void setMaxLength(int val) {
        maxMesgLen = val;
    }

    public void writeProtocolVersion(int version) {
        int size = SZ_HEADER + SZ_MAGIC + SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.PROTOCOL_VERSION_MESG);
        chunk.writeByte(ProtocolMesg.MAGIC_NUMBER[1]);
        chunk.writeByte(ProtocolMesg.MAGIC_NUMBER[2]);
        chunk.writeByte(ProtocolMesg.MAGIC_NUMBER[3]);
        chunk.writePackedLong(version);
        chunk.done();
    }

    public void writeProtocolVersionResponse(int version) {
        int size = SZ_HEADER + SZ_MAGIC + SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG);
        chunk.writeByte(ProtocolMesg.MAGIC_NUMBER[1]);
        chunk.writeByte(ProtocolMesg.MAGIC_NUMBER[2]);
        chunk.writeByte(ProtocolMesg.MAGIC_NUMBER[3]);
        chunk.writePackedLong(version);
        chunk.done();
    }

    public void writeConnectionConfig(long uuid,
                                      long maxDialogs,
                                      long maxLength,
                                      long maxTotLen,
                                      long heartbeatInterval) {
        int size = SZ_HEADER + 5 * SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.CONNECTION_CONFIG_MESG);
        chunk.writePackedLong(uuid);
        chunk.writePackedLong(maxDialogs);
        chunk.writePackedLong(maxLength);
        chunk.writePackedLong(maxTotLen);
        chunk.writePackedLong(heartbeatInterval);
        chunk.done();
    }

    public void writeConnectionConfigResponse(long maxDialogs,
                                              long maxLength,
                                              long maxTotLen,
                                              long heartbeatInterval) {
        int size = SZ_HEADER + 4 * SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG);
        chunk.writePackedLong(maxDialogs);
        chunk.writePackedLong(maxLength);
        chunk.writePackedLong(maxTotLen);
        chunk.writePackedLong(heartbeatInterval);
        chunk.done();
    }

    public void writeNoOperation() {
        int size = SZ_HEADER;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.NO_OPERATION_MESG);
        chunk.done();
    }

    public void writeConnectionAbort(ProtocolMesg.ConnectionAbort.Cause cause,
                                     String detail) {
        int size = SZ_HEADER + 1 + SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, true);
        chunk.writeByte(ProtocolMesg.CONNECTION_ABORT_MESG);
        chunk.writeByte((byte) cause.ordinal());
        chunk.writeUTF8(detail, maxMesgLen);
        chunk.done();
    }

    public void writePing(long cookie) {
        int size = SZ_HEADER + SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.PING_MESG);
        chunk.writePackedLong(cookie);
        chunk.done();
    }

    public void writePingAck(long cookie) {
        int size = SZ_HEADER + SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.PINGACK_MESG);
        chunk.writePackedLong(cookie);
        chunk.done();
    }

    public void writeDialogStart(boolean sampled,
                                 boolean finish,
                                 boolean cont,
                                 int typeno,
                                 long dialogId,
                                 long timeout,
                                 IOBufSliceList bytes) {
        if (finish && cont) {
            throw new IllegalArgumentException(
                    "The flags finish and cont " +
                    "cannot be both true in DialogStart");
        }
        writeDialogStartWithoutFlagCheck(
                sampled, finish, cont, typeno, dialogId, timeout, bytes);
    }

    /* for test */
    public void writeDialogStartWithoutFlagCheck(
            boolean sampled,
            boolean finish,
            boolean cont,
            int typeno,
            long dialogId,
            long timeout,
            IOBufSliceList bytes) {
        byte ident = ProtocolMesg.DIALOG_START_MESG;
        ident |= (byte) (sampled ? 0x04 : 0x00);
        ident |= (byte) (finish ? 0x02 : 0x00);
        ident |= (byte) (cont ? 0x01 : 0x00);
        int len = 0;
        IOBufSliceList.Entry curr = bytes.head();
        while (curr != null) {
            len += curr.buf().remaining();
            curr = curr.next();
        }
        if (len > maxMesgLen) {
            throw new IllegalArgumentException(
                    String.format(
                        "Length of the message is %d, but limit is %d",
                        len, maxMesgLen));
        }
        int size = SZ_HEADER + 4 * SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ident);
        chunk.writePackedLong(typeno);
        chunk.writePackedLong(dialogId);
        chunk.writePackedLong(timeout);
        chunk.writePackedLong(len);
        chunk.writeBytes(bytes);
        chunk.done();
    }

    public void writeDialogFrame(boolean finish,
                                 boolean cont,
                                 long dialogId,
                                 IOBufSliceList bytes) {
        if (finish && cont) {
            throw new IllegalArgumentException(
                    "The flags finish and cont " +
                    "cannot be both true in DialogStart");
        }
        writeDialogFrameWithoutFlagCheck(
                finish, cont, dialogId, bytes);
    }

    /* for test */
    public void writeDialogFrameWithoutFlagCheck(
            boolean finish,
            boolean cont,
            long dialogId,
            IOBufSliceList bytes) {
        byte ident = ProtocolMesg.DIALOG_FRAME_MESG;
        ident |= (byte) (finish ? 0x02 : 0x00);
        ident |= (byte) (cont ? 0x01 : 0x00);
        int len = 0;
        IOBufSliceList.Entry curr = bytes.head();
        while (curr != null) {
            len += curr.buf().remaining();
            curr = curr.next();
        }
        if (len > maxMesgLen) {
            throw new IllegalArgumentException(
                    String.format(
                        "Length of the message is %d, but limit is %d",
                        len, maxMesgLen));
        }
        int size = SZ_HEADER + 2 * SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ident);
        chunk.writePackedLong(dialogId);
        chunk.writePackedLong(len);
        chunk.writeBytes(bytes);
        chunk.done();
    }

    public void writeDialogAbort(ProtocolMesg.DialogAbort.Cause cause,
                                 long dialogId,
                                 String detail) {
        int size = SZ_HEADER + 1 + SZ_LONG;
        ChannelOutput.Chunk chunk = output.beginChunk(size, false);
        chunk.writeByte(ProtocolMesg.DIALOG_ABORT_MESG);
        chunk.writeByte((byte) cause.ordinal());
        chunk.writePackedLong(dialogId);
        chunk.writeUTF8(detail, maxMesgLen);
        chunk.done();
    }
}
