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

import oracle.kv.impl.async.BytesInput;

/**
 * Messages of the dialog protocol.
 *
 * Please see
 * https://sleepycat-tools.us.oracle.com/trac/wiki/JEKV/AsyncDialogProtocol for
 * the protocol.
 *
 * Please also update {@link ProtocolReader} and {@link ProtocolWriter} when
 * update the protocol message.
 */
public abstract class ProtocolMesg {

    public static final byte[] MAGIC_NUMBER = { 0x01, 0x44, 0x4C, 0x47 };

    public static final byte PROTOCOL_VERSION_MESG = 0x01;
    public static final byte PROTOCOL_VERSION_RESPONSE_MESG = 0x02;
    public static final byte CONNECTION_CONFIG_MESG = 0x03;
    public static final byte CONNECTION_CONFIG_RESPONSE_MESG = 0x04;
    public static final byte NO_OPERATION_MESG = 0x08;
    public static final byte CONNECTION_ABORT_MESG = 0x09;
    public static final byte PING_MESG = 0x0A;
    public static final byte PINGACK_MESG = 0x0B;
    public static final byte DIALOG_START_MESG = 0x10;
    public static final byte DIALOG_FRAME_MESG = 0x20;
    public static final byte DIALOG_ABORT_MESG = 0x30;

    /**
     * Current version of the protocol.
     *
     * TODO: Might need to refactor code with reader and writer when we know
     * better about how our protocol might upgrade.
     */
    public static final int CURRENT_VERSION = 1;

    public static byte getType(byte b) {
        byte masked = (byte) (b & 0xF0);
        return (masked == 0) ? b : masked;
    }

    public abstract byte type();

    /**
     * Discards the message and does necessary clean up.
     */
    public abstract void discard();

    /**
     * ProtocolVersion message.
     *
     * ------------------------------------------------------------------------
     * | field order  |     type      |  bit pattern    |       content       |
     * ------------------------------------------------------------------------
     * |       0      |     byte      |  0000 0001      |     identifier      |
     * ------------------------------------------------------------------------
     * |       1      |     byte      |  0100 0100      |  magic number ("D") |
     * ------------------------------------------------------------------------
     * |       2      |     byte      |  0100 1100      |  magic number ("L") |
     * ------------------------------------------------------------------------
     * |       3      |     byte      |  0100 0111      |  magic number ("G") |
     * ------------------------------------------------------------------------
     * |       4      |packed integer |     n/a         |    protocolVersion  |
     * ------------------------------------------------------------------------
     *
     */
    public static class ProtocolVersion extends ProtocolMesg {

        public final int version;

        ProtocolVersion(int version) {
             this.version = version;
        }

        @Override
        public byte type() {
            return PROTOCOL_VERSION_MESG;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof ProtocolVersion)) {
                return false;
            }

             ProtocolVersion that = (ProtocolVersion) obj;
             return (this.version == that.version);
        }

        @Override
        public int hashCode() {
            return version;
        }

        @Override
        public String toString() {
            return String.format("ProtocolVersion(%s)", version);
        }
    }

    /**
     * ProtocolVersionResponse message.
     *
     * ------------------------------------------------------------------------
     * | field order  |     type      |  bit pattern    |       content       |
     * ------------------------------------------------------------------------
     * |       0      |     byte      |  0000 0010      |     identifier      |
     * ------------------------------------------------------------------------
     * |       1      |     byte      |  0100 0100      |  magic number ("D") |
     * ------------------------------------------------------------------------
     * |       2      |     byte      |  0100 1100      |  magic number ("L") |
     * ------------------------------------------------------------------------
     * |       3      |     byte      |  0100 0111      |  magic number ("G") |
     * ------------------------------------------------------------------------
     * |       4      |packed integer |     n/a         |    protocolVersion  |
     * ------------------------------------------------------------------------
     */
    public static class ProtocolVersionResponse extends ProtocolMesg {

        public final int version;

        ProtocolVersionResponse(int version) {
             this.version = version;
        }

        @Override
        public byte type() {
            return PROTOCOL_VERSION_RESPONSE_MESG;
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof ProtocolVersionResponse)) {
                return false;
            }

            ProtocolVersionResponse that = (ProtocolVersionResponse) obj;
            return (this.version == that.version);
        }

        @Override
        public int hashCode() {
             return version;
        }

        @Override
        public String toString() {
            return String.format(
                    "ProtocolVersionResponse(%s)", version);
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }
    }

    /**
     * ConnectionConfig message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0000 0011  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      | packed integer|     n/a     |            uuid          |
     * ------------------------------------------------------------------------
     * |      2      | packed integer|     n/a     |         maxDialogs       |
     * ------------------------------------------------------------------------
     * |      3      | packed integer|     n/a     |         maxLength        |
     * ------------------------------------------------------------------------
     * |      4      | packed integer|     n/a     |         maxTotLen        |
     * ------------------------------------------------------------------------
     * |      5      | byte sequence |     n/a     |      heartbeatInterval   |
     * ------------------------------------------------------------------------
     */
    public static class ConnectionConfig extends ProtocolMesg {

        public final long uuid;
        public final long maxDialogs;
        public final long maxLength;
        public final long maxTotLen;
        public final long heartbeatInterval;

        ConnectionConfig(long uuid,
                         long maxDialogs,
                         long maxLength,
                         long maxTotLen,
                         long heartbeatInterval) {
            this.uuid = uuid;
            this.maxDialogs = maxDialogs;
            this.maxLength = maxLength;
            this.maxTotLen = maxTotLen;
            this.heartbeatInterval = heartbeatInterval;
        }

        @Override
        public byte type() {
            return CONNECTION_CONFIG_MESG;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof ConnectionConfig)) {
                 return false;
            }

             ConnectionConfig that = (ConnectionConfig) obj;
             return ((this.uuid == that.uuid) &&
                     (this.maxDialogs == that.maxDialogs) &&
                     (this.maxLength == that.maxLength) &&
                     (this.maxTotLen == that.maxTotLen) &&
                     (this.heartbeatInterval ==
                      that.heartbeatInterval));
        }

        @Override
        public int hashCode() {
            int hash = 17;
            hash = hash * 31 + (int) uuid;
            hash = hash * 31 + (int) maxDialogs;
            hash = hash * 31 + (int) maxLength;
            hash = hash * 31 + (int) maxTotLen;
            hash = hash * 31 + (int) heartbeatInterval;
            return hash;
        }

        @Override
        public String toString() {
            return String.format(
                    "ConnectionConfig(uuid=%x " +
                    "maxDialogs=%d maxLength=%d maxTotLen=%d " +
                    "heartbeatInterval=%d)",
                    uuid, maxDialogs, maxLength, maxTotLen,
                    heartbeatInterval);
        }
    }


    /**
     * ConnectionConfigResponse message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0000 0100  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      | packed integer|     n/a     |          maxDialogs      |
     * ------------------------------------------------------------------------
     * |      2      | packed integer|     n/a     |          maxLength       |
     * ------------------------------------------------------------------------
     * |      3      | packed integer|     n/a     |          maxTotLen       |
     * ------------------------------------------------------------------------
     * |      4      | byte sequence |     n/a     |      heartbeatInterval   |
     * ------------------------------------------------------------------------
     *
     *
     */
    public static class ConnectionConfigResponse extends ProtocolMesg {

        public final long maxDialogs;
        public final long maxLength;
        public final long maxTotLen;
        public final long heartbeatInterval;

        ConnectionConfigResponse(long maxDialogs,
                                 long maxLength,
                                 long maxTotLen,
                                 long heartbeatInterval) {
            this.maxDialogs = maxDialogs;
            this.maxLength = maxLength;
            this.maxTotLen = maxTotLen;
            this.heartbeatInterval = heartbeatInterval;
        }

        @Override
        public byte type() {
            return CONNECTION_CONFIG_RESPONSE_MESG;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof ConnectionConfigResponse)) {
                return false;
            }

             ConnectionConfigResponse that = (ConnectionConfigResponse) obj;
             return ((this.maxDialogs == that.maxDialogs) &&
                     (this.maxLength == that.maxLength) &&
                     (this.maxTotLen == that.maxTotLen) &&
                     (this.heartbeatInterval ==
                      that.heartbeatInterval));
        }

        @Override
        public int hashCode() {
            int hash = 17;
            hash = hash * 31 + (int) maxDialogs;
            hash = hash * 31 + (int) maxLength;
            hash = hash * 31 + (int) maxTotLen;
            hash = hash * 31 + (int) heartbeatInterval;
            return hash;
        }

        @Override
        public String toString() {
            return String.format(
                    "ConnectionConfigResponse(" +
                    "maxDialogs=%d maxLength=%d maxTotLen=%d " +
                    "heartbeatInterval=%d)",
                    maxDialogs, maxLength, maxTotLen, heartbeatInterval);
        }
    }

    /**
     * NoOperation message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0000 1000  |  message identifier      |
     * ------------------------------------------------------------------------
     *
     */
    public static class NoOperation extends ProtocolMesg {

        NoOperation() { }

        @Override
        public byte type() {
            return NO_OPERATION_MESG;

        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof NoOperation)) {
                 return false;
            }

             return true;
        }

        @Override
        public int hashCode() {
             return 0;
        }

        @Override
        public String toString() {
            return String.format("NoOperation");
        }
    }

    /**
     * ConnectionAbort message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0000 1001  |          identifier      |
     * ------------------------------------------------------------------------
     * |      1      |      byte     |  xxxx xxxx  |          errno           |
     * ------------------------------------------------------------------------
     * |      2      | packed integer|     n/a     |          length          |
     * ------------------------------------------------------------------------
     * |      3      | byte sequence |     n/a     |   detail of length bytes |
     * ------------------------------------------------------------------------
     *
     */
    public static class ConnectionAbort extends ProtocolMesg {

        public enum Cause {
            UNKNOWN_REASON(0),
            ENDPOINT_SHUTDOWN(1),
            HEARTBEAT_TIMEOUT(2),
            IDLE_TIMEOUT(3),
            INCOMPATIBLE_ERROR(4),
            PROTOCOL_VIOLATION(5);

            private Cause(int ordinal) {
                if (ordinal != ordinal()) {
                    throw new IllegalStateException();
                }
            }
        }
        final static Cause[] CAUSES = Cause.values();

        public final Cause cause;
        public final String detail;

        ConnectionAbort(Cause cause, String detail) {
            this.cause = cause;
            this.detail = detail;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public byte type() {
            return CONNECTION_ABORT_MESG;
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof ConnectionAbort)) {
                 return false;
            }

             ConnectionAbort that = (ConnectionAbort) obj;
             return ((this.cause.equals(that.cause)) &&
                     (this.detail.equals(that.detail)));
        }

        @Override
        public int hashCode() {
             return cause.hashCode() * 31 + detail.hashCode();
        }

        @Override
        public String toString() {
            return String.format("ConnectionAbort(" +
                    "cause=%s detail=%s)",
                    cause, detail);
        }
    }

    /**
     * Ping message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0000 1010  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      | packed integer|     n/a     |           cookie         |
     * ------------------------------------------------------------------------
     *
     */
    public static class Ping extends ProtocolMesg {

        public final long cookie;

        Ping(long cookie) {
            this.cookie = cookie;
        }

        @Override
        public byte type() {
            return PING_MESG;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof Ping)) {
                 return false;
            }

             Ping that = (Ping) obj;
             return (this.cookie == that.cookie);
        }

        @Override
        public int hashCode() {
             return (int) cookie;
        }

        @Override
        public String toString() {
            return String.format("Ping(%d)", cookie);
        }
    }

    /**
     * PingAck message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0000 1011  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      | packed integer|     n/a     |           cookie         |
     * ------------------------------------------------------------------------
     *
     */
    public static class PingAck extends ProtocolMesg {

        public final long cookie;

        PingAck(long cookie) {
            this.cookie = cookie;
        }

        @Override
        public byte type() {
            return PINGACK_MESG;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof PingAck)) {
                 return false;
            }

             PingAck that = (PingAck) obj;
             return (this.cookie == that.cookie);
        }

        @Override
        public int hashCode() {
             return (int) cookie;
        }

        @Override
        public String toString() {
            return String.format("PingAck(%d)", cookie);
        }
    }

    /**
     * DialogStart message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0001 0---  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      |      flag     |  ---- -x--  |           sampled        |
     * ------------------------------------------------------------------------
     * |      2      |      flag     |  ---- --x-  |           finish         |
     * ------------------------------------------------------------------------
     * |      3      |      flag     |  ---- ---x  |            cont          |
     * ------------------------------------------------------------------------
     * |      4      | packed integer|     n/a     |           typeno        |
     * ------------------------------------------------------------------------
     * |      5      | packed integer|     n/a     |         dialogID         |
     * ------------------------------------------------------------------------
     * |      6      | packed integer|     n/a     |       timeoutMillis      |
     * ------------------------------------------------------------------------
     * |      7      | packed integer|     n/a     |           length         |
     * ------------------------------------------------------------------------
     * |      8      | byte sequence |     n/a     |   data of length bytes   |
     * ------------------------------------------------------------------------
     *
     */
    public static class DialogStart extends ProtocolMesg {

        public final boolean sampled;
        public final boolean finish;
        public final boolean cont;
        public final int typeno;
        public final long dialogId;
        public final long timeoutMillis;
        public final BytesInput frame;

        DialogStart(boolean sampled,
                    boolean finish,
                    boolean cont,
                    int typeno,
                    long dialogId,
                    long timeoutMillis,
                    BytesInput frame) {
            this.sampled = sampled;
            this.finish = finish;
            this.cont = cont;
            this.typeno = typeno;
            this.dialogId = dialogId;
            this.timeoutMillis = timeoutMillis;
            this.frame = frame;
        }

        @Override
        public byte type() {
            return DIALOG_START_MESG;
        }

        @Override
        public void discard() {
            frame.discard();
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof DialogStart)) {
                return false;
            }

             DialogStart that = (DialogStart) obj;
             return ((this.sampled == that.sampled) &&
                     (this.finish == that.finish) &&
                     (this.cont == that.cont) &&
                     (this.typeno == that.typeno) &&
                     (this.dialogId == that.dialogId) &&
                     (this.timeoutMillis == that.timeoutMillis) &&
                     (this.frame.equals(that.frame)));
        }

        @Override
        public int hashCode() {
            int hash = 17;
            int prime = 31;
            hash = hash * prime + Boolean.valueOf(sampled).hashCode();
            hash = hash * prime + Boolean.valueOf(finish).hashCode();
            hash = hash * prime + Boolean.valueOf(cont).hashCode();
            hash = hash * prime + typeno;
            hash = hash * prime + (int) dialogId;
            hash = hash * prime + (int) timeoutMillis;
            hash = hash * prime + frame.hashCode();
            return hash;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(
                    String.format("DialogStart(" +
                    "sampled=%s finish=%s cont=%s dialogType=%d " +
                    "dialogId=%s timeoutMillis=%d " +
                    "frame=%s)",
                    sampled, finish, cont, typeno,
                    Long.toString(dialogId, 16),
                    timeoutMillis, frame));
            return builder.toString();
        }
    }

    /**
     * DialogFrame message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0010 00--  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      |      flag     |  ---- --x-  |           finish         |
     * ------------------------------------------------------------------------
     * |      2      |      flag     |  ---- ---x  |            cont          |
     * ------------------------------------------------------------------------
     * |      3      | packed integer|     n/a     |         dialogID         |
     * ------------------------------------------------------------------------
     * |      4      | packed integer|     n/a     |           length         |
     * ------------------------------------------------------------------------
     * |      5      | byte sequence |     n/a     |   data of length bytes   |
     * ------------------------------------------------------------------------
     *
     */
    public static class DialogFrame extends ProtocolMesg {

        public final boolean finish;
        public final boolean cont;
        public final long dialogId;
        public final BytesInput frame;

        DialogFrame(boolean finish,
                    boolean cont,
                    long dialogId,
                    BytesInput frame) {
            this.finish = finish;
            this.cont = cont;
            this.dialogId = dialogId;
            this.frame = frame;
        }

        @Override
        public byte type() {
            return DIALOG_FRAME_MESG;
        }

        @Override
        public void discard() {
            frame.discard();
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof DialogFrame)) {
                 return false;
            }

             DialogFrame that = (DialogFrame) obj;
             return ((this.finish == that.finish) &&
                     (this.cont == that.cont) &&
                     (this.dialogId == that.dialogId) &&
                     (this.frame.equals(that.frame)));
        }

        @Override
        public int hashCode() {
            int hash = 17;
            int prime = 31;
            hash = hash * prime + Boolean.valueOf(finish).hashCode();
            hash = hash * prime + Boolean.valueOf(cont).hashCode();
            hash = hash * prime + (int) dialogId;
            hash = hash * prime + frame.hashCode();
            return hash;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(
                    String.format("DialogFrame(" +
                    "finish=%s cont=%s dialogId=%s frame=%s)",
                    finish, cont, Long.toString(dialogId, 16), frame));
            return builder.toString();
        }
    }


    /**
     * DialogAbort message.
     *
     * ------------------------------------------------------------------------
     * | field order |      type     | bit pattern |          content         |
     * ------------------------------------------------------------------------
     * |      0      |      byte     |  0011 0000  |     message identifier   |
     * ------------------------------------------------------------------------
     * |      1      |      byte     |  xxxx xxxx  |           errno          |
     * ------------------------------------------------------------------------
     * |      2      | packed integer|     n/a     |         dialogID         |
     * ------------------------------------------------------------------------
     * |      3      | packed integer|     n/a     |           length         |
     * ------------------------------------------------------------------------
     * |      4      | byte sequence |     n/a     |   data of length bytes   |
     * ------------------------------------------------------------------------
     *
     */
    public static class DialogAbort extends ProtocolMesg {

        public enum Cause {
            UNKNOWN_REASON(0),
            /*
             * The CONNECTION_ABORT flag is deprecated on the wire protocol.
             * DialogAbort messages should not be sent when the connection is
             * aborted. The reason is that this cause type is vague and covers
             * various categories that needs to be handled differently. A
             * follow-up ConnectionAbort message with detailed cause is sent and
             * thus should be used to handle error in this case.
             *
             * The enum is not removed due to compatibility reason and is only
             * used internally for marking the abort type.
             *
             * See [KVSTORE-441] and [KVSTORE-458] issue 5.
             */
            CONNECTION_ABORT(1),
            ENDPOINT_SHUTTINGDOWN(2),
            TIMED_OUT(3),
            UNKNOWN_TYPE(4);

            private Cause(int ordinal) {
                if (ordinal != ordinal()) {
                    throw new IllegalStateException();
                }
            }
        }
        final static Cause[] CAUSES = Cause.values();

        public final Cause cause;
        public final long dialogId;
        public final String detail;

        DialogAbort(Cause cause,
                    long dialogId,
                    String detail) {
            this.cause = cause;
            this.dialogId = dialogId;
            this.detail = detail;
        }

        @Override
        public byte type() {
            return DIALOG_ABORT_MESG;
        }

        @Override
        public void discard() {
            /* no need for clean up */
        }

        @Override
        public boolean equals(Object obj) {
             if (! (obj instanceof DialogAbort))
                 return false;

             DialogAbort that = (DialogAbort) obj;
             return ((this.cause == that.cause) &&
                     (this.dialogId == that.dialogId) &&
                     (this.detail.equals(that.detail)));
        }

        @Override
        public int hashCode() {
            int hash = 17;
            hash = hash * 31 + cause.hashCode();
            hash = hash * 31 + (int) dialogId;
            hash = hash * 31 + detail.hashCode();
            return hash;
        }

        @Override
        public String toString() {
            return String.format("DialogAbort(" +
                    "cause=%s, dialogId=%s, detail=%s)",
                    cause, Long.toString(dialogId, 16), detail);
        }
    }

}
