/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import javax.transaction.xa.Xid;

import com.sleepycat.je.utilint.Timestamp;
import com.sleepycat.util.PackedInteger;
import com.sleepycat.utilint.StringUtils;

/**
 * This class holds convenience methods for marshalling internal JE data to and
 * from the log.
 */
public class LogUtils {
    /* Storage sizes for int, long in log. */
    public static final int SHORT_BYTES = 2;
    public static final int INT_BYTES = 4;
    public static final int LONG_BYTES = 8;
    public static final int UNSIGNED_INT_BYTES = 4;

    private static final boolean DEBUG = false;

    /*
     * We can return the same byte[] for 0 length arrays.
     */
    public static final byte[] ZERO_LENGTH_BYTE_ARRAY = new byte[0];

    /*
     * The je.logCharset system property can be specified when running
     * DbPrintLog to work around the charset issue in JE 5.0 and earlier (see
     * [#15296] below).  For example, because of this issue, on a z/OS system
     * the trace messages and other internal strings (such as the checkpoint
     * invoker) are stored in the log in EBCDIC encoding.  The following system
     * property allows such strings to be viewed correctly in the DbPrintLog
     * output:
     *      -Dje.logCharset=IBM1047
     *
     * WARNING: Do not specify this property when running an application that
     * writes to the log.  It is only for reading a log (such as with
     * DbPrintLog) that was written with a non-ANSI-based default charset.
     */
    private static Charset logCharset = null;
    static {
        final String charsetName = System.getProperty("je.logCharset");
        if (charsetName != null && charsetName.length() > 0) {
            try {
                logCharset = Charset.forName(charsetName);
            } catch (RuntimeException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    /**
     * Marshall a long into the next 4 bytes in this buffer. Necessary when the
     * long is used to hold an unsigned int.
     */
    public static void writeUnsignedInt(ByteBuffer buf, long value) {
        buf.put((byte) (value >>> 0));
        buf.put((byte) (value >>> 8));
        buf.put((byte) (value >>> 16));
        buf.put((byte) (value >>> 24));
    }

    /**
     * Unmarshall the next four bytes which hold an unsigned int into a long.
     */
    public static long readUnsignedInt(ByteBuffer buf) {
        long ret = (buf.get() & 0xFFL) << 0;
        ret += (buf.get() & 0xFFL) << 8;
        ret += (buf.get() & 0xFFL) << 16;
        ret += (buf.get() & 0xFFL) << 24;
        return ret;
    }

    /*
     * Marshall objects.
     */

    /**
     * Write a short into the log.
     */
    public static void writeShort(ByteBuffer logBuf, short i) {
        byte b = (byte) ((i >> 0) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 8) & 0xff);
        logBuf.put(b);
    }

    /**
     * Read a short from the log.
     */
    public static short readShort(ByteBuffer logBuf) {
        return (short) (((logBuf.get() & 0xFF) << 0) +
                        ((logBuf.get() & 0xFF) << 8));
    }

    /**
     * Write an int into the log.
     */
    public static void writeInt(ByteBuffer logBuf, int i) {
        byte b = (byte) ((i >> 0) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 8) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 16) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 24) & 0xff);
        logBuf.put(b);
    }

    /**
     * Read a int from the log.
     */
    public static int readInt(ByteBuffer logBuf) {
        int ret = (logBuf.get() & 0xFF) << 0;
        ret += (logBuf.get() & 0xFF) << 8;
        ret += (logBuf.get() & 0xFF) << 16;
        ret += (logBuf.get() & 0xFF) << 24;
        return ret;
    }

    /**
     * @return log storage size for an int.
     */
    public static int getIntLogSize() {
        return INT_BYTES;
    }

    /**
     * Write a packed int into the log.
     */
    public static void writePackedInt(ByteBuffer logBuf, int i) {
        int off = logBuf.arrayOffset();
        int newPos =
            PackedInteger.writeInt(logBuf.array(),
                                   logBuf.position() + off, i);
        logBuf.position(newPos - off);
    }

    /**
     * Read a packed int from the log.
     */
    public static int readPackedInt(ByteBuffer logBuf) {
        byte a[] = logBuf.array();
        int oldPos = logBuf.position();
        int off = logBuf.arrayOffset() + oldPos;
        int len = PackedInteger.getReadIntLength(a, off);
        int val = PackedInteger.readInt(a, off);
        logBuf.position(oldPos + len);
        return val;
    }

    /**
     * @return log storage size for a packed int.
     */
    public static int getPackedIntLogSize(int i) {
        return PackedInteger.getWriteIntLength(i);
    }

    /**
     * Write an int into the log in MSB order.  Used for ordered keys.
     */
    public static void writeIntMSB(ByteBuffer logBuf, int i) {
        byte b = (byte) ((i >> 24) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 16) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 8) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 0) & 0xff);
        logBuf.put(b);
    }

    /**
     * Read a int from the log in MSB order.  Used for ordered keys.
     */
    public static int readIntMSB(ByteBuffer logBuf) {
        int ret = (logBuf.get() & 0xFF) << 24;
        ret += (logBuf.get() & 0xFF) << 16;
        ret += (logBuf.get() & 0xFF) << 8;
        ret += (logBuf.get() & 0xFF) << 0;
        return ret;
    }

    /**
     * Write a long into the log.
     */
    public static void writeLong(ByteBuffer logBuf, long l) {
        byte b =(byte) (l >>> 0);
        logBuf.put(b);
        b =(byte) (l >>> 8);
        logBuf.put(b);
        b =(byte) (l >>> 16);
        logBuf.put(b);
        b =(byte) (l >>> 24);
        logBuf.put(b);
        b =(byte) (l >>> 32);
        logBuf.put(b);
        b =(byte) (l >>> 40);
        logBuf.put(b);
        b =(byte) (l >>> 48);
        logBuf.put(b);
        b =(byte) (l >>> 56);
        logBuf.put(b);
    }

    /**
     * Read a long from the log.
     */
    public static long readLong(ByteBuffer logBuf) {
        long ret = (logBuf.get() & 0xFFL) << 0;
        ret += (logBuf.get() & 0xFFL) << 8;
        ret += (logBuf.get() & 0xFFL) << 16;
        ret += (logBuf.get() & 0xFFL) << 24;
        ret += (logBuf.get() & 0xFFL) << 32;
        ret += (logBuf.get() & 0xFFL) << 40;
        ret += (logBuf.get() & 0xFFL) << 48;
        ret += (logBuf.get() & 0xFFL) << 56;
        return ret;
    }

    /**
     * @return log storage size for a long.
     */
    public static int getLongLogSize() {
        return LONG_BYTES;
    }

    /**
     * Write a packed long into the log.
     */
    public static void writePackedLong(ByteBuffer logBuf, long l) {
        int off = logBuf.arrayOffset();
        int newPos =
            PackedInteger.writeLong(logBuf.array(),
                                    logBuf.position() + off, l);
        logBuf.position(newPos - off);
    }

    /**
     * Read a packed long from the log.
     */
    public static long readPackedLong(ByteBuffer logBuf) {
        byte a[] = logBuf.array();
        int oldPos = logBuf.position();
        int off = logBuf.arrayOffset() + oldPos;
        int len = PackedInteger.getReadLongLength(a, off);
        long val = PackedInteger.readLong(a, off);
        logBuf.position(oldPos + len);
        return val;
    }

    /**
     * @return log storage size for a packed long.
     */
    public static int getPackedLongLogSize(long l) {
        return PackedInteger.getWriteLongLength(l);
    }

    /**
     * Write a byte array into the log. The size is stored first as an integer.
     */
    public static void writeByteArray(ByteBuffer logBuf, byte[] b) {

        if (b == null) {
            writePackedInt(logBuf, -1);
            return;
        }

        /* Write the length. */
        writePackedInt(logBuf, b.length);

        /* Add the data itself. */
        logBuf.put(b);                     // data
    }

    /**
     * Write a byte array into the log. The size is stored first as an integer.
     */
    public static void writeByteArray(ByteBuffer logBuf,
                                      byte[] b,
                                      int off,
                                      int len) {

        if (b == null) {
            writePackedInt(logBuf, -1);
            return;
        }

        /* Write the length. */
        writePackedInt(logBuf, len);

        /* Add the data itself. */
        logBuf.put(b, off, len);
    }

    /**
     * Read a byte array from the log. The size is stored first as an integer.
     */
    public static byte[] readByteArray(ByteBuffer logBuf) {
        int size = readPackedInt(logBuf);
        if (DEBUG) {
            System.out.println("pos = " + logBuf.position() +
                               " byteArray is " + size + " on read");
        }

        if (size < 0) {
            return null;
        }

        if (size == 0) {
            return ZERO_LENGTH_BYTE_ARRAY;
        }

        byte[] b = new byte[size];
        logBuf.get(b);               // read it out
        return b;
    }

    /**
     * Read a byte array from the log. The size is stored first as an integer.
     */
    public static void readByteArrayNoLength(ByteBuffer logBuf,
                                             byte[] outBuf,
                                             int offset,
                                             int size) {
        if (size < 0) {
            return;
        }

        assert outBuf.length >= offset + size;
        logBuf.get(outBuf, offset, size);
    }

    /**
     * @return log storage size for a byteArray
     */
    public static int getByteArrayLogSize(byte[] b) {
        return getByteArrayLogSize((b != null) ? b.length : -1);
    }

    /**
     * @return log storage size for a byteArray of a given size. If the array
     * is null, pass -1 for the len param.
     */
    public static int getByteArrayLogSize(int len) {
        if (len == -1) {
            return LogUtils.getPackedIntLogSize(-1);
        } else {
            assert len >= 0;
            return LogUtils.getPackedIntLogSize(len) + len;
        }
    }

    /**
     * Write a byte array into the log. No size is stored.
     */
    public static void writeBytesNoLength(ByteBuffer logBuf, byte[] b) {

        /* Add the data itself. */
        logBuf.put(b);
    }

    /**
     * Read a byte array from the log.  The size is not stored.
     */
    public static byte[] readBytesNoLength(ByteBuffer logBuf, int size) {
        if (DEBUG) {
            System.out.println("pos = " + logBuf.position() +
                               " byteArray is " + size + " on read");
        }

        if (size == 0) {
            return ZERO_LENGTH_BYTE_ARRAY;
        }

        byte[] b = new byte[size];
        logBuf.get(b);               // read it out
        return b;
    }

    /**
     * Write a string into the log. The size is stored first as an integer.
     */
    public static void writeString(ByteBuffer logBuf,
                                   String stringVal) {
        writeByteArray(logBuf, StringUtils.toUTF8(stringVal));
    }

    /**
     * Read a string from the log. The size is stored first as an integer.
     */
    public static String readString(ByteBuffer logBuf,
                                    int entryVersion) {
        final byte[] bytes = readByteArray(logBuf);

        /*
         * Use logCharset only prior to version 9, since in version 9
         * UTF8 is always used.  See logCharset for details.
         */
        if (entryVersion >= 9) {
            return StringUtils.fromUTF8(bytes);
        }
        if (logCharset != null) {
            return new String(bytes, logCharset);
        }
        return new String(bytes);
    }

    /**
     * @return log storage size for a string
     */
    public static int getStringLogSize(String s) {
        return getByteArrayLogSize(StringUtils.toUTF8(s));
    }

    /**
     * Write a timestamp into the log.
     */
    public static void writeTimestamp(ByteBuffer logBuf, Timestamp time) {
        writePackedLong(logBuf, time.getTime());
    }

    /**
     * Read a timestamp from the log.
     */
    public static Timestamp readTimestamp(ByteBuffer logBuf) {
        long millis = readPackedLong(logBuf);
        return new Timestamp(millis);
    }

    /**
     * @return log storage size for a timestamp
     */
    public static int getTimestampLogSize(Timestamp time) {
        return PackedInteger.getWriteLongLength(time.getTime());
    }

    /**
     * Write a boolean into the log.
     */
    public static void writeBoolean(ByteBuffer logBuf, boolean bool) {
        byte val = bool ? (byte) 1 : (byte) 0;
        logBuf.put(val);
    }

    /**
     * Read a boolean from the log.
     */
    public static boolean readBoolean(ByteBuffer logBuf) {
        byte val = logBuf.get();
        return (val == (byte) 1) ? true : false;
    }

    /**
     * @return log storage size for a boolean.
     */
    public static int getBooleanLogSize() {
        return 1;
    }

    /*
     * Dumping support.
     */
    public static boolean dumpBoolean(ByteBuffer itemBuffer,
                                      StringBuilder sb,
                                      String tag) {
        sb.append("<");
        sb.append(tag);
        sb.append(" exists = \"");
        boolean exists = readBoolean(itemBuffer);
        sb.append(exists);
        if (exists) {
            sb.append("\">");
        } else {
            /* Close off the tag, we're done. */
            sb.append("\"/>");
        }
        return exists;
    }

    /**
     * The byte[]'s in Xid's are known to be 255 or less in length.  So instead
     * of using read/writeByteArray(), we can save 6 bytes per record by making
     * the byte[] length be 1 byte instead of 4.
     */
    public static int getXidSize(Xid xid) {
        byte[] gid = xid.getGlobalTransactionId();
        byte[] bqual = xid.getBranchQualifier();
        return
            INT_BYTES +                         // FormatId
            1 +                                 // gxid length byte
            1 +                                 // bqual length byte
            (gid == null ? 0 : gid.length) +    // gid bytes
            (bqual == null ? 0 : bqual.length); // bqual bytes
    }

    /*
     * Xid.gid[] and bqual[] can't be longer than 64 bytes so we can get away
     * with writing the length in one byte, rather than 4.
     */
    public static void writeXid(ByteBuffer logBuf, Xid xid) {
        byte[] gid = xid.getGlobalTransactionId();
        byte[] bqual = xid.getBranchQualifier();

        writeInt(logBuf, xid.getFormatId());

        if (gid == null) {
            logBuf.put((byte) -1);
        } else {
            logBuf.put((byte) (gid.length));
            logBuf.put(gid);
        }

        if (bqual == null) {
            logBuf.put((byte) -1);
        } else {
            logBuf.put((byte) (bqual.length));
            logBuf.put(bqual);
        }
    }

    /*
     * Xid.gid[] and bqual[] can't be longer than 64 bytes so we can get away
     * with writing the length in one byte, rather than 4.
     */
    public static Xid readXid(ByteBuffer logBuf) {
        int formatId = readInt(logBuf);

        int gidLen = logBuf.get();
        byte[] gid = null;
        if (gidLen >= 0) {
            gid = new byte[gidLen];
            logBuf.get(gid);
        }

        int bqualLen = logBuf.get();
        byte[] bqual = null;
        if (bqualLen >= 0) {
            bqual = new byte[bqualLen];
            logBuf.get(bqual);
        }

        return new XidImpl(formatId, gid, bqual);
    }

    public static class XidImpl implements Xid {
        private int formatId;
        private byte[] gid;
        private byte[] bqual;

        /* public for unit tests. */
        public XidImpl(int formatId, byte[] gid, byte[] bqual) {
            this.formatId = formatId;
            this.gid = gid;
            this.bqual = bqual;
        }

        public int getFormatId() {
            return formatId;
        }

        public byte[] getGlobalTransactionId() {
            return gid;
        }

        public byte[] getBranchQualifier() {
            return bqual;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof XidImpl)) {
                return false;
            }

            XidImpl xid = (XidImpl) o;
            if (xid.getFormatId() != formatId) {
                return false;
            }
            if (compareByteArrays(xid.getGlobalTransactionId(), gid) &&
                compareByteArrays(xid.getBranchQualifier(), bqual)) {
                return true;
            }

            return false;
        }

        @Override
        public int hashCode() {
            int code = formatId;
            if (gid != null) {
                for (int i = 0; i < gid.length; i++) {
                    code += gid[i];
                }
            }
            if (bqual != null) {
                for (int i = 0; i < bqual.length; i++) {
                    code += bqual[i];
                }
            }
            return code;
        }

        private boolean compareByteArrays(byte[] b1, byte[] b2) {
            if (b1 == null ||
                b2 == null) {
                return b1 == b2;
            }

            if (b1.length != b2.length) {
                return false;
            }

            for (int i = 0; i < b1.length; i++) {
                if (b1[i] != b2[i]) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("<Xid formatId=\"").append(formatId);
            sb.append("\" gTxnId=\"");
            if (gid == null) {
                sb.append("null");
            } else {
                sb.append(new String(gid));
            }
            sb.append("\" bqual=\"");
            if (bqual == null) {
                sb.append("null");
            } else {
                sb.append(new String(bqual));
            }
            sb.append("\"/>");
            return sb.toString();
        }
    }
}
