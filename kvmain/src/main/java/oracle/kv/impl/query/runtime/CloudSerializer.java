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

package oracle.kv.impl.query.runtime;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.sleepycat.util.PackedInteger;

import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.table.FieldValue;

/**
 * A Class containing util methods for serializing the part of the query plan
 * that must be sent to the cloud driver to be executed there.
 */
public class CloudSerializer {

    /**
     * An interface for serializing a kvstore FieldValue to the binary format
     * expected by the cloud driver. The interface is implemented by a proxy
     * class. The proxy creates and passes an instance of that class to the 
     * PreparedStatementImpl.serializeForCloudDriver() method. 
     */
    public static interface FieldValueWriter {
        void writeFieldValue(DataOutput out, FieldValue value) throws IOException;
    }

    /* TODO: Use StandardCharsets version in Java 8 */
    private static final Charset utf8 = Charset.forName("UTF-8");


    public static void writeSortedPackedInt(DataOutput out, int value)
            throws IOException {

        final byte[] buf = new byte[PackedInteger.MAX_LENGTH];
        final int offset = PackedInteger.writeSortedInt(buf, 0, value);
        out.write(buf, 0, offset);
    }

    public static void writeArrayLength(DataOutput out, Object array)
        throws IOException {

        int len = (array != null ? Array.getLength(array) : -1);

        if (len < -1) {
            throw new IllegalArgumentException(
                "Invalid sequence length: " + len);
        }

        writeSortedPackedInt(out, len);
    }

    public static void writeString(
        String str,
        DataOutput out) throws IOException {

        if (str == null) {
            writeSortedPackedInt(out, -1);
            return;
        }

        final ByteBuffer buffer = utf8.encode(str);
        final int length = buffer.limit();
        writeSortedPackedInt(out, length);
        if (length > 0) {
            out.write(buffer.array(), 0, length);
        }
    }

    public static void writeByteArray(DataOutput out, byte[] array)
        throws IOException {

        final int length = (array == null ? -1 : Array.getLength(array));
        writeArrayLength(out, array);
        if (length > 0) {
            out.write(array);
        }
    }

    public static void writeIntArray(
        int[] array,
        boolean packed,
        DataOutput out) throws IOException {

        writeArrayLength(out, array);

        if (array != null) {
            for (int val : array) {
                if (packed) {
                    writeSortedPackedInt(out, val);
                } else {
                    out.writeInt(val);
                }
            }
        }
    }

    static void writeStringArray(
        String[] array,
        DataOutput out) throws IOException {

        writeArrayLength(out, array);

        if (array != null) {
            for (final String s : array) {
                writeString(s, out);
            }
        }
        return;
    }

    public static void writeIter(
        PlanIter iter,
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        if (iter == null) {
            out.writeByte(-1);
            return;
        }

        iter.writeForCloud(out, driverVersion, valWriter);
    }

    static void writeIters(
        PlanIter[] iters,
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        writeArrayLength(out, iters);

        for (final PlanIter iter : iters) {
            iter.writeForCloud(out, driverVersion, valWriter);
        }
    }

    static void writeSortSpecs(
        SortSpec[] array,
        DataOutput out) throws IOException {

        writeArrayLength(out, array);

        if (array != null) {
            for (final SortSpec s : array) {
                out.writeBoolean(s.theIsDesc);
                out.writeBoolean(s.theNullsFirst);
            }
        }
        return;
    }
}
