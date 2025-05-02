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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.UserDataControl;

import com.sleepycat.util.PackedInteger;

/**
 * Holds a Value for a request, optimized to avoid array allocations/copies.
 * Value is serialized on the client from a Value object and deserialized on
 * the service into a byte array.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RequestValue implements FastExternalizable {

    /* The value field is non-null on the client and null on the service. */
    private final Value value;
    /* The bytes field is non-null on the service and null on the client. */
    private final byte[] bytes;

    /**
     * Used by the client.
     */
    RequestValue(Value value) {
        this.value = checkNull("value", value);
        this.bytes = null;
    }

    /**
     * To create a server format instance.
     */
    private RequestValue(byte[] bytes) {
        this.value = null;
        this.bytes = checkNull("bytes", bytes);
    }

    /**
     * Deserialize into byte array.
     * Used by the service when deserializing a request.
     */
    public RequestValue(DataInput in, short serialVersion)
        throws IOException {

        value = null;
        bytes = Value.readFastExternal(in, serialVersion);
    }

    /**
     * FastExternalizable writer.
     * Used by both the client and the server when serializing a request.
     * Format:
     * <ol>
     * <li> <i>[Choice]</i> ({@link Value}) <i>value</i> // on client
     * <li> <i>[Choice]</i> On server:
     *    <ol type="a">
     *    <li> ({@link Value#writeFastExternal(DataOutput, short, byte[])
     *         Value.writeFastExternal}) {@link #getBytes bytes}
     *    </ol>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        if (value != null) {
            value.writeFastExternal(out, serialVersion);
        } else {
            Value.writeFastExternal(out, serialVersion, bytes);
        }
    }

    /**
     * Used by the service.
     */
    byte[] getBytes() {
        if (bytes == null) {
            /* Only occurs in tests when RMI is bypassed. */
            return value.toByteArray();
        }
        return bytes;
    }

    int getRegionId() {
        if (value == null) {
            return PackedInteger.readInt(bytes, 1);
        }
        return value.getRegionId();
    }

    Format getValueFormat() {
        if (value == null) {
            return Format.fromFirstByte(bytes[0]);
        }
        return value.getFormat();
    }

    @Override
    public String toString() {
        return UserDataControl.displayValue(value, bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RequestValue)) {
            return false;
        }
        final RequestValue other = (RequestValue) obj;
        return Objects.equals(value, other.value) &&
            Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, bytes);
    }

    @Override
    public RequestValue deserializedForm(short serialVersion) {
        return (value != null) ?
            new RequestValue(value.toByteArray()) :
            new RequestValue(Value.fromByteArray(bytes));
    }
}
