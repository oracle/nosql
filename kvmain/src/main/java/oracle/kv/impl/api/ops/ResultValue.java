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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.Value;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.UserDataControl;

/**
 * Holds a Value for a result, optimized to avoid array allocations/copies.
 * Value is serialized on the service from a byte array and deserialized on
 * the client into a Value object.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ResultValue implements FastExternalizable {

    /* The value field is non-null on the client and null on the service. */
    private final Value value;
    /* The bytes field is non-null on the service and null on the client. */
    private final byte[] bytes;
    /* A byte array to represent an empty record on the wire to the client */
    private static final byte[] EMPTY_BYTES = new byte[] {0};

    /**
     * Used by the service.
     */
    ResultValue(byte[] bytes) {
        this.value = null;
        if (bytes == null || bytes.length == 0) {
            this.bytes = EMPTY_BYTES;
        } else {
            this.bytes = bytes;
        }
    }

    /**
     * Used to convert to client format.
     */
    private ResultValue(Value value) {
        this.value = value;
        this.bytes = null;
    }

    /**
     * Deserialize into byte array.
     * Used by the client when deserializing a response.
     */
    public ResultValue(DataInput in, short serialVersion)
        throws IOException {

        bytes = null;
        value = new Value(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Used by both the client and the server when
     * serializing a response.  Format:
     * <ol>
     * <li> <i>[Choice]</i> ({@link Value}) <i>value</i> // on client
     * <li> <i>[Choice]</i> On server:
     *    <ol type="a">
     *    <li> {@link Value#writeFastExternal(DataOutput, short, byte[])
     *         Value.writeFastExternal(} {@link #getBytes bytes)}
     *    </ol>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out,
                                  short serialVersion)
        throws IOException {

        if (value != null) {
            value.writeFastExternal(out, serialVersion);
        } else {
            Value.writeFastExternal(out, serialVersion, bytes);
        }
    }

    /**
     * Used by the client.
     */
    Value getValue() {
        if (value == null) {
            /* Only occurs in tests when RMI is bypassed. */
            return Value.fromByteArray(bytes);
        }
        return value;
    }

    byte[] getBytes() {
        return bytes;
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
        if (!(obj instanceof ResultValue)) {
            return false;
        }
        final ResultValue other = (ResultValue) obj;
        return Objects.equals(value, other.value) &&
            Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, bytes);
    }

    @Override
    public ResultValue deserializedForm(short serialVersion) {
        return (bytes != null) ?
            new ResultValue(Value.fromByteArray(bytes)) :
            new ResultValue(value.toByteArray());
    }
}
