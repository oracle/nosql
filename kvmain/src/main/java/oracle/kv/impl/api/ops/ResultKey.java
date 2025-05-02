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
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * This class holds results of a key iteration over a set of keys.
 * For each key it has 2-3 pieces of information:
 * 1. the key (a byte[])
 * 2. a boolean indicator of whether the key has a non-zero expiration time
 * 3. if an expiration time is present, the actual time (a long)
 *
 * Prior to the implementation of the TTL feature only the key bytes were
 * needed by key scan results. The new state was introduced in release 4.0.
 *
 * @since 4.0
 * @see #writeFastExternal FastExternalizable format
 */
public class ResultKey implements FastExternalizable {

    private final byte[] keyBytes;
    private final long expirationTime;

    public ResultKey(byte[] keyBytes,
                     long expirationTime) {
        checkNull("keyBytes", keyBytes);
        this.keyBytes = keyBytes;
        this.expirationTime = expirationTime;
    }

    public ResultKey(byte[] keyBytes) {
        this(keyBytes, 0);
    }

    /**
     * FastExternalizable constructor. This must be compatible with the
     * pre-TTL constructor for KeysIterateResult which had only the key bytes.
     */
    public ResultKey(DataInput in, short serialVersion)
        throws IOException {

        keyBytes = readNonNullByteArray(in);
        expirationTime = Result.readTimestamp(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getKeyBytes keyBytes}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@linkplain
     *      #hasExpirationTime <i>non-zero expiration time</i>}
     * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
     *      #getExpirationTime expirationTime} // if value is non-zero
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullByteArray(out, keyBytes);
        Result.writeTimestamp(out, expirationTime, serialVersion);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ResultKey)) {
            return false;
        }
        final ResultKey other = (ResultKey) obj;
        return Arrays.equals(keyBytes, other.keyBytes) &&
            (expirationTime == other.expirationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyBytes, expirationTime);
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public boolean hasExpirationTime() {
        return (expirationTime != 0);
    }

    public long getExpirationTime() {
        return expirationTime;
    }
}
