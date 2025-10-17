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
import static oracle.kv.impl.util.SerialVersion.CREATION_TIME_VER;
import static oracle.kv.impl.util.SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER;
import static oracle.kv.impl.util.SerializationUtil.toDeserializedForm;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Holds key and value as byte arrays to avoid conversion to Key and Value
 * objects on the service side.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ResultKeyValueVersion implements FastExternalizable {

    private final byte[] keyBytes;
    private final ResultValue resultValue;
    private final Version version;
    private final long expirationTime;
    private final long modificationTime;
    private final long creationTime;
    private final boolean isTombstone;

    public ResultKeyValueVersion(byte[] keyBytes,
                                 byte[] valueBytes,
                                 Version version,
                                 long expirationTime,
                                 long creationTime,
                                 long modificationTime,
                                 boolean isTombstone) {
        checkNull("keyBytes", keyBytes);
        checkNull("version", version);
        this.keyBytes = keyBytes;
        this.resultValue = new ResultValue(valueBytes);
        this.version = version;
        this.expirationTime = expirationTime;
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
        this.isTombstone = isTombstone;
    }

    /** Constructor to implement deserializedForm */
    ResultKeyValueVersion(ResultKeyValueVersion other, short serialVersion) {
        keyBytes = other.keyBytes;
        resultValue = toDeserializedForm(other.resultValue, serialVersion);
        version = other.version;
        expirationTime = other.expirationTime;
        creationTime = other.creationTime;
        modificationTime = other.modificationTime;
        isTombstone = other.isTombstone;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    public ResultKeyValueVersion(DataInput in, short serialVersion)
        throws IOException {

        keyBytes = readNonNullByteArray(in);
        resultValue = new ResultValue(in, serialVersion);
        version = Version.createVersion(in, serialVersion);
        expirationTime = Result.readTimestamp(in, serialVersion);
        modificationTime = Result.readTimestamp(in, serialVersion);
        if (serialVersion >= TABLE_ITERATOR_TOMBSTONES_VER) {
            isTombstone = in.readBoolean();
        } else {
            isTombstone = false;
        }
        if (serialVersion >= CREATION_TIME_VER) {
            creationTime = in.readLong();
        } else {
            creationTime = 0;
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getKeyBytes keyBytes}
     * <li> ({@link ResultValue}) <i>resultValue</i>
     * <li> ({@link Version}) {@link #getVersion version}
     * <li> ({@link DataOutput#writeBoolean boolean}) <i>expirationTime
     *      non-zero</i>
     * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
     *      #getExpirationTime expirationTime} // if non-zero
     * <li> <i>[Optional]</i> ({@link DataOutput#writeBoolean boolean})
     *      <i>modificationTime non-zero</i>
     * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
     *      #getModificationTime modificationTime}
     * <li> <i>[Optional]</i> ({@link DataOutput#writeBoolean boolean}) {@link
     *      #getIsTombstone is a tombstone}
     *      // for {@code serialVersion}
     *      {@link SerialVersion#TABLE_ITERATOR_TOMBSTONES_VER} or greater
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullByteArray(out, keyBytes);
        resultValue.writeFastExternal(out, serialVersion);
        version.writeFastExternal(out, serialVersion);
        Result.writeTimestamp(out, expirationTime, serialVersion);
        Result.writeTimestamp(out, modificationTime, serialVersion);
        if (serialVersion >= TABLE_ITERATOR_TOMBSTONES_VER) {
            out.writeBoolean(isTombstone);
        } else if (isTombstone) {
            throw new IllegalStateException("Result is a tombstone while its " +
                                            "serial version=" + serialVersion +
                                            " is less than the minimum " +
                                            "required version=" +
                                            TABLE_ITERATOR_TOMBSTONES_VER);
        }
        if (serialVersion >= CREATION_TIME_VER) {
            out.writeLong(creationTime);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ResultKeyValueVersion)) {
            return false;
        }
        final ResultKeyValueVersion other = (ResultKeyValueVersion) obj;
        return Arrays.equals(keyBytes, other.keyBytes) &&
               resultValue.equals(other.resultValue) &&
               version.equals(other.version) &&
               (expirationTime == other.expirationTime) &&
               (creationTime == other.creationTime) &&
               (modificationTime == other.modificationTime) &&
               (isTombstone == other.isTombstone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyBytes, resultValue, version, expirationTime,
            creationTime, modificationTime, isTombstone);
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public Value getValue() {
        return resultValue.getValue();
    }

    public byte[] getValueBytes() {
        return resultValue.getBytes();
    }

    public Version getVersion() {
        return version;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public boolean getIsTombstone() {
        return isTombstone;
    }

    @Override
    public ResultKeyValueVersion deserializedForm(short serialVersion) {
        return new ResultKeyValueVersion(this, serialVersion);
    }
}
