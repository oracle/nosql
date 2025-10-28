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

import static oracle.kv.impl.util.SerialVersion.CLOUD_MR_TABLE;
import static oracle.kv.impl.util.SerialVersion.CREATION_TIME_VER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.impl.api.table.Region;
import oracle.kv.table.TimeToLive;


public class PutResolve extends Put {
    private final boolean isTombstone;
    private final long creationTime;
    private final long lastModificationTime;

    /* expiration time in system time */
    private final long expirationTimeMs;
    private final int localRegionId;

    public volatile static boolean MRTABLE_CRDT_TEST = false;

    /**
     * Constructs a put-resolve operation with a table id.
     */
    public PutResolve(byte[] keyBytes,
                      Value value,
                      long tableId,
                      Choice prevValChoice,
                      long expirationTimeMs,
                      boolean updateTTL,
                      boolean isTombstone,
                      long creationTime,
                      long lastModificationTime,
                      int localRegionId) {
        super(OpCode.PUT_RESOLVE, keyBytes, value, prevValChoice, tableId,
              TimeToLive.DO_NOT_EXPIRE, updateTTL,
              false, /* isSQLUpdate */
              null,  /*allIndexes*/
              null,  /*allIndexIds*/
              null   /*indexesToUpdate*/);
        this.isTombstone = isTombstone;
        this.creationTime = creationTime;
        this.lastModificationTime = lastModificationTime;
        this.expirationTimeMs = expirationTimeMs;
        if (localRegionId != Region.NULL_REGION_ID) {
            Region.checkId(localRegionId, true /* isExternalRegion */);
        }
        this.localRegionId = localRegionId;
    }

    /** Constructor to implement deserializedForm */
    private PutResolve(PutResolve other, short serialVersion) {
        super(other, serialVersion);
        isTombstone = other.isTombstone;
        creationTime = other.creationTime;
        lastModificationTime = other.lastModificationTime;
        expirationTimeMs = other.expirationTimeMs;
        if (includeCloudMRTable(serialVersion)) {
            localRegionId = other.localRegionId;
        } else {
            if (Region.isMultiRegionId(other.localRegionId)) {
                throw new IllegalStateException("Serial version " +
                    serialVersion + " does not support providing external " +
                    "region Id , must be " + CLOUD_MR_TABLE + " or greater");
            }
            localRegionId = Region.NULL_REGION_ID;
        }
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    PutResolve(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.PUT_RESOLVE, in, serialVersion);
        isTombstone = in.readBoolean();
        lastModificationTime = in.readLong();
        expirationTimeMs = in.readLong();
        if (includeCloudMRTable(serialVersion)) {
            localRegionId = in.readInt();
        } else {
            localRegionId = Region.NULL_REGION_ID;
        }
        if (serialVersion >= CREATION_TIME_VER) {
            creationTime = in.readLong();
        } else {
            creationTime = 0;
        }
    }

    @Override
    public boolean performsRead() {
        /* Override the conditional return in Put */
        return true;
    }

    @Override
    public byte[] getValueBytes() {
        /*
         * Remote ID will be checked when executing the operation.
         */
        return requestValue.getBytes();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getTimestamp() {
        return lastModificationTime;
    }

    /**
     * @return the remote region id which is from record.
     */
    public int getRemoteRegionId() {
        return requestValue.getRegionId();
    }

    public long getExpirationTimeMs() {
        return expirationTimeMs;
    }

    /**
     * Returns the table version from the value byte[] for a non-key-only put,
     * or 0 for a key-only put, a delete, or a tombstone.
     *
     * @return table version for put or 0 for delete
     */
    int getTableVer(int offset) {
        if (isDelete() || isTombstone() || keyOnlyPut(offset)) {
            return 0;
        }
        /* for a non-key-only table put, next byte is table version */
        return getValueBytes()[offset];
    }

    public boolean isTombstone() {
        return isTombstone;
    }

    /**
     * Returns true if the op is a PUT operation from a key-only table, false
     * otherwise
     */
    boolean keyOnlyPut(int offset) {
        if (isDelete() || isTombstone()) {
            return false;
        }
        /* if false, value bytes have more data than format and region id */
        return getValueBytes().length == offset;
    }

    /**
     * Computes offset of value byte[] after format and region id. Note that
     * the value is empty for a key-only entry. Otherwise, the first byte after
     * the offset represents the table version.
     */
    int computeOffset() {
        final byte[] valueBytes = getValueBytes();
        return Value.getValueOffset(valueBytes);
    }

    /**
     * Writes this object to the output stream. Format:
     * <ol>
     * <li> ({@link Put}) {@code super}
     * <li> ({@code boolean}) {@code isTombstone}
     * <li> ({@code long}) {@code timestamp}
     * <li> ({@code long}) {@code expirationTimeMs} for {@code serialVersion}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        out.writeBoolean(isTombstone);
        out.writeLong(lastModificationTime);
        out.writeLong(expirationTimeMs);
        if (includeCloudMRTable(serialVersion)) {
            out.writeInt(localRegionId);
        } else {
            if (Region.isMultiRegionId(localRegionId)) {
                throw new IllegalStateException("Serial version " +
                    serialVersion + " does not support providing external " +
                    "region Id , must be " + CLOUD_MR_TABLE + " or greater");
            }
        }
        if (serialVersion >= CREATION_TIME_VER) {
            out.writeLong(creationTime);
        }
    }

    /**
     * @return the local region id which is from WriteOptions. Only valid when
     * table is an external Multi Region table.
     */
    public int getLocalRegionId() {
        return localRegionId;
    }

    public boolean isExternalMultiRegion() {
        return Region.isMultiRegionId(localRegionId);
    }

    private static boolean includeCloudMRTable(short serialVersion) {
        return serialVersion >= CLOUD_MR_TABLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof PutResolve)) {
            return false;
        }
        final PutResolve other = (PutResolve) obj;
        return (isTombstone == other.isTombstone) &&
            (creationTime == other.creationTime) &&
            (lastModificationTime == other.lastModificationTime) &&
            (expirationTimeMs == other.expirationTimeMs) &&
            (localRegionId == other.localRegionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isTombstone, creationTime,
            lastModificationTime, expirationTimeMs, localRegionId);
    }

    @Override
    public PutResolve deserializedForm(short serialVersion) {
        return new PutResolve(this, serialVersion);
    }
}
