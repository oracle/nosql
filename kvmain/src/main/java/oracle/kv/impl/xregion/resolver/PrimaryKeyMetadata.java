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

package oracle.kv.impl.xregion.resolver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.FastExternalizable;

/**
 * Object represent the primary key metadata
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class PrimaryKeyMetadata implements KeyMetadata, FastExternalizable {

    private final long timestamp;
    private final int regionId;

    public PrimaryKeyMetadata(long timestamp,
                              int regionId) {
        this.timestamp = timestamp;
        this.regionId = regionId;
    }

    public PrimaryKeyMetadata(DataInput in,
                              @SuppressWarnings("unused") short serialVersion)
        throws IOException {
        timestamp = in.readLong();
        regionId = in.readInt();
    }

    /**
     * Returns the timestamp when the key is inserted, updated or deleted.
     *
     * @return the timestamp when the key is inserted, updated or deleted.
     */
    @Override
    public long getTimestamp() {
         return timestamp;
    }

    /**
     * Returns the id of region where the key inserted, updated or deleted.
     *
     * @return id of region where the key inserted, updated or deleted.
     */
    @Override
    public int getRegionId() {
        return regionId;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeLong long}) {@link #getTimestamp()}
     * <li> ({@link DataOutput#writeInt int}) {@link #getRegionId()}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        out.writeLong(timestamp);
        out.writeInt(regionId);
    }
}
