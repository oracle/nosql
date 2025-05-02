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
import static oracle.kv.impl.util.SerialVersion.BULK_PUT_RESOLVE;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeArrayLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import oracle.kv.impl.api.bulk.BulkPut.KVPair;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.SerialVersion;

/**
 * A put-batch operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class PutBatch extends MultiKeyOperation {

    private final List<KVPair> kvPairs;
    private final long[] tableIds;
    private final boolean overwrite;
    private final boolean usePutResolve;
    /*
     * local region is used for row values that don't have a region, implying
     * they are "local" writes
     */
    private final int localRegionId;

    public PutBatch(List<KVPair> le, long[] tableIds,
                    boolean overwrite, boolean usePutResolve,
                    int localRegionId) {
        super(OpCode.PUT_BATCH, null, null, null);
        checkNull("le", le);
        for (final KVPair element : le) {
            checkNull("le element", element);
        }
        this.kvPairs = le;
        this.tableIds = tableIds;
        this.usePutResolve = usePutResolve;
        this.localRegionId = localRegionId;
        if (usePutResolve) {
            /* putresolve implies overwrite */
            overwrite = true;
        }
        this.overwrite = overwrite;
    }

    PutBatch(DataInput in, short serialVersion) throws IOException {

        super(OpCode.PUT_BATCH, in, serialVersion);
        final int kvPairCount = readNonNullSequenceLength(in);
        kvPairs = new ArrayList<>(kvPairCount);

        for (int i = 0; i < kvPairCount; i++) {
            kvPairs.add(new KVPair(in, serialVersion));
        }

        final int tableIdCount = readSequenceLength(in);
        if (tableIdCount == -1) {
            tableIds = null;
        } else {
            tableIds = new long[tableIdCount];
            for (int i = 0; i < tableIdCount; i++) {
                tableIds[i] = in.readLong();
            }
        }

        overwrite = in.readBoolean();
        if (serialVersion >= BULK_PUT_RESOLVE) {
            usePutResolve = in.readBoolean();
            localRegionId = in.readInt();
        } else {
            usePutResolve = false;
            localRegionId = Region.NULL_REGION_ID;
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link MultiKeyOperation} {@code super}
     * <li> ({@link SerializationUtil#writeNonNullCollection non-null
     *      collection}) {@link #getKvPairs kvPairs}
     * <li> ({@link SerializationUtil#writeArrayLength sequence length})
     *      <i>number of table IDs</i>
     * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}{@code []})
     *      <i>table IDs</i>
     * <li> ({@code boolean}) {@link #getUsePutResolve usePutResolve} // since
     *      version {@link SerialVersion#BULK_PUT_RESOLVE}
     * <li> ({@code int}) {@link #getLocalRegionId localRegionId} // since
     *      version {@link SerialVersion#BULK_PUT_RESOLVE}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeNonNullCollection(out, serialVersion, kvPairs);
        writeArrayLength(out, tableIds);

        if (tableIds != null) {
            for (final long tableId : tableIds) {
                out.writeLong(tableId);
            }
        }

        out.writeBoolean(overwrite);
        if (serialVersion >= BULK_PUT_RESOLVE) {
            out.writeBoolean(usePutResolve);
            out.writeInt(localRegionId);
        } else if (usePutResolve || localRegionId != 0) {
            throw new IllegalStateException(
                "usePutResolve is not supported in serial version: " +
                serialVersion);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof PutBatch)) {
            return false;
        }
        final PutBatch other = (PutBatch) obj;
        return kvPairs.equals(other.kvPairs) &&
            Arrays.equals(tableIds, other.tableIds) &&
            (overwrite == other.overwrite) &&
            (usePutResolve == other.usePutResolve) &&
            (localRegionId == other.localRegionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), kvPairs, tableIds,
                            overwrite, usePutResolve, localRegionId);
    }

    List<KVPair> getKvPairs() {
        return kvPairs;
    }

    @Override
    public long[] getTableIds() {
        return tableIds;
    }

    @Override
    public boolean performsWrite() {
        return true;
    }

    boolean getOverwrite() {
        return overwrite;
    }

    boolean getUsePutResolve() {
        return usePutResolve;
    }

    int getLocalRegionId() {
        return localRegionId;
    }

    /**
     * An external multi-region request comes from the cloud service or
     * other entity that works only with region ids and not region names.
     * Such a request has set the region id in the request
     */
    boolean isExternalMultiRegion() {
        return Region.isMultiRegionId(localRegionId);
    }
}
