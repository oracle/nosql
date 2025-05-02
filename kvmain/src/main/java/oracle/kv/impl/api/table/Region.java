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

package oracle.kv.impl.api.table;

import static oracle.kv.impl.util.SerialVersion.CLOUD_MR_TABLE;
import static oracle.kv.impl.util.SerialVersion.TABLE_MD_IN_STORE_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */

/**
 * A Region value is used to identify each kvstore instance that participates
 * in a multi-region table.
 *
 * In multi-region replication, rows that are written at one source region are
 * replicated to other target regions. The source region is the region which
 * initially received the application's write operation. Source region ids are
 * used to handle conflict resolution:
 *
 * case 1) If a target region receives multiple rows with the same key,
 *         the source region is a factor in chosing which write to apply
 *
 * case 2) The source region id is used as part of the processing of CRDT
 *           fields like MR_Counters
 *
 * Row values that are of type oracle.kv.Value.Format.MULTI_REGION_TABLE have
 * the source region id while rows of type Format.TABLE do not.
 *
 * The use of MULTI_REGION_TABLE vs TABLE rows, and the way the source region
 * is specified differs in on-premise kvstore vs NoSQL Cloud service kvstore.
 *
 * On-premise
 * ----------
 * Multi-region tables are declared to be so at creation time, and every
 * row persisted for that table is of type MULTI_REGION_TABLE.
 *
 * Because there is no externally defined set of unique region ids, each region
 * participating an MR table thinks of its own region id as LOCAL_REGION_ID,
 * and independently maintains a mapping of other regions to ids. When storing
 * a value generated locally, the region ID is always LOCAL_REGION_ID. That is,
 * while all the replicated copies of that row have the same logical source
 * region id, the actual region id value in each copy depends on its local
 * mapping.
 *
 * When receiving a row from a remote region, the kvstore uses its
 * RegionMapper to convert that to the appropriate value to store locally. It
 * will be a value that is always greater than LOCAL_REGION_ID and less than or
 * equal to REGION_ID_MAX.
 *
 * For example, for table foo, replicated to region A and region B
 *
 * At regionA, suppose RegionMapper defines region B as 63, then
 *
 *  - for rows that originate at regionA, the regionID used is LOCAL_REGION_ID
 *  - for rows received from region B, the regionId used is 63
 *
 * At regionB, suppose RegionMapper defines region A as 43, then:
 *
 *  - for rows that originate at regionB, the regionID used is LOCAL_REGION_ID
 *  - for rows received from region A, the regionId used is 43
 *
 * NoSQL Cloud Service
 * -------------------
 * Region ids are externally defined and guaranteed to be unique across
 * regions; there is no local mapping as there is for on-premise.  This
 * approach of managing the region ids outside of kvstore is called the
 * "externalRegion" approach.
 *
 * Region ids are encoded according to the NOSQL Cloud Service service
 * directory, and will be valid integer values. The source region is passed in
 * as an argument to the kvstore request only in cases where it is needed for
 * conflict resolution:
 *  - queries: the source region is always passed in via ExecuteOptions, to be
 *     available for CRDT value updates
 *
 *  - putResolve and deleteResolve operations: These methods are only used when
 *     kvstore is receiving a replicated row. In this case, conflict resolution
 *     must be done by the ConflictResolver, and the row's source region is
 *     passed in via WriteOptions.
 *
 * Note that the source region id is never passed in for "local" write
 * operations such as put or delete, regardless of whether the table is a
 * singleton or MR table, because there is no need for conflict resolution.
 *
 * For example, for table foo, replicated to region A and region B, suppose
 * A's region id == 33 and B's region Id = 77
 *
 * At regionA
 *  - for rows received from region B, the regionId passed in is 77
 *  - for CRDT value, it updates values for regionId = 33
 *  - for local put and delete operations, no region id is passed in
 *
 * At regionB
 *  - for rows received from region A, the regionId used is 33
 *  - for CRDT value, it updates values for regionId = 77
 *  - for local put and delete operations, no region id is passed in
 *
 * Additional notes on cloud MR tables
 *
 * - Note that cloud MR tables persist rows in both MR and TABLE format.
 *   Rows that arrived locally with be in TABLE format, and rows that were
 *   received from remote sources are in MULTREGION_TABLE format.
 *
 * - When transmitting rows to target regions, the cloud MR system will fill
 *   in the region id for TABLE rows, so that the receiving region will
 *   have that source region id available.
 *
 * - Operations on mrtables may need to do additional work, such as
 *   saving tombstones. This is triggered explicitly by the ExecuteOperations/
 *   WriteOperations.doTombstone() method, and is not related to type of
 *   row used.
 */
public class Region implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1L;

    /* Local region ID cannot change */
    public static final int LOCAL_REGION_ID = 1;
    public static final int REGION_ID_START = 2;
    static final int REGION_ID_MAX = 1000000;
    public static final int UNKNOWN_REGION_ID = -1;
    /** Reserved region ID for non-multi-region entries */
    public static final int NULL_REGION_ID = 0;

    /**
     * Region name.
     */
    private final String name;

    /**
     * Region ID. Active regions have ID &gt;= 0. Negative
     * IDs indicate an inactive region. Negative IDs are
     * never exposed outside of the object.
     */
    private final int id;

    Region(String name, int id) {
        this(name, id, true);
    }

    Region(String name, int id, boolean active) {
        checkId(id, false);
        if (name == null) {
            throw new IllegalArgumentException("Region name cannot be null");
        }
        if (!active && (id == LOCAL_REGION_ID)) {
            throw new IllegalStateException("Cannot make local region inactive");
        }
        this.name = name;
        this.id = active ? id : -id;
    }

    /* Constructor for FastExternalizable */
    public Region(DataInput in, short serialVersion) throws IOException {
        name = readNonNullString(in, serialVersion);
        id = readPackedInt(in);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString
     *      non-null String}) {@code name}
     * <li> ({@link SerializationUtil#writePackedInt int}) <i>region ID</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        if (!isActive() && (serialVersion < TABLE_MD_IN_STORE_VERSION)) {
            throw new IllegalStateException("Serial version " +
                                            serialVersion +
                                            " does not support inactive regions");
        }
        if (serialVersion < CLOUD_MR_TABLE) {
            if (id > REGION_ID_MAX) {
                throw new IllegalStateException("Serial version " +
                    serialVersion + " does not support region ID > " +
                    REGION_ID_MAX);
            }
        }
        writeNonNullString(out, serialVersion, name);
        writePackedInt(out, id);
    }

    /**
     * Gets the region name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the region ID.
     */
    public int getId() {
        return isActive() ? id : -id;
    }

    /**
     * Returns true if this region object represents the local region.
     */
    public boolean isLocal() {
        return id == LOCAL_REGION_ID;
    }

    /**
     * Throws IllegalArgumentException if the specified ID is not a valid
     * region ID.
     */
    public static void checkId(int id, boolean isExternalRegionId) {
        if (isExternalRegionId) {
            if (id < LOCAL_REGION_ID) {
                throw new IllegalArgumentException("Invalid region ID: " + id);
            }
        } else {
             if ((id < LOCAL_REGION_ID) || (id > REGION_ID_MAX)) {
                 throw new IllegalArgumentException("Invalid region ID: " + id);
             }
        }
    }

    /**
     * Returns true if it is multi-region id.
     */
    public static boolean isMultiRegionId(int id) {
        return id > NULL_REGION_ID;
    }

    boolean isActive() {
        return id >= 0;
    }

    String toJsonString() {
        return "{\"name\":\"" + name +
                "\",\"id\":" + getId() +
                ",\"active\":" + (isActive() ? "true" :
                                               "false") + "}";
    }

    @Override
    public String toString() {
        return "Region[" + name + " " + getId() +
                (isActive() ? " active" : " inactive") + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Region)) {
            return false;
        }
        final Region other = (Region) obj;
        return name.equals(other.name) && (id == other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }
}
