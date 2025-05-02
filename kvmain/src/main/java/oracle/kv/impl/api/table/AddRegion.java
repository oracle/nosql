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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Add (create) region. Used for both remote regions (add) or the local
 * region (set or reset).
 */
class AddRegion extends TableChange {
    private static final long serialVersionUID = 1L;

    private final Region region;

    AddRegion(Region region, int seqNum) {
        super(seqNum);
        this.region = checkNull("region", region);
    }

    AddRegion(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        region = new Region(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        region.writeFastExternal(out, serialVersion);
    }

    @Override
    TableImpl apply(TableMetadata md) {
        md.addRegion(region);
        return null;
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.ADD_REGION;
    }

    @Override
    public String toString() {
        return "AddRegion[" + region + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof AddRegion)) {
            return false;
        }
        final AddRegion other = (AddRegion) obj;
        return region.equals(other.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            region);
    }
}
