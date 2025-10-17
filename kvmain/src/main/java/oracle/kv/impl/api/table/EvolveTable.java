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

import static oracle.kv.impl.util.SerialVersion.BEFORE_IMAGE_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.table.TimeToLive;

/**
 * A TableChange that evolves an existing table.
 */
class EvolveTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String namespace;
    private final FieldMap fields;
    private final TimeToLive ttl;
    private final TimeToLive beforeImgTTL;
    private final String description;
    private final IdentityColumnInfo identityColumnInfo;
    private final Set<Integer> regions;

    EvolveTable(TableImpl table, int seqNum) {
        super(seqNum);
        tableName = table.getFullName();
        namespace = table.getInternalNamespace();
        fields = table.getFieldMap();
        ttl = table.getDefaultTTL();
        beforeImgTTL = table.getBeforeImageTTL();
        description = table.getDescription();
        identityColumnInfo = table.getIdentityColumnInfo();
        regions = table.isChild() ? null : table.getRemoteRegions();
    }

    EvolveTable(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        tableName = readString(in, serialVersion);
        namespace = readString(in, serialVersion);
        fields = new FieldMap(in, serialVersion);
        ttl = readFastExternalOrNull(in, serialVersion,
                                     TimeToLive::readFastExternal);
        if (serialVersion >= BEFORE_IMAGE_VERSION) {
            beforeImgTTL = readFastExternalOrNull(
                in, serialVersion, TimeToLive::readFastExternal);
        } else {
            beforeImgTTL = null;
        }
        description = readString(in, serialVersion);
        identityColumnInfo = readFastExternalOrNull(in, serialVersion,
                                                    IdentityColumnInfo::new);
        regions = readCollection(in, serialVersion, HashSet::new,
                                 (inp, sv) -> readPackedInt(inp));
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, tableName);
        writeString(out, serialVersion, namespace);
        fields.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, ttl);
        if (serialVersion >= BEFORE_IMAGE_VERSION) {
            writeFastExternalOrNull(out, serialVersion, beforeImgTTL);
        }
        writeString(out, serialVersion, description);
        writeFastExternalOrNull(out, serialVersion, identityColumnInfo);
        writeCollection(out, serialVersion, regions,
                        (i, outp, sv) -> writePackedInt(outp, i));
    }

    @Override
    TableImpl apply(TableMetadata md) {
        return md.evolveTable(namespace, tableName, fields, ttl,
                              beforeImgTTL, description,
                              identityColumnInfo, regions);
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.EVOLVE_TABLE;
    }
}
