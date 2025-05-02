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

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;
import static oracle.kv.impl.util.SerialVersion.JSON_COLLECTION_VERSION;
import static oracle.kv.impl.util.SerialVersion.SCHEMALESS_TABLE_VERSION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;
import oracle.kv.table.FieldDef;
import oracle.kv.table.TimeToLive;

/**
 * A TableChange to create/add a new table
 */
class AddTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String parentName;
    private final String namespace;
    private final List<String> primaryKey;
    private final List<Integer> primaryKeySizes;
    private final List<String> shardKey;
    private final FieldMap fields;
    private final TimeToLive ttl;
    private final boolean r2compat;
    private final int schemaId;
    private final String description;
    private final ResourceOwner owner;
    private final boolean sysTable;
    private final TableLimits limits;
    private final IdentityColumnInfo identityColumnInfo;
    private final Set<Integer> regionIds;
    private final boolean schemaless;
    private final Map<String, FieldDef.Type> jsonCollectionMRCounters;

    AddTable(TableImpl table, int seqNum) {
        super(seqNum);
        name = table.getName();
        namespace = table.getInternalNamespace();
        final TableImpl parent = (TableImpl) table.getParent();
        parentName = (parent == null) ? null : parent.getFullName();
        primaryKey = table.getPrimaryKey();
        primaryKeySizes = table.getPrimaryKeySizes();
        shardKey = table.getShardKey();
        fields = table.getFieldMap();
        ttl = table.getDefaultTTL();
        r2compat = table.isR2compatible();
        schemaId = table.getSchemaId();
        description = table.getDescription();
        owner = table.getOwner();
        sysTable = table.isSystemTable();
        limits = (parent == null) ? table.getTableLimits() : null;
        identityColumnInfo = table.getIdentityColumnInfo();
        regionIds = table.isChild() ? null : table.getRemoteRegions();
        schemaless = table.isJsonCollection();
        jsonCollectionMRCounters = table.getJsonCollectionMRCounters();
    }

    AddTable(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        name = readString(in, serialVersion);
        parentName = readString(in, serialVersion);
        namespace = readString(in, serialVersion);
        primaryKey = readCollection(in, serialVersion, ArrayList::new,
                                    SerializationUtil::readString);
        primaryKeySizes = readCollection(in, serialVersion,
                                         ArrayList::new,
                                         (inp, sv) -> inp.readInt());
        shardKey = readCollection(in, serialVersion, ArrayList::new,
                                  SerializationUtil::readString);
        fields = new FieldMap(in, serialVersion);
        ttl = readFastExternalOrNull(in, serialVersion,
                                     TimeToLive::readFastExternal);
        r2compat = in.readBoolean();
        schemaId = in.readInt();
        description = readString(in, serialVersion);
        owner = readFastExternalOrNull(in, serialVersion, ResourceOwner::new);
        sysTable = in.readBoolean();
        limits = readFastExternalOrNull(in, serialVersion, TableLimits::new);
        identityColumnInfo = readFastExternalOrNull(in, serialVersion,
                                                    IdentityColumnInfo::new);
        regionIds = readCollection(in, serialVersion, HashSet::new,
                                   (inp, sv) -> readPackedInt(inp));
        if (serialVersion >= SCHEMALESS_TABLE_VERSION) {
            schemaless = in.readBoolean();
        } else {
            schemaless = false;
        }
        if (serialVersion >= JSON_COLLECTION_VERSION && schemaless) {
            jsonCollectionMRCounters =
                TableImpl.readMRCounters(in, serialVersion);
        } else {
            jsonCollectionMRCounters = null;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        writeString(out, serialVersion, parentName);
        writeString(out, serialVersion, namespace);
        writeCollection(out, serialVersion, primaryKey,
                        WriteFastExternal::writeString);
        writeCollection(out, serialVersion, primaryKeySizes,
                        (i, outp, sv) -> outp.writeInt(i));
        writeCollection(out, serialVersion, shardKey,
                        WriteFastExternal::writeString);
        fields.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, ttl);
        out.writeBoolean(r2compat);
        out.writeInt(schemaId);
        writeString(out, serialVersion, description);
        writeFastExternalOrNull(out, serialVersion, owner);
        out.writeBoolean(sysTable);
        writeFastExternalOrNull(out, serialVersion, limits);
        writeFastExternalOrNull(out, serialVersion, identityColumnInfo);
        writeCollection(out, serialVersion, regionIds,
                        (i, outp, sv) -> writePackedInt(outp, i));
        if (serialVersion >= SCHEMALESS_TABLE_VERSION) {
            out.writeBoolean(schemaless);
        } else if (schemaless) {
            throw new IllegalStateException(
                "JsonCollection tables not supported in serial version: " +
                serialVersion);
        }
        /* only write MR Counters if this is a JSON collection */
        if (serialVersion >= JSON_COLLECTION_VERSION && schemaless) {
            TableImpl.writeMRCounters(jsonCollectionMRCounters,
                                      out, serialVersion);
        } else if (jsonCollectionMRCounters != null) {
            throw new IllegalStateException(
                "MR Counters in JsonCollection tables not supported in serial" +
                " version: " +  serialVersion);
        }
    }

    @Override
    TableImpl apply(TableMetadata md) {
        TableImpl ret =
            md.insertTable(namespace, name, parentName,
                           primaryKey, primaryKeySizes, shardKey, fields,
                           ttl, limits, r2compat, schemaId, description,
                           owner, sysTable, identityColumnInfo, regionIds,
                           schemaless, jsonCollectionMRCounters);
        return ret;
    }

    @Override
    ChangeType getChangeType() {
        return StandardChangeType.ADD_TABLE;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof AddTable)) {
            return false;
        }
        final AddTable other = (AddTable) obj;
        return Objects.equals(name, other.name) &&
            Objects.equals(parentName, other.parentName) &&
            Objects.equals(namespace, other.namespace) &&
            Objects.equals(primaryKey, other.primaryKey) &&
            Objects.equals(primaryKeySizes, other.primaryKeySizes) &&
            Objects.equals(shardKey, other.shardKey) &&
            Objects.equals(fields, other.fields) &&
            Objects.equals(ttl, other.ttl) &&
            (r2compat == other.r2compat) &&
            (schemaId == other.schemaId) &&
            Objects.equals(description, other.description) &&
            Objects.equals(owner, other.owner) &&
            (sysTable == other.sysTable) &&
            Objects.equals(limits, other.limits) &&
            Objects.equals(identityColumnInfo, other.identityColumnInfo) &&
            Objects.equals(regionIds, other.regionIds) &&
            schemaless == other.schemaless &&
            Objects.equals(jsonCollectionMRCounters,
                           other.jsonCollectionMRCounters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            name,
                            parentName,
                            namespace,
                            primaryKey,
                            primaryKeySizes,
                            shardKey,
                            fields,
                            ttl,
                            r2compat,
                            schemaId,
                            description,
                            owner,
                            sysTable,
                            limits,
                            identityColumnInfo,
                            regionIds,
                            schemaless,
                            jsonCollectionMRCounters);
    }
}
