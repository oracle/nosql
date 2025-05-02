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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;

import static oracle.kv.impl.util.SerialVersion.MRT_INFO_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1;
import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readMap;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeMap;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;
import static oracle.kv.table.TableAPI.SYSDEFAULT_NAMESPACE_NAME;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.IndexImpl.IndexStatus;
import oracle.kv.impl.api.table.TableImpl.TableStatus;
import oracle.kv.impl.metadata.InfoList;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.security.Ownable;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.systables.TableMetadataDesc;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

/**
 * This is internal implementation that wraps Table and Index metadata
 * operations such as table/index creation, etc.
 *
 * TableMetadata stores tables in a tree.  The top level is a map from
 * name (String) to Table and contains top-level tables only.  Each top-level
 * table may or may not contain child tables.  When this class is serialized
 * the entire tree of Table objects, along with their contained Index objects,
 * is serialized.
 *
 * When a table lookup is performed it must be done top-down.  First the lookup
 * walks to the "root" of the metadata structure, which is the map contained in
 * this instance.  For top-level tables the lookup is a simple get.  For child
 * tables the code unwinds down the stack of parents to get the child.
 *
 * When a table is first inserted into TableMetadata it is assigned a
 * numeric id. Ids are allocated from the keyId member.
 *
 * Note that this implementation is not synchronized. If multiple threads
 * access a table metadata instance concurrently, and at least one of the
 * threads modifies the table metadata structurally, it must be synchronized
 * externally.
 */
public class TableMetadata implements TableMetadataHelper,
                                      Metadata<TableChangeList>,
                                      Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * The change brought back some dead code as part of this change.
     * We brought back read and write side of FastExternalizable old
     * format because of an upgrade issue [KVSTORE-2588]. As part of the
     * revert patch, we kept the read and write both side of the code to
     * keep the change cleaner. This change should be removed when deprecate
     * 25.1 release of kvstore. We can revert this changeset when the
     * prerequisite version is updated to >=25.1.
     */
    private static final short INFO_LIST_VERSION = SerialVersion.V23;

    /*
     * Cluster during upgrade or new cluster will have namespaces field null.
     * This means semantically that there is only one namespace:
     * the sysdefault ns:
     * {@link oracle.kv.table.TableAPI#SYSDEFAULT_NAMESPACE_NAME}
     */
    private Map<String, NamespaceImpl> namespaces;

    /*
     * Active regions. Map of region name to Region instance. The field will be
     * null if there are no active regions. Region names are case-insensitive.
     */
    private Map<String, Region> activeRegions;

    /*
     * The names of all known regions, past and present. index = regionId - 1
     * This field is null if there are no known regions.
     *
     * The entry at index 0 (regionId=1) is the local region name. The local
     * region name may change. The entries at index > 0 are remote regions
     * and do not change.
     *
     * Currently region names are never removed, so the code assume this
     * array does not contain nulls. If region names are ever GCed, null
     * entries would need to be handled (reused).
     */
    private ArrayList<String> knownRegions;

    /* Index into the knowRegions array for the local region name */
    private static int LOCAL_REGION_INDEX = Region.LOCAL_REGION_ID - 1;

    /*
     * Map of table names to top-level table instances. Table names are
     * case-insensitive. The name used as the key is the string returned by
     * Table.getFullNamespaceName(). This field may be null indicating
     * that this is simply the header. A null value is not backward compatible
     * and will cause an old node to fail if it was encountered. However this
     * is preferable to the node continuing with incorrect metadata.
     */
    private Map<String, Table> tables;

    private int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    private long keyId = INITIAL_KEY_ID;

    public static final int INITIAL_KEY_ID = 1;

    /*
     * Record of changes to the metadata. If null no changes will be kept.
     */
    private final List<TableChange> changeHistory;

    /*
     * Serial version of the shallow copy, only valid when isShallow()
     * returns true
     */
    private final short serialVersion;

    /* The max id assigned to an index */
    private long maxIndexId;

    public static class NamespaceImpl
            implements FastExternalizable, Ownable, Serializable {

        private static final long serialVersionUID = 1L;

        private final String namespace;
        private final ResourceOwner owner;

        /* owner is null for the "sysdefault" namespace */
        NamespaceImpl(String namespace, ResourceOwner owner) {
            this.namespace = namespace;
            this.owner = owner == null ? null : new ResourceOwner(owner);
        }

        NamespaceImpl(DataInput in, short serialVersion) throws IOException {
            namespace = readString(in, serialVersion);
            owner = readFastExternalOrNull(in, serialVersion,
                                           ResourceOwner::new);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion, namespace);
            writeFastExternalOrNull(out, serialVersion, owner);
        }

        public String getNamespace() {
            return namespace;
        }

        @Override
        public ResourceOwner getOwner() {
            return owner;
        }

        String toJsonString() {
            return owner == null ?
                    "{\"name\":\"" + namespace + "\"}" :
                    "{\"name\":\"" + namespace +
                    "\",\"owner\":" +
                    owner.toJsonString() + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof NamespaceImpl)) {
                return false;
            }
            final NamespaceImpl other = (NamespaceImpl) obj;
            return Objects.equals(namespace, other.namespace) &&
                Objects.equals(owner, other.owner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, owner);
        }
    }

    /**
     * Construct a table metadata object. If keepChanges is true any changes
     * made to are recorded and can be accessed through the getMetadataInfo()
     * interface.
     *
     * @param keepChanges true if keep the changes
     */
    public TableMetadata(boolean keepChanges) {
        changeHistory = keepChanges ? new LinkedList<>() : null;
        serialVersion = 0;
        initializeTables(null);
    }

    /**
     * Constructs a shallow metadata object. The instance will not contain
     * table instances.
     */
    private TableMetadata(TableMetadata md, short serialVersion) {
        if (serialVersion <= 0) {
            throw new IllegalArgumentException("serialVersion must be > 0");
        }
        this.serialVersion = serialVersion;
        namespaces = md.namespaces;
        activeRegions = md.activeRegions;
        knownRegions = md.knownRegions;
        changeHistory = md.changeHistory;
        seqNum = md.seqNum;
        keyId = md.keyId;
        maxIndexId = md.maxIndexId;
        tables = null;
    }

    public TableMetadata(DataInput in, short serialVersion)
        throws IOException
    {
        namespaces = readMap(in, serialVersion,
                             TableMetadata::createCompareMap,
                             SerializationUtil::readString,
                             NamespaceImpl::new);
        activeRegions = readMap(in, serialVersion,
                                TableMetadata::createCompareMap,
                                SerializationUtil::readString,
                                Region::new);
        knownRegions = readCollection(in, serialVersion, ArrayList::new,
                                      SerializationUtil::readString);

        tables = readMap(in, serialVersion,
                         TableMetadata::createCompareMap,
                         SerializationUtil::readString,
                         /* Always top level tables */
                         (inp, sv) -> new TableImpl(inp, sv, null));

        seqNum = readPackedInt(in);
        keyId = in.readLong();
        if (serialVersion >= QUERY_VERSION_16) {
            maxIndexId = in.readLong();
        } else {
            maxIndexId = 0;
        }

        changeHistory = readCollection(in, serialVersion, LinkedList::new,
                                       TableChange::readTableChange);
        this.serialVersion = in.readShort();
    }

    public static <V> Map<String, V> createCompareMap() {
        return new TreeMap<String, V>(FieldComparator.instance);
    }

    @Override
    public void writeFastExternal(DataOutput out, short sv)
        throws IOException
    {
        writeMap(out, sv, namespaces, WriteFastExternal::writeString);
        writeMap(out, sv, activeRegions, WriteFastExternal::writeString);
        writeCollection(out, sv, knownRegions, WriteFastExternal::writeString);
        writeMap(out, sv, tables, WriteFastExternal::writeString,
                 (val, outp, sver) ->
                 ((TableImpl) val).writeFastExternal(outp, sver));
        writePackedInt(out, seqNum);
        out.writeLong(keyId);
        if (sv >= QUERY_VERSION_16) {
            out.writeLong(maxIndexId);
        }
        writeCollection(out, sv, changeHistory, TableChange::writeTableChange);
        out.writeShort(serialVersion);
    }

    /**
     * Initializes the table map. If md is not null the map is initialized from
     * the table map in the specified metadata. The (possibly empty) initialized
     * table map will subsequently be updated, either via triggers representing
     * updates to the table schema, or by reading individual component
     * information from the metadataDatabase.
     */
    public final void initializeTables(TableMetadata md) {
        assert tables == null;
        tables = createCompareMap();
        if (md != null) {
            tables.putAll(md.tables);
        }
    }

    /**
     * Creates a shallow copy of this TableMetadata object. The returned
     * metadata does not contain the table and index instances.
     *
     * @return the new TableMetadata instance
     */
    public TableMetadata getShallowCopy(short serialVer) {
        return new TableMetadata(this, serialVer);
    }

    /**
     * Returns true if the metadata instance is a shallow copy.
     */
    public boolean isShallow() {
        return tables == null;
    }

    /**
     * Returns the serial version of this instance.
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Adds the specified table hierarchy to the metadata. If the table already
     * exists it is replaced.
     */
    public void addTableHierarchy(TableImpl table) {
        if (!table.isTop()) {
            throw new IllegalStateException("Adding table hierarchy with a" +
                                            " non-top level table: " +
                                            table.getFullNamespaceName());
        }
        tables.put(table.getFullNamespaceName(), table);
    }

    /**
     * Removes the specified table hierarchy.
     */
    public void removeTableHierarchy(TableImpl table) {
        if (!table.isTop()) {
            throw new IllegalStateException("Removing table hierarchy with a" +
                                            " non-top level table: " +
                                            table.getFullNamespaceName());
        }
        tables.remove(table.getFullNamespaceName());
    }

    public TableImpl addTable(String namespace,
                              String name,
                              String parentName,
                              List<String> primaryKey,
                              List<Integer> primaryKeySizes,
                              List<String> shardKey,
                              FieldMap fieldMap,
                              TimeToLive ttl,
                              TableLimits limits,
                              boolean r2compat,
                              int schemaId,
                              String description,
                              ResourceOwner owner) {

        return addTable(namespace, name, parentName,
                        primaryKey, primaryKeySizes, shardKey, fieldMap, ttl,
                        limits, r2compat, schemaId, description, owner, false,
                        null, null, false, null);
    }

    public TableImpl addTable(String namespace,
                              String name,
                              String parentName,
                              List<String> primaryKey,
                              List<Integer> primaryKeySizes,
                              List<String> shardKey,
                              FieldMap fieldMap,
                              TimeToLive ttl,
                              TableLimits limits,
                              boolean r2compat,
                              int schemaId,
                              String description,
                              ResourceOwner owner,
                              boolean sysTable,
                              IdentityColumnInfo identityColumnInfo,
                              Set<Integer> regionIds,
                              boolean jsonCollection,
                              Map<String, FieldDef.Type> mrCounters) {
        final TableImpl table = insertTable(namespace, name, parentName,
                                            primaryKey, primaryKeySizes,
                                            shardKey,
                                            fieldMap,
                                            ttl, limits,
                                            r2compat, schemaId,
                                            description,
                                            owner, sysTable,
                                            identityColumnInfo,
                                            regionIds,
                                            jsonCollection, mrCounters);
        addTableChange(table);
        return table;
    }

    private void addTableChange(TableImpl table) {
        bumpSeqNum();
        table.setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new AddTable(table, seqNum));
        }
    }

    /**
     * Drops a table. If the table has indexes or child tables an
     * IllegalArgumentException is thrown. If markForDelete is true the table's
     * status is set to DELETING and is not removed.
     *
     * @param tableName the table name
     * @param markForDelete if true mark the table as DELETING
     *
     * @return the table that was dropped
     */
    public TableImpl dropTable(String namespace,
                               String tableName,
                               boolean markForDelete) {
        final TableImpl table = removeTable(namespace, tableName, markForDelete);

        bumpSeqNum();
        /*
         * If marked for delete the table will remain in the md, so still
         * need to set its seq num.
         */
        table.setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new DropTable(namespace, tableName,
                                            markForDelete, seqNum));
        }
        return table;
    }

    /**
     * Evolves a table using new fields but only if it's not already been done
     * and if the supplied version indicates that the evolution started with
     * the latest table version.
     *
     * If this operation was retried the evolution may have already been
     * applied.  Check field equality and if equal, consider the evolution
     * done.
     *
     * @return true if the evolution happens, false otherwise
     *
     * @throws IllegalCommandException if an attempt is made to evolve a version
     * other than the latest table version
     */
    public boolean evolveTable(TableImpl table, int tableVersion,
                               FieldMap fieldMap, TimeToLive ttl,
                               String description,
                               boolean systemTable,
                               IdentityColumnInfo newIdentityColumnInfo,
                               Set<Integer> regions) {

        if (table.isSystemTable() != systemTable) {
            if (systemTable) {
                throw new IllegalCommandException
                    ("Table " +
                     NameUtils.makeQualifiedName(null, null, table.getName()) +
                     " is not system table");
            }
            throw new IllegalCommandException
                ("Cannot evolve table " +
                 NameUtils.makeQualifiedName(null, null, table.getName()));
        }

        if ((regions != null) && !table.isMultiRegion()) {
            throw new IllegalCommandException
                    ("Cannot evolve table " +
                     NameUtils.makeQualifiedName(null, null, table.getName()) +
                     " is not a multi-region table");
        }

        /* Exit if nothing has changed */ // TODO - what about description?
        if (fieldMap.equals(table.getFieldMap()) &&
            compareTTL(ttl, table.getDefaultTTL()) &&
            compareIdentityColumn(table.getIdentityColumnInfo(),
                                  newIdentityColumnInfo) &&
            compareRegions(table.isChild() ? null : table.getRemoteRegions(),
                           regions)) {
            return false;
        }

        if (tableVersion != table.numTableVersions()) {
            throw new IllegalCommandException
                ("Table evolution must be performed on the latest version, " +
                 "version supplied is " + tableVersion + ", latest is " +
                 table.numTableVersions());
        }

        table.evolve(fieldMap, ttl, description, newIdentityColumnInfo, null,
                     regions);
        bumpSeqNum();
        table.setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new EvolveTable(table, seqNum));
        }
        return true;
    }

    private boolean compareTTL(TimeToLive ttl1, TimeToLive ttl2) {
        return (ttl1 == null && ttl2 == null) ||
               (ttl2 != null && ttl2.equals(ttl1));
    }

    private boolean compareIdentityColumn(IdentityColumnInfo oldIdInfo,
                                          IdentityColumnInfo newIdInfo) {
       return (oldIdInfo == null && newIdInfo == null) ||
              (newIdInfo != null && newIdInfo.equals(oldIdInfo));
    }

    private boolean compareRegions(Set<Integer> oldRegions,
                                   Set<Integer> newRegions) {
        return (oldRegions == null && newRegions == null) ||
               (oldRegions != null && oldRegions.equals(newRegions));
    }

    /**
     * Sets the table limits.
     */
    public void setLimits(TableImpl table, TableLimits newLimits) {
        table.setTableLimits(newLimits);
        bumpSeqNum();
        table.setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new TableLimit(table, seqNum));
        }
    }

    public void addIndex(String namespace,
                         String indexName,
                         String tableName,
                         List<String> fields,
                         List<FieldDef.Type> types,
                         boolean indexNulls,
                         boolean isUnique,
                         String description) {

        final IndexImpl index = insertIndex(namespace, indexName, tableName,
                                            fields, types, indexNulls,
                                            isUnique, description);
        bumpSeqNum();
        index.getTable().setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new AddIndex(namespace, indexName, tableName,
                                           fields, types, indexNulls, isUnique,
                                           description, seqNum));
        }
    }

    public void addTextIndex(String namespace,
                             String indexName,
                             String tableName,
                             List<AnnotatedField> fields,
                             Map<String, String> properties,
                             String description) {
        final List<String> fieldNames = new ArrayList<>(fields.size());
        final Map<String, String> annotations = new HashMap<>(fields.size());
        IndexImpl.populateMapFromAnnotatedFields(fields,
                                                 fieldNames,
                                                 annotations);

        final IndexImpl index = insertTextIndex(namespace, indexName, tableName,
                                                fieldNames, annotations,
                                                properties, description);

        bumpSeqNum();
        index.getTable().setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new AddIndex(namespace, indexName, tableName,
                                           fieldNames, annotations, properties,
                                           description, seqNum));
        }
    }

    public void dropIndex(String namespace,
                          String indexName,
                          String tableName) {
        final TableImpl table = removeIndex(namespace, indexName, tableName);
        bumpSeqNum();
        table.setSequenceNumber(seqNum);
        if (changeHistory != null) {
            changeHistory.add(new DropIndex(namespace,
                                            indexName,
                                            tableName,
                                            seqNum));
        }
    }

    public boolean updateIndexStatus(String namespace,
                                     String indexName,
                                     String tableName,
                                     IndexStatus status) {
        final IndexImpl index = changeIndexStatus(namespace, indexName,
                                                  tableName, status);
        if (index != null) {
            bumpSeqNum();
            index.getTable().setSequenceNumber(seqNum);
            if (changeHistory != null) {
                changeHistory.add(new UpdateIndexStatus(index, seqNum));
            }
            return true;
        }
        return false;
    }

    /*
     * Add the table described.  It must not exist or an exception is thrown.
     * If it has a parent the parent must exist.
     */
    TableImpl insertTable(String namespace,
                          String name,
                          String parentName,
                          List<String> primaryKey,
                          List<Integer> primaryKeySizes,
                          List<String> shardKey,
                          FieldMap fields,
                          TimeToLive ttl,
                          TableLimits limits,
                          boolean r2compat,
                          int schemaId,
                          String description,
                          ResourceOwner owner,
                          boolean sysTable,
                          IdentityColumnInfo identityColumnInfo,
                          Set<Integer> regionIds,
                          boolean jsonCollection,
                          Map<String, FieldDef.Type> mrCounters) {
        if (r2compat) {
            verifyIdNotUsed(name);
        }

        if (regionIds != null) {
            if (getLocalRegionName() == null) {
                throw new IllegalCommandException("Cannot create table," +
                                                  " the local region name" +
                                                  " has not been set");
            }

            /*
             * Since the region IDs were probably generated from the current
             * mapper, it is unlikely that they are not valid. However, they
             * could have changed since that time.
             */
            for (int regionId : regionIds) {
                /* Will throw IAE if ID is bad */
                final int index = RegionMapperImpl.toIndex(regionId);
                final String regionName = knownRegions.get(index);
                if (regionName == null) {
                    throw new IllegalCommandException("Cannot create table," +
                                                      " unknown region ID: " +
                                                      regionId);
                }
                if (!isActiveRegion(regionName)) {
                    throw new IllegalCommandException("Cannot create table," +
                                                      " unknown region: " +
                                                      regionName);
                }
            }
        }

        final TableImpl table;
        if (parentName != null) {
            final TableImpl parent = getTable(namespace,
                                              parentName,
                                              true);
            if (parent.childTableExists(name)) {
                throw new IllegalArgumentException
                    ("Cannot create table.  Table exists: " +
                     NameUtils.makeQualifiedName(namespace, name, parentName));
            }

            if (parent.isSystemTable() != sysTable) {
                throw new IllegalArgumentException
                    ("Cannot create table " + name + ". It must" +
                     ((sysTable) ? " not" : "") + " be a system table, " +
                     "because its parent is " +
                     ((sysTable) ? "" : " not") + " a system table");
            }
            parent.checkChildLimit(name);
            table = new TableImpl(namespace, name, parent,
                                  primaryKey, primaryKeySizes, shardKey,
                                  fields, ttl, limits, r2compat, schemaId,
                                  description, true, owner,
                                  sysTable, identityColumnInfo, regionIds,
                                  jsonCollection, mrCounters);
            table.setId(allocateId(table));
            parent.addChild(table);
        } else {
            final String namespaceName = NameUtils
                .makeQualifiedName(namespace, name);
            if (tables.containsKey(namespaceName)) {
                throw new IllegalArgumentException
                    ("Cannot create table.  Table exists: " + namespaceName);
            }
            table = new TableImpl(namespace, name, null,
                                  primaryKey, primaryKeySizes, shardKey,
                                  fields, ttl, limits, r2compat, schemaId,
                                  description, true, owner,
                                  sysTable, identityColumnInfo, regionIds,
                                  jsonCollection, mrCounters);
            table.setId(allocateId(table));
            tables.put(namespaceName, table);
        }
        return table;

    }

    /**
     * Evolves the table described.  It must not exist or an exception is thrown.
     * The metadata sequence number is not changed.
     */
    TableImpl evolveTable(String namespace,
                          String tableName,
                          FieldMap fields,
                          TimeToLive ttl,
                          String description,
                          IdentityColumnInfo identityColumnInfo,
                          Set<Integer> regions) {
        final TableImpl table = getTable(namespace, tableName, true);
        table.evolve(fields, ttl, description, identityColumnInfo, null, regions);
        return table;
    }

    /**
     * Removes a table. If the table has indexes or child tables an
     * IllegalArgumentException is thrown. If markForDelete is true the table's
     * status is set to DELETING and is not removed. If markForDelete is false
     * the table status mut be DELETING, otherwise an IllegalStateException is
     * thrown.
     *
     * @param tableName the table name
     * @param markForDelete if true mark the table as DELETING
     *
     * @return the removed table
     */
    TableImpl removeTable(String namespace,
                          String tableName,
                          boolean markForDelete) {
        final TableImpl table = checkForRemove(namespace, tableName,
                                               false /* indexes allowed */);
        if (markForDelete) {
            table.setStatus(TableStatus.DELETING);
            return table;
        }
        if (!table.getStatus().isDeleting()) {
            throw new IllegalStateException("Attempt to remove table but" +
                                            " status is not DELETING " + table);
        }
        final Table parent = table.getParent();

        if (parent != null) {
            ((TableImpl)parent).removeChild(table.getName());
        } else {
            /* a top-level table */
            tables.remove(
                NameUtils.makeQualifiedName(namespace, table.getName()));
        }
        return table;
    }

    /**
     * Checks to see if it is ok to remove this table. Returns the table
     * instance if the table can be removed. Throws IllegalCommandException
     * if the table does not exist or if it is a system table.  If
     * indexesAllowed is false an IllegalCommandException is thrown if
     * the table contains indexes.  If childTablesAllowed is false an
     * IllegalCommandException is thrown if the table is referenced by
     * child tables.
     *
     * @param namespace table namespace
     * @param tableName the table name
     * @param indexesAllowed whether to succeed if the table has indexes
     * @param childTablesAllowed whether to succeed if the table has child
     * tables
     *
     * @return table instance
     */
    public TableImpl checkForRemove(String namespace,
                                    String tableName,
                                    boolean indexesAllowed,
                                    boolean childTablesAllowed) {
        final TableImpl table = getTable(namespace, tableName, true);
        final String qname = NameUtils
            .makeQualifiedName(namespace, null, tableName);
        /* getTable(..., true) can return null?? */
        if (table == null) {
            throw new IllegalCommandException
                ("Table " + qname + " does not exist");
        }

        if (table.isSystemTable()) {
            throw new IllegalCommandException
                ("Cannot remove system table: " + qname);
        }

        if (!childTablesAllowed && !table.getChildTables().isEmpty()) {
            throw new IllegalCommandException
                ("Cannot remove table " + qname +
                 ", it is still referenced by " +
                 "child tables");
        }

        if (!indexesAllowed && !table.getIndexes().isEmpty()) {
            throw new IllegalCommandException
                    ("Cannot remove table " + qname +
                     ", it still contains indexes");
        }

        return table;
    }

    /**
     * Same as {@link #checkForRemove(String,String,boolean,boolean)} with
     * childTablesAllowed being false.
     *
     * @param namespace table namespace
     * @param tableName the table name
     * @param indexesAllowed whether to succeed if the table has indexes
     *
     * @return table instance
     */
    public TableImpl checkForRemove(String namespace,
                                    String tableName,
                                    boolean indexesAllowed) {
        return checkForRemove(namespace, tableName, indexesAllowed, false);
    }

    IndexImpl insertIndex(String namespace,
                          String indexName,
                          String tableName,
                          List<String> fields,
                          List<FieldDef.Type> types,
                          boolean indexNulls,
                          boolean isUnique,
                          String description) {
        final TableImpl table = getTable(namespace, tableName, true);
        if (table.isDeleting()) {
            throw new IllegalCommandException
                ("Cannot add index " + indexName + " on table: " +
                 NameUtils.makeQualifiedName(namespace, null, tableName) +
                 ", it is being removed");
        }
        if (table.getIndex(indexName) != null) {
            throw new IllegalArgumentException
                ("Index exists: " + indexName + " on table: " +
                 NameUtils.makeQualifiedName(namespace, null, tableName));
        }
        final IndexImpl index = new IndexImpl(indexName, table, fields,
                                              types, indexNulls, isUnique,
                                              description);
        index.setStatus(IndexStatus.POPULATING);
        index.setId(allocateIndexId());
        table.addIndex(index);
        return index;
    }

    TableImpl removeIndex(String namespace,
                          String indexName,
                          String tableName) {
        final TableImpl table = getTable(namespace, tableName, true);
        if (table.isSystemTable()) {
            throw new IllegalCommandException
                ("Cannot remove index " + indexName + " on system table: " +
                 NameUtils.makeQualifiedName(namespace, null, tableName));
        }

        final Index index = table.getIndex(indexName);
        if (index == null) {
            throw new IllegalArgumentException
                ("Index does not exist: " + indexName + " on table: " +
                 NameUtils.makeQualifiedName(namespace, null, tableName));
        }
        table.removeIndex(indexName);

        return table;
    }

    /*
     * Update the index status to the desired status.  If a change was made
     * return the Index, if the status is unchanged return null, allowing
     * this operation to be an idempotent no-op.
     */
    IndexImpl changeIndexStatus(String namespace,
                                String indexName,
                                String tableName,
                                IndexStatus status) {
        final TableImpl table = getTable(namespace, tableName, true);

        final IndexImpl index = (IndexImpl) table.getIndex(indexName);
        if (index == null) {
            throw new IllegalArgumentException
                ("Index does not exist: " + indexName + " on table: " +
                 NameUtils.makeQualifiedName(namespace, null, tableName));
        }
        if (index.getStatus() == status) {
            return null;
        }
        index.setStatus(status);
        return index;
    }

    IndexImpl insertTextIndex(String namespace,
                              String indexName,
                              String tableName,
                              List<String> fields,
                              Map<String, String> annotations,
                              Map<String, String> properties,
                              String description) {
        final TableImpl table = getTable(namespace, tableName, true);
        if (table.getTextIndex(indexName) != null) {
            throw new IllegalArgumentException
                ("Text Index exists: " + indexName + " on table: " +
                 NameUtils.makeQualifiedName(namespace, null, tableName));
        }
        final IndexImpl index = new IndexImpl(indexName, table, fields, null,
                                              true, false,
                                              annotations, properties,
                                              description);
        index.setStatus(IndexStatus.POPULATING);
        table.addIndex(index);
        return index;
    }

    /**
     * Return the named table.
     *
     * @param tableName is a "." separated path to the table name, e.g.
     * parent.child.target.  For top-level tables it is a single
     * component
     */
    public TableImpl getTable(String namespace,
                              String tableName,
                              boolean mustExist) {

        final String[] path = TableImpl.parseFullName(tableName);
        return getTable(namespace, path, mustExist);
    }

    /**
     * For compatibility only (used in KVProxy). Do not use internally.
     */
    public TableImpl getTable(String tableName) {
        String namespace =
            NameUtils.getNamespaceFromQualifiedName(tableName);
        String fullTableName =
            NameUtils.getFullNameFromQualifiedName(tableName);
        return getTable(namespace, fullTableName);
    }

    public TableImpl getTable(String namespace,
                              String tableName,
                              String parentName) {
        final StringBuilder sb = new StringBuilder();
        if (parentName != null) {
            sb.append(parentName).append(NameUtils.CHILD_SEPARATOR);
        }
        if (tableName != null) {
            sb.append(tableName);
        }
        return getTable(namespace, sb.toString(), false);
    }

    /**
     * @see TableMetadataHelper
     */
    @Override
    public TableImpl getTable(String namespace, String tableName) {
        return getTable(namespace, tableName, false);
    }

    /**
     * @see TableMetadataHelper
     */
    @Override
    public TableImpl getTable(String namespace, String[] tablePath, int cost) {
        // TODO - cost should never be > 0 on this call, but may be during
        // unit tests. Would be nice to add an assert or an ISE
        return getTable(namespace, tablePath, false);
    }

    /**
     * @see TableMetadataHelper
     */
    @Override
    public RegionMapper getRegionMapper() {
        return new RegionMapperImpl(this);
    }

    private TableImpl getTable(String namespace, String[] path,
                               boolean mustExist) {

        if (path == null || path.length == 0) {
            return null;      // TODO ??? thow exception if mustExist is true??
        }

        final String firstKey = NameUtils.makeQualifiedName(namespace, path[0]);
        TableImpl targetTable =  (TableImpl) tables.get(firstKey);

        if (path.length > 1) {
            for (int i = 1; i < path.length && targetTable != null; i++) {
                try {
                    targetTable = getChildTable(path[i], targetTable);
                } catch (IllegalArgumentException ignored) {
                    targetTable = null;
                    break;
                }
            }
        }

        if (targetTable == null && mustExist) {
            throw new IllegalArgumentException
                ("Table: " + makeQualifiedName(namespace, path) +
                 " does not exist in " + this);
        }
        return targetTable;
    }

    public boolean tableExists(String namespace,
                               String tableName,
                               String parentName) {
        return (getTable(namespace, tableName, parentName) != null);
    }

    /**
     * Returns the specified Index or null if it, or its containing table
     * does not exist.
     */
    public Index getIndex(String namespace,
                          String tableName,
                          String indexName) {
        final TableImpl table = getTable(namespace, tableName);
        if (table != null) {
            return table.getIndex(indexName);
        }
        return null;
    }

    public Index getTextIndex(String namespace,
                              String tableName,
                              String indexName) {
        final TableImpl table = getTable(namespace, tableName);
        if (table != null) {
            return table.getTextIndex(indexName);
        }
        return null;
    }

    private static String makeQualifiedName(String namespace,
                                            String[] pathName) {
        final StringBuilder sb = new StringBuilder();
        for (String step : pathName) {
            sb.append(step);
        }
        return NameUtils.makeQualifiedName(namespace, null, sb.toString());
    }

    /**
     * Return the named child table.
     */
    public TableImpl getChildTable(String tableName, Table parent) {
        return (TableImpl) parent.getChildTable(tableName);
    }

     /**
     * Gets a MetadataInfo object for the given key.
     *
     * @return a metadata info object or null
     */
    public MetadataInfo getInfo(MetadataKey mdKey) {
        if (mdKey instanceof TableMetadataKey) {
            return getTable((TableMetadataKey)mdKey);
        }
        if (mdKey instanceof TableListKey) {
            return getTables((TableListKey)mdKey);
        }
        if (mdKey instanceof RegionMapperKey) {
            return new RegionMapperImpl(this);
        }
        if (mdKey instanceof SysTableListKey) {
            return getSystemTables();
        }
        if (mdKey instanceof MRTableListKey) {
            return getMRTables(((MRTableListKey)mdKey).includeLocalOnly());
        }
        throw new IllegalArgumentException("Unknown metadata key: " + mdKey);
    }

    /*
     * Get a table from TableMetadataKey.  This is used by RepNodes to return
     * tables requested by clients.  In this path it's necessary to filter out
     * created, but not-yet-populated indexes.
     */
    private TableImpl getTable(TableMetadataKey mdKey) {
        final String tableName =
            NameUtils.getFullNameFromQualifiedName(mdKey.tableName);
        final String namespace =
            NameUtils.getNamespaceFromQualifiedName(mdKey.tableName);
        return filterTable(getTable(namespace, tableName));
    }

    /**
     * Filter out created, but not-yet-populated indexes.
     */
    public static TableImpl filterTable(TableImpl table) {
        if (table != null && table.getIndexes().size() > 0) {
            /* clone, filter */
            table = table.clone();

            for (final Iterator<Map.Entry<String, Index>> it =
                    table.getMutableIndexes().entrySet().iterator();
                it.hasNext();) {
                final Map.Entry<String, Index> entry = it.next();
                if (!((IndexImpl)entry.getValue()).getStatus().isReady()) {
                    it.remove();
                }
            }
        }
        return table;
    }

    /**
     * Return all top-level tables. This returns the actual
     * map and should not be modified.
     */
    public Map<String, Table> getTables() {
        return tables;
    }

    private TableList getTables(TableListKey key) {
        final TableList list = new TableList(seqNum);
        final Map<String, Table> map = getTables(key.namespace);
        for (Table table : map.values()) {
            list.add(filterTable((TableImpl)table));
        }
        return list;
    }

    /**
     * Returns a copy of TableMetadata object which is compatible with
     * the specified serial version. The metadata may be missing components
     * which are not compatible with the serial version.
     */
    public TableMetadata getCompatible(short serVersion) {
        if (serVersion >= SerialVersion.CURRENT) {
            return this;
        }

        TableMetadata res = getCopy();
        Iterator<Map.Entry<String, Table>> iterator =
            res.getTables().entrySet().iterator();

        while (iterator.hasNext()) {
            Table t = iterator.next().getValue();
            if (serVersion < ((TableImpl)t).getRequiredSerialVersion()) {
                iterator.remove();
            }
        }
        return res;
    }

    /* public for access from ShowCommand (for now) */
    public Map<String, Table> getTables(String namespace) {

        final Map<String, Table> nsTables = createCompareMap();

        if (namespace == null) {
            for (Map.Entry<String, Table> entry : tables.entrySet()) {
                if (!entry.getKey().contains("" +
                    NameUtils.NAMESPACE_SEPARATOR)) {
                    nsTables.put(
                        NameUtils.getFullNameFromQualifiedName(entry.getKey()),
                        entry.getValue());
                }
            }
            return nsTables;
        }

        final String prefix = namespace + NameUtils.NAMESPACE_SEPARATOR;

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith(prefix.toLowerCase())) {
                nsTables.put(
                    NameUtils.getFullNameFromQualifiedName(entry.getKey()),
                    entry.getValue());
            }
        }
        return nsTables;
    }

    /*
     * Gets a list of multi-region tables. If includeLocalOnly is true MR tables
     * that are not subscribed to remote regions are included in the list.
     * Otherwise only MR tables subscribed to remote regions are included.
     */
    TableList getMRTables(boolean includeLocalOnly) {
        final TableList list = new TableList(seqNum);
        for (Table table : tables.values()) {
            final TableImpl impl = (TableImpl)table;
            if (impl.isMultiRegion()) {
                if (includeLocalOnly || !impl.getRemoteRegions().isEmpty()) {
                    list.add(filterTable(impl));
                }
            }
        }
        return list;
    }

    TableList getSystemTables() {
        final TableList list = new TableList(seqNum);
        for (Table table : tables.values()) {
            TableImpl impl = (TableImpl)table;
            if (impl.isSystemTable()) {
                list.add(impl);
            }
        }
        return list;
    }

    /**
     * Returns a sorted list of all tables. In this method parent and child
     * tables are listed independently, in alphabetical order, which means
     * parents first.
     */
    public List<String> listTables(String namespace) {
        final List<String> list = listTables(namespace, true);
        Collections.sort(list);
        return list;
    }

    /**
     * Adds all table names, parent and child to the list.  Child tables are
     * listed before parent tables because the iteration is depth-first.  This
     * simplifies code that does things like removing all tables or code that
     * depends on this order.  If other orders are desirable a parameter or
     * other method could be added to affect the iteration.
     *
     * @param namespace if non-null, only tables in the specified namespace are
     * returned and the names are full names (no namespace prefix). If null,
     * only tables in sysdefault namespace are returned, unless the allTables
     * parameter is true.
     *
     * @param allTables if true, all tables are returned unless the namespace
     * parameter is non-null. In this case the names have the namespace prefix
     * added. The format is [namespace:]full-table-name.
     */
    public List<String> listTables(final String namespace,
                                   final boolean allTables) {
        final List<String> tableList = new ArrayList<>();
        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    if (namespace != null &&
                        namespace.equalsIgnoreCase(
                            ((TableImpl)table).getInternalNamespace())) {
                        tableList.add(table.getFullName());
                    } else if (namespace == null) {
                        if (allTables) {
                            tableList.add(table.getFullNamespaceName());
                        } else if (
                            ((TableImpl)table).getInternalNamespace() == null) {
                            tableList.add(table.getFullName());
                        }
                    }
                    return true;
                }
            });
        return tableList;
    }

    /**
     * Returns the number of tables in the structure, including
     * child tables.
     */
    private int numTables() {
        final int[] num = new int[1];
        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    ++num[0];
                    return true;
                }
            });
        return num[0];
    }

    /**
     * Returns true if there are no tables defined.
     *
     * @return true if there are no tables defined
     */
    public boolean isEmpty() {
        return tables.isEmpty();
    }

    /**
     * Returns all available namespaces.
     */
    public Set<String> listNamespaces() {
        final Set<String> keySet = new HashSet<>();
        keySet.add(SYSDEFAULT_NAMESPACE_NAME);
        if (namespaces != null) {
            keySet.addAll(namespaces.keySet());
        }

        return keySet;
    }

    /**
     * Returns all text indexes.
     */
    public List<Index> getTextIndexes() {

        final List<Index> textIndexes = new ArrayList<>();

        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    textIndexes.addAll
                        (table.getIndexes(Index.IndexType.TEXT).values());
                    return true;
                }
            });

        return textIndexes;
    }

    /**
     * Convenience method for getting all text index names.
     */
    public Set<String> getTextIndexNames() {
        final Set<String> textIndexNames = new HashSet<>();
        for (Index ti : getTextIndexes()) {
            textIndexNames.add(ti.getName());
        }
        return textIndexNames;
    }

    private void bumpSeqNum() {
        seqNum++;
    }

    /**
     * Sets the metadata sequence number to the specified
     * new sequence number iff the new sequence number is
     * greater than the current sequence number.
     */
    public void setSeqNum(int newSeqNum) {
        if (newSeqNum > seqNum) {
            seqNum = newSeqNum;
        }
    }

    /*
     * Bump and return a new table id. Verify that the string version of
     * the id doesn't already exist as a table name.  If so, bump again.
     */
    private long allocateId(TableImpl table) {
        /* Special case the table metadata system table which has a fixed ID */
        if (table.isSystemTable() &&
            (table.getName().equalsIgnoreCase(TableMetadataDesc.TABLE_NAME))) {
            return TableMetadataDesc.METADATA_TABLE_ID;
        }
        while (true) {
            ++keyId;
            try {
                verifyIdNotUsed(TableImpl.createIdString(keyId));
                return keyId;
            } catch (IllegalArgumentException iae) {
                /* try the next id */
            }
        }
    }

    /*
     * Returns the highest table ID that has been allocated.
     */
    public long getMaxTableId() {
        return keyId;
    }

    private synchronized long allocateIndexId() {
        return ++maxIndexId;
    }

    /* -- From Metadata -- */

    @Override
    public MetadataType getType() {
        return MetadataType.TABLE;
    }

    @Override
    public int getSequenceNumber() {
        return seqNum;
    }

    @Override
    public TableChangeList getChangeInfo(int startSeqNum) {
        return new TableChangeList(seqNum, getChanges(startSeqNum));
    }

    @Override
    public TableMetadata pruneChanges(int limitSeqNum, int maxChanges) {
        final int firstChangeSeqNum = getFirstChangeSeqNum();
        if (firstChangeSeqNum == -1) {
            /* No changes to prune. */
            return this;
        }

        final int firstRetainedChangeSeqNum =
                            Math.min(getSequenceNumber() - maxChanges + 1,
                                     limitSeqNum);
        if (firstRetainedChangeSeqNum <= firstChangeSeqNum) {
            /* Nothing to prune. */
            return this;
        }

        for (final Iterator<TableChange> itr = changeHistory.iterator();
             itr.hasNext() &&
             (itr.next().getSequenceNumber() < firstRetainedChangeSeqNum);) {
            itr.remove();
        }
        return this;
    }

    /* Not private for unit tests */
    int getFirstChangeSeqNum() {
        return (changeHistory == null) ? -1 :
                    changeHistory.isEmpty() ?
                                -1 : changeHistory.get(0).getSequenceNumber();
    }

    /* Unit tests */
    int getChangeHistorySize() {
        return (changeHistory == null) ? 0 : changeHistory.size();
    }

    /* -- Change support methods -- */

    private List<TableChange> getChanges(int startSeqNum) {

        /* Skip if we are out of date, or don't have changes */
        if ((startSeqNum >= seqNum) ||
            (changeHistory == null) ||
            changeHistory.isEmpty()) {
            return null;
        }

        /* Also skip if they are way out of date (or not initialized) */
        if (startSeqNum < changeHistory.get(0).getSequenceNumber()) {
            return null;
        }

        List<TableChange> list = null;

        for (TableChange change : changeHistory) {
            if (change.getSequenceNumber() > startSeqNum) {
                if (list == null) {
                    list = new LinkedList<>();
                }
                list.add(change);
            }
        }
        return list;
    }

    /**
     * Updates the metadata from the specified table change object. Returns the
     * top level table instance that has been modified or null if the metadata
     * was updated but no tables were changed.
     *
     * @param change
     * @return the table that has been modified or null
     */
    public TableImpl apply(TableChange change) {
        final TableImpl table = change.apply(this);
        seqNum = change.getSequenceNumber();

        if (table != null) {
            table.setSequenceNumber(seqNum);
        }

        if (changeHistory != null) {
            changeHistory.add(change);
        }
        return table;
    }

    /**
     * Creates a copy of this TableMetadata object.
     *
     * @return the new TableMetadata instance
     */
    @Override
    public TableMetadata getCopy() {
        return (TableMetadata)Metadata.super.getCopy();
    }

    @Override
    public String toString() {
        return "TableMetadata[" + seqNum + ", n:" +
                (namespaces == null ? "-" : namespaces.size()) + ", " +
                (tables == null ? "(shallow)" : "t:" + tables.size()) + ", " +
                (changeHistory == null ? "-" : changeHistory.size()) + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TableMetadata)) {
            return false;
        }
        final TableMetadata other = (TableMetadata) obj;
        return Objects.equals(namespaces, other.namespaces) &&
            Objects.equals(activeRegions, other.activeRegions) &&
            Objects.equals(knownRegions, other.knownRegions) &&
            Objects.equals(tables, other.tables) &&
            (seqNum == other.seqNum) &&
            (keyId == other.keyId) &&
            Objects.equals(changeHistory, other.changeHistory) &&
            (serialVersion == other.serialVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaces,
                            activeRegions,
                            knownRegions,
                            tables,
                            seqNum,
                            keyId,
                            changeHistory,
                            serialVersion);
    }

    /**
     * Compares two TableMetadata instances by comparing all tables, including
     * child tables.  This might logically be implemented as an override of
     * equals but that might mean adding hashCode() to avoid warnings and
     * that's not necessary.  If anyone ever needs a true equals() overload
     * then this can change.
     *
     * @return true if the objects have the same content, false otherwise.
     */
    public boolean compareMetadata(final TableMetadata omd) {
        final int num = numTables();
        if (num == omd.numTables()) {
            final int[] numCompared = new int[1];
                iterateTables(new TableMetadataIteratorCallback() {
                        @Override
                        public boolean tableCallback(Table table) {
                            if (!existsAndEqual((TableImpl) table, omd)) {
                                return false;
                            }
                            ++numCompared[0];
                            return true;
                        }
                    });
                return numCompared[0] == num;
        }
        return false;
    }

    /**
     * Iterates all tables and ensures that the string version of the
     * id for the table doesn't match the idString. Throws if it
     * exists.  This is called when creating a new table in r2compat mode.
     */
    private void verifyIdNotUsed(final String idString) {
        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    final String tableId = ((TableImpl)table).getIdString();
                    if (tableId.equals(idString)) {
                        throw new IllegalArgumentException
                            ("Cannot create a table overlay with the name " +
                             idString + ", it exists as a table Id");
                    }
                    return true;
                }
            });
    }

    /**
     * Returns true if the table name exists in the TableMetadata and
     * the two tables are equal.
     */
    private static boolean existsAndEqual(TableImpl table,
                                          TableMetadata md) {
        final TableImpl otherTable = md.getTable(table.getInternalNamespace(),
                                                 table.getFullName());
        if (otherTable != null && table.equals(otherTable)) {

            /*
             * Check child tables individually.  Table equality does not
             * consider children.
             */
            for (Table child : table.getChildTables().values()) {
                if (!existsAndEqual((TableImpl) child, md)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Key to request a specific table.
     */
    public static class TableMetadataKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;
        private final String tableName;

        public TableMetadataKey(final String tableName) {
            this.tableName = tableName;
        }

        public TableMetadataKey(final String namespace,
                                final String tableName) {
            this.tableName = NameUtils.makeQualifiedName(namespace, tableName);
        }

        public TableMetadataKey(DataInput in, short serialVersion)
            throws IOException
        {
            tableName = readString(in, serialVersion);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion, tableName);
        }

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public MetadataKeyType getMetadataKeyType() {
            return StdMetadataKeyType.TABLE_KEY;
        }

        @Override
        public String toString() {
            return "TableMetadataKey[" +
                   (tableName != null ? tableName : "null") + "]";
        }
    }

    /**
     * Key to request tables.
     */
    public static class TableListKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;
        private final String namespace;

        /* Get all tables in the specified namespace */
        public TableListKey(String namespace) {
            this.namespace = namespace;
        }

        public TableListKey(DataInput in, short serialVersion)
            throws IOException
        {
            namespace = readString(in, serialVersion);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion, namespace);
        }

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public short getRequiredSerialVersion() {
            return INFO_LIST_VERSION;
        }

        @Override
        public MetadataKeyType getMetadataKeyType() {
            return StdMetadataKeyType.TABLE_LIST;
        }

        @Override
        public String toString() {
            return "TableListKey[" +
                   (namespace != null ? namespace : "null") + "]";
        }
    }

    /**
     * Key to request the list of multi-region tables.
     */
    public static class MRTableListKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean includeLocalOnly;

        public MRTableListKey(boolean includeLocalOnly) {
            this.includeLocalOnly = includeLocalOnly;
        }

        public MRTableListKey(DataInput in,
                              @SuppressWarnings("unused") short serialVersion)
                throws IOException {
            includeLocalOnly = in.readBoolean();
        }

        private boolean includeLocalOnly() {
            return includeLocalOnly;
        }

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public void writeFastExternal(DataOutput out, short sv)
                throws IOException {
            out.writeBoolean(includeLocalOnly);
        }

        @Override
        public short getRequiredSerialVersion() {
            return MRT_INFO_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1;
        }

        @Override
        public MetadataKeyType getMetadataKeyType() {
            return StdMetadataKeyType.MR_TABLE_LIST;
        }

        @Override
        public String toString() {
            return "MRTableListKey[" + includeLocalOnly + "]";
        }
    }

    /**
     * Key to request the list of all system tables.
     */
    public static class SysTableListKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;

        public static final SysTableListKey INSTANCE = new SysTableListKey();

        private SysTableListKey() { }

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public void writeFastExternal(DataOutput out, short sv) { }

        @Override
        public short getRequiredSerialVersion() {
            return INFO_LIST_VERSION;
        }

        @Override
        public MetadataKeyType getMetadataKeyType() {
            return StdMetadataKeyType.SYS_TABLE_LIST;
        }

        @Override
        public String toString() {
            return "SysTableListKey[]";
        }
    }

    /**
     * Key to request a region mapper.
     */
    public static class RegionMapperKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;

        public static final RegionMapperKey INSTANCE = new RegionMapperKey();

        private RegionMapperKey() { }

        @Override
        public void writeFastExternal(DataOutput out, short sv) { }

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public short getRequiredSerialVersion() {
            return INFO_LIST_VERSION;
        }

        @Override
        public MetadataKeyType getMetadataKeyType() {
            return StdMetadataKeyType.REGION_MAPPER;
        }

        @Override
        public String toString() {
            return "RegionMapperKey[]";
        }
    }

    /**
     * Iterate over all tables, calling back to the callback for each.
     */
    public void iterateTables(TableMetadataIteratorCallback callback) {
        for (Table table : getTables().values()) {
            if (!iterateTables(table, callback)) {
                break;
            }
        }
    }

    /**
     * Implements iteration of all tables, depth-first (i.e. child tables are
     * visited before parents.
     */
    private static boolean
        iterateTables(Table table, TableMetadataIteratorCallback callback) {
        for (Table child : table.getChildTables().values()) {
            if (!iterateTables(child, callback)) {
                return false;
            }
        }
        if (!callback.tableCallback(table)) {
            return false;
        }
        return true;
    }

    /**
     * An interface used for operations that need to iterate the entire tree of
     * metadata.
     */
    public interface TableMetadataIteratorCallback {

        /**
         * Returns true if the iteration should continue, false if not.
         */
        boolean tableCallback(Table t);
    }

    private void ensureNamespaceMap() {
        if (namespaces == null) {
            namespaces = createCompareMap();
            /*
             * use TreeMap to present the keys in order
             * Note: TreeMap doesn't allow a null key.
             */
        }
    }

    public boolean hasNamespace(String namespace) {
        if (NameUtils.isInternalInitialNamespace(namespace)) {
            return true;
        }

        if (namespaces == null) {
            return false;
        }

        return namespaces.containsKey(namespace);
    }

    public NamespaceImpl getNamespace(String namespace) {
        if ( NameUtils.isInternalInitialNamespace(namespace) ) {
            /* In the case of the "sysdefault" namespace use a null value for
            ResourceOwner */
            return new NamespaceImpl(namespace, null /* ResourceOwner */);
        }

        if (namespaces == null) {
            return null;
        }

        return namespaces.get(namespace);
    }

    public void addNamespace(NamespaceImpl namespace) {
        ensureNamespaceMap();
        namespaces.put(namespace.getNamespace(), namespace);
    }

    public NamespaceImpl createNamespace(String namespace, ResourceOwner owner)
    {
        if ( hasNamespace(namespace) ) {
            throw new IllegalArgumentException("Cannot create " +
                "namespace. Namespace already exists: '" + namespace + "' ");
        }

        NamespaceImpl nsObj = new NamespaceImpl(namespace, owner);
        ensureNamespaceMap();
        namespaces.put(namespace, nsObj);

        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddNamespaceChange(namespace, owner, seqNum));
        }

        return nsObj;
    }

    public NamespaceImpl dropNamespace(String namespace) {
        if ( !hasNamespace(namespace) ) {
            throw new IllegalArgumentException("Cannot drop " +
                "namespace. Namespace doesn't exist: '" + namespace + "' ");
        }

        if ( !isNamespaceEmpty(namespace) ) {
            throw new IllegalArgumentException("Cannot drop " +
                "namespace. Namespace is not empty: '" + namespace + "' ");
        }

        ensureNamespaceMap();
        NamespaceImpl res = namespaces.remove(namespace);

        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new RemoveNamespaceChange(namespace, seqNum));
        }

        return res;
    }

    /**
     * Returns true for namespaces that do not contain any tables.
     * For the "sysdefault" namespace
     * {@link oracle.kv.table.TableAPI#SYSDEFAULT_NAMESPACE_NAME} it always
     * returns false.
     */
    public boolean isNamespaceEmpty(String namespace) {
        if (NameUtils.isInternalInitialNamespace(namespace)) {
            return false;
        }

        if ( !hasNamespace(namespace) ) {
            return true;
        }

        for (Table table : getTables().values()) {
            if ( namespace!=null &&
                namespace.equals(table.getNamespace())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Return all namespaces. This may return the actual
     * map and should not be modified.
     */
    public Map<String, NamespaceImpl> getNamespaces() {
        return namespaces == null ? Collections.emptyMap() : namespaces;
    }

    /**
     * Gets the region object for the local region. If the local region name
     * has not been set, null is returned.
     */
    public Region getLocalRegion() {
        final String localName = getLocalRegionName();
        if (localName == null) {
            return null;
        }
        final Region region = getActiveRegion(localName);
        assert region != null;
        return region;
    }

    /**
     * Gets the local region name. Returns null if it has not been set.
     */
    private String getLocalRegionName() {
        return (knownRegions == null) ? null :
                                        knownRegions.get(LOCAL_REGION_INDEX);
    }

    /**
     * Translates region id to region name, or null if not exist
     * @param rid region id
     * @return region name
     */
    public String getRegionName(int rid) {
        return TableMetadata.RegionMapperImpl.getRegionName(knownRegions, rid);
    }

    /**
     * Sets the local region name. Returns true if the metadata was changed.
     * The local region name can be set iif it is not already set or it does not
     * match an existing region and there are no active multi-region tables.
     * Throws IllegalCommandException is the local name could not be set.
     *
     * @return the new region if it was changed otherwise null
     */
    public Region setLocalRegionName(String localName) {
        TableImpl.validateRegionName(localName);

        final String oldName = getLocalRegionName();

        /*
         * If the local name has not already been set (oldName == null) we
         * allow setting the name even if there are MR tables for backward
         * compatibility. The first release did not support a local region name.
         */
        if (oldName != null) {
            /*
             * Can only change the local region name if there are no active MR
             * tables.
             */
            for (Table table : getTables().values()) {
                if (((TableImpl)table).isMultiRegion()) {
                    throw new IllegalCommandException("Cannot change local" +
                                                      " region name," +
                                                      " milti-region tables are" +
                                                      " active");
                }
            }
        }

        /* If exact same name, done */
        if (localName.equals(oldName)) {
            /* No change */
            return null;
        }

        /* No regions defined, just set it */
        if (knownRegions == null) {
            knownRegions = new ArrayList<>();
            /* Note, this cannot be a set() since the ArrayList is empty */
            knownRegions.add(localName);
            return addActiveRegion(new Region(localName, Region.LOCAL_REGION_ID));
        }

        /* Check to see if the name is a remote region */
        assert LOCAL_REGION_INDEX == 0;
        for (int i = LOCAL_REGION_INDEX+1; i < knownRegions.size(); i++) {
            if (knownRegions.get(i).equalsIgnoreCase(localName)) {
                throw new IllegalCommandException("Region " + localName +
                                                  " is a remote region");
            }
        }
        knownRegions.set(LOCAL_REGION_INDEX, localName);
        return addActiveRegion(new Region(localName, Region.LOCAL_REGION_ID));
    }

    /**
     * Creates the specified region. Returns the new region or null if the
     * region already exist.
     */
    public Region createRegion(String regionName) {
        TableImpl.validateRegionName(regionName);

        final Region region = getActiveRegion(regionName);
        if (region != null) {
            if (region.isLocal()) {
                throw new IllegalCommandException("Region " + regionName +
                                                  " is the local region");
            }
            /* Already defined */
            return null;
        }

        if (knownRegions == null) {
            knownRegions = new ArrayList<>();
            /* Reserve index 0 for local region (ID=1) */
            knownRegions.add(null);
        } else {
            /* Check to see if this region was known before and dropped */
            for (int i = 1; i < knownRegions.size(); i++) {
                if (knownRegions.get(i).equalsIgnoreCase(regionName)) {
                    return addActiveRegion(
                        new Region(regionName,
                                   RegionMapperImpl.toRegionId(i)));
                }
            }
        }

        if (knownRegions.size() >= Region.REGION_ID_MAX) {
            throw new IllegalCommandException("Exceeded max number of regions");
        }

        knownRegions.add(regionName);
        return addActiveRegion(new Region(regionName, knownRegions.size()));
    }

    /**
     * Adds the specified region to the active list and creates a change
     * entry if needed.
     */
    private Region addActiveRegion(Region region) {
        if (activeRegions == null) {
            activeRegions = createCompareMap();
        }
        activeRegions.put(region.getName(), region);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddRegion(region, seqNum));
        }
        return region;
    }

    /**
     * Drops the specified region from the metadata. Returns the region instance
     * that was removed, or null if the metadata was not modified. Throws
     * IllegalCommandException if the region is still active (referenced by any
     * table) or the region is the local region.
     */
    public Region dropRegion(String regionName) {
        final Region region = getActiveRegion(regionName);
        if (region == null) {
            return null;
        }

        if (region.isLocal()) {
            throw new IllegalCommandException("Cannot drop local region");
        }

        final int regionId = region.getId();
        String tableName = null;
        int inUse = 0;
        for (Table table : tables.values()) {
            final TableImpl t = (TableImpl)table;
            if (t.inRegion(regionId)) {
                tableName = t.getFullName();
                inUse++;
            }
        }
        if (inUse > 1) {
            throw new IllegalCommandException(
                        "Cannot remove region " + regionName +
                        " because it is still in use by " + inUse + " tables");
        } else if (tableName != null) {
            throw new IllegalCommandException(
                        "Cannot remove region " + regionName +
                        " because it is still in use by " + tableName);
        }
        activeRegions.remove(regionName);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new RemoveRegion(regionName, seqNum));
        }
        /* Return an inactive region */
        return new Region(regionName, regionId, false);
    }

    public Region getActiveRegion(String regionName) {
        return activeRegions == null ? null : activeRegions.get(regionName);
    }

    /**
     * Returns true if the specified name is an active region.
     */
    private boolean isActiveRegion(String regionName) {
        return getActiveRegion(regionName) != null;
    }

    /**
     * Returns the set of all known regions (active and inactive).
     */
    public Set<Region> getKnownRegions() {
        if (knownRegions == null) {
            return Collections.emptySet();
        }
        final Set<Region> regions = new HashSet<>();
        for (int i = 0; i < knownRegions.size(); i++) {
            final String regionName = knownRegions.get(i);
            /* The region name may be null if the local region was not set */
            if (regionName == null) {
                assert i == LOCAL_REGION_INDEX;
                continue;
            }
            Region region = activeRegions.get(regionName);
            if (region == null) {
                /* Not active, create an inactive instance */
                region = new Region(regionName, i+1, false);
            }
            regions.add(region);
        }
        return regions;
    }

    /**
     * Updates, adding a remote region or (re)setting the local region.
     */
    void addRegion(Region region) {
        if (activeRegions == null) {
            activeRegions = createCompareMap();
        }

        /*
         * Need to special case the local region. If it is being added, check to
         * see if the local region is already set. If so, remove it from the
         * active list before continuing.
         */
        if (region.isLocal()) {
            final String oldName = getLocalRegionName();
            if (oldName != null) {
                activeRegions.remove(oldName);
            }
        }

        if (region.isActive()) {
            activeRegions.put(region.getName(), region);
        }
        final int index = RegionMapperImpl.toIndex(region.getId());
        if (knownRegions == null) {
            knownRegions = new ArrayList<>(index + 1);
        }
        /*
         * If there was a gap in region IDs, the index may not be the very next
         * entry in the list. In this case add nulls up to the new index as
         * needed so that the set() functions properly.
         */
        while (knownRegions.size() < index + 1) {
            knownRegions.add(null);
        }
        knownRegions.set(index, region.getName());
    }

    /**
     * Updates, removing region.
     */
    void removeRegion(String regionName) {
        activeRegions.remove(regionName);
    }

    /**
     * List of tables.
     */
    public static class TableList extends InfoList<Table> {
        private static final long serialVersionUID = 1L;

        TableList(int seqNum) {
            super(seqNum);
        }

        public TableList(DataInput in, short serialVersion)
            throws IOException
        {
            super(in, serialVersion, (i, s) -> new TableImpl(i, s, null));
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeFastExternal(
                out, serialVersion,
                (t, o, s) -> {
                    final TableImpl table = (TableImpl) t;
                    if (table.getParent() != null) {
                        throw new IllegalStateException(
                            "Expected top level table: " + table);
                    }
                    table.writeFastExternal(o, s);
                });
        }

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public MetadataInfoType getMetadataInfoType() {
            return MetadataInfoType.TABLE_LIST;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof TableList)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    /**
     * Implementation of a standalone region mapper.
     */
    public static class RegionMapperImpl implements RegionMapper,
                                                    MetadataInfo,
                                                    Serializable {
        private static final long serialVersionUID = 1L;

        private final int seqNum;

        /* See field descriptions in containing class */
        private final List<String> knownRegions;
        private final Map<String, Region> activeRegions;

        /**
         * Construct a region mapper from the specified table metadata.
         */
        private RegionMapperImpl(TableMetadata md) {
            seqNum = md.seqNum;
            knownRegions = md.knownRegions;
            activeRegions = md.activeRegions;
        }

        public RegionMapperImpl(DataInput in, short serialVersion)
            throws IOException
        {
            seqNum = readPackedInt(in);
            knownRegions = readCollection(in, serialVersion, ArrayList::new,
                                          SerializationUtil::readString);
            activeRegions = readMap(in, serialVersion,
                                    TableMetadata::createCompareMap,
                                    SerializationUtil::readString,
                                    Region::new);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writePackedInt(out, seqNum);
            writeCollection(out, serialVersion, knownRegions,
                            WriteFastExternal::writeString);
            writeMap(out, serialVersion, activeRegions,
                     WriteFastExternal::writeString);
        }

        /* Converts a region ID to an index into the known region list */
        private static int toIndex(int regionId) {
            Region.checkId(regionId, false /* isExternalRegion */);
            return regionId - 1;
        }

        /* Convers a know region list index to region ID */
        private static int toRegionId(int index) {
            return index + 1;
        }

        /* -- From RegionMapper -- */

        private static String getRegionName(List<String> knownRegions,
                                           int regionId) {
            if (knownRegions == null) {
                return null;
            }
            final int index = toIndex(regionId);
            return (index >= knownRegions.size()) ? null :
                                                    knownRegions.get(index);
        }

        @Override
        public String getRegionName(int regionId) {
            return getRegionName(knownRegions, regionId);
        }

        @Override
        public int getRegionId(String regionName) {
            if (activeRegions == null) {
                return Region.UNKNOWN_REGION_ID;
            }
            final Region region = activeRegions.get(regionName);
            return region == null ? Region.UNKNOWN_REGION_ID : region.getId();
        }

        @Override
        public Map<Integer, String> getKnownRegions() {
            if (knownRegions == null) {
                return null;
            }
            final Map<Integer, String> map = new HashMap<>();
            for (int i = 0; i < knownRegions.size(); i++) {
                final String regionName = knownRegions.get(i);
                if (regionName != null) {
                    map.put(toRegionId(i), regionName);
                }
            }
            return map.isEmpty() ? null : map;
        }

        /* -- From RegionMapper and MetadataInfo -- */

        @Override
        public boolean isEmpty() {
            return (knownRegions == null) ? true : knownRegions.isEmpty();
        }

        @Override
        public int getSequenceNumber() {
            return seqNum;
        }

        /* -- From MetadataInfo -- */

        @Override
        public MetadataType getType() {
            return MetadataType.TABLE;
        }

        @Override
        public MetadataInfoType getMetadataInfoType() {
            return MetadataInfoType.REGION_MAPPER;
        }

        @Override
        public String toString() {
            return "RegionMapper[" + seqNum + ", " + isEmpty() + "]";
        }
    }
}
