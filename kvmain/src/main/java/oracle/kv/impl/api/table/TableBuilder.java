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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;

/**
 * TableBuilder is a class used to construct Tables and complex data type
 * instances.  The instances themselves are immutable.  The pattern used
 * is
 * 1.  create TableBuilder
 * 2.  add state in terms of data types
 * 3.  build the desired object
 *
 * When constructing a child table the parent table's Table object is required.
 * This requirement makes it easy to add the necessary parent information to
 * the child table.  It is also less error prone than requiring the caller to
 * add the parent's key fields by hand.
 *
 * There is a special case is where the builder is created from a JSON string
 * which may already have the parent information encoded.  That area is a
 * TODO until a final decision on what JSON string format(s) are allowed for
 * this if it is even publicly supported.
 */
public class TableBuilder extends TableBuilderBase {

    /* May be null */
    private final RegionMapper regionMapper;

    private final String name;

    private String description;

    /* These apply only to tables */

    private final TableImpl parent;

    private final String namespace;

    private List<String> primaryKey;

    private List<String> shardKey;

    private HashMap<String,Integer> primaryKeySizes;

    private boolean r2compat;

    private ResourceOwner owner;

    private final boolean sysTable;

    private boolean jsonCollection;

    private Map<String, FieldDef.Type> mrCounters;

    /*
     * TODO - If there is a parent specified, should the passed in namespace
     * match the parent's namespace?
     */
    private TableBuilder(
        String namespace,
        String name,
        String description,
        Table parent,
        boolean copyParentInfo,
        boolean isSysTable,
        RegionMapper regionMapper) {

        this.name = name;
        this.namespace = namespace;
        this.description = description;
        r2compat = false;
        this.parent = (parent != null ? (TableImpl) parent : null);
        this.sysTable = isSysTable;
        this.regionMapper = regionMapper;
        primaryKey = new ArrayList<>();
        shardKey = new ArrayList<>();

        /* Add the key columns of the parent table to this TableBuilder. */
        if (parent != null && copyParentInfo) {
            addParentInfo();
        }

        TableImpl.validateTableName(name, sysTable);
        if (namespace != null) {
            TableImpl.validateNamespace(namespace);
        }
    }

    /**
     * There is no need to go more than one level because the primary key of
     * each table includes the fields of its ancestors.
     */
    private void addParentInfo() {
        for (String fieldName : parent.getPrimaryKey()) {
            fields.put(parent.getFieldMapEntry(fieldName, true));
            primaryKey.add(fieldName);
        }

        /*
         * Copy the primary key sizes of the parent to this object's map
         * of key sizes. There's a translation from array of int to the
         * map.
         */
        if (parent.getPrimaryKeySizes() != null) {
            primaryKeySizes =
                new HashMap<String, Integer>();
            final List<String> parentKeys = parent.getPrimaryKey();
            for (int i = 0; i < parent.getPrimaryKeySizes().size(); i++) {
                int index = parent.getPrimaryKeySizes().get(i);
                if (index != 0) {
                    primaryKeySizes.put(parentKeys.get(i), index);
                }
            }
        }
    }

    /**
     * Getters/Setters
     */
    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean getJsonCollection() {
        return jsonCollection;
    }

    @Override
    public TableBuilderBase setJsonCollection() {
        jsonCollection = true;
        return this;
    }

    @Override
    public TableBuilderBase setMRCounters(
        Map<String, FieldDef.Type> mrCounters) {
        this.mrCounters = mrCounters;
        return this;
    }

    @Override
    public Map<String, FieldDef.Type> getMRCounters() {
        return mrCounters;
    }

    @Override
    public TableBuilderBase setDescription(String description) {
        this.description = description;
        return this;
    }

    public TableImpl getParent() {
        return parent;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getShardKey() {
        return shardKey;
    }

    public boolean isR2compatible() {
        return r2compat;
    }

    @Override
    public TableBuilderBase setR2compat(boolean value) {
        this.r2compat = value;
        return this;
    }

    public ResourceOwner getOwner() {
        return owner;
    }

    public TableBuilderBase setOwner(ResourceOwner newOwner) {
        this.owner = newOwner;
        return this;
    }

    public boolean isSysTable() {
        return sysTable;
    }

    @Override
    public RegionMapper getRegionMapper() {
        return regionMapper;
    }

    @Override
    public void addRegion(String regionName) {
        if (regionMapper == null) {
            throw new IllegalStateException("Unable to map region name");
        }
        final int regionId = regionMapper.getRegionId(regionName);
        if (regionId < 0) {
            throw new IllegalArgumentException("Unknown region: " + regionName);
        }
        addRegion(regionId);
    }

    /**
     * Adds a region ID.
     */
    public void addRegion(int regionId) {
        Region.checkId(regionId, false /* isExternalRegion */);
        if (regions == null) {
            regions = new HashSet<>();
        }
        regions.add(regionId);
    }

    /**
     * Table-only methods
     */
    @Override
    public TableBuilderBase primaryKey(String ... key) {
        for (String field : key) {
            if (primaryKey.contains(field)) {
                throw new IllegalArgumentException
                    ("The primary key field already exists: " + field);
            }
            primaryKey.add(field);
        }
        return this;
    }

    @Override
    public void validatePrimaryKeyFields() {
        if (primaryKey == null) {
            return;
        }
        for (String key : primaryKey) {
            FieldMapEntry fme = fields.getFieldMapEntry(key);
            if (fme == null) {
                throw new IllegalArgumentException
                    ("Field does not exist: " + key);
            }
            if (fme.getFieldDef().isMRCounter()) {
                throw new IllegalArgumentException("An MR_Counter column " +
                    "cannot be a primary key.");
            }
            if (fme.getDefaultValueInternal() != null) {
                throw new IllegalArgumentException
                    ("Primary key fields can not have default values");
            }
        }

        /*
         * Validate regular fields as well. This used to be done directly
         * in FieldMapEntry
         */
        for (FieldMapEntry fme : fields.getFieldProperties()) {
            if (!primaryKey.contains(fme.getFieldName())) {
                if (!fme.isNullable() &&
                    (fme.getDefaultValueInternal() == null)) {
                    throw new IllegalArgumentException(
                        "Not nullable field " + fme.getFieldName() +
                        " must have a default value");
                }
            }
        }
    }

    @Override
    public TableBuilderBase shardKey(String ... key) {
        if (parent != null) {
            throw new IllegalArgumentException
                ("Child tables cannot have a shard key.");
        }
        for (String field : key) {
            if (shardKey.contains(field)) {
                throw new IllegalArgumentException
                    ("The shard key field already exists: " + field);
            }
            shardKey.add(field);
        }
        return this;
    }

    @Override
    public TableBuilderBase primaryKey(final List<String> pKey) {
        this.primaryKey = pKey;
        return this;
    }

    @Override
    public TableBuilderBase shardKey(final List<String> mKey) {
        if (parent != null) {
            throw new IllegalArgumentException
                ("Child tables cannot have a shard key.");
        }
        this.shardKey = mKey;
        return this;
    }

    @Override
    public TableBuilderBase primaryKeySize(String keyField, int size) {
        if (primaryKey == null) {
            throw new IllegalArgumentException
                ("primaryKeySize() cannot be called before primaryKey()");
        }

        int index = primaryKey.indexOf(keyField);
        if (index < 0) {
            throw new IllegalArgumentException
                ("Field is not part of primary key: " + keyField);
        }

        /*
         * If the field is present (it may not be), make sure it's an integer.
         * If not present, the type validation will happen on table creation.
         */
        FieldDef field = getField(keyField);
        if (field != null && !field.isInteger()) {
            throw new IllegalArgumentException
                ("primaryKeySize() requires an INTEGER field type: " +
                 keyField);
        }

        if (size <= 0 || size > 5) {
            throw new IllegalArgumentException("Invalid primary key size: " +
                                               size + ". Size must be 1-5.");
        }
        /*
         * Ignore a size of 5, which is the largest size possible
         * for an integer of any value.
         */
        if (size < 5) {
            if (primaryKeySizes == null) {
                primaryKeySizes = new HashMap<String, Integer>();
            }
            primaryKeySizes.put(keyField, size);
        }
        return this;
    }

    /**
     * Build the actual TableImpl
     */
    @Override
    public TableImpl buildTable() {
        /*
         * If the shard key is not provided it defaults to:
         * o the primary key if this is a top-level table
         * o the parent's shard key if this is a child table.
         */
        if (shardKey == null || shardKey.isEmpty()) {
            if (parent != null) {
                shardKey = new ArrayList<>(parent.getShardKey());
            } else {
                shardKey = primaryKey;
            }
        }

        /*
         * if primaryKey and shardKey names differ from their field
         * definitions in case, use the field definition versions
         */
        fixupKeyNames();

        TableImpl table =
            new TableImpl(NameUtils.switchToInternalUse(namespace),
                          getName(),
                          parent,
                          getPrimaryKey(),
                          getPrimaryKeySizes(),
                          getShardKey(),
                          fields,
                          ttl,
                          beforeImageTTL,
                          null, /*limits*/
                          r2compat,
                          0,
                          getDescription(),
                          true, /*validate*/
                          owner,
                          sysTable,
                          getIdentityColumnInfo(),
                          regions,
                          jsonCollection,
                          mrCounters);
        if (sequenceDef != null) {
            table.setIdentitySequenceDef(sequenceDef);
        }
        return table;
    }

    @Override
    void validateFieldAddition(final String fieldName,
                               final String pathName,
                               final FieldMapEntry fme) {

        super.validateFieldAddition(fieldName, pathName, fme);

        /*
         * Cannot add a field that has the same name as a primary key field.
         */
        if (parent != null) {
            if (parent.isKeyComponent(fieldName)) {
                throw new IllegalArgumentException
                    ("Cannot add field, it already exists in primary key " +
                        "fields of its parent table: " + fieldName);
            }
        }
    }

    /*
     * Used to validate the state of the builder to ensure that it can be used
     * to build a table.  The simplest way to do this is to actually build one
     * and let TableImpl do the validation.  This method is used by tests and
     * the CLI.
     */
    @Override
    public TableBuilderBase validate() {
        buildTable();
        return this;
    }

    /*
     * Show the current state of the table.  The simplest way is to create a
     * not-validated table and display it.
     */
    public String toJsonString(boolean pretty) {
        TableImpl t = new TableImpl(NameUtils.switchToInternalUse(namespace),
                                    getName(),
                                    parent,
                                    getPrimaryKey(),
                                    getPrimaryKeySizes(),
                                    getShardKey(),
                                    fields,
                                    ttl,
                                    beforeImageTTL,
                                    null, /*limits*/
                                    r2compat,
                                    0,
                                    getDescription(),
                                    false, /*validate*/
                                    owner,
                                    sysTable,
                                    getIdentityColumnInfo(),
                                    regions,
                                    jsonCollection,
                                    mrCounters);
        return t.toJsonString(pretty, regionMapper);
    }

    public List<Integer> getPrimaryKeySizes() {
        if (primaryKeySizes == null) {
            return null;
        }
        ArrayList<Integer> list = new ArrayList<>(primaryKey.size());
        for (String key : primaryKey) {
            Integer size = primaryKeySizes.get(key);
            if (size == null) {
                size = 0;
            }
            list.add(size);
        }
        return list;
    }

    /**
     * Build a Table from its JSON format.
     */
    public static TableImpl fromJsonString(
        String jsonString,
        Table parent) {
        return TableJsonUtils.fromJsonString(jsonString, (TableImpl) parent);
    }

    /**
     * Create a table builder
     */
    public static TableBuilder createTableBuilder(String namespace,
                                                  String name,
                                                  String description,
                                                  Table parent,
                                                  boolean copyParentInfo,
                                                  RegionMapper regionMapper) {
        return new TableBuilder(namespace, name, description,
                                parent, copyParentInfo, false, regionMapper);
    }

    public static TableBuilder createTableBuilder(String namespace,
                                                  String name,
                                                  String description,
                                                  Table parent,
                                                  RegionMapper regionMapper) {
        return createTableBuilder(namespace, name, description, parent, true,
                                  regionMapper);
    }

    /*
     * For unit tests. These builders do not fully support multi-region tables.
     */
    public static TableBuilder createTableBuilder(String namespace,
                                                  String name) {
        return createTableBuilder(namespace, name, null, null, false, null);
    }

    public static TableBuilder createTableBuilder(String name,
                                                  String description,
                                                  Table parent) {
        /* If a parent is specified use its namespace */
        return createTableBuilder((parent == null) ? null: parent.getNamespace(),
                                  name, description, parent, true, null);
    }

    public static TableBuilder createTableBuilder(String name) {
        return createTableBuilder(null, name, null, null, false, null);
    }

    /**
     * Create a builder for system tables.
     */
    public static TableBuilder createSystemTableBuilder(String name) {
        return new TableBuilder(null, name, null, null, false, true, null);
    }

    /**
     * Creates an ArrayBuilder.
     */
    public static ArrayBuilder createArrayBuilder(String description) {
        return new ArrayBuilder(description);
    }

    public static ArrayBuilder createArrayBuilder() {
        return new ArrayBuilder();
    }

    /**
     * Creates an MapBuilder.
     */
    public static MapBuilder createMapBuilder(String description) {
        return new MapBuilder(description);
    }

    public static MapBuilder createMapBuilder() {
        return new MapBuilder();
    }

    /**
     * Creates a RecordBuilder.
     */
    public static RecordBuilder createRecordBuilder(
        String name,
        String description) {
        return new RecordBuilder(name, description);
    }

    public static RecordBuilder createRecordBuilder(String name) {
        return new RecordBuilder(name);
    }


    /*
     * Make list of field names in primaryKey and shardKey the same
     * as their corresponding definitions, using the latter
     */
    private void fixupKeyNames() {
        /* unconditionally rewrite the lists */
        primaryKey = fixupKeyNames(primaryKey);
        shardKey = fixupKeyNames(shardKey);
    }

    private List<String> fixupKeyNames(List<String> list) {
        /* unconditionally rewrite the lists */
        if (list == null) {
            return null;
        }
        List<String> newList = new ArrayList<String>(list.size());
        for (String field : list) {
            FieldMapEntry entry = fields.getFieldMapEntry(field);
            if (entry == null) {
                throw new IllegalArgumentException(
                    "Key field has not been created in table: " + field);
            }
            newList.add(entry.getFieldName());
        }
        return newList;
    }
}
