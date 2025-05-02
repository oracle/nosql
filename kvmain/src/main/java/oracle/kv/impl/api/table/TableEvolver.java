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

import java.util.HashSet;

import oracle.kv.table.Table;

/**
 * TableEvolver is a class used to evolve existing tables.  It has accessors
 * and modifiers for valid evolution operations and double checks them against
 * the current state of the class.
 * <p>
 * The general usage pattern is:
 * TableEvolver evolver = TableEvolver.createTableEvolver(Table);
 * ...do things...
 * Table newTable = evolver.evolveTable();
 * The resulting table can be passed to the TableMetadata.evolveTable()
 * method.
 * <p>
 * Schema evolution on tables allows the following transformations:
 * <ul>
 * <li> Add a non-key field to a table
 * <li> Remove a non-key field from a table (the system creates Avro defaults
 * for all fields which makes this possible)
 * </ul>
 * These operations *should* be added, but aren't yet allowed:
 * <ul>
 * <li> Change the nullable and default value of a field, probably restricted
 * to non-key (primary or index) fields.
 * <li> Change the description of a field.
 * <li> Direct field rename (implemented as remove, re-create with same type).
 * <li> Add values to enum (at the end)
 * <li> Change the table description.
 * </ul>
 * These operations are not possible:
 * <ul>
 * <li> Modify/remove/add a field that is part of the primary key
 * <li> Remove an indexed field or modify it in an incompatible manner.
 * <li> Change the type or name of any field (rename maybe allowed, see above)
 * <li> NOTE: at some point it may be useful to allow some changes to
 * primary key fields, such as description, default value. Primary
 * key fields cannot be nullable.
 * </ul>
 */
public class TableEvolver extends TableBuilderBase {
    private final TableImpl table;
    private final int evolvedVersion;
    private String description;

    /* May be null */
    private final RegionMapper regionMapper;

    private TableEvolver(TableImpl table, RegionMapper regionMapper) {
        super(table.getFieldMap().clone());
        this.table = table;
        this.regionMapper = regionMapper;
        ttl = table.getDefaultTTL();
        description = table.getDescription();
        evolvedVersion = table.getTableVersion();
        if (evolvedVersion != table.numTableVersions()) {
            throw new IllegalArgumentException
                ("Table evolution must be performed on the latest version");
        }
        /* Use a copy of the region set */
        regions = (table.isMultiRegion() && table.isTop()) ?
            new HashSet<>(table.getRemoteRegions()) : null;
    }

    public static TableEvolver createTableEvolver(Table table) {
        return new TableEvolver(((TableImpl)table).clone(), null);
    }

    public static TableEvolver createTableEvolver(Table table,
                                                  RegionMapper regionMapper) {
        return new TableEvolver(((TableImpl)table).clone(), regionMapper);
    }

    @Override
    public String getBuilderType() {
        return "Evolver";
    }

    /**
     * Accessors
     */
    public TableImpl getTable() {
        return table;
    }

    public int getTableVersion() {
        return evolvedVersion;
    }

    public RecordDefImpl getRecord(String fieldName) {
        return (RecordDefImpl) getField(fieldName);
    }

    public MapDefImpl getMap(String fieldName) {
        return (MapDefImpl) getField(fieldName);
    }

    public ArrayDefImpl getArray(String fieldName) {
        return (ArrayDefImpl) getField(fieldName);
    }

    RecordEvolver createRecordEvolver(RecordDefImpl record) {
        if (record == null) {
            throw new IllegalArgumentException
                ("Null record passed to createRecordEvolver");
        }
        return new RecordEvolver(record);
    }

    @Override
    public TableBuilderBase setDescription(String description) {
        this.description = description;
        return this;
    }

    @Override
    public RegionMapper getRegionMapper() {
        return regionMapper;
    }

    @Override
    public void addRegion(String regionName) {
        if (table.isChild()) {
            throw new IllegalStateException("Cannot add region to a child " +
                "table. ");
        }
        if (regionMapper == null) {
            throw new IllegalStateException("Unable to map region name");
        }
        if (regions == null) {
            throw new IllegalArgumentException("Table is not a multi-region" +
                                               " table");
        }
        final int regionId = regionMapper.getRegionId(regionName);
        if (regionId < 0) {
            throw new IllegalArgumentException("Unknown region: " + regionName);
        }
        if (regionId == Region.LOCAL_REGION_ID) {
            throw new IllegalArgumentException("Cannot add the local region");
        }
        regions.add(regionId);
    }

    @Override
    public void dropRegion(String regionName) {
        if (table.isChild()) {
            throw new IllegalStateException("Cannot drop region for a child " +
                "table. ");
        }
        if (regionMapper == null) {
            throw new IllegalStateException("Unable to map region name");
        }
        if (regions == null) {
            throw new IllegalArgumentException("Table is not a multi-region" +
                                               " table");
        }
        final int regionId = regionMapper.getRegionId(regionName);
        if (regionId < 0) {
            throw new IllegalArgumentException("Unknown region: " + regionName);
        }
        if (regionId == Region.LOCAL_REGION_ID) {
            throw new IllegalArgumentException("Cannot drop the local region: " +
                                               regionName);
        }
        if (!regions.remove(regionId)) {
            throw new IllegalArgumentException("Table is not in region: " +
                                               regionName);
        }
    }

    /**
     * Do the evolution.  Reset the fields member to avoid accidental
     * updates to the live version of the table just evolved.  This way
     * this instance can be reused, which is helpful in testing.
     */
    public TableImpl evolveTable() {

        if (hasSetIdentity) {
            table.evolve(fields, ttl, description,
                         getIdentityColumnInfo(), sequenceDef, regions);
        } else {
            if (table.hasIdentityColumn()) {
                String idColName = table.getFieldMap().getFieldName(
                    table.getIdentityColumn());

                if (!fields.exists(idColName)) {
                    /* column with IDENTITY was dropped */
                    table.evolve(fields, ttl, description, null, null, regions);
                } else {
                    /* column with IDENTITY still exists but index might
                       have changed */
                    table.evolve(fields, ttl, description,
                        new IdentityColumnInfo(fields.getFieldPos(idColName),
                            table.getIdentityColumnInfo().isIdentityGeneratedAlways(),
                            table.getIdentityColumnInfo().isIdentityOnNull()),
                                 sequenceDef, regions);
                }
            } else {
                table.evolve(fields, ttl, description, null, null, regions);
            }
        }

        /*
         * Reset the fields member to avoid accidental updates to the
         * live version of the table just evolved.
         */
        fields = fields.clone();
        return table;
    }

    /**
     * Show the current state of the table.
     */
    public String toJsonString(boolean pretty) {

        TableImpl t = table.clone();

        t.evolve(fields, ttl, description,
                getIdentityColumnInfo(), sequenceDef, regions);
        return t.toJsonString(pretty, regionMapper);
    }

    @Override
    void validateFieldAddition(final String fieldName,
                               final String pathName,
                               final FieldMapEntry field) {
        super.validateFieldAddition(fieldName, pathName, field);
        table.validateFieldAddition(pathName, field);
    }

    /**
     * Fields cannot be removed if they are part of the primary
     * key or participate in an index.
     */
    @Override
    void validateFieldRemoval(TablePath tablePath) {
        if (table.isKeyComponent(tablePath.getPathName())) {
            throw new IllegalArgumentException(
                "Cannot remove a primary key field: " +
                tablePath.getPathName());
        }
        if (table.isIndexKeyComponent(tablePath)) {
            throw new IllegalArgumentException(
                "Cannot remove an index key field: " +
                tablePath.getPathName());
        }
    }
}
