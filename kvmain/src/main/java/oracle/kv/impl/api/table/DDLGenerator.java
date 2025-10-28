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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.table.ArrayDef;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.Index;
import oracle.kv.table.JsonDef;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.TimestampDef;

/**
 * Generates the Table Definition and Index Definition DDLs from Table
 */
public class DDLGenerator {

    private final TableImpl table;
    private final StringBuilder tableSb = new StringBuilder();
    private final String tableDDL;
    private final List<String> indexDDLs = new ArrayList<>();
    private final RegionMapper regionMapper;

    /*
     * Indicate if include identity info in generated create-table ddl, this
     * may be useful for migrator before able to handle migrating data for
     * identity column.
     *
     * This flag doesn't affect generating alter table ddl using genAlterDdl()
     * method.
     */
    private final boolean includeIdentityInfo;

    public DDLGenerator(Table table, RegionMapper regionMapper) {
        this(table, false, regionMapper);
    }

    public DDLGenerator(Table table,
                        boolean withIfNotExists,
                        RegionMapper regionMapper) {

        this(table, withIfNotExists, regionMapper,
             true /* includeIdentityInfo */);
    }

    public DDLGenerator(Table table,
                        boolean withIfNotExists,
                        RegionMapper regionMapper,
                        boolean includeIdentityInfo) {
        this.table = (TableImpl) table;
        this.regionMapper = regionMapper;
        this.includeIdentityInfo = includeIdentityInfo;
        tableDDL = generateDDL(withIfNotExists);
        generateAllIndexDDL(withIfNotExists);
    }

    /*
     * Compatibility for the cloud and non-region users
     */
    public DDLGenerator(Table table,
                        boolean withIfNotExists) {
        this(table, withIfNotExists, null);
    }

    /*
     * Used by the cloud, does not support multi-region tables yet
     */
    public DDLGenerator(final String jsonTable, boolean withIfNotExists) {
        this(TableJsonUtils.fromJsonString(jsonTable, null),
             withIfNotExists, null);
    }

    /**
     * For unit tests. Will not handle multi-region tables.
     */
    DDLGenerator(final String jsonTable) {
        this(jsonTable, false);
    }

    /** Retrieve the DDL which describes the table */
    public String getDDL() {
        return tableDDL;
    }

    /** Retrieve the index DDLs */
    public List<String> getAllIndexDDL() {
        return indexDDLs;
    }

    /**
     * Generates alter table ddl(s) to change original table to given new table
     *
     * Alter table ddls returned may contain 4 types of alter table ddl:
     *   - alter table <table> (add|drop|modify <field>,...)
     *   - alter table <table> using ttl <n> <unit>
     *   - alter table <table> add regions <region>,<region>..
     *   - alter table <table> drop regions <region>,<region>..
     */
    public String[] genAlterDdl(Table newTable) {
        /* the name of given table should be same as original table */
        if (!table.getFullNamespaceName()
                 .equalsIgnoreCase(newTable.getFullNamespaceName())) {
            throw new IllegalArgumentException(
                "The specified new table is different table, original=" +
                newTable.getFullNamespaceName() +
                " new=" + table.getFullNamespaceName());
        }

        TableImpl oldT = table;
        TableImpl newT = (TableImpl)newTable;

        /* primary key can't be changed */
        if (!primarykeyEquals(oldT, newT)) {
            throw new IllegalArgumentException(
                "Can't modify primary key fields, original=" +
                formatPrimKey(oldT) + ", new=" + formatPrimKey(newT));
        }

        /* can't alter table from singleton table to multi-region table */
        if (oldT.isMultiRegion() != newT.isMultiRegion()) {
            throw new IllegalArgumentException(
                "The original table is " + (oldT.isMultiRegion() ? "" : "NOT") +
                " multi-region table, the new table must " +
                (oldT.isMultiRegion() ? "" : "NOT") +
                " be multi-region table either");
        }

        final List<String> ddls = new ArrayList<String>();

        /* add | drop | modify field */
        String ddl = getAlterFieldsDDL(newT);
        if (ddl != null) {
            ddls.add(ddl);
        }

        /* set ttl */
        if (!ttlEquals(oldT, newT, true)) {
           ddl = getAlterTtlDDL(newT);
           if (ddl != null) {
               ddls.add(ddl);
           }
        }

        if (!ttlEquals(oldT, newT, false)) {
           ddl = getAlterBeforeImageTtlDDL(newT);
           assert(ddl != null);
           ddls.add(ddl);
        }

        /* add | drop <region>,<region> */
        if (!TableImpl.equalsRemoteRegions(oldT.getRemoteRegions(),
                                           newT.getRemoteRegions())) {
            if (regionMapper == null) {
                throw new IllegalArgumentException(
                        "DDLGenerator for a multi-region table requires a " +
                        "RegionMapper");
            }

            /* add region */
            ddl = getAlterRegions(newT, true /* add */);
            if (ddl != null) {
                ddls.add(ddl);
            }

            /* drop region */
            ddl = getAlterRegions(newT, false /* add */);
            if (ddl != null) {
                ddls.add(ddl);
            }
        }

        return ddls.toArray(new String[ddls.size()]);
    }

    /**
     * Generate the Table DDL definition
     */
    private String generateDDL(boolean withIfNotExists) {

        /* Append the table preamble */
        tableSb.append("CREATE TABLE ");
        if (withIfNotExists) {
            tableSb.append("IF NOT EXISTS ");
        }
        tableSb.append(table.getFullNamespaceName()).append(" (");

        /*
         * List that contains all the parent key fields which needs to be
         * ignored while constructing the DDL. For the top most level table,
         * this list is empty
         */
        List<String> rejectKeyList = new ArrayList<String>();
        Table parentTable = table.getParent();

        if (parentTable != null) {
            rejectKeyList = parentTable.getPrimaryKey();
        }
        FieldMap fieldMap = table.getFieldMap();

        /*
         * Generate the DDL for all the fields of this table
         */
        getAllFieldDDL(true, fieldMap, rejectKeyList);

        tableSb.append(", ");

        /*
         * Generate the DDL of the keys
         */
        if (parentTable != null) {
            getChildTableKeyDDL(rejectKeyList);
        } else {
            getParentTableKeyDDL();
        }

        /* add MR counter declarations if present in a JSON Collection */
        getJsonCollectionCounters();

        tableSb.append(")");

        /*
         * Generate the DDL statement for the TTL if it exists
         */
        TimeToLive defaultTTL = table.getDefaultTTL();
        if (defaultTTL != null) {
            appendTTL(defaultTTL);
        }

        TimeToLive beforeTTL = table.getBeforeImageTTL();
        if (beforeTTL != null) {
            appendBeforeImageTTL(beforeTTL);
        }

        if (table.isMultiRegion() && table.isTop()) {
            if (regionMapper == null) {
                throw new IllegalArgumentException(
                        "DDLGenerator for a multi-region table requires a " +
                        "RegionMapper");
            }
            tableSb.append(" IN REGIONS ");
            tableSb.append(regionMapper.getRegionName(Region.LOCAL_REGION_ID));
            for (int regionId : table.getRemoteRegions()) {
                tableSb.append(", ");
                final String regionName = regionMapper.getRegionName(regionId);
                if (regionName == null) {
                    throw new IllegalArgumentException(
                        "Unable to create DDL, unknown region ID " + regionId);
                }
                tableSb.append(regionName);
            }
        }

        if (table.isJsonCollection()) {
            tableSb.append(" AS JSON COLLECTION");
        }

        return tableSb.toString();
    }


    /**
     * Generate the Index definition DDLs for the table
     */
    private void generateAllIndexDDL(boolean withIfNotExists) {

        Map<String, Index> indexes = table.getIndexes();

        if (indexes.size() != 0) {
            for (Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
                Index index = indexEntry.getValue();
                indexDDLs.add(getIndexDDL(index, withIfNotExists));
            }
        }
    }

    /**
     * Generate the DDL for all the fields of this table
     *
     * @param fieldMap
     * @param rejectKeyList empty for top most level table. For the child tables
     *                      this list contains the primary keys fields of its
     *                      top level parent tables which needs to be ignored
     *                      while building the DDL
     */
    private void getAllFieldDDL(boolean topLevelFields,
                                FieldMap fieldMap,
                                List<String> rejectKeyList) {

        int numFields = fieldMap.size();
        /* handle identity column only during processing top fields */
        boolean hasIdentity = includeIdentityInfo &&
                              topLevelFields &&
                              table.hasIdentityColumn();

        boolean first = true;
        for (int i = 0; i < numFields; i++) {

            FieldMapEntry entry = fieldMap.getFieldMapEntry(i);
            String fname = entry.getFieldName();

            if (rejectKeyList.contains(fname)) {
                continue;
            }

            if (first) {
                first = false;
            } else {
                tableSb.append(", ");
            }

            FieldDef field = entry.getFieldDef();

            getFieldDDL(field, fname);

            if (field.isMRCounter()) {
                /* no addition info for MR_COUNTER type */
                continue;
            }

            if (hasIdentity && i == table.getIdentityColumn()) {
                appendIdentityInfo(table.getIdentityColumnInfo(),
                                   table.getIdentitySequenceDef());
            } else {
                boolean notNull =
                    !entry.isNullable() && !table.isKeyComponent(fname);
                appendFieldInfo(field, notNull, entry.getDefaultValue());
            }
        }
    }

    /**
     * Appends field information including:
     *   - uuid info
     *   - not nullable
     *   - default value
     *   - comments
     */
    private void appendFieldInfo(FieldDef field,
                                 boolean notNull,
                                 FieldValue defValue) {
        /* append UUID */
        if (field.isUUIDString()) {
            tableSb.append(" AS UUID");
            if (((StringDefImpl)field).isGenerated()) {
                tableSb.append(" GENERATED BY DEFAULT");
            }
        }

        if (notNull) {
            tableSb.append(" NOT NULL");
        }

        if (!defValue.isNull() && !field.isMRCounter()) {

            tableSb.append(" DEFAULT ");
            if (defValue.isString()) {
                tableSb.append("\"");
            }
            /* Add ' for default value of timestamp */
            if (field.getType() == Type.TIMESTAMP) {
                tableSb.append("'");
            }
            tableSb.append(defValue.toString());
            if (field.getType() == Type.TIMESTAMP) {
                tableSb.append("'");
            }
            if (defValue.isString()) {
                tableSb.append("\"");
            }
        }

        String description = field.getDescription();

        if (description != null) {
            tableSb.append(" COMMENT \"").append(description)
                .append("\"");
        }
    }

    /**
     * Appends identity column info and its definition
     */
    private void appendIdentityInfo(IdentityColumnInfo info, SequenceDef def) {

        if (info.isIdentityGeneratedAlways()) {
            tableSb.append(" GENERATED ALWAYS");
        } else {
            tableSb.append(" GENERATED BY DEFAULT");
            if (info.isIdentityOnNull()) {
                tableSb.append(" ON NULL");
            }
        }
        tableSb.append(" AS IDENTITY");

        if (def != null) {
            tableSb.append(" (");
            if (def.getStartValue() != null) {
                tableSb.append(" START WITH ")
                       .append(formatValue(def.getStartValue()));
            }

            if (def.getIncrementValue() != null) {
                tableSb.append(" INCREMENT BY ")
                       .append(formatValue(def.getIncrementValue()));
            }

            if (def.getMaxValue() != null) {
                tableSb.append(" MAXVALUE ")
                       .append(formatValue(def.getMaxValue()));
            }

            if (def.getMinValue() != null) {
                tableSb.append(" MINVALUE ")
                       .append(formatValue(def.getMinValue()));
            }

            if (def.getCacheValue() != null) {
                tableSb.append(" CACHE ")
                       .append(formatValue(def.getCacheValue()));
            }

            if (def.getCycle()) {
                tableSb.append(" CYCLE ");
            }
            tableSb.append(")");
        }
    }

    private String formatValue(FieldValueImpl val) {
        if (val.isNumeric()) {
            if (val.isNumber()) {
                Object value = ((NumberValueImpl)val).getNumericValue();
                if (value instanceof BigDecimal) {
                    return ((BigDecimal) value).toPlainString();
                }
                return value.toString();
            }
        }
        return val.toString();
    }

    /**
     * Appends TTL info: USING TTL <n> <unit>
     */
    private void appendTTL(TimeToLive ttl) {
        tableSb.append(" USING TTL ")
               .append(ttl.getValue())
               .append(" ")
               .append(ttl.getUnit().name());
    }

    private void appendBeforeImageTTL(TimeToLive ttl) {

        if (ttl != null) {
            tableSb.append(" ENABLE BEFORE IMAGE USING TTL ").
                    append(ttl.getValue()).
                    append(" ").
                    append(ttl.getUnit().name());
        } else {
            tableSb.append(" DISABLE BEFORE IMAGE");
        }
    }

    /**
     * Generate the DDL for the generic table field.
     *
     * @param field generic FieldDef instance
     * @param fname name of the field
     */
    private void getFieldDDL(FieldDef field, String fname) {

        switch (field.getType()) {
            case STRING:
            case BINARY:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
                /*
                 * Generate the ddl for String, Binary, Boolean, Double
                 * Float type fields
                 */
                getDDL(field, fname);
                break;
            case JSON:
                /*
                 * Generate the ddl for JSON type field
                 */
                getJsonDDL(field.asJson(), fname);
                break;

            case ENUM:
                /*
                 * Generate the ddl for Enum type fields
                 */
                EnumDef enumField = (EnumDef)field;
                getEnumDDL(enumField, fname);
                break;

            case FIXED_BINARY:
                /*
                 * Generate the ddl for fixed binary type fields
                 */
                FixedBinaryDef fixedBinaryField = (FixedBinaryDef)field;
                getFixedBinaryDDL(fixedBinaryField, fname);
                break;

            case INTEGER:
            case LONG:
            case NUMBER:
                /*
                 * Generate the ddl for INTGER, LONG and NUMBER type fields
                 */
                getNumericDDL(field, fname);
                break;

            case ARRAY:
                /*
                 * Generate the ddl for Array type fields
                 */
                ArrayDef arrayField = (ArrayDef)field;
                getArrayDDL(arrayField, fname);
                break;

            case MAP:
                /*
                 * Generate the ddl for Map type fields
                 */
                MapDef mapField = (MapDef)field;
                getMapDDL(mapField, fname);
                break;

            case RECORD:
                /*
                 * Generate the ddl for Record type fields
                 */
                RecordDef recField = (RecordDef)field;
                getRecordDDL(recField, fname);
                break;

            case TIMESTAMP:
                /*
                 * Generate the ddl for Timestamp type fields
                 */
                TimestampDef tsField = (TimestampDef)field;
                getTimestampDDL(tsField, fname);
                break;

            case ANY:
            case ANY_ATOMIC:
            case ANY_RECORD:
            case ANY_JSON_ATOMIC:
            case EMPTY:
            case GEOMETRY:
            case POINT:
            case JSON_INT_MRCOUNTER:
            case JSON_LONG_MRCOUNTER:
            case JSON_NUM_MRCOUNTER:
                /* invalid types for table column */
                throw new IllegalStateException("Unexpected type");
        }
    }

    /**
     * @param index Index instance
     * @return DDL representing the Index
     */
    private String getIndexDDL(Index index, boolean withIfNotExists) {

        StringBuilder sb = new StringBuilder();

        /* Append the index preamble */
        sb.append("CREATE");

        if (index.getType() == Index.IndexType.TEXT) {
            sb.append(" FULLTEXT");
        }

        sb.append(" INDEX ");
        if (withIfNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(index.getName())
            .append(" ON ").append(table.getFullNamespaceName()).append("(");

        /* Append the fields */
        IndexImpl indexImpl = (IndexImpl)index;
        for (int i = 0; i < indexImpl.numFields(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(TableJsonUtils.toExternalIndexField(indexImpl, i, true));

            /*
             * Append the text index field annotation
             */
            if (index.getType() == Index.IndexType.TEXT) {
                String field = indexImpl.getFields().get(i);
                String annotationField = index.getAnnotationForField(field);
                if (annotationField != null) {
                    sb.append(" ").append(annotationField);
                }
            }
        }

        sb.append(")");

        if (index.getType() != Index.IndexType.TEXT) {
            if (!indexImpl.indexesNulls()) {
                sb.append(" WITH NO NULLS");
            }
            if (indexImpl.isUnique()) {
                sb.append(" WITH UNIQUE KEYS PER ROW");
            }
        }

        /*
         * Append the text index properties
         */
        if (index.getType() == Index.IndexType.TEXT) {
            Map<String, String> properties = indexImpl.getProperties();

            for (Map.Entry<String, String> entry : properties.entrySet()) {
                sb.append(" ").append(entry.getKey()).append(" = ")
                    .append(entry.getValue());
            }
        }

        /*
         * Append the index description
         */
        String description = index.getDescription();

        if (description != null) {
            sb.append(" COMMENT \"").append(description)
                .append("\"");
        }

        return sb.toString();
    }

    /**
     * Generates the ddl for String, Binary, Boolean, Double, Float
     * Number and Json type fields
     */
    private void getDDL(FieldDef field, String fname) {

        String type = field.getType().toString();

        if (fname == null) {
            tableSb.append(type);
            return;
        }

        tableSb.append(fname).append(" ").append(type);
    }

    /**
     * Generates the ddl for Enum type fields
     */
    private void getEnumDDL(EnumDef field, String fname) {

        String type = field.getType().toString();

        if (fname == null) {

            /*append enum for maps and arrays*/
            tableSb.append(type).append("(");
        } else {
            tableSb.append(fname).append(" ").append(type).append("(");
        }

        String symbol = null;
        String[] allSymbols = field.getValues();
        int numSymbols = allSymbols.length;

        for (int i = 0; i < numSymbols; i++) {
            symbol = allSymbols[i];

            if (i == numSymbols - 1) {
                break;
            }

            tableSb.append(symbol).append(", ");
        }

        if (symbol != null) {
            tableSb.append(symbol);
        }

        tableSb.append(")");
    }

    /**
     * Generate the ddl for fixed binary type fields
     */
    private void getFixedBinaryDDL(FixedBinaryDef field, String fname) {

        if (fname == null) {
            tableSb.append("BINARY(").append(field.getSize()).append(")");
            return;
        }

        tableSb.append(fname).append(" ").append("BINARY(")
            .append(field.getSize()).append(")");
    }

    /**
     * Generates the ddl for INTEGER, LONG and NUMBER type fields
     */
    private void getNumericDDL(FieldDef field, String fname) {

        getDDL(field, fname);

        if (field.isMRCounter()) {
            tableSb.append(" AS MR_COUNTER");
        }
    }

    /**
     * Generate the ddl for Array type fields
     */
    private void getArrayDDL(ArrayDef field, String fname) {

        FieldDef element = field.getElement();

        if (fname == null) {
            tableSb.append(field.getType().toString()).append("(");

            /*
             * Recursively generate the field DDL of the Array element field
             */
            getFieldDDL(element, null);
            tableSb.append(")");
            return;
        }

        tableSb.append(fname).append(" ").append(field.getType().toString())
            .append("(");

        /*
         * Recursively generate the field DDL of the Array element field
         */
        getFieldDDL(element, null);
        tableSb.append(")");
    }

    /**
     * Generate the ddl for Map type fields
     */
    private void getMapDDL(MapDef field, String fname) {

        FieldDef element = field.getElement();

        if (fname == null) {
            tableSb.append(field.getType().toString()).append("(");

            /*
             * Recursively generate the field DDL of the Array element field
             */
            getFieldDDL(element, null);
            tableSb.append(")");
            return;
        }

        tableSb.append(fname).append(" ").append(field.getType().toString())
            .append("(");

        /*
         * Recursively generate the field DDL of the Array element field
         */
        getFieldDDL(element, null);
        tableSb.append(")");
    }

    /**
     * Generate the ddl for Record type fields
     */
    private void getRecordDDL(RecordDef field, String fname) {

        RecordDefImpl fieldImpl = (RecordDefImpl)field;
        FieldMap fieldMap = fieldImpl.getFieldMap();

        if (fname == null) {
            tableSb.append(field.getType().toString()).append("(");

            /*
             * Recursively generate the field DDLs of the Record fields
             */
            getAllFieldDDL(false, fieldMap, new ArrayList<String>());

            tableSb.append(")");
            return;
        }

        tableSb.append(fname).append(" ").append(field.getType().toString())
            .append("(");

        /*
         * Recursively generate the field DDLs of the Record fields
         */
        getAllFieldDDL(false, fieldMap, new ArrayList<String>());
        tableSb.append(")");
    }

    /**
     * Generate the ddl for timestamp type fields
     */
    private void getTimestampDDL(TimestampDef field, String fname) {

        if (fname == null) {
            tableSb.append("TIMESTAMP(").append(field.getPrecision()).append(")");
            return;
        }

        tableSb.append(fname).append(" ").append("TIMESTAMP(")
            .append(field.getPrecision()).append(")");
    }

    /**
     * Generate ddl for JSON field.
     */
    private void getJsonDDL(JsonDef field, String fname) {

        getDDL(field, fname);

        /* Json MR counters */
        if (field.allMRCounterFields() != null) {
            tableSb.append("(");

            boolean first = true;
            for (Map.Entry<String, Type> e :
                 field.allMRCounterFields().entrySet()) {

                if (first) {
                    first = false;
                } else {
                    tableSb.append(", ");
                }
                tableSb.append(e.getKey()).append(" AS ")
                    .append(e.getValue()).append(" MR_COUNTER");
            }

            tableSb.append(")");
        }
    }

    /**
     * Generate the DDL statement for the Child Table key
     */
    private void getChildTableKeyDDL(List<String> rejectKeyList) {

        tableSb.append("PRIMARY KEY(");
        String nextKey = "";

        /*
         * Table primary key fields
         */
        List<String> primaryKey = table.getPrimaryKey();

        List<Integer> primaryKeySizes = table.getPrimaryKeySizes();
        int keyIndex = rejectKeyList.size();

        int pKeySize = primaryKey.size();

        for (int i = 0; i < pKeySize; i++) {

            nextKey = primaryKey.get(i);

            /*
             * If the field is a part of parent table primary key, do not
             * include it in the child table DDL statement
             */
            if (rejectKeyList.contains(nextKey)) {
                continue;
            }

            if (i == pKeySize - 1) {
                break;
            }

            tableSb.append(nextKey);
            appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
            tableSb.append(", ");
        }

        tableSb.append(nextKey);
        appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
        tableSb.append(")");
    }

    /**
     * Generate the DDL statement for the Parent Table key
     */
    private void getParentTableKeyDDL() {

        /*
         * Table primary key fields
         */
        List<String> primaryKey = table.getPrimaryKey();

        List<Integer> primaryKeySizes = table.getPrimaryKeySizes();
        int keyIndex = 0;

        /*
         * Table shard key fields
         */
        List<String> shardKey = table.getShardKey();
        int sKeySize = shardKey.size();
        Iterator<String> pKeyIterator = primaryKey.iterator();

        tableSb.append("PRIMARY KEY(");

        /*
         * Retrieve the shard fields
         */
        if (sKeySize != 0) {

            tableSb.append("SHARD(");
            String nextKey = "";

            for (int i = 0; i < sKeySize; i++) {

                nextKey = shardKey.get(i);
                pKeyIterator.next();

                if (i == sKeySize - 1) {
                    break;
                }

                tableSb.append(nextKey);
                appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
                tableSb.append(", ");
            }

            tableSb.append(nextKey);
            appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
            tableSb.append(")");

            if (pKeyIterator.hasNext()) {
                tableSb.append(", ");
            }
        }

        String nextKey = "";

        /*
         * Retrieve the primary key fields
         */
        while (pKeyIterator.hasNext()) {

            nextKey = pKeyIterator.next();

            if (!pKeyIterator.hasNext()) {
                break;
            }

            tableSb.append(nextKey);
            appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
            tableSb.append(", ");
        }

        tableSb.append(nextKey);
        appendPrimaryKeySizes(primaryKeySizes, keyIndex++);
        tableSb.append(")");
    }

    /**
     * Append the primary key sizes
     */
    private void appendPrimaryKeySizes(List<Integer> primaryKeySizes,
                                       int keyIndex) {

        if (primaryKeySizes != null && keyIndex != primaryKeySizes.size()) {
            Integer pKeySize = primaryKeySizes.get(keyIndex);

            if (pKeySize != 0) {
                tableSb.append("(")
                       .append(pKeySize.toString())
                       .append(")");
            }
        }
    }

    /*
	 * Appends add | drop | modify field
	 * Note: modify field only support modify identity attributes
	 */
    private String getAlterFieldsDDL(TableImpl newT) {
        TableImpl oldT = table;

        tableSb.setLength(0);

        /* add_field_statement | drop_field_statement */
        walkFields(null, oldT.getFieldMap(), newT.getFieldMap(), newT);

        /* modify_field_statement */
        appendModifyIdentity(newT);

        if (tableSb.length() > 0) {
            return "ALTER TABLE " + table.getFullNamespaceName() +
                   "(" + tableSb.toString() + ")";
        }
        return null;
    }

    /**
     * Walk fields in original table and new table, appends
     *   1. add <field information> if field exists in new table only.
     *   2. drop field if field exists in original table only.
     *   3. throw IAE if attempt to change type of field.
     */
    private void walkFields(String fpath,
                            FieldMap oldFlds,
                            FieldMap newFlds,
                            TableImpl newT) {

        String idField = null;
        if (newT != null) {
            int idx = newT.getIdentityColumn();
            if (idx >= 0) {
                idField = newT.getFields().get(idx);
            }
        }

        List<String> skipFlds = (newT != null ? newT.getPrimaryKey() : null);

        /* append add <field info> */
        for (String name : newFlds.getFieldNames()) {
            if (skipFlds != null && skipFlds.contains(name)) {
                continue;
            }

            FieldMapEntry newE = newFlds.getFieldMapEntry(name);
            if (!oldFlds.exists(name)) {
                /* add field */
                if (tableSb.length() > 0) {
                    tableSb.append(", ");
                }
                appendAddField(newE, makeFullPath(fpath, name));

                /* append identity info */
                if (newT != null && idField != null && name.equals(idField)) {
                    appendIdentityInfo(newT.getIdentityColumnInfo(),
                                       newT.getIdentitySequenceDef());
                }
            } else {
                walkField(makeFullPath(fpath, name),
                          oldFlds.getFieldDef(name),
                          newE.getFieldDef());
            }
        }

        /* append drop <field> */
        for (String name : oldFlds.getFieldNames()) {
            if (skipFlds != null && skipFlds.contains(name)) {
                continue;
            }
            if (!newFlds.exists(name)) {
                if (tableSb.length() > 0) {
                    tableSb.append(", ");
                }
                tableSb.append("DROP ").append(makeFullPath(fpath, name));
            }
        }
    }

    private void walkField(String fpath,
                           FieldDefImpl oldF,
                           FieldDefImpl newF) {
        if (oldF.isComplex() && newF.isComplex()) {
            if (oldF.getType() != newF.getType()) {
                throw new IllegalArgumentException(
                        "Can't modify the type of field '" + fpath +
                        "', original=" + oldF.getType() +
                        ", new=" + newF.getType());
            }
        } else {
            if (!oldF.equals(newF)) {
                throw new IllegalArgumentException(
                        "Can't modify the type of field '" + fpath +
                        "', original=" + oldF.toJsonString(false) +
                        ", new=" + newF.toJsonString(false));
            }
        }

        switch (oldF.getType()) {
        case RECORD:
            walkFields(fpath,
                       ((RecordDefImpl)oldF).getFieldMap(),
                       ((RecordDefImpl)newF).getFieldMap(),
                       null /* tableImpl */);
            break;
        case ARRAY:
            walkField(makeFullPath(fpath, TableImpl.BRACKETS),
                      (FieldDefImpl)oldF.asArray().getElement(),
                      (FieldDefImpl)newF.asArray().getElement());
            break;
        case MAP:
            walkField(makeFullPath(fpath, TableImpl.VALUES),
                      (FieldDefImpl)oldF.asMap().getElement(),
                      (FieldDefImpl)newF.asMap().getElement());
            break;
        default:
            break;
        }
    }

    private void appendAddField(FieldMapEntry entry, String fname) {
        FieldDefImpl fdef = entry.getFieldDef();

        tableSb.append("ADD ");
        getFieldDDL(fdef, fname);
        if (!fdef.isComplex()) {
            appendFieldInfo(fdef, !entry.isNullable(), entry.getDefaultValue());
        }
    }

    /*
     * If there is a change on identity column, build the ddl:
     *   1. Both old and new table have identity column but some
     *      information changed:
     *        MODIFY <field> <identity info of new table>
     *   2. Old table has identity column but new table hasn't:
     *        MODIFY <field> DROP IDENTITY
     */
    private void appendModifyIdentity(TableImpl newT) {

        if (!isIdentityModified(newT)) {
            return;
        }

        if (tableSb.length() > 0) {
            tableSb.append(", ");
        }

        String field;
        if (newT.hasIdentityColumn()) {
            /* modify identity */
            field = newT.getFields().get(newT.getIdentityColumn());
            tableSb.append("MODIFY ").append(field);
            appendIdentityInfo(newT.getIdentityColumnInfo(),
                               newT.getIdentitySequenceDef());
        } else {
            TableImpl oldT = table;
            assert(oldT.hasIdentityColumn());

            /* drop identity */
            field = oldT.getFields().get(oldT.getIdentityColumn());
            tableSb.append("MODIFY ")
                   .append(field)
                   .append(" DROP IDENTITY");
        }
    }

    /* Returns true if identity column is modified or dropped. */
    private boolean isIdentityModified(TableImpl newT) {

        TableImpl oldT = table;

        if (identityEquals(oldT, newT)) {
            /* no change to identity column */
            return false;
        }

        if (newT.hasIdentityColumn()) {
            String field = newT.getFields().get(newT.getIdentityColumn());
            if (oldT.getField(field) == null) {
                /*
                 * The identity column is new added, the identity info has
                 * been included in "add <field info>".
                 */
                return false;
            }
        }

        return true;
    }

    /* Returns set ttl ddl: alter table using ttl <n> units */
    private String getAlterTtlDDL(TableImpl newT) {
        TimeToLive ottl = table.getDefaultTTL();
        TimeToLive ttl = newT.getDefaultTTL();

        if (ttl == null && (ottl != null && ottl.getValue() > 0)) {
            ttl = TimeToLive.DO_NOT_EXPIRE;
        }

        if (ttl != null) {
            tableSb.setLength(0);
            appendAlterTablePerfix();
            appendTTL(ttl);
            return tableSb.toString();
        }
        return null;
    }

    private String getAlterBeforeImageTtlDDL(TableImpl newT) {

        tableSb.setLength(0);
        appendAlterTablePerfix();
        appendBeforeImageTTL(newT.getBeforeImageTTL());
        return tableSb.toString();
    }

    /*
     * Returns modify region ddl:
	 *   alter table [add | drop] <region>,<region>
	 */
    private String getAlterRegions(TableImpl newT, boolean add) {
        Set<Integer> regions = new HashSet<Integer>();
        TableImpl oldT = table;
        if (add) {
            for (Integer rgid : newT.getRemoteRegions()) {
                if (!oldT.getRemoteRegions().contains(rgid)) {
                    regions.add(rgid);
                }
            }
        } else {
            for (Integer rgid : oldT.getRemoteRegions()) {
                if (!newT.getRemoteRegions().contains(rgid)) {
                    regions.add(rgid);
                }
            }
        }

        if (regions.isEmpty()) {
            return null;
        }

        tableSb.setLength(0);
        appendAlterTablePerfix();
        appendModifyRegions(regions, add);
        return tableSb.toString();
    }

    private void appendModifyRegions(Set<Integer> regions, boolean toAdd) {
        tableSb.append((toAdd ? " ADD REGIONS " : " DROP REGIONS "));
        boolean first = false;
        for (Integer rgid : regions) {
            if (!first) {
                first = true;
            } else {
                tableSb.append(", ");
            }
            String region = regionMapper.getRegionName(rgid);
            if (region == null) {
                throw new IllegalArgumentException(
                    "Invalid region id: " + rgid);
            }
            tableSb.append(regionMapper.getRegionName(rgid));
        }
    }

    private void appendAlterTablePerfix() {
        tableSb.append("ALTER TABLE ")
               .append(table.getFullNamespaceName());
    }

    /* add MR counter declarations if present in a JSON Collection */
    private void  getJsonCollectionCounters() {
        if (!table.hasJsonCollectionMRCounters()) {
            return;
        }
        for (Map.Entry<String, FieldDef.Type> entry :
                 table.getJsonCollectionMRCounters().entrySet()) {
            tableSb.append(", ").append(entry.getKey()).append(" AS ");
            tableSb.append(entry.getValue()).append(" MR_COUNTER");
        }
    }

    private static String makeFullPath(String path, String name) {
        if (path == null) {
            return name;
        }
        if (name.equals(TableImpl.BRACKETS)) {
            return path + name;
        }
        return path + NameUtils.CHILD_SEPARATOR + name;
    }

    private static boolean primarykeyEquals(TableImpl t1, TableImpl t2) {
        List<String> pkeys = t1.getPrimaryKey();
        List<String> newKeys = t2.getPrimaryKey();

        if (pkeys.size() != newKeys.size() ||
            t1.getShardKeySize() != t2.getShardKeySize()) {
            return false;
        }

        int i = 0;
        for (String field : pkeys) {
            if (!field.equalsIgnoreCase(newKeys.get(i++)) ||
                !t1.getField(field).equals(t2.getField(field))) {
                return false;
            }
        }
        return true;
    }

    private static String formatPrimKey(TableImpl t) {
        StringBuilder sb = new StringBuilder("[");

        int numSKey = t.getShardKeySize();
        sb.append("SHARD(");
        int i = 0;
        for (String f : t.getPrimaryKey()) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(f).append("<")
              .append(t.getField(f).getType()).append(">");
            if ((++i) == numSKey) {
                sb.append(")");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private static boolean identityEquals(TableImpl t0, TableImpl t1) {

        if (!t0.hasIdentityColumn() && !t1.hasIdentityColumn()) {
            /* no identity column in both old and new tables */
            return true;
        }

        return TableImpl.equalsIdentityInfo(t0.getIdentityColumnInfo(),
                                            t1.getIdentityColumnInfo()) &&
                sequenceDefEquals(t0.getIdentitySequenceDef(),
                                  t1.getIdentitySequenceDef());
    }

    private static boolean sequenceDefEquals(SequenceDef def0,
                                             SequenceDef def1) {
        if (def0 != null) {
            return def0.equals(def1);
        }
        return (def1 == null);
    }

    private static boolean ttlEquals(
        TableImpl t0,
        TableImpl t1,
        boolean tableTTL) {

        TimeToLive ttl0 = (tableTTL ? t0.getDefaultTTL() : t0.getBeforeImageTTL());
        TimeToLive ttl1 = (tableTTL ? t1.getDefaultTTL() : t1.getBeforeImageTTL());
        if (isTtlNotExpire(ttl0) && isTtlNotExpire(ttl1)) {
            return true;
        }
        return TableImpl.equalsTTL(ttl0, ttl1);
    }

    private static boolean isTtlNotExpire(TimeToLive ttl) {
        return ttl == null || ttl.equals(TimeToLive.DO_NOT_EXPIRE);
    }
}
