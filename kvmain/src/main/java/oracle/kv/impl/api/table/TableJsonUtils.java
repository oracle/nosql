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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.api.table.serialize.DecoderFactory;
import oracle.kv.impl.query.compiler.Translator.IdentityDefHelper;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.MapValue;
import oracle.kv.table.TimeToLive;
import oracle.nosql.common.json.JsonUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

/**
 * This class provides utilities for interaction with JSON processing
 * libraries, as well as helpful JSON operations, for use in implementing
 * Tables.
 */
/* the deprecation warnings are in the use of Jackson to create JsonFactory */
@SuppressWarnings("deprecation")
public class TableJsonUtils {

    /*
     * There are a number of string constants used for JSON input/output
     * as well as for generating proper Avro schema names.  These are all
     * defined here for simplicity.
     */

    /*
     * Tables JSON input/output format labels
     */
    final static String PARENT = "parent";
    final static String OWNER = "owner";
    final static String SHARDKEY = "shardKey";
    final static String PRIMARYKEY = "primaryKey";
    final static String CHILDREN = "children";
    final static String FIELDS = "fields";
    final static String JSON_VERSION = "json_version";
    final static String R2COMPAT = "r2compat";
    final static String CHILDTABLES = "childTables";
    final static String JSON_COLLECTION = "jsonCollection";
    final static String SYSTABLE = "sysTable";
    final static String DESC = "comment";
    final static String NULLABLE = "nullable";
    final static String MIN = "min";
    final static String MAX = "max";
    final static String MIN_INCL = "min_inclusive";
    final static String MAX_INCL = "max_inclusive";
    final static String COLLECTION = "collection";
    final static String TABLE_ID = "id";
    final static String TTL = "ttl";
    final static String PKEY_SIZES = "primaryKeySizes";
    final static String REGIONS = "regions";
    /* related to indexes */
    final static String INDEXES = "indexes";
    final static String ANNOTATIONS = "annotations";
    final static String PROPERTIES = "properties";
    final static String TABLE = "table";
    final static String WITH_NO_NULLS = "withNoNulls";
    final static String UNIQUE = "withUniqueKeysPerRow";

    /* related to limits */
    final static String LIMITS = "limits";
    final static String READ_LIMIT = "readLimit";
    final static String WRITE_LIMIT = "writeLimit";
    final static String SIZE_LIMIT = "sizeLimit";
    final static String INDEX_LIMIT = "indexLimit";
    final static String INDEX_KEY_SIZE_LIMIT = "indexKeySizeLimit";
    final static String CHILD_TABLE_LIMIT = "childTableLimit";

    /*
     * These are used for construction of JSON nodes representing FieldDef
     * instances.  Some are used for both tables and Avro schemas.
     *
     * Avro and Tables
     */
    final static String NAME = "name";
    final static String NAMESPACE = "namespace";
    final static String TYPE = "type";
    final static String TYPES = "types";
    final static String DEFAULT = "default";
    final static String ENUM_VALS = "symbols";
    final static String NULL = "null";
    final static String IDENTITY = "identity";
    final static String ALWAYS = "always";
    final static String SEQUENCE = "sequence";
    final static String START = "start";
    final static String INCREMENT = "increment";
    final static String CACHE = "cache";
    final static String CYCLE = "cycle";


    /*
     * Avro type strings
     */
    final static String RECORD = "record";
    final static String ENUM = "enum";
    final static String ARRAY = "array";
    final static String MAP = "map";
    final static String INT = "int";
    final static String LONG = "long";
    final static String STRING = "string";
    final static String BOOLEAN = "boolean";
    final static String DOUBLE = "double";
    final static String FLOAT = "float";
    final static String BYTES = "bytes";
    final static String FIXED = "fixed";
    final static String FIXED_SIZE = "size";
    final static String TIMESTAMP = "timestamp";
    final static String TIMESTAMP_PRECISION = "precision";
    final static String MRCOUNTER = "mrcounter";
    final static String ASUUID = "as uuid";
    final static String GENERATED = "generated";
    final static String MRCOUNTERS = "mrcounters";

    private static final DecoderFactory decoderFactory =
        DecoderFactory.get();

    static DecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    static private JsonFactory jsonFactory;

    /*
     * common code to create a JSON parser
     */
    static {
                /*
         * Enable some non-default features:
         *  - allow leading 0 in numerics
         *  - allow non-numeric values such as INF, -INF, NaN for float and
         *    double
         *
         * Disables:
         *  - The "NaN" ("not a number", that is, not real number) float/double
         *    values are output as quoted strings.
         */
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS,
                              true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS,
                              true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS,
                              false);
    }

    public static JsonParser createJsonParser(InputStream in)
        throws IOException {
        return jsonFactory.createParser(in);
    }

    public static JsonParser createJsonParser(Reader in)
        throws IOException {
        return jsonFactory.createParser(in);
    }

    /**
     * Translate the specified Base64 string into a byte array.
     */
    public static String encodeBase64(byte[] buf) {
        return JsonUtils.encodeBase64(buf);
    }

    /**
     * Decode the specified Base64 string into a byte array.
     */
    public static byte[] decodeBase64(String str) {
        return JsonUtils.decodeBase64(str);
    }

    /*
     * From here down are utility methods used to help construct tables
     * and fields from JSON.
     */

    /**
     * This is a generic method used to construct FieldDef objects from
     * the JSON representation of a table.  This code could be spread out
     * across the various classes in per-class fromJson() methods but there
     * is no particular advantage in doing so.  Code is better shared in one
     */
    static FieldDefImpl fromJson(MapValue node) {
        String nameString = getStringFromNode(node, NAME, false);
        String descString = getStringFromNode(node, DESC, false);
        String minString = getStringFromNode(node, MIN, false);
        String maxString = getStringFromNode(node, MAX, false);
        String sizeString = getStringFromNode(node, FIXED_SIZE, false);
        String typeString = getStringFromNode(node, TYPE, true);
        String precisionString = getStringFromNode(node, TIMESTAMP_PRECISION,
                                                   false);
        String mrcounterString = getStringFromNode(node, MRCOUNTER,
                                                   false);
        String asuuidString = getStringFromNode(node, ASUUID, false);
        String generatedString = getStringFromNode(node, GENERATED, false);

        Map<String, Type> jsonMRCounters = null;
        if (node.get(MRCOUNTERS) != null) {
            jsonMRCounters = new HashMap<>();
            MapValue counterMap = node.get(MRCOUNTERS).asMap();
            for (String s : counterMap.getFields().keySet()) {
                Type type = Type.valueOf(counterMap.getFields().
                                         get(s).asString().get());
                jsonMRCounters.put(s, type);
            }
        }

        FieldDef.Type type = FieldDef.Type.valueOf(typeString.toUpperCase());
        boolean mrcounter = Boolean.parseBoolean(mrcounterString);
        boolean asuuid = Boolean.parseBoolean(asuuidString);
        boolean generated = Boolean.parseBoolean(generatedString);

        switch (type) {
        case INTEGER:
            if (mrcounter) {
                return new IntegerDefImpl(mrcounter, descString);
            }
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.Constants.integerDef;
            }
            return new IntegerDefImpl
                (descString,
                minString != null ? Integer.valueOf(minString) : null,
                maxString != null ? Integer.valueOf(maxString) : null);
        case LONG:
            if (mrcounter) {
                return new LongDefImpl(mrcounter, descString);
            }
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.Constants.longDef;
            }
            return new LongDefImpl
                (descString,
                 minString != null ? Long.valueOf(minString) : null,
                 maxString != null ? Long.valueOf(maxString) : null);
        case DOUBLE:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.Constants.doubleDef;
            }
            return new DoubleDefImpl
                (descString,
                 minString != null ? Double.valueOf(minString) : null,
                 maxString != null ? Double.valueOf(maxString) : null);
        case FLOAT:
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.Constants.floatDef;
            }
            return new FloatDefImpl
                (descString,
                 minString != null ? Float.valueOf(minString) : null,
                 maxString != null ? Float.valueOf(maxString) : null);
        case STRING:
            if (asuuid) {
                if (generated) {
                    return FieldDefImpl.Constants.defaultUuidStrDef;
                }
                return FieldDefImpl.Constants.uuidStringDef;
            }
            if (descString == null && minString == null && maxString == null) {
                return FieldDefImpl.Constants.stringDef;
            }
            Boolean minInclusive = getBooleanFromMap(node, MIN_INCL);
            Boolean maxInclusive = getBooleanFromMap(node, MAX_INCL);
            return new StringDefImpl
                (descString, minString, maxString, minInclusive, maxInclusive);
        case NUMBER:
            if (mrcounter) {
                return new NumberDefImpl(mrcounter, descString);
            }
            if (descString == null) {
                return FieldDefImpl.Constants.numberDef;
            }
            return new NumberDefImpl(descString);
        case BINARY:
            if (descString == null) {
                return FieldDefImpl.Constants.binaryDef;
            }
            return new BinaryDefImpl(descString);
        case FIXED_BINARY:
            int size = (sizeString == null ? 0 : Integer.valueOf(sizeString));
            return new FixedBinaryDefImpl(size, descString);
        case BOOLEAN:
            if (descString == null) {
                return FieldDefImpl.Constants.booleanDef;
            }
            return new BooleanDefImpl(descString);
        case TIMESTAMP:
            int precision = (precisionString == null ?
                             TimestampDefImpl.DEF_PRECISION :
                             Integer.valueOf(precisionString));
            return new TimestampDefImpl(precision, descString);
        case ARRAY:
        case MAP:
            FieldValue val = node.get(COLLECTION);
            if (val == null) {
                throw new IllegalArgumentException
                    ("Map and Array require a collection object");
            }
            FieldDefImpl elementDef = fromJson(val.asMap());
            if (type == FieldDef.Type.ARRAY) {
                return FieldDefFactory.createArrayDef(elementDef, descString);
            }
            return  FieldDefFactory.createMapDef(elementDef, descString);
        case RECORD:
            val = node.get(FIELDS);
            if (val == null) {
                throw new IllegalArgumentException
                    ("Record is missing fields object");
            }
            final RecordBuilder builder =
                TableBuilder.createRecordBuilder(nameString, descString);
            ArrayValue arrayNode = val.asArray();
            for (int i = 0; i < arrayNode.size(); i++) {
                MapValue o = arrayNode.get(i).asMap();
                String fieldName = getStringFromNode(o, NAME, true);
                builder.fromJson(fieldName, o);
            }
            try {
                return (FieldDefImpl) builder.build();
            } catch (Exception e) {
                throw new IllegalArgumentException
                ("Failed to build record from JSON, field name: " + nameString );
            }
        case ENUM: {
            FieldValue valuesNode = node.get(ENUM_VALS);
            if (valuesNode == null) {
                throw new IllegalArgumentException
                    ("Enumeration is missing values");
            }
            arrayNode = valuesNode.asArray();
            String values[] = new String[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                values[i] = arrayNode.get(i).asString().get();
            }
            return new EnumDefImpl(values, descString);
        }
        case JSON:
            if (jsonMRCounters != null) {
                return new JsonDefImpl(jsonMRCounters, descString);
            }
            if (descString == null) {
                return FieldDefImpl.Constants.jsonDef;
            }
            return new JsonDefImpl(descString);
        case ANY:
            return FieldDefImpl.Constants.anyDef;
        case ANY_ATOMIC:
            return FieldDefImpl.Constants.anyAtomicDef;
        case ANY_RECORD:
            return FieldDefImpl.Constants.anyRecordDef;
        case ANY_JSON_ATOMIC:
            return FieldDefImpl.Constants.anyJsonAtomicDef;
        case EMPTY:
        default:
            throw new IllegalArgumentException
                ("Cannot construct FieldDef type from JSON: " + type);
        }
    }

    /**
     * Adds an index definition to the table.
     */
    static void indexFromNode(MapValue node, TableImpl table) {

        ArrayValue fields = node.get(FIELDS).asArray();
        ArrayList<String> fieldStrings = new ArrayList<String>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fieldStrings.add(fields.get(i).asString().get());
        }

        ArrayList<FieldDef.Type> typeStrings = null;
        if (node.get(TYPES) != null) {
            ArrayValue types = node.get(TYPES).asArray();
            typeStrings = new ArrayList<FieldDef.Type>(types.size());
            for (int i = 0; i < fields.size(); i++) {
                FieldValue typeNode = types.get(i);
                if (typeNode.isJsonNull()) {
                    typeStrings.add(null);
                } else {
                    typeStrings.add(FieldDef.Type.valueOf(
                                        typeNode.asString().get()));
                }
            }
        }

        String name = getStringFromNode(node, NAME, true);
        String desc = getStringFromNode(node, DESC, false);

        boolean indexNulls = true;
        boolean isUnique = false;

        Boolean bool = getBooleanFromMap(node, WITH_NO_NULLS);
        if (bool != null) {
            indexNulls = !bool.booleanValue();
        }
        bool = getBooleanFromMap(node, UNIQUE);
        if (bool != null) {
            isUnique = bool.booleanValue();
        }

        Map<String,String> annotations = getMapFromNode(node, ANNOTATIONS);
        Map<String,String> properties = getMapFromNode(node, PROPERTIES);
        table.addIndex(new IndexImpl(name, table, fieldStrings, typeStrings,
                                     indexNulls, isUnique,
                                     annotations, properties, desc));
    }

    /**
     * Build Table from JSON string
     *

     * NOTE: this format was test-only in R3, but export/import has made use
     * of it for R4. This means that changes must be made carefully and if
     * state is added to a table or index it needs to be reflected in the JSON
     * format.
     */
    public static TableImpl fromJsonString(String jsonString,
                                           TableImpl parent) {

        MapValue rootNode =
            (MapValue) FieldValueFactory.createValueFromJson(jsonString);
        return fromJson(rootNode, parent);
    }

    private static TableImpl fromJson(MapValue rootNode,
                                      TableImpl parent) {
        /*
         * Create a TableBuilder for the table.
         */
        final String namespace = getStringFromNode(rootNode, NAMESPACE, false);

        TableBuilder tb = null;
        String tname = getStringFromNode(rootNode, NAME, true);
        if (rootNode.get(SYSTABLE) != null) {
            tb =
                TableBuilder.createSystemTableBuilder(tname);
        } else {
            tb = TableBuilder.createTableBuilder
                (namespace,
                 tname,
                 null, /* handle description below */
                 parent, true, null /*regionMapper*/);
        }

        /*
         * Create the primary key and shard key lists
         */
        tb.primaryKey(makeListFromArray(rootNode, PRIMARYKEY));
        if (parent == null) {
            tb.shardKey(makeListFromArray(rootNode, SHARDKEY));
        }

        tb.setDescription(getStringFromNode(rootNode, DESC, false));

        /*
         * The current version doesn't put this field if there is no owner, but
         * older versions may have put null. Handle that.
         */
        if (rootNode.get(OWNER) != null &&
            !rootNode.get(OWNER).isJsonNull()) {
            tb.setOwner(ResourceOwner.fromString(
                            getStringFromNode(rootNode, OWNER, true)));
        }

        if (rootNode.get(TTL) != null) {
            String ttlString = getStringFromNode(rootNode, TTL, true);
            String[] ttlArray = ttlString.split(" ");
            if (ttlArray.length != 2) {
                throw new IllegalArgumentException(
                    "Invalid value for ttl string: " + ttlString);
            }
            tb.setDefaultTTL(TimeToLive.createTimeToLive(
                                 Long.parseLong(ttlArray[0]),
                                 TimeUnit.valueOf(ttlArray[1])));
        }

        if (rootNode.get(PKEY_SIZES) != null) {
            ArrayValue pks = rootNode.get(PKEY_SIZES).asArray();
            List<String> pkey = tb.getPrimaryKey();
            assert pks.size() == pkey.size();
            for (int i = 0; i < pks.size(); i++) {
                int size = pks.get(i).asInteger().get();
                if (size > 0) {
                    tb.primaryKeySize(pkey.get(i), size);
                }
            }
        }

        /* JSON Collection ? */
        if (rootNode.get(JSON_COLLECTION) != null &&
            rootNode.get(JSON_COLLECTION).asBoolean().get()) {
            tb.setJsonCollection();
        }

        /*
         * JSON Collection MR counters. Format is:
         *  "mrcounters" : {
         *    "path" : <type>
         *    ...
         *  }
         * type is the string value of the FieldDef.Type enum
         */
        if (rootNode.get(MRCOUNTERS) != null) {
            MapValueImpl mrMap =
                (MapValueImpl) rootNode.get(MRCOUNTERS).asMap();
            HashMap<String, FieldDef.Type> counters =
                new HashMap<String, FieldDef.Type>();
            for (Map.Entry<String, FieldValue> entry :
                     mrMap.getMap().entrySet()) {
                String counterPath = entry.getKey();
                FieldDef.Type type =
                    FieldDef.Type.valueOf(entry.getValue().asString().get());
                counters.put(counterPath, type);
            }
            tb.setMRCounters(counters);
        }

        /*
         * Add fields.
         */
        ArrayValue arrayNode = rootNode.get(FIELDS).asArray();
        for (int i = 0; i < arrayNode.size(); i++) {
            MapValue node = arrayNode.get(i).asMap();
            String fieldName =
                getStringFromNode(node, NAME, true);
            if (parent == null ||
                !(parent).isKeyComponent(fieldName)) {
                tb.fromJson(fieldName, node);
            }
        }

        /*
         * Add regions for top level tables.
         */
        if (rootNode.get(REGIONS) != null && parent == null) {
            MapValue regions = rootNode.get(REGIONS).asMap();
            for (String s : regions.getFields().keySet()) {
                int regionId = Integer.parseInt(s);
                if (regionId !=  Region.LOCAL_REGION_ID) {
                    tb.addRegion(regionId);
                }
            }
        }

        /*
         * Add identity
         *
         * "identity" : {
         *   "name" : <string>,
         *   "always" : <boolean>,
         *   "null" : <boolean>,
         *   "sequence" : {
         *     "start" : <num>,
         *     "increment" : <num>,
         *     "min" : <num>,
         *     "max" : <num>,
         *     "cache" : <num>,
         *     "cycle" : <boolean>
         *     }
         *  }
         */
        if (rootNode.get(IDENTITY) != null) {
            MapValue identity = rootNode.get(IDENTITY).asMap();
            String name = identity.get(NAME).asString().get();
            boolean isAlways = identity.get(ALWAYS).asBoolean().get();
            boolean isOnNull = identity.get(NULL).asBoolean().get();

            FieldDefImpl fdef = (FieldDefImpl)tb.getField(name);
            FieldValue seqVal = identity.get(SEQUENCE);

            IdentityDefHelper idh = new IdentityDefHelper();
            FieldValue fval;
            if (seqVal != null) {
                MapValue seq = seqVal.asMap();
                idh.setStart(seq.get(START).toString());
                idh.setIncrement(seq.get(INCREMENT).toString());
                fval = seq.get(MAX);
                if (fval != null) {
                    idh.setMax(fval.toString());
                }
                fval = seq.get(MIN);
                if (fval != null) {
                    idh.setMin(fval.toString());
                }
                idh.setCache(seq.get(CACHE).toString());
                idh.setCycle(seq.get(CYCLE).asBoolean().get());
            }
            tb.setIdentity(name, isAlways, isOnNull,
                           new SequenceDefImpl(fdef, idh));
        }

        TableImpl newTable = tb.buildTable();

        /*
         * Add indexes if present
         */
        if (rootNode.get(INDEXES) != null) {
            arrayNode = rootNode.get(INDEXES).asArray();
            for (int i = 0; i < arrayNode.size(); i++) {
                MapValue map = arrayNode.get(i).asMap();
                indexFromNode(map, newTable);
            }
        }

        /*
         * If child tables are fully specified here, add them. Note that
         * the JSON output format may include a simple list of child table
         * names. If that is the case there is nothing that can be done on
         * input because the child tables cannot be constructed from that
         * list, so they are ignored.
         */
        if (rootNode.get(CHILDTABLES) != null) {
            arrayNode = rootNode.get(CHILDTABLES).asArray();
            for (int i = 0; i < arrayNode.size(); i++) {
                MapValue map = arrayNode.get(i).asMap();
                TableImpl child = fromJson(map, newTable);
                newTable.addChild(child);
            }
        }

        return newTable;
    }

    public static BigDecimal jsonParserGetDecimalValue(JsonParser parser)
        throws IOException {

        assert(parser != null);

        try {
            return parser.getDecimalValue();
        } catch (NumberFormatException nfe) {
            throw new JsonParseException(parser, "Malformed numeric value: '" +
                parser.getText(), parser.getCurrentLocation(), nfe);
        }
    }

    private static List<String> makeListFromArray(MapValue node,
                                                  String fieldName) {
        FieldValue valNode = node.get(fieldName);
        if (valNode == null) {
            return null;
        }
        ArrayValue arrayNode = valNode.asArray();
        ArrayList<String> keyList = new ArrayList<>(arrayNode.size());
        for (int i = 0; i < arrayNode.size(); i++) {
            keyList.add(i, arrayNode.get(i).asString().get());
        }
        return keyList;
    }

    /**
     * Returns the string value of the named field in the MapValue
     * if it exists, otherwise null.
     * @param node the containing node
     * @param name the name of the field in the node
     * @param required true if the field must exist
     * @return the string value of the field, or null
     * @throws IllegalArgumentException if the named field does not
     * exist in the node and required is true
     */
    private static String getStringFromNode(MapValue node,
                                            String name,
                                            boolean required) {
        FieldValue val = node.get(name);
        if (val != null) {
            return ((FieldValueImpl)val).castAsString();
        } else if (required) {
            throw new IllegalArgumentException
                ("Missing required field in JSON table representation: " +
                 name);
        }
        return null;
    }

    static Boolean getBooleanFromMap(MapValue map, String field) {
        final FieldValue fieldNode = map.get(field);
        if ((fieldNode == null) || !fieldNode.isBoolean()) {
            return null;
        }
        return Boolean.valueOf(fieldNode.asBoolean().get());
    }

    /**
     * Returns a Map<String, String> of the named field in the MapValue
     * if it exists, otherwise null.
     * @param node the containing node
     * @param name the name of the field in the node
     * @return a map of the name/value pairs in the object, or null
     * @throws IllegalArgumentException if the node exists and it's not a
     * MapValue
     */
    private static Map<String,String> getMapFromNode(MapValue node,
                                                     String name) {
        FieldValue val = node.get(name);
        if (val == null) {
            return null;
        }
        if (!val.isMap()) {
            throw new IllegalArgumentException("Field is not a Map: " +
                                               name);
        }
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<String, FieldValue> entry :
                 val.asMap().getFields().entrySet()) {
            map.put(entry.getKey(),
                    (entry.getValue().isJsonNull() ?
                     null : entry.getValue().asString().get()));
        }
        return map;
    }
}
