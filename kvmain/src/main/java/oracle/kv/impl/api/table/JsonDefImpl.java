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

import static oracle.kv.impl.api.table.TableJsonUtils.jsonParserGetDecimalValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.impl.api.table.TablePath.StepInfo;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.JsonDef;
import oracle.kv.table.LongValue;
import oracle.kv.table.NumberValue;
import oracle.kv.table.StringValue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * JsonDefImpl implements the JsonDef interface.
 */
public class JsonDefImpl extends FieldDefImpl implements JsonDef {

    private static final long serialVersionUID = 1L;
    private static final char DELIMITER = '.';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';

    /*
     * Each MRCounter path -> CRDT type.
     */
    private final Map<String, Type> mrcounterFields;

    /*
     * The steps of each MRCounter field path. Quote and escape are all
     * removed in each step. These are actual field names used in Avro and NSON.
     */
    private transient List<String[]> mrcounterSteps;

    public static TestHook<FieldValueImpl> JSON_MRCOUNTER_TESTHOOK;

    JsonDefImpl(String description) {
        this(null, description);
    }

    JsonDefImpl() {
        this((String)null);
    }

    JsonDefImpl(Map<String, Type> mrcounterMap, String description) {
        super(FieldDef.Type.JSON, description);
        if (mrcounterMap == null) {
            mrcounterFields = null;
            mrcounterSteps = null;
            return;
        }
        mrcounterFields = new HashMap<>();
        mrcounterSteps = new ArrayList<>();
        initMRCounterFieldsAndSteps(mrcounterMap);
    }


    /**
     * Constructor for FastExternalizable
     */
    JsonDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.JSON);
        Map<String, Type> mrcounterMap = null;
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (serialVersion >= SerialVersion.JSON_COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1) {
            mrcounterMap = SerializationUtil.
                readMap(in, serialVersion, HashMap::new,
                        SerializationUtil::readString,
                        Type::readFastExternal);
        }
        if (mrcounterMap == null) {
            mrcounterFields = null;
            mrcounterSteps = null;
            return;
        }
        mrcounterFields = new HashMap<>();
        mrcounterSteps = new ArrayList<>();
        initMRCounterFieldsAndSteps(mrcounterMap);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> <i>[Optional]</i>
     *                      {{@link SerializationUtil#writeMap Map}}
     *                      {@code mrcounterFields}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        SerializationUtil.writeMap(out, serialVersion, mrcounterFields,
                                   WriteFastExternal::writeString);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public JsonDefImpl clone() {
        if (mrcounterFields != null) {
            return new JsonDefImpl(new HashMap<String, Type>(mrcounterFields),
                                   null);
        }
        return FieldDefImpl.Constants.jsonDef;
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public JsonDefImpl asJson() {
        return this;
    }

    @Override
    public boolean equals(Object other) {

        if (!(other instanceof JsonDefImpl)) {
            return false;
        }
        Map<String, Type> otherFields = ((JsonDefImpl)other).mrcounterFields;
        if (mrcounterFields == null) {
            return otherFields == null;
        }
        return mrcounterFields.equals(otherFields);
    }

    @Override
    public int hashCode() {
        int code = 0;
        if (mrcounterFields != null) {
            code = mrcounterFields.size();
            for (Map.Entry<String, Type> entry : mrcounterFields.entrySet()) {
                code += entry.getKey().hashCode() + entry.getValue().hashCode();
            }
        }
        return super.hashCode() + code;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public short getRequiredSerialVersion() {
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (mrcounterFields == null) {
            return super.getRequiredSerialVersion();
        }
        return (short) Math.max(SerialVersion.JSON_COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1,
                                super.getRequiredSerialVersion());
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return false;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {
        return (superType.isJson() || superType.isAny());
    }

    @Override
    public ArrayValueImpl createArray() {
        return new ArrayValueImpl(FieldDefImpl.Constants.arrayJsonDef);
    }

    @Override
    public BooleanValue createBoolean(boolean value) {
        return Constants.booleanDef.createBoolean(value);
    }

    @Override
    public DoubleValue createDouble(double value) {
        return Constants.doubleDef.createDouble(value);
    }

    @Override
    public FloatValue createFloat(float value) {
        return Constants.floatDef.createFloat(value);
    }

    @Override
    public IntegerValue createInteger(int value) {
        return Constants.integerDef.createInteger(value);
    }

    @Override
    public LongValue createLong(long value) {
        return Constants.longDef.createLong(value);
    }

    @Override
    public NumberValue createNumber(int value) {
        return Constants.numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(long value) {
        return Constants.numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(float value) {
        return Constants.numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(double value) {
        return Constants.numberDef.createNumber(value);
    }

    @Override
    public NumberValue createNumber(BigDecimal decimal) {
        return Constants.numberDef.createNumber(decimal);
    }

    @Override
    public MapValueImpl createMap() {
        if (mrcounterFields != null) {
            return new MapValueImpl(new MapDefImpl(this));
        }
        return new MapValueImpl(FieldDefImpl.Constants.mapJsonDef);
    }

    @Override
    public StringValue createString(String value) {
        return Constants.stringDef.createString(value);
    }

    @Override
    public FieldValue createJsonNull() {
        return NullJsonValueImpl.getInstance();
    }

    @Override
    public Map<String, Type> allMRCounterFields() {
        if (mrcounterFields == null) {
            return null;
        }
        return Collections.unmodifiableMap(mrcounterFields);
    }

    public Map<String, Type> allMRCounterFieldsInternal() {
        return mrcounterFields;
    }

    /**
     * Split each counter path to JSON steps. Path expression is step(.step)*
     * There are 3 special char: period(.), double quote(") and escape char(\).
     * And double quote and escape char should be escaped.("\"" or "\\").
     * If step doesn't contain any special char, step is optional to be quoted.
     * Otherwise, step is required to be quoted
     * Note: single quote is a normal char, not used to quote and no need
     * escape for "."
     * For example, these 3 paths be split to array as following:
     * <pre>{@literal
     * a."b".c -> [a, b, c]
     * a."b.c" -> [a, b.c]
     * a."\"b"."c\\" -> [a, "b, c\]
     * }</pre>
     */
    public List<String[]> allMRCounterSteps() {
        if (mrcounterFields == null || mrcounterSteps != null) {
            return mrcounterSteps;
        }
        synchronized(mrcounterFields) {
            if (mrcounterSteps != null) {
                return mrcounterSteps;
            }
            mrcounterSteps = new ArrayList<>();
            for (String fieldPath: mrcounterFields.keySet()) {
                String[] steps = parseStepsFromPath(fieldPath);
                mrcounterSteps.add(steps);
            }
        }
        return mrcounterSteps;
    }

    /**
     * Quote steps and merged them into a full path. The step array is
     * from by above {@link #allMRCounterSteps()}
     * The full Path expression is step(.step)*
     * For example, these 3 arrays be merged to full path as following:
     * [a, b, c] -&gt; a.b.c
     * [a, b.c] -&gt; a."b.c"
     * [a, "b, c\] -&gt; a."\"b"."c\\"
     */
    public static String quoteStepIfNeedAndMerge(String[] steps) {
        String mergedPath = null;
        for (String step : steps) {
            mergedPath = quoteStepIfNeedAndConcat(mergedPath, step);
        }
        return mergedPath;
    }

    /**
     * Quote nextStep and then concatenates it as the next step of curPath.
     * For example, return following results with these parameters:
     * If curPath is null, nextStep is a, return a.
     * If curPath is a, nextStep is b, return a.b
     * If curPath is a, nextStep is b.c, return a."b.c"
     * If curPath is a, nextStep is "b, return a."\"b"
     * If curPath is a, nextStep is c\, return a."c\\"
     */
    public static String quoteStepIfNeedAndConcat(String curPath,
                                                  String nextStep) {
        if (curPath == null) {
            return quoteStepIfNeed(nextStep);
        }
        return curPath + DELIMITER + quoteStepIfNeed(nextStep);
    }

    /**
     * Get MRCounter field type by quoted path. The quoted path is from
     * above {@link #quoteStepIfNeedAndMerge(String[])} or
     * {@link #quoteStepIfNeedAndConcat(String, String)}
     */
    public Type getMRCounterTypeByQuotedPath(String quotedPath) {
        return mrcounterFields.get(quotedPath);
    }

    /*
     * Initialize mrcounterFields and mrcounterSteps by user input counter path
     * map.
     * It splits each counter path into steps. Each step is unquoted after
     * removing escape. Save the steps into mrcounterPathSteps, as cache.
     * Also it removes unnecessary step quote before put path -> type item into
     * mrcounterFields.
     */
    private void initMRCounterFieldsAndSteps(Map<String, Type> mrcounterMap) {
        for (Entry<String, Type> field : mrcounterMap.entrySet()) {
            /*
             * Below parses counter path. It'd be nice if we can use a
             * regex to parse the path, but it requires a complicated regex.
             */
            String fieldOriginalPath = field.getKey();
            String[] pathStepArray = parseStepsFromPath(fieldOriginalPath);
            mrcounterSteps.add(pathStepArray);
            /* This will remove unnecessary step quote */
            String fieldSimplePath = quoteStepIfNeedAndMerge(pathStepArray);
            mrcounterFields.put(fieldSimplePath, field.getValue());
        }
    }

    /*
     * It splits each field path into steps. Each step is unquoted after
     * removing escape.
     */
    private static String[] parseStepsFromPath(String fieldPath) {
        /*
         * Below parses counter path. It'd be nice if we can use a
         * regex to parse the path, but it requires a complicated regex.
         */
        if (fieldPath.indexOf(QUOTE) == -1) {
            /* Simple case: no quote, no escape */
            return fieldPath.split("\\.");
        }
        List<String> steps = new ArrayList<>();
        boolean quote = false;
        boolean escape = false;
        String nextStep = "";
        int length = fieldPath.length();
        for (int i = 0; i < length; i++) {
            char c = fieldPath.charAt(i);
            if (escape) {
                nextStep += c;
                escape = false;
                continue;
            }
            if (c == ESCAPE) {
                escape = true;
                continue;
            }
            if (c == QUOTE) {
                /*
                 * quote state is true when see the first QUOTE, and
                 * quote state is false when see the second QUOTE.
                 */
                quote = !quote;
                continue;
            }
            if (c == DELIMITER && !quote) {
                steps.add(nextStep);
                nextStep = "";
                quote = false;
                continue;
            }
            nextStep += c;
        }
        if (!nextStep.isEmpty()) {
            steps.add(nextStep);
        }
        /* convert list to array */
        int size = steps.size();
        if (size == 0) {
            throw new IllegalStateException(
                "illegal counter path: " + fieldPath);
        }
        return steps.toArray(new String[size]);
    }

    private static String quoteStepIfNeed(String step) {
        String result = "";
        boolean quote = !step.matches(TableImpl.VALID_NAME_CHAR_REGEX);
        for (int i = 0; i < step.length(); i++) {
            char c = step.charAt(i);
            if (c == ESCAPE || c == QUOTE) {
                result += ESCAPE;
            }
            result += c;
        }
        if (!quote) {
            return result;
        }
        return QUOTE + result + QUOTE;
    }

    /**
     * A common method to validate JSON-compatible values.  This includes all
     * atomic types except (FIXED_)BINARY and ENUM. It excludes RECORD, but
     * includes ARRAY and MAP IFF they are instances of JsonArrayValueImpl or
     * JsonMapValueImpl.
     */
    static void validateJsonType(FieldValue value) {
        FieldDef.Type type = value.getType();

        if (type == FieldDef.Type.BINARY ||
            type == FieldDef.Type.FIXED_BINARY ||
            type == FieldDef.Type.ENUM ||
            type == FieldDef.Type.RECORD) {
            throw new IllegalArgumentException
                ("Type is not supported in JSON: " + type);
        }

        if (type == FieldDef.Type.MAP || type ==  FieldDef.Type.ARRAY) {

            FieldDefImpl elemDef = (FieldDefImpl)value.getDefinition();

            if (!elemDef.isSubtype(FieldDefImpl.Constants.jsonDef)) {
                throw new IllegalArgumentException(
                    "Type is not supported in JSON: " + type);
            }
        }
    }

    /**
     * Creates a FieldValue from a Reader. This helper method is used by the
     * putJson(), addJson(), and setJson() methods of the complex values. It
     * factors out IOException handling.
     */
    static FieldValue createFromReader(Reader jsonReader) {
        try {
            JsonParser jp = null;
            try {
                jp = TableJsonUtils.createJsonParser(jsonReader);
                return JsonDefImpl.createFromJson(jp, true);
            } finally {
                if (jp != null) {
                    jp.close();
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Unable to parse JSON input: " + ioe.getMessage(), ioe);
        }
    }

    /**
     * Construct a FieldValue based on arbitrary JSON from the incoming JSON
     * The top-level object may be any valid JSON:
     * 1. an object
     * 2. an array
     * 3. a scalar, including the JSON null value
     *
     * This code creates FieldValue types based on the type inferred from the
     * parser.
     */
    public static FieldValue createFromJson(JsonParser jp, boolean getNext) {

        try {
            JsonToken token = (getNext ? jp.nextToken() : jp.getCurrentToken());
            if (token == null) {
                throw new IllegalStateException(
                    "createFromJson called with null token");
            }

            switch (token) {
            case VALUE_STRING:
                return FieldDefImpl.Constants.stringDef.createString(
                    jp.getText());

            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:

                /*
                 * Handle all numeric types here. 4 types are supported (3
                 * until 4.4):
                 *  INTEGER
                 *  LONG (long and integer)
                 *  DOUBLE (double and float)
                 *  NUMBER (anything that won't fit into the two above)
                 */
                JsonParser.NumberType numberType = jp.getNumberType();

                switch (numberType) {
                case BIG_INTEGER:
                case BIG_DECIMAL:
                    return FieldDefImpl.Constants.numberDef.createNumber(
                        jsonParserGetDecimalValue(jp));
                case INT:
                    return FieldDefImpl.Constants.integerDef.createInteger(
                        jp.getIntValue());
                case LONG:
                    return FieldDefImpl.Constants.longDef.createLong(
                        jp.getLongValue());

                /* Never store FLOAT, use DOUBLE */
                case FLOAT:
                case DOUBLE:
                    /*
                     * If the value is a double special value (NaN or
                     * Infinity/-Infinity) or there is no loss of precision,
                     * the DOUBLE type is used, otherwise uses NUMBER type.
                     */
                    double dbl = jp.getDoubleValue();
                    if (Double.isNaN(dbl) ||
                        (Double.isInfinite(dbl) &&
                         jp.getText().toUpperCase().contains("INF"))) {
                        return FieldDefImpl.Constants.doubleDef
                                .createDouble(dbl);
                    }

                    BigDecimal bd = jsonParserGetDecimalValue(jp);
                    if (Double.isFinite(dbl) &&
                            bd.compareTo(BigDecimal.valueOf(dbl)) == 0) {
                        return FieldDefImpl.Constants.doubleDef
                                .createDouble(dbl);
                    }

                    return FieldDefImpl.Constants.numberDef.createNumber(bd);
                }
                throw new IllegalStateException("Unexpected numeric type: " +
                                                numberType);
            case VALUE_TRUE:

                return FieldDefImpl.Constants.booleanDef.createBoolean(true);

            case VALUE_FALSE:

                return FieldDefImpl.Constants.booleanDef.createBoolean(false);

            case VALUE_NULL:

                return NullJsonValueImpl.getInstance();

            case START_OBJECT:

                return parseObject(jp);

            case START_ARRAY:

                return parseArray(jp);

            case FIELD_NAME:
            case END_OBJECT:
            case END_ARRAY:
            default:
                throw new IllegalStateException(
                    "Unexpected token while parsing JSON: " + token);
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failed to parse JSON input: " + ioe.getMessage());
        }
    }

    /**
     * Creates a JSON map from the parsed JSON object.
     */
    private static FieldValueImpl parseObject(JsonParser jp)
        throws IOException {

        MapValueImpl map = FieldDefImpl.Constants.jsonDef.createMap();

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            String fieldName = jp.currentName();
            if (token == null || fieldName == null) {
                throw new IllegalArgumentException(
                    "null token or field name parsing JSON object");
            }

            /* true tells the method to fetch the next token */
            FieldValue field = createFromJson(jp, true);

            if (field.isJsonNull()) {
                map.put(fieldName, NullJsonValueImpl.getInstance());
            } else {
                map.put(fieldName, field);
            }
        }
        return map;
    }

    /**
     * Creates a JSON array from the parsed JSON array by adding
     */
    private static FieldValueImpl parseArray(JsonParser jp)
        throws IOException {

        ArrayValueImpl array = FieldDefImpl.Constants.jsonDef.createArray();

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            if (token == null) {
                throw new IllegalStateException(
                    "null token while parsing JSON array");
            }

            /* false means don't get the next token, it's been fetched */
            array.add(createFromJson(jp, false));
        }
        return array;
    }

    @Override
    public boolean hasJsonMRCounter() {
        return mrcounterFields != null;
    }

    /**
     * Insert a json MR Counter value into a row at a specified path.
     */
    public static void insertMRCounterField(FieldValueImpl row,
                                            TablePath path,
                                            FieldValueImpl cntVal,
                                            boolean force) {
        insertMRCounterField(row, path.getSteps(), cntVal, 0, force);
    }

    public static void insertMRCounterField(FieldValueImpl val,
                                            List<StepInfo> path,
                                            FieldValueImpl cntVal) {
        insertMRCounterField(val, path, cntVal, 0, true);
    }

    static void insertMRCounterField(FieldValueImpl val,
                                     List<StepInfo> path,
                                     FieldValueImpl cntVal,
                                     int pathPos,
                                     boolean force) {
        String fname = path.get(pathPos).getStep();
        FieldValueImpl fval = val.getElement(fname);

        if (pathPos >= path.size() - 1) {
            /* Insert the MR_Counter value. */
            if (fval != null && !fval.isAtomic()) {
                throw new IllegalArgumentException(
                    "Only atomic values are allowed for MR_Counter field " +
                    "at path: " + path);
            }

            if (fval == null || !fval.isMRCounter() || force) {
                ((MapValueImpl)val).put(fname, cntVal);
            }
            return;
        }

        /* Continue tracking the path. */
        if (fval != null) {
            if (!fval.isMap()) {
                throw new IllegalArgumentException(
                    "Failed to evaluate MR_Counter path: " +
                    printMRCounterPath(path) +
                    ". An array or atomic value found during the evaluation." +
                    fval.getType());
            }
            insertMRCounterField(fval, path, cntVal, ++pathPos, force);

        } else if (val.isRecord()) {
            RecordValueImpl rec = (RecordValueImpl)val;
            int fpos = rec.getFieldPos(fname);
            FieldDefImpl fdef = rec.getFieldDef(fpos);
            fval = fdef.createMap();
            rec.put(fpos, fval);
            insertMRCounterField(fval, path, cntVal, ++pathPos, force);

        } else {
            MapValueImpl subMap = FieldDefImpl.Constants.jsonDef.createMap();
            ((MapValueImpl)val).put(fname, subMap);
            insertMRCounterField(subMap, path, cntVal, ++pathPos, force);
        }
    }

    static String printMRCounterPath(List<StepInfo> steps) {

        StringBuilder sb = new StringBuilder();

        int numSteps = steps.size();

        for (int i = 0; i < numSteps; ++i) {

            StepInfo si = steps.get(i);

            if (si.isQuoted) {
                sb.append("\"").append(si.step).append("\"");
            } else {
                sb.append(si.step);
            }

            if (i < numSteps - 1) {
                sb.append(NameUtils.CHILD_SEPARATOR);
            }
        }

        return sb.toString();
    }
}
