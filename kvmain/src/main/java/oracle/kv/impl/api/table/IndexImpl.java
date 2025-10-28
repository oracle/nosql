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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_12_DEPRECATED_REMOVE_AFTER_PREREQ_25_1;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollectionLength;
import static oracle.kv.impl.util.SerializationUtil.writeMapLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.TablePath.StepInfo;
import oracle.kv.impl.api.table.TablePath.StepKind;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.ExprUtils;
import oracle.kv.impl.query.compiler.Function;
import oracle.kv.impl.query.compiler.FuncRowMetadata;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.RecordValue;
import oracle.nosql.common.json.JsonUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Implementation of the Index interface.  Instances of this class are created
 * and associated with a table when an index is defined.  It contains the index
 * metdata as well as many utility functions used in serializing and
 * deserializing index keys.
 *
 * For the functional specification of indexes as well as a high-level
 * description on how their contents are computed, read the SQL reference
 * book, or the internal queryspec document.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class IndexImpl implements Index, Serializable, FastExternalizable {

    private static final String KEY_TAG = "_key";

    private static final String DOT_BRACKETS = ".[]";

    private static final long serialVersionUID = 1L;

    /* The serial version for v4.3 and before */
    static final int INDEX_VERSION_V0 = 0;

    /*
     * Index serial v1:
     * - Add EMTPY_INDICATOR
     * - NULL_INDICATOR is changed to 0x7f
     */
    static final int INDEX_VERSION_V1 = 1;

    /* The current serial version used in the index */
    static final int INDEX_VERSION_CURRENT = INDEX_VERSION_V1;

    /*
     * The indicator used in key serialization for values that are not
     * one of the special values (SQL NULL, json null, or EMPTY)
     */
    static final byte NORMAL_VALUE_INDICATOR = 0x00;

    /*
     * The indicator representing the EMPTY value (added since v1):
     * The EMPTY value is used if an index path, when evaluated on a
     * table as a query path expr, returns an empty result. For example:
     *  - The index field is an empty array or map.
     *  - The index field is map.<specific-key>, the specific-key is missing
     *    from the map value.
     *  - If the index field is a sub field of json field, the sub field is
     *    missing from the json value and the json value is not sql null or
     *    json null.
     */
    static final byte EMPTY_INDICATOR = 0x7D;

    /* The indicator for JSON null used in key serialization (added since v1) */
    static final byte JSON_NULL_INDICATOR = 0x7E;

    /* The indicator for null value used in key serialization (v1) */
    static final byte NULL_INDICATOR_V1 = 0x7F;

    /* The indicator for null value used in key serialization */
    static final byte NULL_INDICATOR_V0 = 0x01;

    static final byte NUMBER_INDICATOR = 0x02;

    static final byte LONG_INDICATOR = 0x03;

    static final byte DOUBLE_INDICATOR = 0x04;

    static final byte STRING_INDICATOR = 0x05;

    static final byte BOOLEAN_INDICATOR = 0x06;

    /* For testing purpose */
    static final String INDEX_NULL_DISABLE = "test.index.null.disable";

    static final String INDEX_SERIAL_VERSION = "test.index.serial.version";

    /** See {@link TwoRowCache}. */
    private static final ThreadLocal<TwoRowCache> twoRowCache =
        ThreadLocal.withInitial(TwoRowCache::new);

    private static final ThreadLocal<MultiKeyExtractionContext>
        keyExtractionContextCache =
        ThreadLocal.withInitial(MultiKeyExtractionContext::new);

    private static final ExecuteOptions options = new ExecuteOptions();

    /* the index name */
    private final String name;

    /* the (optional) index description, user-provided */
    private final String description;

    /* the associated table */
    private final TableImpl table;

    /*
     * The stringified path exprs that define the index columns. In the case of
     * map indexes a path expr may contain the special steps "._key" and ".[]"
     * to distinguish between the 3 possible ways of indexing a map: (a) all
     * the keys (using a _key step), (b) all the values (using a [] step), or
     * (c) the value of a specific map entry (using the specific key of the
     * entry we want indexed). In case of array indexes, the ".[]" is used.
     *
     * The string format described above is the "old" format.  The newFields
     * list below stores the same strings in the "new" foramt, which is the
     * one that is used in the DDL systax. Specifically, for maps the new format
     * uses ".keys()" and ".values()" instead of "._key", ".[]", and for arrays
     * it uses "[]" instead of ".[]".
     *
     * The new format was introduced in 4.3 and all the data model code was
     * modified to use this format. However, the old format is still here
     * to satisfy the backwards compatimity requirement. The difficult case
     * is when a new server must serialize (via java) and send an IndexImpl
     * to an old client, which of course, expects and undestands the old
     * format only. For this reason, during (de)serialization the old format
     * is used always, and newFields is declared transient.
     *
     * In 4.4, newFields (and newAnnotations) were made persistent. this.fields
     * and this.annotations remained peristent in order to handle the old-client
     * problem mentioned above, as well as existing indexes created by older
     * server versions. newFields was made persistent because in 4.4 steps may
     * be quoted, and the quotes must be preserved when an IndexImpl is
     * serialized to disk (putting the quotes into this.fields would not work
     * because the index would then not work at an old client, since the old
     * client would not be removing the quotes). Notice also that with the
     * introduction of json indexes in 4.4, this.fields alone is not enough
     * to correctly initialize the transient state of IndexImpl, because in
     * the case of json index paths there is no schema to determine whether
     * a .[] step appearing in this.fields should be converted to .values()
     * or to [].
     */
    private final List<String> fields;


    /* status is used when an index is being populated to indicate readiness */
    private IndexStatus status;

    private final Map<String, String> annotations;

    /*
     * properties of the index; used by text indexes only; can be null.
     */
    private final Map<String, String> properties;

    /*
     * Indicates whether this index includes an indicator byte in each field
     * of its binary keys. Indicator bytes were added in 4.2. For older,
     * existing indexes this will be false. For new indexes it is true, unless
     * test-specific state is set to turn this feature off.
     */
    private final boolean isNullSupported;

    /*
     * The version of the index. This data member was introduced in 4.4.
     */
    private final int indexVersion;

    /*
     * This data member was made persistent in 4.4.
     */
    private List<String> newFields;

    /*
     * The declared types for the json index paths (if an index path is not
     * json, its corresponding entry in the list will be null).
     * This data member was introduced in 4.4.
     */
    private final List<FieldDef.Type> types;

    /*
     * This data member was made persistent in 4.4
     */
    private Map<String, String> newAnnotations;

    /*
     * Whether the index indexes SQL NULL and EMPTY values. This is specified
     * in the index creation DDL. The default in the DDL is true (so the default
     * for skipNulls is false). If false is specified in the DDL, a row that
     * contains a NULL value on an indexed field or is missing an indexed field
     * will be skipped during indexing. As a result, the index may not contain
     * at least one entry per row, which further implies that there are certain
     * queries that cannot use this index. The query compiler makes sure that
     * the index will not be used in those cases (e.g. IS NULL or NOT EXISTS
     * predicates).
     */
    private boolean skipNulls;

    /*
     * If true, an error will be raised if the set of index entries extracted
     * from a row contains duplicates, except for the duplicate entries that
     * contain EMPTY on all the indexed fields.
     *
     * Introduced in 21.2
     */
    private boolean isUnique;

    private long id;

    /*
     * transient version of the index column definitions, materialized as
     * IndexField for efficiency. It is technically final but is not because it
     * needs to be initialized in readObject after deserialization.
     */
    private transient List<IndexField> indexFields;

    /*
     * transient indication of whether this is a multiKeyMapIndex.  This is
     * used for serialization/deserialization of map indexes.  It is
     * technically final but is not because it needs to be initialized in
     * readObject after deserialization.
     */
    private transient boolean isMultiKeyMapIndex;

    private transient boolean isPointIndex;

    private transient int geoFieldPos = -1;

    /*
     * transient RecordDefImpl representing the definition of IndexKeyImpl
     * instances for this index.
     */
    private transient RecordDefImpl indexKeyDef;

    /*
     * transient RecordDefImpl representing a full index entry, including the
     * primary key fields
     */
    private volatile transient RecordDefImpl indexEntryDef;

    /*
     * The union of all the multikey index paths. It is used as a "dataguide"
     * to traverse a row and extract the keys for a multikey index.
     */
    private transient IndexField multiKeyPaths;

    /*
     * It contains the positions, within the index definition, of every
     * functional index field over an array/map of atomic values.
     */
    private transient ArrayList<Integer> functionalFieldPositions;

    /*
     * Set to true if this index indexes nested arrays and/or maps
     */
    private transient boolean isNestedMultiKeyIndex;

    /*
     * Set to true if this index indexes both keys and associated values of
     * a single map per row (isNestedMultiKeyIndex == false), with the key
     * field appearing before any of the values() fields. This is info
     * needed by the query optimizer.
     */
    private transient boolean isMapBothIndex;

    /**
     * Index status.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public enum IndexStatus implements FastExternalizable {
        /** Index is transient. */
        TRANSIENT() {
            @Override
            public boolean isTransient() {
                return true;
            }
        },

        /** Index is being populated. */
        POPULATING() {
            @Override
            public boolean isPopulating() {
                return true;
            }
        },

        /** Index is populated and ready for use. */
        READY() {
            @Override
            public boolean isReady() {
                return true;
            }
        };

        private static final IndexStatus[] VALUES = values();

        /**
         * Returns true if this is the {@link #TRANSIENT} type.
         * @return true if this is the {@link #TRANSIENT} type
         */
        public boolean isTransient() {
            return false;
        }

        /**
         * Returns true if this is the {@link #POPULATING} type.
         * @return true if this is the {@link #POPULATING} type
         */
        public boolean isPopulating() {
            return false;
        }

        /**
         * Returns true if this is the {@link #READY} type.
         * @return true if this is the {@link #READY} type
         */
        public boolean isReady() {
            return false;
        }

        static IndexStatus readFastExternal(DataInput in,
                                @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException(
                    "Wrong ordinal for IndexStatus: " + ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    public IndexImpl(String name,
                     TableImpl table,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     String description) {
        this(name, table, fields, types, true, false, null, null, description);
    }

    public IndexImpl(String name,
                     TableImpl table,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     boolean indexNulls,
                     boolean isUnique,
                     String description) {
        this(name, table, fields, types, indexNulls, isUnique,
             null, null, description);
    }

    /* Constructor for Full Text Indexes. */
    public IndexImpl(String name,
                     TableImpl table,
                     List<String> fields,
                     List<FieldDef.Type> types,
                     boolean indexNulls,
                     boolean isUnique,
                     Map<String, String> annotations,
                     Map<String, String> properties,
                     String description) {
        this.name = name;
        this.table = table;
        this.newFields = fields;
        this.types = types;
        this.skipNulls = !indexNulls;
        this.isUnique = isUnique;
        this.newAnnotations = annotations;
        this.properties = properties;
        this.description = description;
        status = IndexStatus.TRANSIENT;
        isNullSupported = areIndicatorBytesEnabled();
        indexVersion = getIndexVersion();

        if (newAnnotations != null) {
            this.annotations = new HashMap<>();
        } else {
            this.annotations = null;
        }

        /* validate initializes indexFields and the other transient state */
        validate();

        this.fields = new ArrayList<>(newFields.size());

        translateToOldFields();
    }

    private void translateToOldFields() {

        final StringBuilder sb = new StringBuilder();

        for (IndexField field : indexFields) {

            for (int s = 0; s < field.numSteps(); ++s) {

                final StepInfo si = field.getStepInfo(s);
                final String step = si.step;

                if (si.kind == StepKind.BRACKETS) {
                    sb.append(TableImpl.BRACKETS);

                } else if (si.kind == StepKind.VALUES) {
                    sb.append(TableImpl.BRACKETS);

                } else if (si.kind == StepKind.KEYS) {
                    sb.append(KEY_TAG);

                } else {
                    /*
                     * If the step was a quoted one, the quotes are removed
                     * in the old path format. Leaving the quotes there would
                     * definitely make the index not work at an old client.
                     * Without the quotes, the index may or may not work at
                     * an old client, depending on what is inside the quotes.
                     */
                    sb.append(step);
                }

                if (s < field.numSteps() - 1) {
                    sb.append(NameUtils.CHILD_SEPARATOR);
                }
            }

            final String oldField = sb.toString();
            sb.delete(0, sb.length());

            fields.add(oldField);

            if (newAnnotations != null) {
                final String annotation =
                    newAnnotations.get(newFields.get(field.getPosition()));
                if (annotation != null) {
                    annotations.put(oldField, annotation);
                }
            }
        }
    }

    /**
     * Constructor for FastExternalizable
     */
    IndexImpl(DataInput in, short serialVersion, TableImpl table)
            throws IOException {
        this.table = table;
        indexVersion = readPackedInt(in);
        name = readNonNullString(in, serialVersion);
        description = readString(in, serialVersion);
        final int nFields = readNonNullSequenceLength(in);
        fields = new ArrayList<>(nFields);
        for (int i = 0; i < nFields; i++) {
            fields.add(i, readNonNullString(in, serialVersion));
        }
        status = IndexStatus.readFastExternal(in, serialVersion);
        final int nAnnotations = readSequenceLength(in);
        if (nAnnotations < 0) {
            annotations = null;
        } else {
            annotations = new HashMap<>(nAnnotations);
            for (int i = 0; i < nAnnotations; i++) {
                final String key = readString(in, serialVersion);
                final String value = readString(in, serialVersion);
                annotations.put(key, value);
            }
        }
        final int nProperties = readSequenceLength(in);
        if (nProperties < 0) {
            properties = null;
        } else {
            properties = new HashMap<>(nProperties);
            for (int i = 0; i < nProperties; i++) {
                final String key = readNonNullString(in, serialVersion);
                final String value = readNonNullString(in, serialVersion);
                properties.put(key, value);
            }
        }
        isNullSupported = in.readBoolean();
        final int nNewFields = readNonNullSequenceLength(in);
        newFields = new ArrayList<>(nNewFields);
        for (int i = 0; i < nNewFields; i++) {
            newFields.add(i, readNonNullString(in, serialVersion));
        }
        final int nTypes = readSequenceLength(in);
        if (nTypes < 0) {
            types = null;
        } else {
            types = new ArrayList<>(nTypes);
            for (int i = 0; i < nTypes; i++) {
                types.add(i, FieldDef.Type.readFastExternal(in, serialVersion));
            }
        }
        final int nNewAnnotations = readSequenceLength(in);
        if (nNewAnnotations < 0) {
            newAnnotations = null;
        } else {
            newAnnotations = new HashMap<>(nNewAnnotations);
            for (int i = 0; i < nNewAnnotations; i++) {
                final String key = readString(in, serialVersion);
                final String value = readString(in, serialVersion);
                newAnnotations.put(key, value);
            }
        }

        skipNulls = in.readBoolean();

        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (serialVersion >=
            QUERY_VERSION_12_DEPRECATED_REMOVE_AFTER_PREREQ_25_1) {
            isUnique = in.readBoolean();
        }

        if (serialVersion >= QUERY_VERSION_16) {
            id = in.readLong();
        }

        validate();
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@code indexVersion}
     * <li> ({@link SerializationUtil#writeNonNullString
     *      non-null String}) {@code name}
     * <li> ({@link SerializationUtil#writeString String}) {@code description}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@code fields} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeNonNullString
     *         non-null String}) <i>field</i>
     *    </ol>
     * <li> ({@link IndexStatus}) {@code status}
     * <li> ({@link SerializationUtil#writeMapLength map length})
     *      {@code annotations} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeString
     *         non-null String}) <i>key</i>
     *    <li> ({@link SerializationUtil#writeString
     *         non-null String}) <i>value</i>
     *    </ol>
     * <li> ({@link SerializationUtil#writeMapLength map length})
     *      {@code properties} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeNonNullString
     *         non-null String}) <i>key</i>
     *    <li> ({@link SerializationUtil#writeNonNullString
     *         non-null String}) <i>value</i>
     *    </ol>
     * <li> ({@code boolean}) {@code isNullSupported}
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) {@code newFields} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeNonNullString
     *         non-null String}) <i>new field</i>
     *    </ol>
     * <li> ({@link SerializationUtil#writeCollectionLength sequence length})
     *      {@code types} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@code FieldDef.Type}) <i>type</i>
     *    </ol>
     * <li> ({@link SerializationUtil#writeMapLength map length})
     *      {@code newAnnotations} <i>length</i>
     * <li> For each element:
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeString
     *         non-null String}) <i>key</i>
     *    <li> ({@link SerializationUtil#writeString
     *         non-null String}) <i>value</i>
     *    </ol>
     * <li> ({@code boolean}) {@code skipNulls}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        writePackedInt(out, indexVersion);
        writeNonNullString(out, serialVersion, name);
        writeString(out, serialVersion, description);
        writeNonNullSequenceLength(out, fields.size());
        for (String fn : fields) {
            writeNonNullString(out, serialVersion, fn);
        }
        status.writeFastExternal(out, serialVersion);
        writeMapLength(out, annotations);
        if (annotations != null) {
            for (Entry<String, String> e : annotations.entrySet()) {
                writeString(out, serialVersion, e.getKey());
                writeString(out, serialVersion, e.getValue());
            }
        }
        writeMapLength(out, properties);
        if (properties != null) {
            for (Entry<String, String> e : properties.entrySet()) {
                writeNonNullString(out, serialVersion, e.getKey());
                writeNonNullString(out, serialVersion, e.getValue());
            }
        }
        out.writeBoolean(isNullSupported);
        writeNonNullSequenceLength(out, newFields.size());
        for (String nf : newFields) {
            writeNonNullString(out, serialVersion, nf);
        }
        writeCollectionLength(out, types);
        if (types != null) {
            for (FieldDef.Type t : types) {
                if (t != null) {
                    t.writeFastExternal(out, serialVersion);
                } else {
                    out.writeByte(-1);
                }
            }
        }
        writeMapLength(out, newAnnotations);
        if (newAnnotations != null) {
            for (Entry<String, String> e : newAnnotations.entrySet()) {
                writeString(out, serialVersion, e.getKey());
                writeString(out, serialVersion, e.getValue());
            }
        }
        out.writeBoolean(skipNulls);

        out.writeBoolean(isUnique);

        if (serialVersion >= QUERY_VERSION_16) {
            out.writeLong(id);
        }
    }

    @Override
    public String toString() {
        return "Index[" + name + ", " + table.getId() + ", " + status + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IndexImpl)) {
            return false;
        }
        final IndexImpl other = (IndexImpl) obj;
        return name.equals(other.name) &&
            Objects.equals(description, other.description) &&
            Objects.equals(table, other.table) &&
            fields.equals(other.fields) &&
            Objects.equals(status, other.status) &&
            Objects.equals(annotations, other.annotations) &&
            Objects.equals(properties, other.properties) &&
            (isNullSupported == other.isNullSupported) &&
            (indexVersion == other.indexVersion) &&
            Objects.equals(newFields, other.newFields) &&
            Objects.equals(types, other.types) &&
            Objects.equals(newAnnotations, other.newAnnotations) &&
            (skipNulls == other.skipNulls) &&
            (isUnique == other.isUnique);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name,
                            description,
                            table,
                            fields,
                            status,
                            annotations,
                            properties,
                            isNullSupported,
                            indexVersion,
                            newFields,
                            types,
                            newAnnotations,
                            skipNulls,
                            isUnique);
    }

    public IndexStatus getStatus() {
        return status;
    }

    public void setStatus(IndexStatus status) {
        this.status = status;
    }

    @Override
    public String getDescription()  {
        return description;
    }

    @Override
    public TableImpl getTable() {
        return table;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public String getName()  {
        return name;
    }

    @Override
    public boolean indexesNulls()  {
        return !skipNulls;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public List<FieldDef.Type> getTypes() {
        return types;
    }

    /*
     * Returns a potentially modifyable list of the string paths for the
     * indexed fields.
     */
    @Override
    public List<String> getFields() {
        initTransientState();
        return newFields;
    }

    public int numFields() {
        return getFields().size();
    }

    public IndexField getIndexPath(int pos) {
        return getIndexFields().get(pos);
    }

    public IndexField getIndexPath(String fieldName) {
        return getIndexFields().get(getIndexKeyDef().getFieldPos(fieldName));
    }

    /**
     * Returns the list of IndexField objects defining the index. It is
     * transient, and if not yet initialized, initialize it.
     */
    public List<IndexField> getIndexFields() {
        initTransientState();
        return indexFields;
    }

    boolean fieldMayHaveSpecialValue(int pos) {
        return getIndexPath(pos).mayHaveSpecialValue();
    }

    /**
     * Returns an list of the fields that define a text index.
     * These are in order of declaration which is significant.
     *
     * @return the field names
     */
    public List<AnnotatedField> getFieldsWithAnnotations() {

        if (!isTextIndex()) {
            throw new IllegalStateException
                ("getFieldsWithAnnotations called on non-text index");
        }

        final List<AnnotatedField> fieldsWithAnnotations =
            new ArrayList<AnnotatedField>(getFields().size());

        final Map<String, String> anns = getAnnotationsInternal();

        for (String field : getFields()) {
            fieldsWithAnnotations.add(
               new AnnotatedField(field, anns.get(field)));
        }
        return fieldsWithAnnotations;
    }

    Map<String, String> getAnnotations() {
        if (isTextIndex()) {
            initTransientState();
            return Collections.unmodifiableMap(newAnnotations);
        }
        return Collections.emptyMap();
    }

    Map<String, String> getAnnotationsInternal() {
        initTransientState();
        return newAnnotations;
    }

    public Map<String, String> getProperties() {
        if (properties != null) {
            return properties;
        }
        return Collections.emptyMap();
    }

    @Override
    public IndexKeyImpl createIndexKey() {
        return new IndexKeyImpl(this, getIndexKeyDef());
    }

    @Deprecated
    @Override
    public IndexKeyImpl createIndexKey(RecordValue value) {

        if (value instanceof IndexKey) {
            throw new IllegalArgumentException(
                "Cannot call createIndexKey with IndexKey argument");
        }

        if (isMultiKey()) {
            throw new IllegalArgumentException(
                "Cannot call createIndexKey on a multikey index");
        }

        final IndexKeyImpl ikey = createIndexKey();
        int i = 0;

        for (IndexField field : getIndexFields()) {
            FieldValueImpl v = ((RecordValueImpl)value).
                               evaluateScalarPath(field, 0);
            if (v != null) {
                ikey.put(i, v);
            }
            i++;
        }

        ikey.validate();
        return ikey;
    }

    /*
     * Creates an IndexKeyImpl from a RecordValue that is known to be
     * a flattened IndexKey. This is used by the query engine where IndexKeys
     * are serialized and deserialized as plain RecordValue instances.
     *
     * Unlike the method below, this assumes a flattened structure in value
     * ("value" must have the same type def as the IndexKey).
     */
    public IndexKeyImpl createIndexKeyFromFlattenedRecord(RecordValue value) {
        final IndexKeyImpl ikey = createIndexKey();
        ikey.copyFrom(value);
        return ikey;
    }

    @Override
    public IndexKey createIndexKeyFromJson(String jsonInput, boolean exact) {
        return createIndexKeyFromJson
            (new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public IndexKey createIndexKeyFromJson(InputStream jsonInput,
                                           boolean exact) {
        final IndexKeyImpl key = createIndexKey();

        /*
         * Using addMissingFields false to not add missing fields, if Json
         * contains a subset of index fields, then build partial index key.
         */
        ComplexValueImpl.createFromJson(key, jsonInput, exact,
                                        false /*addMissingFields*/);
        return key;
    }

    @Override
    public FieldRange createFieldRange(String path) {

        final FieldDef ifieldDef = getIndexKeyDef().getFieldDef(path);

        if (ifieldDef == null) {
            throw new IllegalArgumentException(
                "Field does not exist in index: " + path);
        }
        return new FieldRange(path, ifieldDef, 0);
    }

    /**
     * Returns true if the index comprises only fields from the table's primary
     * key.  Nested types can't be key components so there is no need to handle
     * a complex path.
     */
    public boolean isKeyOnly() {

        for (IndexField ifield : getIndexFields()) {
            if (ifield.isComplex()) {
                return false;
            }
            if (ifield.isEmpty()) {
                return false;
            }
            if (!table.isKeyComponent(ifield.getStep(0))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return true if this index has multiple keys per record.  This can happen
     * if there is an array or map in the index.  An index can only contain one
     * array or map.
     */
    public boolean isMultiKey() {

        if (!isTextIndex()) {
            for (IndexField field : getIndexFields()) {
                if (field.isMultiKey()) {
                    return true;
                }
            }
        }
        return false;
    }

    public RecordDefImpl getIndexKeyDef() {
        initTransientState();
        return indexKeyDef;
    }

    public RecordDefImpl getIndexEntryDef() {
        initTransientState();
        return indexEntryDef;
    }

    public String getFieldName(int i) {
        return getIndexKeyDef().getFieldName(i);
    }

    public FieldDefImpl getFieldDef(int i) {
        return getIndexKeyDef().getFieldDef(i);
    }

    /**
     * Compares if the specified index fields are equals to that of this index,
     * return true if they are equal, otherwise return false.
     */
    public boolean compareIndexFields(List<String> fieldNames) {

        if (fieldNames == null || fieldNames.size() != getFields().size()) {
            return false;
        }

        for (int i = 0; i < fieldNames.size(); i++) {
            final String field = fieldNames.get(i);
            final IndexField ifield = new IndexField(table, field,
                                                     getFieldType(i), i);
            validateIndexField(ifield, true);
            if (!ifield.equals(getIndexFields().get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Initializes the transient list of index fields.  This is used when
     * the IndexImpl was constructed via deserialization and the constructor
     * and validate() were not called.
     *
     * TODO: figure out how to do transient initialization in the
     * deserialization case.  It is not as simple as implementing readObject()
     * because an intact Table is required.  Calling validate() from TableImpl's
     * readObject() does not work either (generates an NPE).
     */
    private void initTransientState() {

        /*
         * Since the indexEntryDef is the last thing to initialize in this
         * method, so check this depends on the setting of indexKeyDef.
         *
         * WARNING !!!: If new transient state is added to IndexImpl (and
         * initialized here) make sure it is initialized BEFORE indexEntryDef.
         */
        if (indexEntryDef != null) {
            return;
        }

        synchronized (this) {

            if (indexEntryDef != null) {
                return;
            }

            geoFieldPos = -1;

            int numFields;
            boolean convertOldPathFormat = false;

            if (newFields == null) {
                convertOldPathFormat = true;
                numFields = fields.size();
                newFields = new ArrayList<String>(numFields);

                if (annotations != null) {
                    newAnnotations = new HashMap<String, String>();
                }
            } else {
                numFields = newFields.size();
            }

            indexFields = new ArrayList<IndexField>(numFields);

            for (int i = 0; i < numFields; ++i) {

                String newField;

                if (convertOldPathFormat) {
                    final String field = fields.get(i);
                    newField = convertOldIndexPath(field);
                } else {
                    newField = newFields.get(i);
                }

                if (convertOldPathFormat) {
                    newFields.add(newField);

                    if (annotations != null) {
                        final String ann = annotations.get(fields.get(i));
                        if (ann != null) {
                            newAnnotations.put(newField, ann);
                        }
                    }
                }

                IndexField indexField = new IndexField(table, newField,
                                                       getFieldType(i), i);

                if (indexField.isPoint()) {
                    isPointIndex = true;
                    geoFieldPos = i;
                }  else if (indexField.isGeometry()) {
                    geoFieldPos = i;
                }

                try {
                    validateIndexField(indexField, false);
                } catch (RuntimeException ex) {
                    if (convertOldPathFormat) {
                        newFields = null;
                        if (newAnnotations != null) {
                            newAnnotations = null;
                        }
                    }
                    throw ex;
                }

                indexFields.add(indexField);
            }

            checkIsMapBothIndex();

            indexKeyDef = createIndexKeyDef();
            indexEntryDef = createIndexEntryDef();
        }
    }

    /*
     * The field path may be in the old format, that uses ._key and .[] steps.
     * If so, it must be converted to the new format.
     *
     * ._key steps are easy: just replace them with .keys().
     *
     * .[] steps may apply to either arrays or maps, and must be converted to
     * [] or .values(), respectively. To do so, we have to know the type of
     * the path up to the .[] step.
     */
    private String convertOldIndexPath(String field) {

        if (field.contains(KEY_TAG)) {
            return field.replace(KEY_TAG, TableImpl.KEYS);
        }

        if (field.contains(DOT_BRACKETS)) {

            final int bracketsIdx = field.indexOf(DOT_BRACKETS);

            final String mapOrArrayPath = field.substring(0, bracketsIdx);

            /*
             * mapOrArrayPath should contain only dot-separated identifiers,
             * so it is ok to call table.findTableField() on it.
             */
            final FieldDefImpl def = table.findTableField(mapOrArrayPath);

            if (def.isArray()) {
                return field.replace(DOT_BRACKETS, TableImpl.BRACKETS);
            }

            if (!def.isMap()) {
                /*
                 * Maybe it's a json index that is being deserialized at an
                 * old client.
                 */
                throw new IllegalArgumentException(
                    "Multikey index path does not contain an array or map. " +
                    "mapOrArrayPath = " + mapOrArrayPath);
            }

            return field.replace(TableImpl.BRACKETS, TableImpl.VALUES);
        }

        /*
         * The path contains dot-separated identifiers only. However, it may
         * still lead to or cross an array. This is possible if the index was
         * created with a KVS version prior to 4.2, because at that time []
         * was optional for arrays. In this case, we MUST find the array step
         * and add the [] after it. This will be done in validateIndexField().
         */
        return field;
    }

    public boolean isMultiKeyMapIndex() {
        initTransientState();
        return isMultiKeyMapIndex;
    }

    public IndexField getMultiKeyPaths() {
        initTransientState();
        return multiKeyPaths;
    }

    public boolean isNestedMultiKeyIndex() {
        initTransientState();
        return isNestedMultiKeyIndex;
    }

    public boolean isMapBothIndex() {
        initTransientState();
        return isMapBothIndex;
    }

    public boolean isGeoIndex() {
        return isGeoPointIndex() || isGeometryIndex();
    }

    public boolean isGeoPointIndex() {
        initTransientState();
        return isPointIndex;
    }

    public boolean isGeometryIndex() {
        initTransientState();
        return geoFieldPos >= 0 && !isPointIndex;
    }

    public int getGeoFieldPos() {
        initTransientState();
        return geoFieldPos;
    }

    int geoMaxCoveringCells() {

        if (properties != null) {
            final String val = properties.get("max_covering_cells");
            if (val != null) {
                Integer.valueOf(val);
            }
        }

        return GeometryUtils.theMaxCoveringCellsForIndex;
    }

    int geoMinCoveringCells() {

        if (properties != null) {
            final String val = properties.get("min_covering_cells");
            if (val != null) {
                Integer.valueOf(val);
            }
        }

        return GeometryUtils.theMinCoveringCellsForIndex;
    }

    int geoMaxSplits() {

        if (properties != null) {
            final String val = properties.get("max_splits");
            if (val != null) {
                Integer.valueOf(val);
            }
        }

        return GeometryUtils.theMaxSplits;
    }

    double geoSplitRatio() {

        if (properties != null) {
            final String val = properties.get("split_ratio");
            if (val != null) {
                Double.valueOf(val);
            }
        }

        return GeometryUtils.theSplitRatio;
    }

    /**
     * If an index path of this index contains a .keys() step, return the
     * position of associated index field. Otherwise return -1. It is used
     * in query/compiler/IndexAnalyzer.java
     */
    public int getPosForKeysField() {

        for (IndexField field : getIndexFields()) {
            if (field.isMapKeys()) {
                return field.getPosition();
            }
        }

        return -1;
    }

    FieldDef.Type getFieldType(int position) {
        if (types == null) {
            return null;
        }
        return types.get(position);
    }

    /**
     * Validate that the name, fields, and types of the index match
     * the table.  This also initializes the (transient) list of index fields in
     * indexFields, so that member must not be used in validate() itself.
     *
     * This method must only be called from the constructor.  It is not
     * synchronized and changes internal state.
     */
    private void validate() {

        TableImpl.validateIdentifier(name,
                                     TableImpl.MAX_NAME_LENGTH,
                                     "Index names");

        IndexField multiKeyField = null;

        if (newFields.isEmpty()) {
            throw new IllegalCommandException(
                "Index requires at least one field");
        }

        assert indexFields == null;

        indexFields = new ArrayList<>(newFields.size());

        int position = 0;
        for (String field : newFields) {

            if (field == null || field.length() == 0) {
                throw new IllegalCommandException(
                    "Invalid (null or empty) index field name");
            }

            IndexField ifield = new IndexField(table, field,
                                               getFieldType(position),
                                               position++);

            if ((ifield.isPoint() || ifield.isGeometry()) && geoFieldPos >= 0) {
                throw new IllegalCommandException(
                    "An index cannot index more than one " +
                    "geometry/point fields");
            }

            if (ifield.isPoint()) {
                isPointIndex = true;
                geoFieldPos = ifield.getPosition();

            } else if (ifield.isGeometry()) {

                if (multiKeyField != null) {
                    throw new IllegalCommandException(
                        "An index cannot index both a geometry field and " +
                        "an array or map field");
                }

                geoFieldPos = ifield.getPosition();
            }

            /*
             * The check for multiKey needs to consider all fields as well as
             * fields that reference into complex types.  A multiKey field may
             * occur at any point in the navigation path (first, interior,
             * leaf).
             *
             * The call to isMultiKey() will set the multiKey state in
             * the IndexField.
             *
             * Allow more than one multiKey field in a single index IFF they
             * are in the same object (map or array).
             */
            validateIndexField(ifield, true);

            if (ifield.isMultiKey() &&
                geoFieldPos >= 0 &&
                (ifield.isGeometry() || geoFieldPos != ifield.getPosition())) {
                throw new IllegalCommandException(
                    "An index cannot index both a geometry field and " +
                    "an array or map field");
            }

            if (ifield.isMultiKey()) {
                multiKeyField = ifield.getMultiKeyField();
            }

            if (indexFields.contains(ifield)) {
                throw new IllegalCommandException(
                    "Index already contains the field: " + field);
            }

            indexFields.add(ifield);
        }

        checkIsMapBothIndex();

        assert newFields.size() == indexFields.size();

        /*
         * initialize transient RecordDef representing the IndexKeyImpl
         * definition.
         */
        indexKeyDef = createIndexKeyDef();
        indexEntryDef = createIndexEntryDef();

        table.checkForDuplicateIndex(this);
    }

    /**
     * Validates the given index path expression (ipath) and returns its data
     * type (which must be one of the indexable atomic types).
     *
     * This call has a side effect of setting the multiKey state in the
     * IndexField so that the lookup need not be done twice.
     */
    private void validateIndexField(IndexField ipath, boolean isNewIndex) {

        int numSteps = ipath.numSteps();
        boolean firstMultiKeyPath = false;
        boolean seenMultikeyStep = false;

        if (numSteps == 0) {
            assert(ipath.getFunction() != null &&
                   ipath.getFunction().isRowProperty());
            ipath.setType(null);
            return;
        }

        /* Check if this is a multikey index path by looking for the right-most
         * multikey step, if any. Notice that pre 4.2 array paths may not have
         * the [] step, so they will not be identified here as multikey; they
         * are handled in the while-loop below. */
        for (int stepIdx = numSteps - 1; stepIdx >= 0; --stepIdx) {

            StepInfo si = ipath.getStepInfo(stepIdx);

            if (!si.isMultiKeyStep()) {
                continue;
            }

            if (si.isKeys() && stepIdx < numSteps - 1) {
                throw new IllegalCommandException(
                    "Invalid index field definition : " + ipath + "\n" +
                    "keys() step is not the last one");
            }

            if (seenMultikeyStep) {
                isNestedMultiKeyIndex = true;
                break;
            }
            seenMultikeyStep = true;

            ipath.setMultiKeyPath(stepIdx);
            ipath.getLastStepInfo().setFieldPos(ipath.position);

            if (!isTextIndex() && multiKeyPaths == null) {

                firstMultiKeyPath = true;
                multiKeyPaths = new IndexField(ipath.getFieldMap(), null, 0);

                for (int i = 0; i < stepIdx; ++i) {
                    StepInfo si2 = ipath.getStepInfo(i);
                    multiKeyPaths.addStepInfo(new StepInfo(si2));
                }

                /* Change a keys() step to a values() step in order to
                 * simplify mergeMultiKeyPath() and extractIndexKeys() */
                if (si.isKeys()) {
                    si = new StepInfo(TableImpl.VALUES, false);
                    si.setKeysPos(ipath.position);
                    multiKeyPaths.addStepInfo(si);
                } else {
                    si = new StepInfo(si);
                    multiKeyPaths.addStepInfo(si);
                }

                if (stepIdx + 1 < numSteps) {

                    ArrayList<StepInfo> branch = new ArrayList<StepInfo>(4);

                    for (int i = stepIdx + 1; i < numSteps; ++i) {
                        si = new StepInfo(ipath.getStepInfo(i));
                        branch.add(si);
                    }

                    multiKeyPaths.getStepInfo(stepIdx).addBranch(branch);

                } else if (ipath.getFunction() != null) {

                    si.setFieldPos(-1);
                    si.setKeysPos(-1);
                    functionalFieldPositions = new ArrayList<Integer>(4);
                    functionalFieldPositions.add(ipath.getPosition());
                }
            }
        }

        seenMultikeyStep = false;
        int stepIdx = 0;
        FieldDefImpl stepDef = ipath.getFirstDef();

        if (stepDef == null) {
            if (ipath.isJsonCollection()) {
                stepDef = FieldDefImpl.Constants.jsonDef;
                /*
                 * check for MR counter, which is not allowed
                 * MR counters must be top-level fields in JSON collections
                 */
                if (table.hasJsonCollectionMRCounters() &&
                    ipath.numSteps() == 1) {
                    if (table.getJsonCollectionMRCounters().containsKey(
                            ipath.toString())) {
                        throw new IllegalCommandException(
                            "Indexes are not supported on " +
                            "MR counters: " + ipath.toString());
                    }
                }
            } else {
                throw new IllegalCommandException(
                    "Invalid index field definition : " + ipath + "\n" +
                    "There is no field named " + ipath.getStep(0));
            }
        }

        /* Check if the path is an mr_counter */
        if (stepDef.isJson()) {
            JsonDefImpl jsonDef = (JsonDefImpl)stepDef.asJson();
            if (jsonDef.hasJsonMRCounter()) {
                String[] splitPath = ipath.toString().split("\\.", 2);
                if (splitPath.length == 2 &&
                    jsonDef.allMRCounterFieldsInternal().
                        containsKey(splitPath[1])) {
                    throw new IllegalCommandException(
                        "Indexes are not supported on " +
                        "MR counters: " + ipath.toString());
                }
            }
        }

        while (stepIdx < numSteps) {

            /*
             * TODO: Prevent any path through these types from
             * participating in a text index, until the text index
             * implementation supports them correctly.
             */
            if (isTextIndex() &&
                (stepDef.isBinary() ||
                 stepDef.isFixedBinary() ||
                 stepDef.isEnum())) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Fields of type " + stepDef.getType() +
                        " cannot participate in a FULLTEXT index.");
            }

            if (stepDef.isJson()) {

                if (!isTextIndex() && firstMultiKeyPath && !ipath.isJson()) {
                    for (int i = stepIdx+1; i < multiKeyPaths.numSteps(); ++i) {
                        StepInfo si = multiKeyPaths.getStepInfo(i);
                        if (si.getKind() == StepKind.REC_FIELD) {
                            si.setKind(StepKind.MAP_FIELD);
                        }
                    }
                }

                ipath.setIsJson();
                stepIdx++;

                if (ipath.getJsonFieldPath() == null) {
                    ipath.setJsonFieldPath(stepIdx);
                }

                if (stepIdx >= numSteps) {
                    break;
                }

                if (ipath.isMultiKeyStep(stepIdx)) {

                    ipath.setIsJsonMultiKey();

                    if (ipath.isKeysStep(stepIdx)) {
                        assert(ipath.getMultiKeyStepPos() == stepIdx);
                        if (getFieldType(ipath.position) != null) {
                            throw new IllegalCommandException(
                                "No type declarion is allowed after keys() " +
                                "step");
                        }
                        ipath.setIsMapKeys();
                        ipath.declaredType = FieldDefImpl.Constants.stringDef;
                        isMultiKeyMapIndex = true;

                    } else if (ipath.isValuesStep(stepIdx)) {
                        assert(ipath.getMultiKeyStepPos() >= stepIdx);
                        if (ipath.getMultiKeyStepPos() == stepIdx) {
                            ipath.setIsMapValues();
                            isMultiKeyMapIndex = true;
                        }

                    } else if (ipath.isBracketsStep(stepIdx)) {
                        assert(ipath.getMultiKeyStepPos() >= stepIdx);
                        if (ipath.getMultiKeyStepPos() == stepIdx) {
                            ipath.setIsArrayValues();
                        }
                    }

                } else {
                    ipath.setIsMapFieldStep(stepIdx);
                }

            } else if (stepDef.isRecord()) {

                ++stepIdx;
                if (stepIdx >= numSteps) {
                    break;
                }

                String step = ipath.getStep(stepIdx);
                stepDef = ((RecordDefImpl)stepDef).getFieldDef(step);

                if (stepDef == null) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "There is no field named \"" + step + "\" after " +
                        "path " + ipath.getPathName(stepIdx - 1, false));
                }

            } else if (stepDef.isArray()) {

                if (ipath.isMultiKey() &&
                    stepIdx > ipath.getMultiKeyStepPos()) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "The path indexes an array after the last multikey " +
                        " step.\nstepIdx = " + stepIdx + " multikeyPos = " +
                        ipath.getMultiKeyStepPos());
                }

                if (seenMultikeyStep) {
                    isNestedMultiKeyIndex = true;
                }
                seenMultikeyStep = true;

                /*
                 * If there is a next step and it is [], consume it.
                 *
                 * Else, if we are creating a new index, throw an exception,
                 * because as of version 4.2 the use of [] is mandatory for
                 * array indexes.
                 *
                 * Otherwise, we must be reading from a store created prior
                 * to v4.2. In this case, the [] may be missing. We must add
                 * it to the index path, because it is expected to be there
                 * in other parts of the code (for example, the index matching
                 * done by query processor).
                 */
                if (stepIdx + 1 < numSteps &&
                    ipath.getStep(stepIdx + 1).equals(TableImpl.BRACKETS)) {
                    ++stepIdx;

                } else if (isNewIndex) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Missing [] after array field ");

                } else {
                    ++stepIdx;
                    ++numSteps;
                    ipath.add(stepIdx, TableImpl.BRACKETS, false);
                    ipath.setMultiKeyPath(stepIdx);
                }

                stepDef = ((ArrayDefImpl)stepDef).getElement();

            } else if (stepDef.isMap()) {

                ++stepIdx;

                if (stepIdx >= numSteps) {
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Can not index a map as a whole; use " +
                        ".values() to index the elements of the map or " +
                        ".keys() to index the keys of the map");
                }

                switch (ipath.getStepKind(stepIdx)) {
                case VALUES: {
                    if (stepIdx - 1 > ipath.getMultiKeyStepPos()) {
                        throw new IllegalCommandException(
                            "Invalid index field definition : " + ipath + "\n" +
                            "The path indexes a map after the last multikey " +
                            "step.");
                    }

                    if (stepIdx == ipath.getMultiKeyStepPos()) {
                        ipath.setIsMapValues();
                        isMultiKeyMapIndex = true;
                    }

                    if (seenMultikeyStep) {
                        isNestedMultiKeyIndex = true;
                    }
                    seenMultikeyStep = true;

                    stepDef = ((MapDefImpl)stepDef).getElement();
                    break;
                }
                case KEYS: {
                    if (stepIdx - 1 > ipath.getMultiKeyStepPos()) {
                        throw new IllegalCommandException(
                            "Invalid index field definition : " + ipath + "\n" +
                            "The path indexes a map after the last multikey " +
                            "step.");
                    }

                    if (stepIdx == ipath.getMultiKeyStepPos()) {
                        ipath.setIsMapKeys();
                        isMultiKeyMapIndex = true;
                    }

                    if (seenMultikeyStep) {
                        isNestedMultiKeyIndex = true;
                    }
                    seenMultikeyStep = true;

                    stepDef = FieldDefImpl.Constants.stringDef;
                    assert(stepIdx == numSteps - 1);
                    break;
                }
                case MAP_FIELD:
                case REC_FIELD: {
                    ipath.setIsMapFieldStep(stepIdx);
                    stepDef = ((MapDefImpl)stepDef).getElement();
                    break;
                }
                case BRACKETS:
                    throw new IllegalCommandException(
                        "Invalid index field definition : " + ipath + "\n" +
                        "Can not use [] after a map field");
                }

            } else {
                ++stepIdx;
                if (stepIdx >= numSteps) {
                    break;
                }

                String step = ipath.getStep(stepIdx);
                throw new IllegalCommandException(
                    "Invalid index field definition : " + ipath + "\n" +
                    "There is no field named \"" + step + "\" after " +
                    "path " + ipath.getPathName(stepIdx - 1, false));
            }
        }

        if (!stepDef.isValidIndexField()) {

            final String warnMsg = "Invalid index field definition : " +
                ipath + "\n" + "Cannot index values of type " + stepDef;

            throw new IllegalCommandException(warnMsg);
        }

        /*
         * If NULLs are allowed in index key, the nullablity of 2 kinds of
         * field below is true:
         *  1. The complex field or nested field of complex type.
         *  2. The simple field is nullable and not a primary key field.
         */
        boolean nullable;
        if (ipath.isJsonCollection()) {
            nullable = !table.isKeyComponent(ipath.getStep(0));
        } else {
            nullable =
                (ipath.isComplex() ||
                 (!table.isKeyComponent(ipath.getStep(0)) &&
                  ipath.getFieldMap().getFieldMapEntry(ipath.getStep(0))
                  .isNullable()));
        }
        if (!this.supportsSpecialValues()) {
            nullable = false;
        }
        ipath.setNullable(nullable);

        if (!isTextIndex() && !firstMultiKeyPath && ipath.isMultiKey()) {
            mergeMultiKeyPath(ipath);
        }

        /*
         * Specific types in index declarations are only allowed for JSON, and
         * in this path, the IndexField's typeDef becomes that of the specified
         * type, and not JSON.
         */
        if (ipath.getDeclaredType() != null) {

            if (!stepDef.isJson()) {

                final String warnMsg = "Invalid index field definition: " +
                    ipath + "\n" + "Specific types are only allowed for " +
                    "JSON data types.";

                throw new IllegalCommandException(warnMsg);
            }

            ipath.compileFunctionalField(ipath.getDeclaredType());
            ipath.setType(ipath.getDeclaredType());
            return;
        }

        if (stepDef.isJson()) {

            boolean noAnnotations = false;
            if (this.newAnnotations == null ||
                this.newAnnotations.size() == 0 ||
                this.newAnnotations.get(ipath.toString()) == null) {

                noAnnotations = true;
            }

            if (noAnnotations) {

                final String warnMsg = "Invalid index field definition: " +
                    ipath + " [stepDef.isJson, declaredType is null, " +
                    " and no annotations specified]\n" +
                    "Must specify data type for JSON index field.";

                throw new IllegalCommandException(warnMsg);
            }

            /* Process the data types in the specified annotations. */
            try {
                final String stepDefAnnotation =
                    this.newAnnotations.get(ipath.toString());

                final JsonParser parser =
                    ESJsonUtil.createParser(stepDefAnnotation);
                final Map<String, Object> annotationMap =
                    ESJsonUtil.parseAsMap(parser);

                /* Protect against invalid annotation entered by user. */
                String fieldType = null;
                final Object fieldTypeObj = annotationMap.get("type");
                if (fieldTypeObj != null) {
                    fieldType = fieldTypeObj.toString();
                }

                if ("string".equals(fieldType)) {
                    ipath.setType(FieldDefImpl.Constants.stringDef);
                    return;
                }

                if ("integer".equals(fieldType)) {
                    ipath.setType(FieldDefImpl.Constants.integerDef);
                    return;
                }

                if ("long".equals(fieldType)) {
                    ipath.setType(FieldDefImpl.Constants.longDef);
                    return;
                }

                if ("double".equals(fieldType)) {
                    ipath.setType(FieldDefImpl.Constants.doubleDef);
                    return;
                }

                if ("boolean".equals(fieldType)) {
                    ipath.setType(FieldDefImpl.Constants.booleanDef);
                    return;
                }

                if ("date".equals(fieldType)) {

                    final Object precisionObjType =
                        annotationMap.get("precision");

                    final String precisionType = (precisionObjType != null ?
                        precisionObjType.toString().toLowerCase() : null);

                    int datePrecision = 0;

                    if (precisionType != null) {
                        if ("millis".equals(precisionType)) {
                            datePrecision = 6;
                        } else if ("nanos".equals(precisionType)) {
                            datePrecision = 9;
                        }
                    }

                    ipath.setType(FieldDefImpl.Constants.
                                  timestampDefs[datePrecision]);
                    return;
                }

                if ("json".equals(fieldType)) {

                    ipath.type = new JsonDefImpl();
                    return;
                }

                final String warnMsg = "Invalid index field definition [" +
                    ipath + "]. Must specify data type for JSON index field.";

                throw new IllegalCommandException(warnMsg);

            } catch (IOException e) {

                final String warnMsg = "Invalid index field definition: " +
                    ipath + "[cause: IOException]\n" +
                    "Must specify valid, parsable data type annotations " +
                    "for JSON index field.";

                throw new IllegalCommandException(warnMsg, e);
            }
        }

        ipath.compileFunctionalField(stepDef);
        ipath.setType(stepDef);
    }

    /*
     * Merge the current multikey index path into this.multiKeyPaths.
     */
    void mergeMultiKeyPath(IndexField ipath) {

        int numSteps = ipath.numSteps();
        int multiKeyStepPos = ipath.getMultiKeyStepPos();
        ArrayList<StepInfo> branch = null;
        int p1 = 0;
        int p2 = 0;

        if (multiKeyStepPos != multiKeyPaths.numSteps() - 1) {
            isNestedMultiKeyIndex = true;
        }

        if (multiKeyStepPos < numSteps - 1) {
            branch = new ArrayList<StepInfo>(4);
            for (int p = multiKeyStepPos + 1; p < numSteps; ++p) {
                branch.add(ipath.getStepInfo(p));
            }
        } else if (ipath.getFunction() != null) {
            if (functionalFieldPositions == null) {
                functionalFieldPositions = new ArrayList<Integer>(4);
            }
            functionalFieldPositions.add(ipath.getPosition());
        }

        for (;
             p1 < multiKeyPaths.numSteps() && p2 <= multiKeyStepPos;
             ++p1, ++p2) {

            StepInfo si1 = multiKeyPaths.getStepInfo(p1);
            StepInfo si2 = ipath.getStepInfo(p2);

            if (si1.kind == si2.kind) {
                if ((si1.kind == StepKind.MAP_FIELD &&
                     !si1.step.equals(si2.step)) ||
                    (si1.kind == StepKind.REC_FIELD &&
                     !si1.step.equalsIgnoreCase(si2.step))) {

                    throw new IllegalCommandException(
                        "Invalid index path " + ipath.getPathName() +
                        " : There is no single path expression the reaches " +
                        "or crosses all the indexed arrays/maps");
                }

                if (si1.kind == StepKind.VALUES && si1.keysPos >= 0) {

                    /* mark ipath as a values() path for which there is also
                     * corresponding keys() path in the index definition */
                    si2.setKeysPos(si1.keysPos);

                    if (p2 == ipath.numSteps() - 1 &&
                        ipath.getFunction() == null) {
                        si1.setFieldPos(ipath.getPosition());
                    }
                }

                continue;
            }

            switch (si2.kind) {
            case KEYS: {
                switch(si1.kind) {
                case VALUES:
                    si1.setKeysPos(ipath.getPosition());
                    return;
                case BRACKETS:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both an array and a map");
                default:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot apply a keys() step to a record (or " +
                        "map viewed as a record)");
                }
            }
            case VALUES: {
                switch (si1.kind) {
                case BRACKETS:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both an array and a map");
                default:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both a record (or " +
                        "map viewed as record) and a map");
                }
            }
            case BRACKETS: {
                switch (si1.kind) {
                case VALUES:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both a map and an array");
                case MAP_FIELD:
                case REC_FIELD:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both a record (or map " +
                        "viewed as a record) and an array");
                default:
                    assert(false);
                    return;
                }
            }
            case MAP_FIELD:
            case REC_FIELD:
                switch (si1.kind) {
                case BRACKETS: {
                    if (p1 == ipath.numSteps() - 1 && si1.branches == null) {
                        throw new IllegalCommandException(
                            "Conflicting index path " + ipath.getPathName() +
                            " : Cannot index a field as both an array of " +
                            "atomic values and an array of complex values");
                    }
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both an array and a " +
                        "record (or map viewed as a record)");
                }
                default:
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a field as both a map and a record");
                }
            }
        }

        /* If we reached at the end of this.multiKeyPaths ...*/
        if (p1 == multiKeyPaths.numSteps()) {

            StepInfo si1 = multiKeyPaths.getStepInfo(p1-1);

            /* If we also reached at the end of ipath.multiley_path_prefix ...
             * examples: (a.b.c[].d, a.b.c[])
             *           (a.keys(), a.values()) */
            if (p2 > multiKeyStepPos) {

                StepInfo si2 = ipath.getStepInfo(p2-1);

                if (si2.isBrackets()) {
                    if (si1.branches != null && branch == null ||
                        si1.branches == null && branch != null) {
                        throw new IllegalCommandException(
                            "Conflicting index path " + ipath.getPathName() +
                            " : Cannot index an array as both an array of " +
                            "atomic values and an array of complex values");
                    }
                } else {
                    assert(si2.isValues() && si1.isValues());

                    if (si1.keysPos >= 0 &&
                        si1.fieldPos() < 0 &&
                        si1.branches == null) {

                        if (branch != null) {
                            si1.addBranch(branch);
                        }

                        return;
                    }

                    if (si1.branches != null && branch == null ||
                        si1.branches == null && branch != null) {
                        throw new IllegalCommandException(
                            "Conflicting index path " + ipath.getPathName() +
                            " : Cannot index a map as both a map of " +
                            "atomic values and a map of complex values");
                    }
                }

                if (branch != null) {
                    si1.addBranch(branch);
                }
                return;
            }

            /* We reached at the end of this.multiKeyPaths, but not at the
             * end of ipath.multiley_path_prefix */
            StepInfo si2 = ipath.getStepInfo(p2);

            if (si1.isBrackets()) {
                if (si1.fieldPos() >= 0) {
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index an array as both an array of " +
                        "atomic values and an array of complex values");
                }

                assert(si1.branches != null);

                if (si2.isBrackets()) {
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index an array as both an array of " +
                        "records/maps and an array of arrays");
                }

            } else {
                assert(si1.isValues());
                if (si1.fieldPos() >= 0) {
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a map as both a map of " +
                        "atomic values and a map of complex values");
                }

                if (si2.isBrackets() && si1.branches != null) {
                    throw new IllegalCommandException(
                        "Conflicting index path " + ipath.getPathName() +
                        " : Cannot index a map as both a map of " +
                        "records/maps and a map of arrays");
                }
            }

            /* Add the remaining steps of ipath to this.multiKeyPaths */
            for (; p2 <= multiKeyStepPos; ++p2) {

                si2 = ipath.getStepInfo(p2);

                if (si2.isKeys()) {
                    StepInfo keys = new StepInfo("values()", false);
                    keys.setKeysPos(ipath.getPosition());
                    multiKeyPaths.addStepInfo(keys);
                } else {
                    multiKeyPaths.addStepInfo(new StepInfo(si2));
                }
            }

            if (branch != null) {
                multiKeyPaths.getStepInfo(multiKeyPaths.numSteps() - 1).
                              addBranch(branch);
            }

            return;
        }

        /* We didn't reach at the end of this.multiKeyPaths, so we reached
         * the end of ipath.multiley_path_prefix */
        assert(p1 < multiKeyPaths.numSteps());

        StepInfo si1 = multiKeyPaths.getStepInfo(p1);
        StepInfo si2 = ipath.getStepInfo(p2-1);

        /* If there is nothing after ipath.multiley_path_prefix */
        if (branch == null) {
            if (si2.isBrackets()) {
                throw new IllegalCommandException(
                    "Conflicting index path " + ipath.getPathName() +
                    " : Cannot index an array as both an array of " +
                    "atomic values and an array of complex values");
            }
            else if (si2.isValues()) {
                throw new IllegalCommandException(
                    "Conflicting index path " + ipath.getPathName() +
                    " : Cannot index a map as both a map of " +
                    "atomic values and a map of complex values");
            }

            /* ipath is a keys() path. This has been handled already in the
             * for-loop above */
            return;
        }

        if (si1.isBrackets()) {
            throw new IllegalCommandException(
                "Conflicting index path " + ipath.getPathName() +
                " : the input to its name_path suffix is an array");
        }

        multiKeyPaths.getStepInfo(p1-1).addBranch(branch);
    }

    private void checkIsMapBothIndex() {

        if (multiKeyPaths == null || isNestedMultiKeyIndex) {
            return;
        }

        StepInfo si = multiKeyPaths.getLastStepInfo();

        if (si.keysPos < 0) {
            return;
        }

        if (si.fieldPos() >= 0) {
            if (si.keysPos < si.fieldPos()) {
                isMapBothIndex = true;
            }
        } else {
            if (si.branches != null) {
                for (ArrayList<StepInfo> branch : si.branches) {
                    StepInfo si2 = branch.get(branch.size() - 1);
                    if (si2.fieldPos() < si.keysPos) {
                        return;
                    }
                }

                isMapBothIndex = true;
                return;
            }

            if (functionalFieldPositions != null) {
                for (int pos : functionalFieldPositions) {
                    if (pos < si.keysPos) {
                        return;
                    }
                }

                isMapBothIndex = true;
                return;
            }
        }
    }

    /**
     * Creates a binary index key from the binary key and data of a row.
     *
     * @param key the key bytes
     *
     * @param data the row's data bytes
     *
     * @param keyOnly true if the index only uses key fields.  This
     * optimizes deserialization.
     *
     * @return the byte[] serialization of an index key or null if there
     * is no entry associated with the row, or the row does not match a
     * table record.
     *
     * While not likely it is possible that the record is not actually  a
     * table record and the key pattern happens to match.  Such records
     * will fail to be deserialized and throw an exception.  Rather than
     * treating this as an error, silently ignore it.
     *
     * TODO: maybe make this faster.  Right now it turns the key and data
     * into a Row and extracts from that object which is a relatively
     * expensive operation, including full Avro deserialization.
     */
    public byte[] extractIndexKey(
        byte[] key,
        byte[] data,
        long creationTime,
        long modTime,
        long expTime,
        int size,
        boolean keyOnly) {

        final RowImpl row = twoRowCache.get().getRow(table, key, data, keyOnly);
        if (row == null) {
            return null;
        }

        row.setCreationTime(creationTime);
        row.setModificationTime(modTime);
        row.setExpirationTime(expTime);
        row.setStorageSize(size);

        return extractIndexKey(row);
    }

    public byte[] extractIndexKey(RowImpl row) {

        MultiKeyExtractionContext ctx = keyExtractionContextCache.get();
        ctx.init(row, multiKeyPaths, numFields(), 1);
        Object[] keyValues = ctx.keyValues;

        for (IndexField field : getIndexFields()) {

            Function func = field.getFunction();
            if (func != null && func.isRowProperty()) {
                continue;
            }

            FieldValueImpl fv = row.evaluateScalarPath(field, 0);
            keyValues[field.position] = fv;
        }

        return serializeIndexKey(ctx);
    }

    /**
     * Extracts multiple index keys from a single record.  This is used if
     * one of the indexed fields is an array.  Only one array is allowed
     * in an index.
     *
     * @param key the key bytes
     *
     * @param data the row's data bytes
     *
     * @param keyOnly true if the index only uses key fields.  This
     * optimizes deserialization.
     *
     * @return a List of byte[] serializations of index keys or null if there
     * is no entry associated with the row, or the row does not match a
     * table record.  This list may contain duplicate values.  The caller is
     * responsible for handling duplicates (and it does).
     *
     * While not likely it is possible that the record is not actually  a
     * table record and the key pattern happens to match.  Such records
     * will fail to be deserialized and throw an exception.  Rather than
     * treating this as an error, silently ignore it.
     *
     * TODO: can this be done without reserializing to Row?  It'd be
     * faster but more complex.
     *
     * 1.  Deserialize to RowImpl
     * 2.  Find the map or array value and get its size
     * 3.  for each map or array entry, serialize a key using that entry
     */
    public List<byte[]> extractIndexKeys(
        byte[] key,
        byte[] data,
        long creationTime,
        long modTime,
        long expTime,
        int size,
        boolean keyOnly,
        int maxKeysPerRow) {

        final RowImpl row = twoRowCache.get().getRow(table, key, data, keyOnly);
        if (row == null) {
            return null;
        }

        row.setCreationTime(creationTime);
        row.setModificationTime(modTime);
        row.setExpirationTime(expTime);
        row.setStorageSize(size);
        return extractIndexKeys(row, maxKeysPerRow);
    }

    private static class MultiKeyExtractionContext {

        final EmptyValueImpl empty = EmptyValueImpl.getInstance();

        IndexField paths;

        int pathPos;

        RowImpl row;

        FieldValueImpl value;

        Object[] keyValues = new Object[16];

        ArrayList<byte[]> binaryKeys;

        int maxKeysPerRow;

        boolean haveEmptyEntry;

        final FieldValueImpl[] externalVars = new FieldValueImpl[1];

        void init(
            RowImpl inRow,
            IndexField mkpaths,
            int numFields,
            int maxKeys) {

            paths = mkpaths;
            this.row = inRow;
            value = inRow;
            pathPos = 0;
            binaryKeys = new ArrayList<byte[]>(64);
            if (keyValues.length < numFields) {
                keyValues = new Object[numFields];
            }
            for (int i = 0; i < keyValues.length; ++i) {
                keyValues[i] = null;
            }
            this.maxKeysPerRow = maxKeys;
        }

        void addKey(byte[] key) {

            if (binaryKeys.size() >= maxKeysPerRow) {
                throw new IllegalArgumentException(
                    "Number of index keys per row exceeds the limit of " +
                    maxKeysPerRow + " keys");
            }
            binaryKeys.add(key);
        }
    }

    public List<byte[]> extractIndexKeys(RowImpl row, int maxKeysPerRow) {

        MultiKeyExtractionContext ctx = keyExtractionContextCache.get();
        ctx.init(row, multiKeyPaths, numFields(), maxKeysPerRow);
        Object[] keyValues = ctx.keyValues;

        /* Evaluate the non-multikey paths first */
        for (IndexField field : getIndexFields()) {

            if (field.isMultiKey()) {
                continue;
            }

            Function func = field.getFunction();
            if (func != null && func.isRowProperty()) {
                continue;
            }

            keyValues[field.position] = row.evaluateScalarPath(field, 0);
        }

        if (isGeometryIndex()) {
            return extractGeometryIndexKeys(ctx);
        }

        //System.out.println("XXXX extractIndexKeys for index: " +
        //                   getName() + " multiKeyPaths = " +
        //                   multiKeyPaths.getPathName(true) +
        //                   " multiKeyPaths num steps = " + multiKeyPaths.numSteps());

        extractIndexKeys(ctx);
        return ctx.binaryKeys;
    }

    public void extractIndexKeys(MultiKeyExtractionContext ctx) {

        int numSteps = ctx.paths.numSteps();
        FieldValueImpl ctxVal = ctx.value;
        StepInfo si = ctx.paths.getStepInfo(ctx.pathPos);
        ArrayList<ArrayList<StepInfo>> branches;
        ArrayList<Integer> funcPositions = functionalFieldPositions;
        FieldValueImpl bval;

        if (ctxVal.isEMPTY() || ctxVal.isNull()) {

            switch (si.getKind()) {
            case REC_FIELD:
            case MAP_FIELD: {
                ++ctx.pathPos;
                /* Keep walking down the data guide. We are sure to find a
                 * BRACKETS or VALUES step. */
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;
            }
            case BRACKETS:
            case VALUES: {
                branches = si.getBranches();

                if (branches != null) {
                    for (ArrayList<StepInfo> branch : branches) {
                        StepInfo leaf = branch.get(branch.size() - 1);
                        assert(leaf.fieldPos() >= 0);
                        ctx.keyValues[leaf.fieldPos()] = ctxVal;
                    }
                }

                if (ctx.pathPos == numSteps - 1) {
                    if (branches == null) {
                        if (si.getKind() == StepKind.BRACKETS) {
                            if (si.fieldPos() >= 0) {
                                ctx.keyValues[si.fieldPos()] = ctxVal;
                            }
                            if (funcPositions != null) {
                                for (int pos : funcPositions) {
                                    ctx.keyValues[pos] = ctxVal;
                                }
                            }
                        } else {
                            if (si.fieldPos() >= 0) {
                                ctx.keyValues[si.fieldPos()] = ctxVal;
                            }
                            if (si.keysPos >= 0) {
                                ctx.keyValues[si.keysPos] = ctxVal;
                            }
                            if (funcPositions != null) {
                                for (int pos : funcPositions) {
                                    ctx.keyValues[pos] = ctxVal;
                                }
                            }
                        }
                    } else if (si.keysPos >= 0) {
                        ctx.keyValues[si.keysPos] = ctxVal;
                    }

                    byte[] key = serializeIndexKey(ctx);
                    if (key != null) {
                        ctx.addKey(key);
                    }
                    return;
                }

                if (si.keysPos >= 0) {
                    ctx.keyValues[si.keysPos] = ctxVal;
                }

                ++ctx.pathPos;
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;
            }
            case KEYS:
                assert(false);
                return;
            }
        }

        if (ctxVal.isAtomic()) {

            switch (si.getKind()) {
            case REC_FIELD:
            case MAP_FIELD:
                ++ctx.pathPos;
                ctx.value = ctx.empty;
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;
            case BRACKETS:
            case VALUES:
                branches = si.getBranches();

                if (branches != null) {
                    for (ArrayList<StepInfo> branch : branches) {
                        int fieldPos = branch.get(branch.size() - 1).fieldPos();
                        assert(fieldPos >= 0);
                        ctx.keyValues[fieldPos] = ctx.empty;
                    }
                }

                if (ctx.pathPos == numSteps - 1) {
                    if (branches == null) {
                        if (si.getKind() == StepKind.BRACKETS) {
                            if (si.fieldPos() >= 0) {
                                ctx.keyValues[si.fieldPos()] = ctxVal;
                            }
                            if (funcPositions != null) {
                                for (int pos : funcPositions) {
                                    ctx.keyValues[pos] = ctxVal;
                                }
                            }
                        } else {
                            if (si.fieldPos() >= 0) {
                                ctx.keyValues[si.fieldPos()] = ctx.empty;
                            }
                            if (si.keysPos >= 0) {
                                ctx.keyValues[si.keysPos] = ctx.empty;
                            }
                            if (funcPositions != null) {
                                for (int pos : funcPositions) {
                                    ctx.keyValues[pos] = ctxVal;
                                }
                            }

                        }
                    } else if (si.keysPos >= 0) {
                        ctx.keyValues[si.keysPos] = ctx.empty;
                    }

                    byte[] key = serializeIndexKey(ctx);
                    if (key != null) {
                        ctx.addKey(key);
                    }
                    return;
                }

                if (si.keysPos >= 0) {
                    ctx.keyValues[si.keysPos] = ctx.empty;
                }

                ++ctx.pathPos;
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;

            case KEYS:
                assert(false);
                return;
            }
        }

        if (ctxVal.isRecord()) {
            RecordValueImpl rec = (RecordValueImpl)ctxVal;

            switch (si.getKind()) {
            case REC_FIELD:
            case MAP_FIELD: {
                String fname = si.getStep();
                FieldValueImpl fv;

                if (rec instanceof RowImpl &&
                    fname.equals(FuncRowMetadata.COL_NAME)) {
                    String metadataStr = ((RowImpl)rec).getRowMetadata();
                    if (metadataStr == null) {
                        // evaluate to EmptyValue when rowMetadata is null.
                        fv = EmptyValueImpl.getInstance();
                    } else {
                        fv = (FieldValueImpl) FieldValueFactory.
                            createValueFromJson(metadataStr);
                    }
                } else {
                    fv = rec.get(fname);
                }

                if (fv == null && !(rec instanceof JsonCollectionRowImpl)) {
                    throw new IllegalArgumentException(
                        "Unexpected null field value in path " + ctx.paths +
                        " at step " + fname + " in record :\n" + rec);
                }

                ++ctx.pathPos;
                ctx.value = (fv == null ? ctx.empty : fv);
                assert(ctx.pathPos < numSteps);
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;
            }
            case BRACKETS:
            case VALUES:
            case KEYS: {
                assert(false);
                return;
            }
            }
        }

        if (ctxVal.isMap()) {
            MapValueImpl map = (MapValueImpl)ctxVal;

            switch (si.getKind()) {
            case REC_FIELD:
            case MAP_FIELD: {
                String fname = si.getStep();
                FieldValueImpl fv = map.get(fname);

                ++ctx.pathPos;
                ctx.value = (fv == null ? ctx.empty : fv);
                assert(ctx.pathPos < numSteps);
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;
            }
            case BRACKETS: {
                branches = si.getBranches();
                if (branches != null) {
                    for (ArrayList<StepInfo> branch : branches) {
                        bval = ctxVal.evaluateScalarPath(branch, 0);
                        int fieldPos = branch.get(branch.size() - 1).fieldPos();
                        assert(fieldPos >= 0);
                        ctx.keyValues[fieldPos] = bval;
                    }
                }

                if (ctx.pathPos == numSteps - 1) {
                    if (si.fieldPos() >= 0) {
                        assert(branches == null);
                        ctx.keyValues[si.fieldPos()] = ctxVal;
                    }
                    if (funcPositions != null) {
                        for (int pos : funcPositions) {
                            ctx.keyValues[pos] = ctxVal;
                        }
                    }
                    byte[] key = serializeIndexKey(ctx);
                    if (key != null) {
                        ctx.addKey(key);
                    }
                    return;
                }

                ++ctx.pathPos;
                extractIndexKeys(ctx);
                --ctx.pathPos;
                return;
            }
            case VALUES: {
                if (map.size() == 0) {
                    ctx.value = ctx.empty;
                    extractIndexKeys(ctx);
                    return;
                }

                branches = si.getBranches();

                for (Map.Entry<String, FieldValue> entry : map.getMap().entrySet()) {

                    String mapkey = entry.getKey();
                    FieldValueImpl fv = (FieldValueImpl)entry.getValue();

                    if (branches != null) {
                        for (ArrayList<StepInfo> branch : branches) {
                            bval = fv.evaluateScalarPath(branch, 0);
                            int fieldPos = branch.get(branch.size() - 1).fieldPos();
                            assert(fieldPos >= 0);
                            ctx.keyValues[fieldPos] = bval;
                        }
                    }

                    if (ctx.pathPos == numSteps - 1) {
                        if (si.fieldPos() >= 0 || si.keysPos >= 0) {
                            if (si.fieldPos() >= 0) {
                                assert(branches == null);
                                ctx.keyValues[si.fieldPos()] = fv;
                            }
                            if (si.keysPos >= 0) {
                                ctx.keyValues[si.keysPos] = mapkey;
                            }
                        }
                        if (funcPositions != null) {
                            for (int pos : funcPositions) {
                                IndexField ifield = getIndexPath(pos);
                                if (ifield.isMapKeys()) {
                                    ctx.keyValues[pos] = mapkey;
                                } else {
                                    ctx.keyValues[pos] = fv;
                                }
                            }
                        }

                        byte[] key = serializeIndexKey(ctx);
                        if (key != null) {
                            ctx.addKey(key);
                        }
                        continue;
                    }

                    if (si.keysPos >= 0) {
                        ctx.keyValues[si.keysPos] = mapkey;
                    }

                    ++ctx.pathPos;
                    ctx.value = fv;
                    extractIndexKeys(ctx);
                    --ctx.pathPos;
                }
                return;
            }
            case KEYS: {
                assert(false);
                return;
            }
            }
        }

        if (ctxVal.isArray()) {
            ArrayValueImpl arr = (ArrayValueImpl)ctxVal;

            switch (si.getKind()) {
            case BRACKETS: {
                if (arr.size() == 0) {
                    ctx.value = ctx.empty;
                    extractIndexKeys(ctx);
                    return;
                }

                branches = si.getBranches();

                for (int i = 0; i < arr.size(); ++i) {

                    FieldValueImpl fv = arr.get(i);

                    if (branches != null) {
                        for (ArrayList<StepInfo> branch : branches) {
                            bval = fv.evaluateScalarPath(branch, 0);
                            int fieldPos = branch.get(branch.size() - 1).fieldPos();
                            assert(fieldPos >= 0);
                            ctx.keyValues[fieldPos] = bval;
                        }
                    }

                    if (ctx.pathPos == numSteps - 1) {
                        if (si.fieldPos() >= 0) {
                            ctx.keyValues[si.fieldPos()] = fv;
                        }
                        if (funcPositions != null) {
                            for (int pos : funcPositions) {
                                ctx.keyValues[pos] = fv;
                            }
                        }

                        byte[] key = serializeIndexKey(ctx);
                        if (key != null) {
                            ctx.addKey(key);
                        }
                        continue;
                    }

                    ++ctx.pathPos;
                    ctx.value = fv;
                    extractIndexKeys(ctx);
                    --ctx.pathPos;
                }
                return;
            }
            case REC_FIELD:
            case MAP_FIELD:
            case VALUES: {
                throw new IllegalArgumentException(
                    "Failed to extract index keys from row: Unexpected array " +
                    "value encountered. The index definition did not include " +
                    "[] after path " +
                    ctx.paths.getPathName(ctx.pathPos, false));
            }
            case KEYS:
                assert(false);
                return;
            }
        }

    }

    private List<byte[]> extractGeometryIndexKeys(
        MultiKeyExtractionContext ctx) {

        RowImpl row = ctx.row;
        Object[] keyValues = ctx.keyValues;
        ArrayList<byte[]> returnList;
        int geoPos = getGeoFieldPos();

        IndexField geomPath = getIndexPath(getGeoFieldPos());

        FieldValueImpl geomVal = row.evaluateScalarPath(geomPath, 0);

        if (geomVal.isNull() || geomVal.isEMPTY()) {
            keyValues[geoPos] = geomVal;
            byte[] serKey = serializeIndexKey(ctx);
            returnList = new ArrayList<byte[]>(1);
            returnList.add(serKey);

        } else {
            final List<String> geoHashes =
                CompilerAPI.getGeoUtils().
                hashGeometry(geomVal,
                             geoMaxCoveringCells(),
                             geoMinCoveringCells(),
                             geoMaxSplits(),
                             geoSplitRatio());

            returnList = new ArrayList<byte[]>(geoHashes.size());

            for (String hash : geoHashes) {

                keyValues[geoPos] = hash;
                byte[] serKey = serializeIndexKey(ctx);
                /*
                  System.out.println("Added geohash " + hash +
                  " to index " + getName() +
                  " for geometry " + mapOrArrayVal +
                  " index key = " +
                  PlanIter.printByteArray(serKey));
                */

                returnList.add(serKey);
            }
        }

        return returnList;
    }

    private byte[] serializeIndexKey(MultiKeyExtractionContext ctx) {

        boolean isEmptyEntry = true;

        for (Object keyValue : ctx.keyValues) {
            if (keyValue != EmptyValueImpl.getInstance()) {
                isEmptyEntry = false;
                break;
            }
        }

        if (isEmptyEntry) {
            if (ctx.haveEmptyEntry) {
                return null;
            }
            ctx.haveEmptyEntry = true;
        }

        TupleOutput out = null;

        try {
            out = new TupleOutput();

            for (IndexField field : getIndexFields()) {

                Object val = ctx.keyValues[field.position];

                if (val instanceof String) {

                    assert(field.isGeometry() || field.isMapKeys());

                    Function func = field.getFunction();

                    if (func == null) {
                        if (field.isNullable()) {
                            out.writeByte(NORMAL_VALUE_INDICATOR);
                        }
                        out.writeString((String)val);
                    } else {
                        FieldValueImpl fval = new StringValueImpl((String)val);
                        ctx.externalVars[0] = fval;

                        List<FieldValueImpl> res = ExprUtils.
                            computeConstPlan(null,
                                             null,
                                             options,
                                             field.functionalFieldPlan,
                                             field.numIterators,
                                             field.numRegisters,
                                             ctx.externalVars);
                        if (res.isEmpty()) {
                            fval = ctx.empty;
                        } else if (res.size() > 1) {
                            throw new IllegalArgumentException(
                                    "Indexed function " + func.getName() +
                                    " returns more than 1 items");
                        } else {
                            fval = res.get(0);
                        }

                        /*
                         * If the value is NULL or EMPTY, and the index does not
                         * support indexing of NULL/EMPTY, it is not possible to
                         * create a binary index key. In this case, this row has
                         * no entry for this index.
                         */
                        if (!this.supportsSpecialValues() &&
                            fval.isSpecialValue()) {
                            return null;
                        }

                        if (skipNulls && (fval.isNull() || fval.isEMPTY())) {
                            return null;
                        }

                        serializeValue(out, fval, field);
                    }

                } else {
                    FieldValueImpl fval = (FieldValueImpl)val;
                    Function func = field.getFunction();

                    if (func != null) {
                        switch (func.getCode()) {
                        case FN_CREATION_TIME:
                            long creationTime = ctx.row.getCreationTime();
                            fval = FieldDefImpl.Constants.timestampDefs[3].
                                createTimestamp(new Timestamp(creationTime));
                            break;
                        case FN_CREATION_TIME_MILLIS:
                            long creationTimeMillis = ctx.row.getCreationTime();
                            fval = FieldDefImpl.Constants.longDef.
                                createLong(creationTimeMillis);
                            break;
                        case FN_MOD_TIME:
                            long modTime = ctx.row.getLastModificationTime();
                            fval = FieldDefImpl.Constants.timestampDefs[3].
                                   createTimestamp(new Timestamp(modTime));
                            break;
                        case FN_EXPIRATION_TIME:
                            long expTime = ctx.row.getExpirationTime();
                            fval = FieldDefImpl.Constants.timestampDefs[0].
                                   createTimestamp(new Timestamp(expTime));
                            break;
                        case FN_EXPIRATION_TIME_MILLIS:
                            long expTimeMs = ctx.row.getExpirationTime();
                            fval = FieldDefImpl.Constants.longDef.
                                   createLong(expTimeMs);
                            break;
                        case FN_ROW_STORAGE_SIZE:
                            int size = ctx.row.getStorageSize();
                            fval = FieldDefImpl.Constants.integerDef.
                                   createInteger(size);
                            break;
                        default:
                            ctx.externalVars[0] = fval;

                            List<FieldValueImpl> res = ExprUtils.
                                computeConstPlan(null,
                                                 null,
                                                 options,
                                                 field.functionalFieldPlan,
                                                 field.numIterators,
                                                 field.numRegisters,
                                                 ctx.externalVars);
                            if (res.isEmpty()) {
                                fval = ctx.empty;
                            } else if (res.size() > 1) {
                                throw new IllegalArgumentException(
                                    "Indexed function " + func.getName() +
                                    " returns more than 1 items");
                            } else {
                                fval = res.get(0);
                            }
                        }
                    }

                    /*
                     * If the value is NULL or EMPTY, and the index does not
                     * support indexing of NULL/EMPTY, it is not possible to
                     * create a binary index key. In this case, this row has
                     * no entry for this index.
                     */
                    if (!this.supportsSpecialValues() &&
                        fval.isSpecialValue()) {
                        return null;
                    }

                    if (skipNulls && (fval.isNull() || fval.isEMPTY())) {
                        return null;
                    }

                    serializeValue(out, fval, field);
                }
            }

            return (out.size() != 0 ? out.toByteArray() : null);

        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Serializes a specific scalar FieldValue that is a component of an index.
     */
    private void serializeValue(
        TupleOutput out,
        FieldValueImpl val,
        IndexField indexField) {

        serializeValue(out, val, indexField, true, SerialVersion.CURRENT);
    }

    private void serializeValue(
        TupleOutput out,
        FieldValueImpl val,
        IndexField indexField,
        boolean fromRow,
        short opSerialVersion) {

        byte indicator = -1;

        /*
         * Handle special values (NULL, json null and EMPTY).
         * Note: for json indexes, indexField.isNullable() will always be true.
         */
        if (indexField.isNullable()) {
            indicator = getIndicator(val, opSerialVersion);
            if (indicator != NORMAL_VALUE_INDICATOR) {
                out.writeByte(indicator);
                return;
            }
        }

        if (fromRow && indexField.isPoint()) {

            final String geohash = CompilerAPI.getGeoUtils().hashPoint(val);

            /*
            System.out.println("Added geohash " + geohash +
                               " to index " + getName() +
                               " for geometry " + val);
            */
            out.writeByte(NORMAL_VALUE_INDICATOR);
            out.writeString(geohash);
            return;
        }

        if (indexField.isJson()) {

            /*
             * Validate the type of the field if the index is defined with a
             * constrained type. This only works for exact matches of specific
             * scalar types. Testing for a scalar vs array index field, when
             * supported, must be done in callers.
             */
            final FieldDefImpl declaredType =
                (indexField.getFunction() != null ?
                 indexField.getType() :
                 indexField.getDeclaredType());

            if (declaredType.isPrecise()) {

                /*
                 * Check that the value belongs to a subtype of the
                 * declared type, and cast to the declared type if needed.
                 */
                final FieldDefImpl valDef = val.getDefinition();
                FieldValueImpl castVal = null;
                if (valDef.isSubtype(declaredType)) {
                    castVal =
                        (FieldValueImpl) val.castToSuperType(declaredType);
                } else if (val.isInteger()) {
                    int baseval = ((IntegerValueImpl)val).get();
                    if (declaredType.isDouble()) {
                        double dbl = baseval;
                        if (baseval == (int)dbl) {
                            castVal = FieldDefImpl.Constants.doubleDef
                                .createDouble(dbl);
                        }
                    } else if (declaredType.isFloat()) {
                        float flt = baseval;
                        if (baseval == (int)flt) {
                            castVal = FieldDefImpl.Constants.floatDef
                                .createFloat(flt);
                        }
                    }
                } else if (val.isLong()) {
                    long baseval = ((LongValueImpl)val).get();
                    if (declaredType.isDouble()) {
                        double dbl = baseval;
                        if (baseval == (long)dbl) {
                            castVal = FieldDefImpl.Constants.doubleDef
                                .createDouble(dbl);
                        }
                    } else if (declaredType.isFloat()) {
                        float flt = baseval;
                        if (baseval == (long)flt) {
                            castVal = FieldDefImpl.Constants.floatDef
                                .createFloat(flt);
                        }
                    }
                }

                if (castVal != null) {
                    val = castVal;
                } else {
                    /* castVal == null means conversion above failed */
                    throw new IllegalArgumentException(
                        "Invalid type for JSON index field: " +
                        indexField + ". Type is " +
                        val.getType() + ", expected type is " +
                        declaredType.getType() + "\nvalue = " + val);
                }
            } else {
                switch (val.getType()) {
                case INTEGER:
                case LONG:
                case DOUBLE:
                case FLOAT:
                    val = val.castAsNumber();
                    indicator = NUMBER_INDICATOR;
                    break;
                case NUMBER:
                    indicator = NUMBER_INDICATOR;
                    break;
                case STRING:
                    indicator = STRING_INDICATOR;
                    break;
                case BOOLEAN:
                    indicator = BOOLEAN_INDICATOR;
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Type not supported in json indexes: " + val.getType());
                }
            }
        }

        if (indicator >= 0) {
            out.writeByte(indicator);
        }

        //TODO: remove this check after CRDT supports index.
        if (val.getDefinition().isMRCounter()) {
            throw new IllegalArgumentException("CRDT does not support index. ");
        }

        switch (val.getType()) {
        case INTEGER:
            out.writeSortedPackedInt(val.asInteger().get());
            break;
        case STRING:
            FieldDefImpl def = indexField.getType();
            if (def.isUUIDString()) {
                out.write(StringValueImpl.packUUID(val.getString()));
                break;
            }
            out.writeString(val.asString().get());
            break;
        case LONG:
            out.writeSortedPackedLong(val.asLong().get());
            break;
        case DOUBLE:
            out.writeSortedDouble(val.asDouble().get());
            break;
        case FLOAT:
            out.writeSortedFloat(val.asFloat().get());
            break;
        case NUMBER:
            out.write(((NumberValueImpl) val).getBytes());
            break;
        case ENUM:
            /* enumerations are sorted by declaration order */
            out.writeSortedPackedInt(val.asEnum().getIndex());
            break;
        case BOOLEAN:
            out.writeBoolean(val.asBoolean().get());
            break;
        case TIMESTAMP:
            out.write(((TimestampValueImpl) val).getBytes(true));
            break;
        default:
            throw new IllegalStateException(
                "Type not supported in indexes: " + val.getType());
        }
    }

    public boolean supportsSpecialValues() {
        return isNullSupported;
    }

    @SuppressWarnings("unused")
    private byte getIndicator(FieldValue val, short opSerialVersion) {

        if (val.isNull()) {
            return getNullIndicator();
        } else if (((FieldValueImpl) val).isEMPTY()) {
            return getEmptyIndicator();
        } else if (val.isJsonNull()) {
            return JSON_NULL_INDICATOR;
        }
        return NORMAL_VALUE_INDICATOR;
    }

    private byte getNullIndicator() {
        return (indexVersion >= INDEX_VERSION_V1) ?
                NULL_INDICATOR_V1 : NULL_INDICATOR_V0;
    }

    private byte getEmptyIndicator() {
        return (indexVersion >= INDEX_VERSION_V1) ?
                EMPTY_INDICATOR : NULL_INDICATOR_V0;
    }

    @SuppressWarnings("unused")
    private FieldValueImpl getSpecialValue(byte indicator,
                                           short opSerialVersion) {
        return (indicator == getNullIndicator() ?
                NullValueImpl.getInstance() :
                (indicator == EMPTY_INDICATOR ?
                 EmptyValueImpl.getInstance() :
                 (indicator == JSON_NULL_INDICATOR ?
                  NullJsonValueImpl.getInstance() : null)));
    }

    /**
     * Creates a binary index key from an IndexKey. In this case
     * the IndexKey may be partially filled.
     *
     * This is the version used by most client-based callers.
     *
     * @return the serialized index key or null if the IndexKey cannot
     * be serialized (e.g. it has null values).
     */
    public byte[] serializeIndexKey(IndexKeyImpl indexKey) {
        return serializeIndexKey(indexKey, SerialVersion.CURRENT);
    }

    private byte[] serializeIndexKey(IndexKeyImpl indexKey,
                                     short opSerialVersion) {
        TupleOutput out = null;

        try {
            out = new TupleOutput();

            final int numFields = getIndexKeyDef().getNumFields();

            for (int i = 0; i < numFields; ++i) {

                final FieldValueImpl val = indexKey.get(i);

                if (val == null) {
                    /* A partial key, done with fields */
                    break;
                }

                if (val.isSpecialValue() && !supportsSpecialValues()) {
                    return null;
                }

                serializeValue(out, val, indexFields.get(i),
                               false, /*fromRow*/
                               opSerialVersion);
            }

            return (out.size() != 0 ? out.toByteArray() : null);

        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    public IndexKeyImpl deserializeIndexKey(byte[] data, boolean partialOK) {
        return deserializeIndexKey(data, partialOK, SerialVersion.CURRENT);
    }

    /**
     * Deserializes binary index keys into IndexKeyImpl. This method is used
     * both at the server and the client:
     *
     * (a) At the client it deserializes binary index keys that are sent
     * from the server during an IndexKeysIterate operation. These keys are
     * "full"index keys that have been extracted from an index at the server
     * partialOK is false in this case.
     *
     * (b) At the server it deserializes binary search index keys that are
     * sent from a client as the start/stop keys of an IndexOperation.
     * partialOK is true in this case.
     *
     * @param data the bytes
     * @param partialOK true if not all fields must be in the data stream.
     * @param opSerialVersion In case (b) above, this is the serial version
     *        used by the IndexOperation.
     * @return an instance of IndexKeyImpl
     */
    IndexKeyImpl deserializeIndexKey(byte[] data,
                                     boolean partialOK,
                                     short opSerialVersion) {
        TupleInput input = null;
        final RecordDefImpl idxKeyDef = getIndexKeyDef();
        final IndexKeyImpl indexKey = new IndexKeyImpl(this, idxKeyDef);

        try {
            input = new TupleInput(data);

            final int numFields = idxKeyDef.getNumFields();

            for (int i = 0; i < numFields; ++i) {

                if (input.available() <= 0) {
                    break;
                }

                final IndexField ifield = getIndexPath(i);
                FieldDefImpl fdef = idxKeyDef.getFieldDef(i);
                FieldDef.Type ftype = fdef.getType();

                if (ifield.mayHaveSpecialValue()) {

                    final byte code = input.readByte();

                    switch (code) {
                    case NORMAL_VALUE_INDICATOR:
                        break;
                    case NUMBER_INDICATOR:
                        fdef = FieldDefImpl.Constants.numberDef;
                        ftype = Type.NUMBER;
                        break;
                    case STRING_INDICATOR:
                        fdef = FieldDefImpl.Constants.stringDef;
                        ftype = Type.STRING;
                        break;
                    case BOOLEAN_INDICATOR:
                        fdef = FieldDefImpl.Constants.booleanDef;
                        ftype = Type.BOOLEAN;
                        break;
                    case EMPTY_INDICATOR:
                    case JSON_NULL_INDICATOR:
                    case NULL_INDICATOR_V1:
                    case NULL_INDICATOR_V0: {
                        final FieldValueImpl specialVal =
                            getSpecialValue(code, opSerialVersion);

                        indexKey.put(i, specialVal);
                        continue;
                    }
                    default:
                        throw new IllegalStateException(
                            "Index key desrialization error: invalid " +
                            "index entry code: " + code);
                    }
                }

                switch (ftype) {
                case INTEGER:
                case STRING:
                case LONG:
                case DOUBLE:
                case FLOAT:
                case NUMBER:
                case ENUM:
                case BOOLEAN:
                case TIMESTAMP:
                    final FieldValue val = fdef.createValue(
                        ftype, FieldValueImpl.readTuple(ftype, fdef, input));
                    indexKey.put(i, val);
                    break;

                 default:
                     throw new IllegalStateException(
                         "Type not supported in indexes: " + ftype);
                }
            }

            if (!partialOK && !indexKey.isComplete()) {
                throw new IllegalStateException(
                    "Missing fields from index data for index " +
                    getName() + ", expected " + numFields +
                    ", received " +   indexKey.size());
            }
            return indexKey;
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
                /* ignore IOE on close */
            } /* CHECKSTYLE:ON */
        }
    }

    /*
     * See javadoc for IndexOperationHandler.reserializeOldKeys().
     */
    byte[] reserializeOldKey(byte[] key, short opVersion) {

        final IndexKeyImpl ikey = deserializeIndexKey(key,
                                                      true, /*partialOk*/
                                                      opVersion);
        return serializeIndexKey(ikey);
    }

    /*
     * See javadoc for IndexKeysIterateHandler.reserializeToOldKeys()
     */
    public byte[] reserializeToOldKey(byte[] indexKey, short opVersion) {

        final IndexKeyImpl ikey = deserializeIndexKey(indexKey,
                                                      false, /* partialOK */
                                                      SerialVersion.CURRENT);

        return serializeIndexKey(ikey, opVersion);
    }

    /**
     * Deserialize the binary format of an index entry into a (flat)
     * RecordValue that contains both the index key values and the
     * associated primary key values.
     */
    public void rowFromIndexEntry(RecordValueImpl row,
                                  byte[] primKey,
                                  byte[] indexKey) {

        TupleInput input = null;

        try {
            input = new TupleInput(indexKey);

            /*
             * Deserialize the index key (not including the associated
             * primary key).
             */
            for (int pos = 0; pos < numFields(); ++pos) {

                if (input.available() <= 0) {
                    throw new IllegalStateException(
                        "Index key deserialization error: the index key " +
                        "is too short");
                }

                final IndexField ifield = getIndexPath(pos);
                FieldDefImpl fdef = indexKeyDef.getFieldDef(pos);
                FieldDef.Type ftype = fdef.getType();

                if (ifield.mayHaveSpecialValue()) {

                    final byte code = input.readByte();

                    switch (code) {
                    case NORMAL_VALUE_INDICATOR:
                        break;
                    case NUMBER_INDICATOR:
                        fdef = FieldDefImpl.Constants.numberDef;
                        ftype = Type.NUMBER;
                        break;
                    case STRING_INDICATOR:
                        fdef = FieldDefImpl.Constants.stringDef;
                        ftype = Type.STRING;
                        break;
                    case BOOLEAN_INDICATOR:
                        fdef = FieldDefImpl.Constants.booleanDef;
                        ftype = Type.BOOLEAN;
                        break;
                    case EMPTY_INDICATOR:
                    case JSON_NULL_INDICATOR:
                    case NULL_INDICATOR_V1:
                    case NULL_INDICATOR_V0: {
                        final FieldValueImpl specialVal =
                            getSpecialValue(code, SerialVersion.CURRENT);

                        row.putInternal(pos, specialVal);
                        continue;
                    }
                    default:
                        throw new IllegalStateException(
                            "Index key deserialization error: invalid " +
                            "index entry code: " + code);
                    }
                }

                switch (ftype) {
                case INTEGER:
                case STRING:
                case LONG:
                case DOUBLE:
                case BOOLEAN:
                case FLOAT:
                case NUMBER:
                case ENUM:
                case TIMESTAMP:
                    final FieldValueImpl val =
                        (FieldValueImpl) fdef.createValue(
                          ftype, FieldValueImpl.readTuple(ftype, fdef, input));
                    row.putInternal(pos, val);
                    break;
                default:
                    throw new IllegalStateException(
                        "Type not supported in indexes: " + ftype);
                }
            }
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
                /* ignore IOE on close */
            } /* CHECKSTYLE:ON */
        }

        /*
         * Deserialize the values of the associated primary key.
         */
        if (primKey != null) {
            if (!table.initRowFromKeyBytes(primKey, numFields(), row)) {
                throw new IllegalStateException(
                    "Failed to deserialize primary key retrieved from index " +
                    getName());
            }
        }
    }

    /**
     * Checks to see if the index contains the *single* named field.
     * For simple types this is a simple contains operation.
     *
     * For complex types this needs to validate for a put of a complex
     * type that *may* contain an indexed field.
     * Validation of such fields must be done later.
     *
     * In the case of a nested field name with dot-separated names,
     * this code simply checks that fieldName is one of the components of
     * the complex field (using String.contains()).
     */
    boolean isIndexPath(TablePath tablePath) {

        for (IndexField indexField : getIndexFields()) {
            if (indexField.equals(tablePath)) {
                    return true;
            }
        }
        return false;
    }

    /**
     * Creates a RecordDef for the flattened key fields. The rules for naming
     * the fields of the new RecordDef are:
     * 1. top-level atomic fields are left as-is (the field name, e.g. "a")
     * 2. paths into nested Records that do not involve arrays or maps are
     * left intact (e.g. "a.b.c")
     * 3. array elements turn into <i>path-to-array</i>[]
     * 4. map elements turn into <i>path-to-map</i>.values()
     * 5. map keys turn into <i>path-to-map</i>.keys()
     *
     * Index fields cannot contain more than one array or map which simplifies
     * the translations.
     */
    private RecordDefImpl createIndexKeyDef() {

        final FieldMap fieldMap = createFieldMapForIndexKey();
        return new RecordDefImpl(fieldMap, null);
    }

    private RecordDefImpl createIndexEntryDef() {

        final FieldMap fieldMap = createFieldMapForIndexKey();

        final RecordDefImpl pkDef = table.getPrimKeyDef();
        final int primKeySize = pkDef.getNumFields();

        for (int i = 0; i < primKeySize; ++i) {

            /*
             * Note: A '#' is placed in front of each prim key column name
             * because a prim key column may also be part of the index key,
             * so without the '#' we could end up with 2 record fields having
             * the same name. Other code that uses this indexEntryDef is
             * careful to access these fields positionally, rather than by
             * name.
             */
            final FieldDefImpl fdef = pkDef.getFieldDef(i);
            final String fname = "#" + pkDef.getFieldName(i);
            final FieldMapEntry fme = new FieldMapEntry(fname, fdef, false, null);

            fieldMap.put(fme);
        }

        return new RecordDefImpl(fieldMap, null);
    }

    private FieldMap createFieldMapForIndexKey() {

        final FieldMap fieldMap = new FieldMap();

        /*
         * Use the list of IndexField because it's already got complex
         * paths translated to a normalized form with use of [] in the
         * appropriate places.
         */
        for (int i = 0; i < newFields.size(); ++i) {

            final IndexField indexField = indexFields.get(i);
            final FieldDefImpl fdef = indexField.getType();
            final String fname = newFields.get(i);
            final FieldMapEntry fme = new FieldMapEntry(fname, fdef);

            fieldMap.put(fme);
        }

        return fieldMap;
    }

    /**
     * Encapsulates a single field in an index, which may be simple or
     * complex.  Simple fields (e.g. "name") have a single component. Fields
     * that navigate into nested fields (e.g. "address.city") have multiple
     * components.  The state of whether a field is simple or complex is kept
     * by TablePath.
     *
     * IndexField adds this state:
     *   multiKeyField -- if this field results in a multi-key index this holds
     *     the portion of the field's path that leads to the FieldValue that
     *     makes it multi-key -- an array or map.  This is used as a cache to
     *     make navigation to that field easier.
     *   multiKeyType -- if multiKeyPath is set, this indicates if the field
     *     is a map key or map value field.
     * Arrays don't need additional state.
     *
     * Field names are case-insensitive, so strings are stored lower-case to
     * simplify case-insensitive comparisons.
     */
    public static class IndexField extends TablePath {

        /* If this ifield is a multikey one, multiKeyField stores a prefix of
         * this ifield up to and including the right-most multikey step. */
        private IndexField multiKeyField;

        /* If this ifield is a multikey one, multiKeyStepPos stores the
         * position of the right-most multikey step. */
        private int multiKeyStepPos = -1;

        private MultiKeyType multiKeyType;

        /*
         * true if any part of the path is JSON. When this is true, if there
         * are remaining steps after the JSON field they are ignored because
         * they may or may not exist in any given JSON document
         */
        private boolean isJson;

        /*
         * true if the multi key field is the JSON field or sub field
         * of JSON field
         */
        private boolean isJsonMultiKey;

        private TablePath jsonFieldPath;

        /* the position of this ifield within the index definition */
        private final int position;

        private FieldDefImpl declaredType;

        /*
         * The data type of this field. If the field is a json field with a
         * declared type, then this.type and this.declaredType are the same
         * type. Furthermore, if the user-declared type is GEOMETRY, both
         * this.type and this.declaredType are STRING, but this.isGeometry
         * is set to true.
         */
        private FieldDefImpl type;

        private boolean isPoint;

        private boolean isGeometry;

        /* the nullability of the field */
        private boolean nullable;

        ExprFuncCall functionalFieldExpr;
        PlanIter functionalFieldPlan;
        int numIterators;
        int numRegisters;

        /* ARRAY is not included because no callers need that information */
        private enum MultiKeyType { NONE, MAPKEY, MAPVALUE, ARRAY }

        private boolean jsonCollection;

        /* public access for use by the query compiler */
        public IndexField(TableImpl table,
                          String field,
                          Type typecode,
                          int position) {

            super(table, field);

            multiKeyType = MultiKeyType.NONE;
            this.position = position;

            if (typecode == Type.GEOMETRY) {
                declaredType = FieldDefImpl.Constants.stringDef;
                isGeometry = true;
            } else if (typecode == Type.POINT) {
                declaredType = FieldDefImpl.Constants.stringDef;
                isPoint = true;
            } else {
                declaredType = (typecode == null ?
                                null :
                                getFieldDefForTypecode(typecode));
            }

            this.jsonFieldPath = null;
            this.jsonCollection = table.isJsonCollection();
        }

        private IndexField(FieldMap fieldMap, String field, int position) {
            super(fieldMap, field);
            multiKeyType = MultiKeyType.NONE;
            this.position = position;
            this.declaredType = null;
            this.jsonFieldPath = null;
        }

        public boolean isMultiKey() {
            return multiKeyField != null;
        }

        public IndexField getMultiKeyField() {
            return multiKeyField;
        }

        public int getMultiKeyStepPos() {
            return multiKeyStepPos;
        }

        private boolean isJsonCollection() {
            return jsonCollection;
        }

        private void setIsJson() {
            isJson = true;
        }

        public boolean isJson() {
            return isJson;
        }

        void setIsJsonMultiKey() {
            isJsonMultiKey = true;
        }

        boolean isJsonMultiKey() {
            return isJsonMultiKey;
        }

        boolean isJsonAsArray() {
            if (isJsonMultiKey()) {
                return !isMapKeys() && !isMapValues();
            }
            return false;
        }

        public boolean isPoint() {
            return isPoint;
        }

        public boolean isGeometry() {
            return isGeometry;
        }

        public int getPosition() {
            return position;
        }

        public FieldDefImpl getType() {
            return type;
        }

        public void setType(FieldDefImpl def) {

            type = (getFunction() != null ?
                    getFunction().getRetType(def).getDef() :
                    def);
        }

        public FieldDefImpl getDeclaredType() {
            return declaredType;
        }

        boolean hasPreciseType() {
            return type.isPrecise();
        }

        boolean isNullable() {
            return nullable;
        }

        boolean mayHaveSpecialValue() {
            /*
             * Only top-level atomic columns may be non-nullable. Such columns
             * are non-EMPTY and non-json-null as well. So, this method is just
             * returns whether the index field is nullable.
             */
            return nullable;
        }

        private void setMultiKeyPath(int pos) {

            multiKeyField = new IndexField(getFieldMap(), null, position);
            for (int i = 0; i <= pos; ++i) {
                final StepInfo si = getStepInfo(i);
                multiKeyField.addStepInfo(new StepInfo(si));
            }
            multiKeyStepPos = pos;
        }

        public boolean isMapKeys() {
            return multiKeyType == MultiKeyType.MAPKEY;
        }

        private void setIsMapKeys() {
            multiKeyType = MultiKeyType.MAPKEY;
        }

        public boolean isMapValues() {
            return multiKeyType == MultiKeyType.MAPVALUE;
        }

        private void setIsMapValues() {
            multiKeyType = MultiKeyType.MAPVALUE;
        }

        private void setIsArrayValues() {
            multiKeyType = MultiKeyType.ARRAY;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }

        private void setJsonFieldPath(int pos) {
            jsonFieldPath = new TablePath(getFieldMap(), (String) null);
            for (int i = 0; i < pos; ++i) {
                final StepInfo si = getStepInfo(i);
                jsonFieldPath.addStepInfo(new StepInfo(si));
            }
        }

        /*  The public visibility is needed for Cloud Http Proxy. */
        public TablePath getJsonFieldPath() {
            return jsonFieldPath;
        }

        @Override
        void setIsMapFieldStep(int pos) {
            getStepInfo(pos).kind = StepKind.MAP_FIELD;
            if (multiKeyField != null && pos < multiKeyField.numSteps()) {
                multiKeyField.getStepInfo(pos).kind = StepKind.MAP_FIELD;
            }
        }

        public ExprFuncCall getFunctionalFieldExpr() {
            return functionalFieldExpr;
        }

        void compileFunctionalField(FieldDefImpl intype) {

            Function func = getFunction();
            String functionArgs = getFunctionArgs();

            if (func == null || func.isRowProperty()) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("declare $ikey ").append(intype.getDDLString());
            sb.append("; ").append(func.getName()).append("($ikey");
            if (functionArgs != null) {
                sb.append(functionArgs);
            }
            sb.append(")");

            //System.out.println("XXXX functional path query: " + sb.toString());

            QueryControlBlock qcb = new QueryControlBlock(
              (TableMetadataHelper)null,
              null,  // statementFactory
              sb.toString().toCharArray(), // queryString
              null, // ExecuteOptions
              null, // sctx
              null,  // namespace
              null); // prepareCallback

            qcb.setIsFunctionalIndexPath();
            qcb.compile();

            if (qcb.getException() != null) {
                throw qcb.getException();
            }

            functionalFieldExpr = (ExprFuncCall)qcb.getRootExpr();
            functionalFieldPlan = qcb.getQueryPlan();
            numIterators = qcb.getNumIterators();
            numRegisters = qcb.getNumRegs();
        }

        /*
         * Returns the DDL type string for the type. It must be a valid
         * JSON index type (see getFieldDefForTypecode).
         */
        public static String getDDLTypeString(FieldDef.Type code) {
            /*
             * POINT and GEOMETRY are special and handled directly because they
             * don't have a corresponding FieldDef class.
             */
            if (code == FieldDef.Type.POINT) {
                return "Point";
            }
            if (code == FieldDef.Type.GEOMETRY) {
                return "Geometry";
            }
            return getFieldDefForTypecode(code).getDDLString();
        }

        /*
         * Returns the FieldDef.Type from the DDL type String, the type
         * should be a valid JSON index type, see types in
         * getFieldDefForTypecode, GEOMETRY and POINT.
         */
        public static FieldDef.Type fromDdlTypeString(String typeString) {
            try {
                return FieldDef.Type.valueOf(typeString.toUpperCase());
            } catch (IllegalArgumentException iae) {
                if (typeString.equalsIgnoreCase("AnyAtomic")) {
                    return FieldDef.Type.ANY_ATOMIC;
                }
                throw new IllegalArgumentException(
                    "Invalid type for JSON index field: " + typeString);
            }
        }

        private static FieldDefImpl getFieldDefForTypecode(FieldDef.Type code) {
            switch (code) {
            case INTEGER:
                return FieldDefImpl.Constants.integerDef;
            case LONG:
                return FieldDefImpl.Constants.longDef;
            case DOUBLE:
                return FieldDefImpl.Constants.doubleDef;
            case BOOLEAN:
                return FieldDefImpl.Constants.booleanDef;
            case STRING:
                return FieldDefImpl.Constants.stringDef;
            case NUMBER:
                return FieldDefImpl.Constants.numberDef;
            case ANY_ATOMIC:
                return FieldDefImpl.Constants.anyAtomicDef;
            default:
                throw new IllegalArgumentException(
                    "Invalid type for JSON index field: " + code);
            }
        }
    }

    @Override
    public Index.IndexType getType() {
        if (annotations == null) {
            return Index.IndexType.SECONDARY;
        }
        return Index.IndexType.TEXT;
    }

    private boolean isTextIndex() {
        return getType() == Index.IndexType.TEXT;
    }

    public static void populateMapFromAnnotatedFields(
         List<AnnotatedField> fields,
         List<String> fieldNames,
         Map<String, String> annotations) {

        for (AnnotatedField f : fields) {
            final String fieldName = f.getFieldName();
            fieldNames.add(fieldName);
            annotations.put(fieldName, f.getAnnotation());
        }
    }

    /**
     * This lightweight class stores an index field, along with
     * an annotation.  Not all index types require annotations;
     * It is used for the mapping specifier in full-text indexes.
     */
    public static class AnnotatedField implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String fieldName;

        private final String annotation;

        public AnnotatedField(String fieldName, String annotation) {
            assert (fieldName != null);
            this.fieldName = fieldName;
            this.annotation = annotation;
        }

        /**
         * The name of the indexed field.
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         *  The field's annotation.  In Text indexes, this is the ES mapping
         *  specification, which is a JSON string and may be null.
         */
        public String getAnnotation() {
            return annotation;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            final AnnotatedField other = (AnnotatedField) obj;

            if (!fieldName.equals(other.fieldName)) {
                return false;
            }

            return (annotation == null ?
                    other.annotation == null :
                    JsonUtils.jsonStringsEqual(annotation, other.annotation));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + fieldName.hashCode();
            if (annotation != null) {
                result = prime * result + annotation.hashCode();
            }
            return result;
        }
    }

    @Override
    public String getAnnotationForField(String fieldName) {
        if (!isTextIndex()) {
            return null;
        }
        return getAnnotationsInternal().get(fieldName);
    }

    public RowImpl deserializeRow(byte[] keyBytes, byte[] valueBytes) {
        return table.createRowFromBytes(keyBytes, valueBytes, false);
    }

    /**
     * Formats the index.
     * @param asJson true if output should be JSON, otherwise tabular.
     */
    public String formatIndex(boolean asJson) {
        if (asJson) {
            TableImpl.JsonFormatter handler =
                TableImpl.createJsonFormatter(true);
            handler.index(table,
                          1,
                          getName(),
                          getDescription(),
                          getType().toString().toLowerCase(),
                          TableJsonUtils.collectIndexFieldInfos(this),
                          indexesNulls(),
                          isUnique,
                          getAnnotationsInternal(),
                          getProperties());
            return handler.toString();
        }
        return TabularFormatter.formatIndex(this);
    }

    private boolean areIndicatorBytesEnabled() {
        if (Boolean.getBoolean(INDEX_NULL_DISABLE)) {
            return false;
        }
        return true;
    }

    public int getIndexVersion() {
        return Integer.getInteger(INDEX_SERIAL_VERSION, INDEX_VERSION_CURRENT);
    }

    /**
     * Returns the minimum version of the server needed to support this
     * index. This version is based on when specific features used by this
     * index were introduced.
     */
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
        short requiredSerialVersion = SerialVersion.MINIMUM;

        if (isUnique) {
            requiredSerialVersion = (short)Math.max(requiredSerialVersion,
                       QUERY_VERSION_12_DEPRECATED_REMOVE_AFTER_PREREQ_25_1);
        }

        return requiredSerialVersion;
    }

    /**
     * This is directly from JE's com.sleepycat.je.tree.Key class and is the
     * default byte comparator for JE's btree.
     *
     * Compare using a default unsigned byte comparison.
     */
    static int compareUnsignedBytes(byte[] key1,
                                    int off1,
                                    int len1,
                                    byte[] key2,
                                    int off2,
                                    int len2) {
        final int limit = Math.min(len1, len2);

        for (int i = 0; i < limit; i++) {
            final byte b1 = key1[i + off1];
            final byte b2 = key2[i + off2];
            if (b1 == b2) {
                continue;
            }
            /*
             * Remember, bytes are signed, so convert to shorts so that we
             * effectively do an unsigned byte comparison.
             */
            return (b1 & 0xff) - (b2 & 0xff);
        }

        return (len1 - len2);
    }

    static int compareUnsignedBytes(byte[] key1, byte[] key2) {
        return compareUnsignedBytes(key1, 0, key1.length, key2, 0, key2.length);
    }

    /**
     * A cache used to avoid materializing the same Row object multiple times
     * when a JE write occurs for a row with more than one index.
     *
     * <p>
     * JE calls the index key extractor multiple times, once for each index,
     * passing the byte array of the key/value. Without a cache, the extractor
     * will materialize the same Row multiple times.
     * </p>
     *
     * <p>
     * The cache contains two rows because JE calls the key extractor for both
     * the old and new value of the row.
     * </p>
     *
     * <p>
     * The cache relies compares the byte array reference to determine
     * equality. This relies on the fact that KVS and JE always allocate a new
     * byte array for different keys and data. It would be more future-proof,
     * but less performant, to compare the byte arrays.
     * </p>
     *
     * <p>
     * One or more indexes for a table may be keyOnly. If cachedKeyOnly is
     * true, the cached row is a primary-key row, only row1 is initialized, and
     * the cached row can only be used for a keyOnly index. In this state, if
     * we are called with the same key for an index with keyOnly false, we
     * discard the cached row and materialize the full row. We _could_ reuse
     * the primary key in that case, but we currently don't bother to.
     * </p>
     * <p>
     * When cachedKeyOnly is false and getRow() is invoked with keyOnly true,
     * the returned row may contain fields in addition to the fields defined by
     * the primary key. It's the caller's responsibility to ignore the
     * additional fields, taking care not to invoke any operations that
     * implicitly rely on the fields being just primary key fields, e.g.
     * equals(), getFieldNames(), copy(), etc.
     * </p>
     */
    private static class TwoRowCache {
        private boolean cachedKeyOnly;
        private byte[] cachedKey;
        private byte[] data1;
        private RowImpl row1;
        private byte[] data2;
        private RowImpl row2;

        RowImpl getRow(final TableImpl table,
                       final byte[] key,
                       final byte[] data,
                       final boolean keyOnly) {

            if (key == null || (!keyOnly && data == null)) {
                throw new IllegalArgumentException();
            }

            if (key != cachedKey || (cachedKeyOnly && !keyOnly)) {
                cachedKey = key;
                cachedKeyOnly = keyOnly;
                data1 = null;
                data2 = null;
                row1 = null;
                row2 = null;
            } else {
                if (keyOnly) {
                    /* We may be returning extra fields beyond the primary
                     * key fields in the row, but that's ok as part of the
                     * method's contract.
                     */
                    return row1;
                }
                if (data == data1) {
                    return row1;
                }
                if (data == data2) {
                    return row2;
                }
                /* Cache is full for this key. Rotate oldest data/row out. */
                data1 = data2;
                row1 = row2;
                data2 = null;
                row2 = null;
            }

            final RowImpl row = table.createRowFromBytes(key, data, keyOnly);
            if (row == null) {
                return null;
            }

            if (keyOnly) {
                row1 = row;
            } else if (data1 == null) {
                data1 = data;
                row1 = row;
            } else if (data2 == null) {
                data2 = data;
                row2 = row;
            } else {
                throw new IllegalStateException();
            }
            return row;
        }
    }
}