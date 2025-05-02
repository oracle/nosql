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

package oracle.kv.table;

import java.io.InputStream;
import java.util.List;

/**
 * Index represents an index on a table in Oracle NoSQL Database.  It is an
 * immutable object created from system metadata.  Index is used to examine
 * index metadata and used as a factory for {@link IndexKey} objects used
 * for IndexKey operations in {@link TableAPI}.
 * <p>
 * Indexes are created and managed using the administrative command line
 * interface.
 *
 * @since 3.0
 */
public interface Index {

    /**
     * Returns the Table on which the index is defined.
     *
     * @return the table
     */
    Table getTable();

    /**
     * Returns the name of the index.
     *
     * @return the index name
     */
    String getName();

    /**
     * Returns an unmodifiable list of the field names that define the index.
     * These are in order of declaration which is significant. This method
     * returns the same list as {@link IndexKey#getFields} for IndexKey
     * instances created by this Index.
     *
     * @return the field names
     */
    List<String> getFields();

    /**
     * Returns wherther the index indexes SQL NULL and EMPTY values. This is
     * specified in the index creation DDL. The default is true. If false, a
     * row that contains a NULL value on an indexed field or is missing an
     * indexed field will be skipped during indexing. As a result, the index
     * may not contain at least one entry per row, which further implies that
     * there are certain queries that cannot use this index. The query compiler
     * makes sure that the index will not be used in those cases (e.g. queries
     * containing IS NULL or NOT EXISTS predicates on the indexed field(s)).
     */ 
    boolean indexesNulls();

    /**
     * Gets the index's description if present, otherwise null.  This is a
     * description of the index that is optionally supplied during
     * definition of the index.
     *
     * @return the description or null
     */
    String getDescription();

    /**
     * Returns the index's IndexType.
     *
     * @since 3.5
     */
    IndexType getType();

    /**
     * Return an annotation for the given field.  Annotations are used only for
     * Full Text Indexes.  Returns null if there is no annotation.
     *
     * @since 3.5
     */
    String getAnnotationForField(String fieldName);

    /**
     * Creates an {@code IndexKey} for this index.  The returned key can only
     * hold fields that are part of this. Other fields are rejected if an
     * attempt is made to set them on the returned object.
     *
     * @return an empty index key based on the index
     */
    IndexKey createIndexKey();

    /**
     * Creates an {@code IndexKey} for the index populated relevant fields from
     * the {@code RecordValue} parameter.  Fields that are not part of the
     * index key are silently ignored.
     * <p>
     * This method is not able to construct index keys for <em>multi-key</em>
     * indexes. These are indexes that include elements of a map or array and
     * can result in multiple index entries, or distinct {@code IndexKey}
     * values for a single row.
     *
     * @param value a {@code RecordValue} instance
     *
     * @return an {@code IndexKey} containing relevant fields from the value
     *
     * @throws IllegalArgumentException if the value does not match the table
     * or index, or if the index contains an array or map
     *
     * @deprecated as of 4.5
     */
    @Deprecated
    IndexKey createIndexKey(RecordValue value);

    /**
     * Creates an {@code IndexKey} based on JSON input.  If the {@code exact}
     * parameter is true the input string must contain an exact match to the
     * index key.  It must not have additional data.  If false, only matching
     * fields will be added and the input may have additional, unrelated data.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the input is malformed
     */
    IndexKey createIndexKeyFromJson(String jsonInput,
                                    boolean exact);

    /**
     * Creates an {@code IndexKey} based on JSON input.  If the {@code exact}
     * parameter is true the input string must contain an exact match to the
     * index key.  It must not have additional data.  If false, only matching
     * fields will be added and the input may have additional, unrelated data.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the input is malformed
     */
    IndexKey createIndexKeyFromJson(InputStream jsonInput,
                                    boolean exact);

    /**
     * Creates a {@code FieldRange} object used to specify a value range for
     * use in a index iteration operation in {@link TableAPI}.
     *
     * @param fieldPath the path to the field from the index
     * to use for the range. This string must match one of the valid index
     * field strings returned by {@link #getFields}.
     *
     * @throws IllegalArgumentException if the field is not defined in the
     * index
     *
     * @return an empty {@code FieldRange} based on the index
     */
    FieldRange createFieldRange(String fieldPath);

    /**
     * The type of an index.  Currently there are two types: SECONDARY is a
     * regular index defined by mapping fields to keys in a secondary database,
     * while TEXT is a full text index defined via a mapping of fields to a
     * text search engine.
     *
     * @since 3.5
     */
    public enum IndexType {

        SECONDARY, TEXT

    }
}
