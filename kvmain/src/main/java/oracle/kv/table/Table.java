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
import java.util.Map;

/**
 * Table is a handle on a table in the Oracle NoSQL Database.  Tables are
 * created in a store using administrative interfaces.  The Table handle is
 * obtained using {@link TableAPI#getTable} and {@link Table#getChildTable}.
 * Table contains immutable metadata for a table and acts as a factory for
 * objects used in the {@link TableAPI} interface.
 * <p>
 * Tables are defined in terms of named fields where each field is defined by
 * an instance of {@link FieldDef}.  A single record in a table is called a
 * {@link Row} and is uniquely identified by the table's {@link PrimaryKey}.
 * The primary key is defined upon construction by an ordered list of fields.
 * Tables also have a {@code shardKey} which is a proper subset of the primary
 * key.  A shard key has the property that all rows in a table that share the
 * same shard key values are stored in the same partition and can be accessed
 * transactionally.  If not otherwise defined the shard key is the same as the
 * table's primary key.
 * <p>
 * A table may be a <i>top-level</i> table, with no parent table, or a
 * <i>child</i> table, which has a parent.  A child table shares its parent
 * table's primary key but adds its own fields to its primary key to uniquely
 * identify child table rows.  It is not possible to define a shard key on
 * a child table.  It inherits its parent table's shard key.
 *
 * @since 3.0
 */
public interface Table {

    /**
     * Gets the named child table if it exists.  The name must specify a
     * direct child of this table.  Null is returned if the table does
     * not exist.
     *
     * @param name the table name for the desired table
     *
     * @return the child table or null
     */
    Table getChildTable(String name);

    /**
     * Returns true if the named child table exists, false otherwise.
     * The name must specify a direct child of this table.
     *
     * @param name the table name for the desired table
     *
     * @return true if the child table exists, false otherwise
     */
    boolean childTableExists(String name);

    /**
     * Gets an unmodifiable view of all child tables of this table.  Only
     * direct child tables are returned.  If no child tables exist an empty map
     * is returned.
     *
     * @return the map of child tables, which may be empty
     */
    Map<String, Table> getChildTables();

    /**
     * Returns the parent table, or null if this table has no parent.
     *
     * @return the parent table or null
     */
    Table getParent();

    /**
     * Returns the current version of the table metadata.  Each time a table
     * is evolved its version number will increment.  A table starts out at
     * version 1.
     *
     * @return the version of the table
     */
    int getTableVersion();

    /**
     * Gets the specified version of the table.  This allows an application to
     * use an older version of a table if it is not prepared to use the latest
     * version (the default).  Table versions change when a table is evolved by
     * adding or removing fields.  Most applications should use the default
     * (latest) version if at all possible.
     *<p>
     * The version must be a valid version between 0 and the value of
     * {@link #numTableVersions}.  If the version is 0 the latest
     * version is used.  Actual version numbers begin with 1.
     *
     * @param version the version to use
     *
     * @return the requested table
     *
     * @throws IllegalArgumentException if the requested version number is
     * negative or out of range of the known versions
     */
    Table getVersion(int version);

    /**
     * Returns the number of versions of this table that exist.  Versions are
     * identified by integer, starting with 1.  Versions are added if the table
     * is modified (evolved) with each new version getting the next integer.
     * <p>
     * By default when a table is accessed its current (latest) version will be
     * used.  It is possible to see an earlier or later version using
     * {@link #getVersion}.
     *
     * @return the number of table versions
     */
    int numTableVersions();

    /**
     * Gets the named index if it exists, null otherwise.
     *
     * @param indexName the name of the index
     *
     * @return the index or null
     */
    Index getIndex(String indexName);

    /**
     * Returns an unmodifiable map of all of the indexes on this table.  If
     * there are no indexes defined an empty map is returned.
     *
     * @return a map of indexes
     */
    Map<String, Index> getIndexes();

    /**
     * Returns an unmodifiable map of all of the indexes of the given type on
     * this table.  If there are no such indexes defined, an empty map is
     * returned.
     *
     * @return a map of indexes
     *
     * @since 3.5
     */
    Map<String, Index> getIndexes(Index.IndexType type);

    /**
     * Gets the short name of the table. If this is a child table only the last
     * component of the full name is returned.
     *
     * @return the last component of the full table name
     */
    String getName();

    /**
     * Returns the table's full name which includes its ancestor
     * tables in a dot (".") separated path. For top-level tables this value
     * is the same as the table name. It doesn't include any namespace
     * component.
     *
     * @return the full table name
     */
    String getFullName();

    /**
     * Returns the table's description if present, otherwise null.  This is a
     * description of the table that is optionally supplied during definition
     * of the table.
     *
     * @return the description or null
     */
    String getDescription();

    /**
     * Returns an unmodifiable list of the field names of the table in
     * declaration order which is done during table definition.  This will
     * never be empty.
     * <p>
     * If the table is a JSON Collection the fields will be those defined in
     * the primary key.
     *
     * @return the fields
     */
    List<String> getFields();

    /**
     * Returns the named field from the table definition, or null if the field
     * does not exist.
     *
     * @return the field or null
     */
    FieldDef getField(String name);

    /**
     * Returns true if the named field is nullable.
     *
     * @param name the name of the field
     *
     * @return true if the named field is nullable
     *
     * @throws IllegalArgumentException if the named field does not exist
     * in the table definition
     */
    boolean isNullable(String name);

    /**
     * Creates an instance using the default value for the named field.
     * The return value is {@link FieldValue} and not a more specific type
     * because in the case of nullable fields the default will be a null value,
     * which is a special value that returns true for {@link #isNullable}.
     *
     * @param name the name of the field
     *
     * @return a default value
     *
     * @throws IllegalArgumentException if the named field does not exist
     * in the table definition
     */
    FieldValue getDefaultValue(String name);

    /**
     * Returns an unmodifable list of the field names that comprise the primary
     * key for the table.  This will never be null.
     *
     * @return the list of fields
     */
    List<String> getPrimaryKey();

    /**
     * Returns an unmodifable list of the shard key fields.  This is a strict
     * subset of the primary key.  This will never be null.
     *
     * @return the list of fields
     */
    List<String> getShardKey();

    /**
     * Creates an empty Row for the table that can hold any field value.
     *
     * @return an empty row
     */
    Row createRow();

    /**
     * Creates a Row for the table populated with relevant fields from the
     * {@code RecordValue} parameter.  Only fields that belong in
     * this table are added.  Other fields are silently ignored.
     *
     * @param value a RecordValue instance containing fields that may or
     * may not be applicable to this table
     *
     * @return the row
     *
     * @throws IllegalArgumentException if the input is an instance of
     * {@link IndexKey}. The definition of IndexKey does not match that
     * of the table's Row.
     *
     * @throws IllegalArgumentException if the table is a JSON collection.
     */
    Row createRow(RecordValue value);

    /**
     * Creates a Row using the default values for all fields.  This includes
     * the primary key fields which are not allowed to be defaulted in put
     * operations.
     *
     * @return a new row
     *
     * @throws IllegalArgumentException if the table is a JSON collection.
     */
    Row createRowWithDefaults();

    /**
     * Creates an empty {@code PrimaryKey} for the table that can only hold
     * fields that are part of the primary key for the table.  Other fields
     * will be rejected if an attempt is made to set them on the returned
     * object.
     *
     * @return an empty primary key
     */
    PrimaryKey createPrimaryKey();

    /**
     * Creates a {@code PrimaryKey} for the table populated with relevant
     * fields from the {@code RecordValue} parameter.  Only fields that belong
     * in this primary key are added.  Other fields are silently ignored.
     *
     * @param value a {@code RecordValue} instance containing fields that may
     * or may not be applicable to this table's primary key.  Only fields that
     * belong in the key are added.
     *
     * @throws IllegalArgumentException if the input is an instance of
     * {@link IndexKey}. The definition of IndexKey does not match that
     * of the table's PrimaryKey.
     */
    PrimaryKey createPrimaryKey(RecordValue value);

    /**
     * Creates a ReturnRow object for the ReturnRow parameter in
     * table put and delete methods in {@link TableAPI} such as
     * {@link TableAPI#put} and {@link TableAPI#delete}.
     *
     * @param returnChoice describes the state to return
     *
     * @return a {@code ReturnRow} containing the choice passed in the
     * parameter
     */
    ReturnRow createReturnRow(ReturnRow.Choice returnChoice);

    /**
     * Creates a Row based on JSON string input.  If the {@code exact}
     * parameter is true the input string must contain an exact match to
     * the table row, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
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
    Row createRowFromJson(String jsonInput, boolean exact);

    /**
     * Creates a Row based on JSON input.
     * <p>
     * For a table with normal schema
     * (i.e. not a JSON collection) If the {@code exact}
     * parameter is true the input string must contain an exact match to
     * the table row, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
     * <p>
     * For a JSON Collection table all fields are put into the row. If
     * {@code exact} is true all primary key fields must be set or an
     * exception is thrown.
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
    Row createRowFromJson(InputStream jsonInput, boolean exact);

    /**
     * Creates a {@code PrimaryKey} based on JSON input.  If the
     * {@code exact}
     * parameter is true the input string must contain an exact match to
     * the primary key, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
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
    PrimaryKey createPrimaryKeyFromJson(String jsonInput,
                                        boolean exact);

    /**
     * Creates a {@code PrimaryKey} based on JSON input.  If the
     * {@code exact}
     * parameter is true the input string must contain an exact match to
     * the primary key, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
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
    PrimaryKey createPrimaryKeyFromJson(InputStream jsonInput,
                                        boolean exact);

    /**
     * Creates a {@code FieldRange} object used to specify a value range for use
     * in a table iteration operation in {@link TableAPI}.
     *
     * @param fieldName the name of the field from the PrimaryKey to use for
     * the range
     *
     * @throws IllegalArgumentException if the field is not defined in the
     * table's primary key
     *
     * @return an empty {@code FieldRange} based on the table and field
     */
    FieldRange createFieldRange(String fieldName);

    /**
     * Returns a MultiRowOptions object initialized with ancestor
     * and child tables from the list of table names and/or FieldRange.
     *
     * @param tableNames an optional list of fully-qualified table names to
     * include in results, or null.  This list must not include the name of
     * this (the target) table.
     *
     * @param fieldRange an optional FieldRange to be used in construction of
     * MultiRowOptions, or null.
     *
     * @throws IllegalArgumentException if neither parameter has information or
     * this table is included in the list of table names.
     */
    MultiRowOptions createMultiRowOptions(List<String> tableNames,
                                          FieldRange fieldRange);

    /**
     * Clone the table.
     *
     * @return a deep copy of the table
     */
    Table clone();

    /**
     * Returns the default TimeToLive for the table or null if not set.  This
     * default TTL is applied when a row is put without an explicit TTL value.
     * If the default is not set and a row is put without an explicit TTL
     * the row does not expire.
     *
     * @return the default TimeToLive or null
     *
     * @since 4.0
     */
    TimeToLive getDefaultTTL();

    /**
     * Returns the namespace in which the table is defined. Note that the
     * "sysdefault" namespace will return
     * {@link TableAPI#SYSDEFAULT_NAMESPACE_NAME}.
     *
     * @return the namespace for the table
     * @since 18.3
     */
    String getNamespace();

    /**
     * Returns the table's full name which includes its ancestor tables in a
     * dot (".") separated path, prefixed with the namespace in the format
     * namespace:full-name for namespaces other than the "sysdefault" namespace.
     * For tables created in the "sysdefault" namespace, the return value just
     * contains the full name without a namespace prefix.
     *
     * @return the namespace qualified table name
     * @since 18.3
     */
    String getFullNamespaceName();
}
