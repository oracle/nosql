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

import java.util.List;

/**
 * RecordDef represents a record type, which specifies a set of
 * {@link RecordValue}s that conform to this type. The type definition declares
 * a number of fields, where a field consists of a name and an associated type.
 * The field types need not be the same. The declaration order of the fields is
 * important.  Based on this order, each field has a unique position, which is
 * a number between 0 and N - 1, where N is the number of fields. In addition
 * to its name, type, and position, each field may be declared nullable or
 * non-nullable (with the default being nullable), and may have an associated
 * default value. The default value is required for non-nullable fields;
 * otherwise, the default default-value is the NULL value (an instance of
 * FieldValue for which the isNull() method returns true).
 *
 * A {@link RecordValue} conforms to a record type if (a) it has the same number
 * of fields as the type, (b) the fields in the record value and the record type
 * have the same names and appear in the same order, and (c) the field values in
 * the record have the same type as the type of the corresponding fields in the
 * record type.
 *
 * @since 3.0
 */
public interface RecordDef extends FieldDef {

    /**
     * Get the number of fields declared by this record type.
     *
     * @return the number of fields declared by this record type
     *
     * @since 4.2
     */
    int getNumFields();

    /**
     * Get the names of the fields declared by this record type. The names are
     * returned in declaration order.
     *
     * @return an unmodifiable list of the field names in declaration order
     *
     * @deprecated as of 4.2 Use getFieldNames()
     */
    @Deprecated
    List<String> getFields();

    /**
     * Get the names of the fields declared by this record type. The names are
     * returned in declaration order.
     *
     * @return an unmodifiable list of the field names in declaration order
     *
     * @since 4.2
     */
    List<String> getFieldNames();

    /**
     * Returns true if the record tyoe contains a field with the given name
     *
     * @return true if the record tyoe contains a field with the given name,
     * false otherwise
     */
    public boolean contains(String name);

    /**
     * Get the type of the field with the given name.
     *
     * @param name the name of the field
     *
     * @return the type of the field with the given name, or null if there is
     * no field with such a name
     *
     * @deprecated as of 4.2 Use getFieldDef(String)
     */
    @Deprecated
    FieldDef getField(String name);

    /**
     * Get the type of the field with the given name.
     *
     * @param name the name of the field
     *
     * @return the type of the field with the given name, or null if there is
     * no field with such a name
     *
     * @since 4.2
     */
    FieldDef getFieldDef(String name);

    /**
     * Get the type of the field at the given position.
     *
     * @param pos the index in the list of fields of the field to return
     *
     * @return the type of the field at the specified position.
     *
     * @throws IndexOutOfBoundsException if the position is negative or
     * greater or equal to the number of fields declared by this record type.
     *
     * @since 4.0
     *
     * @deprecated as of 4.2 Use getFieldDef(int)
     */
    @Deprecated
    FieldDef getField(int pos);

    /**
     * Get the type of the field at the given position.
     *
     * @param pos the position of the field in the list of fields
     *
     * @return the type of the field at the specified position.
     *
     * @throws IndexOutOfBoundsException if the position is negative or
     * greater or equal to the number of fields declared by this record type.
     *
     * @since 4.2
     */
    FieldDef getFieldDef(int pos);

    /**
     * Get the name of the field at the given position.
     *
     * @param pos the position of the field in the list of fields
     *
     * @return the name of the field at the given position.
     *
     * @throws IndexOutOfBoundsException if the position is negative or
     * greater or equal to the number of fields declared by this record type.
     *
     * @since 4.0
     */
    String getFieldName(int pos);

    /**
     * Returns the position of the field with the given name
     *
     * @return the position of the field with the given name.
     *
     * @throws IllegalArgumentException if there is no field with the given name
     *
     * @since 4.2
     */
    int getFieldPos(String fname);

    /**
     * Get the name of the record type. Record types require names even
     * if they are nested or used as an array or map element.
     *
     * @return the name of the record type
     */
    String getName();

    /**
     * Returns true if the named field is nullable.
     *
     * @param name the name of the field
     *
     * @return true if the named field is nullable
     *
     * @throws IllegalArgumentException if there is no field with the given name
     */
    boolean isNullable(String name);

    /**
     * Returns true if the field at the given position is nullable.
     *
     * @param pos the position of the field in the list of fields
     *
     * @return true if the named field is nullable
     *
     * @throws IndexOutOfBoundsException if the position is negative or
     * greater or equal to the number of fields declared by this record type.
     *
     * @since 4.2
     */
    boolean isNullable(int pos);

    /**
     * Returns the default value for the named field.
     * The return value is {@link FieldValue} and not a more specific type
     * because in the case of nullable fields the default will be a null value,
     * which is a special value that returns true for {@link #isNullable}.
     *
     * @param name the name of the field
     *
     * @return a default value
     *
     * @throws IllegalArgumentException if there is no field with the given name
     */
    FieldValue getDefaultValue(String name);

    /**
     * Returns the default value for the field at the given position.
     * The return value is {@link FieldValue} and not a more specific type
     * because in the case of nullable fields the default will be a null value,
     * which is a special value that returns true for {@link #isNullable}.
     *
     * @param pos the position of the field in the list of fields
     *
     * @return a default value
     *
     * @throws IndexOutOfBoundsException if the position is negative or
     * greater or equal to the number of fields declared by this record type.
     *
     * @since 4.2
     */
    FieldValue getDefaultValue(int pos);

    /**
     * @return a deep copy of this object
     */
    @Override
    public RecordDef clone();
}
