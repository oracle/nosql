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
 * IndexKey is a specialization of RecordValue to represent a key used to
 * access a specific index defined on a table. It may contain all or part of a
 * the fields associated with the index.  If partial the fields must be set in
 * order of significance, as defined by the list returned by {@link #getFields}.
 * If an attempt is made to set a field that is not part of the index
 * IllegalArgumentException is thrown.
 *<p>
 * IndexKey instances do not share the same {@link RecordDef} definition as the
 * corresponding {@link Row} objects on the same table. Instead, they have a
 * <em>flattened</em> name space, simplifying their use. Values for fields in
 * an IndexKey can be created using a positional put, {@link
 * RecordValue#put(int, FieldValue)}, or by name, where the name must be one of
 * the strings returned by {@link #getFields}.
 *<p>
 * IndexKey objects are constructed using {@link Index#createIndexKey()}},
 * and {@link Index#createIndexKeyFromJson}.
 *
 * @since 3.0
 */

public interface IndexKey extends RecordValue {

    /**
     * Returns the Index associated with this key.
     *
     * @return the Index
     */
    Index getIndex();

    /**
     * Returns an unmodifiable list of fields, in key order, that
     * comprise this key.  This method returns the same list as {@link
     * Index#getFields} for this instance's Index.
     *
     * @return the fields
     *
     * @since 3.0.6
     * @deprecated as of 4.2 Please use the equivalent getFieldNames() method.
     */
    @Deprecated
    @Override
    List<String> getFields();

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The new value of the field will be the special
     * EMPTY value that returns true for {@link FieldValue#isEMPTY}.
     *
     * @param position the position of the field.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record.
     *
     * @since 4.4
     */
    IndexKey putEMPTY(int position);

    /**
     * Puts an EMPTY value in the named field, silently overwriting existing
     * values. EMPTY is a special value that returns true for
     * {@link FieldValue#isEMPTY}.
     *
     * @param fieldName name of the desired field
     *
     * @return this
     *
     * @since 4.4
     */
    IndexKey putEMPTY(String fieldName);

}
