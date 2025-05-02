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
 * Defines parameters used in multi-row operations. A multi-row operation
 * selects rows from at least one table, called the target table, and optionally
 * selects rows from its ancestor and descendant tables.
 * <p>
 * The target table is defined by the {@link PrimaryKey} or {@link IndexKey}
 * passed as the first parameter of the multi-row method.  These include
 * <ul>
 * <li>{@link TableAPI#multiGet}
 * <li>{@link TableAPI#multiGetKeys}
 * <li>{@link TableAPI#tableIterator(PrimaryKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#tableKeysIterator(PrimaryKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#tableIterator(IndexKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#tableKeysIterator(IndexKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#multiDelete}
 * </ul>
 * <p>

 * By default only matching records from the target table are returned or
 * deleted.  {@code MultiRowOptions} can be used to specify whether the
 * operation should affect (return or delete) records from ancestor and/or
 * descendant tables for matching records.  In addition {@code MultiRowOptions}
 * can be used to specify sub-ranges within a table or index for all operations
 * it supports using {@link FieldRange}.
 * <p>
 * When results from multiple tables are returned they are always returned with
 * results from ancestor tables first even if the iteration is in reverse order
 * or unordered.  In this case results from multiple tables are mixed.  Because
 * an index iteration can result in multiple index entries matching the same
 * primary record it is possible to get duplicate return values for those
 * records as well as specified ancestor tables.  It is not valid to specify
 * child tables for index operations.  It is up to the caller to handle
 * duplicates and filter results from multiple tables.
 * <p>
 * The ability to return ancestor and child table rows and keys is useful and
 * can avoid additional calls from the client but it comes at a cost and should
 * be used only when necessary.  In the case of ancestor tables it means
 * verification or fetching of the ancestor row.  In the case of child tables
 * it means that the iteration cannot end until it has scanned all child table
 * records which may involve iteration over uninteresting records.
 *
 * @since 3.0
 */
public class MultiRowOptions {
    private FieldRange fieldRange;
    private List<Table> ancestors;
    private List<Table> children;

    /**
     * Full constructor requiring all members.
     */
    public MultiRowOptions(FieldRange fieldRange,
                           List<Table> ancestors,
                           List<Table> children) {
        this.fieldRange = fieldRange;
        this.ancestors = ancestors;
        this.children = children;
    }

    /**
     * A convenience constructor that takes only {@link FieldRange}.
     */
    public MultiRowOptions(FieldRange fieldRange) {
        this.fieldRange = fieldRange;
        this.ancestors = null;
        this.children = null;
    }

    /**
     * Gets the FieldRange to be used to restrict the range of the operation.
     *
     * @return the range or null if not set
     */
    public FieldRange getFieldRange() {
        return fieldRange;
    }

    /**
     * Gets the list of ancestor tables to be included in an operation that
     * returns multiple rows or keys.
     *
     * @return the list or null if none have been set
     */
    public List<Table> getIncludedParentTables() {
        if (ancestors != null && !ancestors.isEmpty()) {
            return ancestors;
        }
        return null;
    }

    /**
     * Gets the list of child tables to be included in an operation that
     * returns multiple rows or keys.
     *
     * @return the list or null if none have been set
     */
    public List<Table> getIncludedChildTables() {
        if (children != null && !children.isEmpty()) {
            return children;
        }
        return null;
    }

    /**
     * Restricts the selected rows to those matching a
     * range of field values for the first unspecified field in the target key.
     * <p>
     * The first unspecified key field is defined as the first key field
     * following the fields in a partial target key, or the very first key
     * field when a null target key is specified.
     * <p>
     * This method may only be used when a partial or null target key is
     * specified.  If a complete target key is given, this member must be null.
     *
     * @param newFieldRange the range to use
     *
     * @return this
     */
    public MultiRowOptions setFieldRange(FieldRange newFieldRange) {
        this.fieldRange = newFieldRange;
        return this;
    }

    /**
     * Specifies the parent (ancestor) tables for which rows are selected by
     * the operation.
     * <p>
     * Each table must be an ancestor table (parent, grandparent, etc.)
     * of the target table.
     * <p>
     * A row selected from a parent table will have that row's complete primary
     * key, which is a partial primary key for the row selected from the target
     * table.  At most one row from each parent table will be selected for each
     * selected target table row.
     * <p>
     * Rows from a parent table are always returned before rows from its
     * child tables.  This is the case for both forward and reverse iterations.
     * <p>
     * For an index method, fetching the parent table rows
     * requires additional operations per selected index key.  Redundant
     * fetches (when multiple selected target table rows have the same
     * parent) can be avoided by maintaining and checking a set of already
     * selected keys.
     *
     * @param newAncestors the list to set
     *
     * @return this
     */
    public MultiRowOptions setIncludedParentTables(List<Table> newAncestors) {
        this.ancestors = newAncestors;
        return this;
    }

    /**
     * Specifies the child (descendant) tables for which rows are selected by
     * the operation.
     * <p>
     * Each child table must be an descendant table (child, grandchild, great-
     * grandchild, etc.) of the target table.
     * <p>
     * The rows selected from each child table will have key field values
     * matching those in the key of a selected target table row.  Multiple
     * child table rows may be selected for each selected target table row.
     * <p>
     * Rows from a parent table are always returned before rows from its
     * child tables for forward iteration.  Reverse iterations will return
     * child rows first.
     * <p>
     * Child tables may not be specified for inclusion in an index operation.
     *
     * @param newChildren the list to set
     *
     * @return this
     */
    public MultiRowOptions setIncludedChildTables(List<Table> newChildren) {
        this.children = newChildren;
        return this;
    }
}
