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

/**
 * ReturnRow is used with put and delete operations to return the previous row
 * value and version. If either property is returned the expiration time for the
 * row will also be valid. If neither value nor version is returned expiration
 * time is undefined.
 * <p>
 * A ReturnRow instance may be used as the {@code
 * prevRecord} parameter to methods such as {@link TableAPI#put(Row,
 * ReturnRow, WriteOptions)}.
 * </p>
 * <p>
 * For best performance, it is important to choose only the properties that are
 * required.  The store is optimized to avoid I/O when the requested
 * properties are in cache.
 * </p>
 * <p>Note that because both properties are optional, the version property,
 * value property, or both properties may be null, in which case expiration
 * time is undefined.</p>
 *
 * @since 3.0
 */
public interface ReturnRow extends Row {

    /**
     * Returns the Choice of what information is returned.
     */
    Choice getReturnChoice();

    /**
     * Specifies whether to return the row value, version, both or neither.
     * <p>
     * For best performance, it is important to choose only the properties that
     * are required.  The store is optimized to avoid I/O when the requested
     * properties are in cache.  </p>
     */
    public enum Choice {

        /**
         * Return the value only.
         */
        VALUE(true, false),

        /**
         * Return the version only.
         */
        VERSION(false, true),

        /**
         * Return both the value and the version.
         */
        ALL(true, true),

        /**
         * Do not return the value or the version.
         */
        NONE(false, false);

        private final boolean needValue;
        private final boolean needVersion;

        private Choice(boolean needValue, boolean needVersion) {
            this.needValue = needValue;
            this.needVersion = needVersion;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needValue() {
            return needValue;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needVersion() {
            return needVersion;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needValueOrVersion() {
            return needValue || needVersion;
        }
    }
}
