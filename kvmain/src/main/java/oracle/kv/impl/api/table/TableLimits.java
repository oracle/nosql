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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */

/**
 * Container for table limits.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class TableLimits implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1L;

    /* Initial implementation */
    private static final int V1 = 1;

    private static final int CURRENT_VERSION = V1;

    /* Schema version of this instance */
    private final int version;

    /**
     * Limit value to indicate that no limit is to be enforced
     */
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /*
     * Limit value to indicate that a limit is not to be changed
     */
    public static final int NO_CHANGE = -1;

    /**
     * TableLimit instance to set a table read-only.
     */
    public static TableLimits READ_ONLY =
                        new TableLimits(NO_CHANGE, 0, NO_CHANGE);

    /**
     * TableLimit instance to disable all read and write accesses to a table
     */
    public static TableLimits NO_ACCESS = new TableLimits(0, 0, NO_CHANGE);

    /*
     * The default populate limit is 100%. This default (vs. NO_LIMIT) prevents
     * users from going over their allocation in the case that it is not set.
     */
    private static int DEFAULT_POPULATE_LIMIT = 100;

    /**
     * The limit values. A value less than NO_LIMIT indicates that a limit
     * is present and should be enforced. A value of NO_CHANGE indicates that
     * that limit should not be changed. Limit objects with NO_CHANGE values
     * are used to set values through the API and should not be used for
     * enforcement before init() is called.
     */

    /* KB/sec */
    private int readLimit;
    private int writeLimit;
    /* GB */
    private int sizeLimit;
    /* # of indexes */
    private int indexLimit;
    /* # of child tables */
    private int childTableLimit;
    /* index key size limit in bytes */
    private int indexKeySizeLimit;
    /* index population limit in % of read/write limits */
    private int indexPopulateLimit;

    /**
     * Constructs a table limit object. The read and write limits are specified
     * in KB/second. Size limit is in GB. A value of NO_LIMIT indicates that
     * no limit is enforced. A value of NO_CHANGE will indicate that a limit
     * is not changed when this object is applied to an existing table with
     * limits.
     *
     * @param readLimit the read limit in KB/second
     * @param writeLimit the write limit in KB/second
     * @param sizeLimit the table size limit in GB
     * @param maxIndexes the maximum number of indexes
     * @param maxChildren the maximum number child tables
     * @param indexKeySizeLimit the maximum size of an index key in bytes
     * @param indexPopulateLimit the % of throughput that index
     * population may consume
     *
     * @throws IllegalArgumentException if indexPopulateLimitPercent is not
     * NO_CHANGE or NO_LIMIT and is less than 1 or greater than 100
     */
    public TableLimits(int readLimit, int writeLimit,
                       int sizeLimit, int maxIndexes,
                       int maxChildren, int indexKeySizeLimit,
                       int indexPopulateLimit) {
        version = CURRENT_VERSION;
        this.readLimit = readLimit;
        this.writeLimit = writeLimit;
        this.sizeLimit = sizeLimit;
        this.indexLimit = maxIndexes;
        this.childTableLimit = maxChildren;
        this.indexKeySizeLimit = indexKeySizeLimit;
        checkIndexPopulateLimit(indexPopulateLimit);
        this.indexPopulateLimit = indexPopulateLimit;
    }

    /**
     * Constructor for FastExternalizable.
     */
    public TableLimits(DataInput in,
                       @SuppressWarnings("unused") short serialVersion)
        throws IOException
    {
        version = readPackedInt(in);
        readLimit = readPackedInt(in);
        writeLimit = readPackedInt(in);
        sizeLimit = readPackedInt(in);
        indexLimit = readPackedInt(in);
        childTableLimit = readPackedInt(in);
        indexKeySizeLimit = readPackedInt(in);
        indexPopulateLimit = readPackedInt(in);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code version}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code readLimit}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code writeLimit}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code sizeLimit}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code indexLimit}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code childTableLimit}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code indexKeySizeLimit}
     * <li> ({@link SerializationUtil#writePackedInt packed int})
     *      {@code indexPopulateLimit}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        writePackedInt(out, version);
        writePackedInt(out, readLimit);
        writePackedInt(out, writeLimit);
        writePackedInt(out, sizeLimit);
        writePackedInt(out, indexLimit);
        writePackedInt(out, childTableLimit);
        writePackedInt(out, indexKeySizeLimit);
        writePackedInt(out, indexPopulateLimit);
    }

    /*
     * Checks index populate limit. Valid values are 1 through 100, NO_CHANGE,
     * and NO_LIMIT.
     * TODO - Should NO_LIMIT be allowed?
     */
    private void checkIndexPopulateLimit(int value) {
        if ((value == NO_CHANGE) || (value == NO_LIMIT)) {
            return;
        }
        if ((value < 1) || (value > 100)) {
            throw new IllegalArgumentException("Invalid index populate limit" +
                                               " percent: " + value);
        }
    }

    /**
     * Constructs a table limit object. The read and write limits are specified
     * in KB/second. Size limit is in GB. A value of NO_LIMIT indicates that
     * no limit is enforced. A value of NO_CHANGE will indicate that a limit
     * is not changed when this object is applied to an existing table with
     * limits.
     *
     * @param readLimit the read limit in KB/second
     * @param writeLimit the write limit in KB/second
     * @param sizeLimit the table size limit in GB
     * @param maxIndexes the maximum number of indexes
     * @param maxChildren the maximum number child tables
     * @param indexKeySizeLimit the maximum size of an index key in bytes
     */
    public TableLimits(int readLimit, int writeLimit,
                       int sizeLimit, int maxIndexes,
                       int maxChildren, int indexKeySizeLimit) {
        this(readLimit, writeLimit, sizeLimit,
             maxIndexes, maxChildren, indexKeySizeLimit,
             NO_CHANGE);
    }

    /**
     * A convenience constructor for a table limit object with only the
     * read, write, and size limits. Limits on indexes, child
     * tables, or index key size are not changed.
     *
     * @param readLimit the read limit in KB/second
     * @param writeLimit the write limit in KB/second
     * @param sizeLimit the table size limit in GB
     */
    public TableLimits(int readLimit, int writeLimit, int sizeLimit) {
        this(readLimit, writeLimit, sizeLimit,
             NO_CHANGE, NO_CHANGE, NO_CHANGE, NO_CHANGE);
    }

    /**
     * A convenience constructor for a table limit object that only sets
     * the index populate limit percent.
     */
    public TableLimits(int indexPopulateLimit) {
        this(NO_CHANGE, NO_CHANGE, NO_CHANGE, NO_CHANGE, NO_CHANGE, NO_CHANGE,
             indexPopulateLimit);
    }

    /*
     * Initializes this instance. If any values have not been set (-1)
     * they are initialized to their default value or the value from the old
     * limits if non-null.
     */
    void init(TableLimits oldLimits) {
        if (readLimit < 0) {
            readLimit = (oldLimits == null) ? NO_LIMIT :
                                              oldLimits.getReadLimit();
        }
        if (writeLimit < 0) {
            writeLimit = (oldLimits == null) ? NO_LIMIT :
                                               oldLimits.getWriteLimit();
        }
        if (sizeLimit < 0) {
            sizeLimit = (oldLimits == null) ? NO_LIMIT :
                                              oldLimits.getSizeLimit();
        }
        if (indexLimit < 0) {
            indexLimit = (oldLimits == null) ? NO_LIMIT :
                                               oldLimits.getIndexLimit();
        }
        if (childTableLimit < 0) {
            childTableLimit = (oldLimits == null) ? NO_LIMIT :
                                                 oldLimits.getChildTableLimit();
        }
        if (indexKeySizeLimit < 0) {
            indexKeySizeLimit = (oldLimits == null) ? NO_LIMIT :
                                               oldLimits.getIndexKeySizeLimit();
        }
        /* See note above on why the default is not NO_LIMIT */
        if (indexPopulateLimit < 0) {
            indexPopulateLimit = (oldLimits == null) ? DEFAULT_POPULATE_LIMIT :
                                              oldLimits.getIndexPopulateLimit();
        }
    }

    /**
     * Returns true if this any limits need to be enforced.
     * @return true if this any limits need to be enforced
     */
    public boolean hasLimits() {
        return hasThroughputLimits() ||
               hasSizeLimit() ||
               hasIndexLimit() ||
               hasChildTableLimit() ||
               hasIndexKeySizeLimit();
    }

    /* -- Throughput Limits -- */

    /**
     * Gets the read throughput limit.
     *
     * @return the read throughput limit
     */
    public int getReadLimit() {
        assert readLimit >= 0;
        return readLimit;
    }

    /**
     * Gets the write throughput limit.
     *
     * @return the write throughput limit
     */
    public int getWriteLimit() {
        assert writeLimit >= 0;
        return writeLimit;
    }

    public boolean isReadAllowed() {
        return readLimit > 0;
    }

    public boolean isWriteAllowed() {
        return writeLimit > 0;
    }

    /**
     * Returns true if either read or write throughput limits are set..
     *
     * @return true if either read or write throughput limits are set
     */
    public boolean hasThroughputLimits() {
        assert readLimit >= 0;
        assert writeLimit >= 0;
        return readLimit < Integer.MAX_VALUE ||
               writeLimit < Integer.MAX_VALUE;
    }

    /**
     * Gets the index populate limit percentage.
     *
     * @return the index populate limit percentage or NO_LIMIT.
     */
    public int getIndexPopulateLimit() {
        assert indexPopulateLimit > 0;
        return indexPopulateLimit;
    }

    /* -- Size Limit -- */

    public boolean hasSizeLimit() {
        assert sizeLimit >= 0;
        return sizeLimit < Integer.MAX_VALUE;
    }

    /**
     * Gets the size limit.
     *
     * @return the size limit
     */
    public int getSizeLimit() {
        assert sizeLimit >= 0;
        return sizeLimit;
    }

    /* -- Index Limit -- */

    boolean hasIndexLimit() {
        assert indexLimit >= 0;
        return indexLimit < Integer.MAX_VALUE;
    }

    int getIndexLimit() {
        assert indexLimit >= 0;
        return indexLimit;
    }

    /* -- Child Table Limit -- */

    boolean hasChildTableLimit() {
        assert childTableLimit >= 0;
        return childTableLimit < Integer.MAX_VALUE;
    }

    int getChildTableLimit() {
        assert childTableLimit >= 0;
        return childTableLimit;
    }

    /* -- Index Key Size Limit -- */

    public boolean hasIndexKeySizeLimit() {
        assert indexKeySizeLimit >= 0;
        return indexKeySizeLimit < Integer.MAX_VALUE;
    }

    public int getIndexKeySizeLimit() {
        assert indexKeySizeLimit >= 0;
        return indexKeySizeLimit;
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        /*
         * Checks whether this is a newer version which we don't support. This
         * should not happen as the Admin should prevent sending a new
         * version out until the store has been upgraded.
         */
        if (version > CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version +
                                  ", current version is " + CURRENT_VERSION);
        }

        /*
         * A little cheating going on here. The index populate limit was
         * added after V1 was released but before TableLimits was used.
         * Since 0 is not a possible value, if it is 0 then we know that
         * this instance has deserialized from an older version (which
         * should not happen). Adding a check for safety.
         */
        if (indexPopulateLimit == 0) {
            indexPopulateLimit = DEFAULT_POPULATE_LIMIT;
        }
    }

    @Override
    public String toString() {
        return "TableLimits[" + parseLimit(readLimit) + ", " +
                                parseLimit(writeLimit) + ", " +
                                parseLimit(sizeLimit) + ", " +
                                parseLimit(indexLimit) + ", " +
                                parseLimit(childTableLimit) + ", " +
                                parseLimit(indexKeySizeLimit) + ", " +
                                parseLimit(indexPopulateLimit) + "]";
    }

    private String parseLimit(int value) {
        return (value < 0) ? "NO_CHANGE" :
                       (value < Integer.MAX_VALUE ? Integer.toString(value) :
                                                    "NO_LIMIT");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TableLimits)) {
            return false;
        }
        final TableLimits other = (TableLimits) obj;
        return (readLimit == other.readLimit) &&
            (writeLimit == other.writeLimit) &&
            (sizeLimit == other.sizeLimit) &&
            (indexLimit == other.indexLimit) &&
            (childTableLimit == other.childTableLimit) &&
            (indexKeySizeLimit == other.indexKeySizeLimit) &&
            (indexPopulateLimit == other.indexPopulateLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readLimit,
                            writeLimit,
                            sizeLimit,
                            indexLimit,
                            childTableLimit,
                            indexKeySizeLimit,
                            indexPopulateLimit);
    }
}
