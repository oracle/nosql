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

package oracle.kv.impl.rep.table;

import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;

import oracle.kv.KeySizeLimitException;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableLimits;

/**
 *
 */
public class IndexKeyCreator implements SecondaryKeyCreator,
                                        SecondaryMultiKeyCreator {

    private volatile IndexImpl index;

    /*
     * Index key size limit. If there is no limit, set to Integer.MAX_VALUE
     */
    private volatile int keySizeLimit;
    /* used for limit calculation */
    private volatile int numStringFields;

    /*
     * Keep this state to make access faster
     */
    private final boolean keyOnly;
    private final boolean isMultiKey;

    private final int maxKeysPerRow;


    public IndexKeyCreator(IndexImpl index, int maxKeysPerRow) {
        setIndex(index);
        this.keyOnly = index.isKeyOnly();
        this.isMultiKey = index.isMultiKey();
        this.maxKeysPerRow = maxKeysPerRow;
    }

    boolean primaryKeyOnly() {
        return keyOnly;
    }

    public boolean isMultiKey() {
        return isMultiKey;
    }

    /**
     * Refreshes the index reference. It should be refreshed when the table
     * metadata is updated.
     *
     * @param newIndex the new index object.
     */
    final void setIndex(IndexImpl newIndex) {
        index = newIndex;
        final TableLimits tl =
                    index.getTable().getTopLevelTable().getTableLimits();

        /* Set to Integer.MAX_VALUE if no limit */
        keySizeLimit = ((tl != null) && tl.hasIndexKeySizeLimit()) ?
                                  tl.getIndexKeySizeLimit() : Integer.MAX_VALUE;
        if (keySizeLimit != Integer.MAX_VALUE) {
            /*
             * num String fields is used to subtract the string size
             * overhead when calculating index key size
             */
            numStringFields = 0;
            for (int i = 0; i < index.numFields(); i++) {
                if (index.getFieldDef(i).isString()) {
                    ++numStringFields;
                }
            }
        }
    }

    /* -- From SecondaryKeyCreator -- */

    @Override
    public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                      DatabaseEntry key,
                                      DatabaseEntry data,
                                      long modTime,
                                      long expTime,
                                      int size,
                                      DatabaseEntry result) {
        byte[] res =
            index.extractIndexKey(key.getData(),
                                  (data != null ? data.getData() : null),
                                  modTime,
                                  expTime,
                                  size,
                                  keyOnly);
        if (res != null) {
            checkKeySizeLimit(res.length);
            result.setData(res);
            return true;
        }
        return false;
    }

    /* -- From SecondaryMultiKeyCreator -- */

    @Override
    public void createSecondaryKeys(SecondaryDatabase secondaryDb,
                                    DatabaseEntry key,
                                    DatabaseEntry data,
                                    long modTime,
                                    long expTime,
                                    int size,
                                    Set<DatabaseEntry> results) {

        /*
         * Ideally we'd pass the results Set to index.extractIndexKeys but
         * IndexImpl is client side as well and DatabaseEntry is not currently
         * in the client classes pulled from JE.  DatabaseEntry is simple, but
         * also references other JE classes that are not client side.  It is a
         * slippery slope.
         *
         * If the extra object allocations show up in profiling then something
         * can be done.
         */
        List<byte[]> res = index.extractIndexKeys(key.getData(),
                                                  data.getData(),
                                                  modTime,
                                                  expTime,
                                                  size,
                                                  keyOnly,
                                                  maxKeysPerRow);
        if (res != null) {
            for (byte[] bytes : res) {
                /* check size limit for each individual key added */
                checkKeySizeLimit(bytes.length);
                boolean added = results.add(new DatabaseEntry(bytes));

                if (!added && index.isUnique()) {
                    throw new IllegalArgumentException(
                        "Duplicate index entry for unique index: " +
                        index.getName());
                }
            }
        }
    }

    /**
     * Throws KeySizeLimitException if an index key size limit is set for the
     * index and the key length is greater than the limit.
     *
     * Subtract overhead from the length before comparing:
     *  1. one byte per component that is a special value indicator such as
     *  EMPTY, NULL, JSON NULL or 0 for a normal value. This indicator exists
     *  only for nullable fields so we're "giving away" a byte for each
     *  non-nullable component (e.g. primary key fields). Primary key size
     *  limits will be enforced elsewhere.
     *  2. one byte per String component to account for the size of the string
     *  that's included in the length. This is done to make index key the same
     *  as primary key size enforcement which does not include that overhead.
     *
     * @param length key length to check
     */
    private void checkKeySizeLimit(int length) {
        /* subtract overhead from raw byte length */
        int checkLen = length - index.numFields() - numStringFields;
        if (checkLen > keySizeLimit) {
            throw new KeySizeLimitException(index.getTable().getFullName(),
                                            index.getName(),
                                            keySizeLimit,
                                            "index key of " + checkLen +
                                            " bytes exceeded limit of " +
                                            keySizeLimit);
        }
    }

    @Override
    public String toString() {
        return "IndexKeyCreator[" + index.getName() + "]";
    }
}
