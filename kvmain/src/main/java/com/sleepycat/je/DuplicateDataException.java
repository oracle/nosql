/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je;

/**
 * Thrown by {@link Cursor#putCurrent Cursor.putCurrent} if the old and new
 * data are not equal according to the configured duplicate comparator or
 * default comparator.
 *
 * <p>If the old and new data are unequal according to the comparator, this
 * would change the sort order of the record, which would change the cursor
 * position, and this is not allowed.  To change the sort order of a record,
 * delete it and then re-insert it.</p>
 *
 * <p>The {@link Transaction} handle is <em>not</em> invalidated as a result of
 * this exception.</p>
 *
 * @since 4.0
 */
public class DuplicateDataException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public DuplicateDataException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private DuplicateDataException(String message,
                                   OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(
        String msg,
        OperationFailureException clonedCause) {

        return new DuplicateDataException(msg, clonedCause);
    }
}
