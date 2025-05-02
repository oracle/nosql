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

import com.sleepycat.je.txn.Locker;

/**
 * Thrown if an attempt is made to use a {@link Transaction} after it has been
 * invalidated as the result of an XA failure.  The invalidation occurs when
 * {@code XAResource.end} is called by the resource manager with a {@code
 * XAResource.TMFAIL} flag.
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * @since 4.0
 */
public class XAFailureException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public XAFailureException(Locker locker) {
        super(locker, true /*abortOnly*/,
              "The TM_FAIL flag was passed to XAEnvironment.end().",
              null /*cause*/);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private XAFailureException(String message,
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

        return new XAFailureException(msg, clonedCause);
    }
}
