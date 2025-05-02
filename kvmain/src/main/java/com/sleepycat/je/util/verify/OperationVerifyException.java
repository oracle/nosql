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

package com.sleepycat.je.util.verify;

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.utilint.NotSerializable;

/**
 * Thrown during btree verification if a problem is detected in an IN. The
 * exception causes verification of the DB to abort and the verifier will
 * proceed to the next DB.
 *
 * <p>An OperationFailureException is used so that it can safely be thrown
 * by fetch operations and will percolate up through the verifier's
 * cursor operation. It is an internal exception and should never surface
 * through the API.</p>
 */
@SuppressWarnings("serial")
public class OperationVerifyException extends OperationFailureException
    implements NotSerializable {

    public OperationVerifyException() {
        super("Internal BtreeVerifier signal");
    }

    /**
     * Only for use by wrapSelf methods.
     */
    private OperationVerifyException(
        final String message,
        final OperationFailureException cause) {

        super(message, cause);
    }

    @Override
    public OperationFailureException wrapSelf(
        final String msg,
        final OperationFailureException clonedCause) {

        return new OperationVerifyException(msg, clonedCause);
    }
}
