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

package oracle.kv.impl.rep;

/**
 * Thrown when an RN environment is invalid while processing a Request.
 * The exception is used when a request failed due to an
 * EnvironmentFailureException and the transaction can be retried on
 * another Rn. The RepNodeServiceFaultHandler rethrows this exception
 * as a RNUnavailableException. This exception is not returned to the
 * client. It is used to differentiate between a request that failed
 * and can be retried from one that cannot.
 *
 */
public class EnvironmentFailureRetryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EnvironmentFailureRetryException(Throwable cause) {
        super(cause);
    }
}
