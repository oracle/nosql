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

package oracle.kv.pubsub;

/**
 * Exception raised when a subscription fails. It will be raised to caller,
 * and signaled via onError implemented by subscriber.
 */
public class SubscriptionFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /* id of the failed subscriber */
    private final NoSQLSubscriberId sid;

    /**
     * @hidden
     *
     * Constructor of a general subscription failure
     *
     * @param sid     id of failed subscriber
     * @param msg     the exception message
     */
    public SubscriptionFailureException(NoSQLSubscriberId sid,
                                        String msg) {
        this(sid, msg, null);
    }

    /**
     * @hidden
     *
     * Constructor of a subscription failure with cause
     *
     * @param sid     id of failed subscriber
     * @param msg     exception message
     * @param cause   exception being wrapped
     */
    public SubscriptionFailureException(NoSQLSubscriberId sid,
                                        String msg,
                                        Throwable cause) {
        super(msg, cause);
        this.sid = sid;
    }

    /**
     * Gets id of of the failed subscriber
     *
     * @return id of of the failed subscriber
     */
    public NoSQLSubscriberId getSubscriberId() {
        return sid;
    }
}
