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

import oracle.kv.FaultException;

/**
 * Exception that will be raised when NoSQL Publisher cannot be created due
 * to errors, or experiences irrecoverable errors and must be closed. It is a
 * wrapper of FaultException.
 */
public class PublisherFailureException extends FaultException {

    private static final long serialVersionUID = 1L;

    /*
     * From the reactive stream specification, Rule Publisher.4, when the
     * PublisherFailureException is raised in the context of a subscription, it
     * must signal onError to subscriber.
     *
     * https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification
     *
     */

    /* subscriber index if the failure happens in a context of subscriber */
    private final NoSQLSubscriberId subscriberId;

    /**
     * @hidden
     *
     * Creates an instance of exception with context of subscription.
     *
     * @param subscriberId   subIndex of NoSQL subscriber using the publisher
     * @param msg        error message
     * @param isRemote   true if failure at remote
     * @param cause      cause of the failure
     */
    PublisherFailureException(NoSQLSubscriberId subscriberId,
                              String msg,
                              boolean isRemote,
                              Throwable cause) {
        super(msg, cause, isRemote);
        this.subscriberId = subscriberId;
    }

    /**
     * @hidden
     *
     * Creates an instance of exception when the publisher initiates
     * and before a subscriber is available, without a context of subscription.
     *
     * @param msg        error message
     * @param isRemote   true if failure at remote
     * @param cause      cause of the failure
     */
    public PublisherFailureException(String msg,
                                     boolean isRemote,
                                     Throwable cause) {
        this(null, msg, isRemote, cause);
    }

    /**
     * Gets the subscriber id that was trying to use the publisher
     *
     * @return  the subscriber id that was trying to use the publisher
     */
    public NoSQLSubscriberId getSubscriberId() {
        return subscriberId;
    }
}
