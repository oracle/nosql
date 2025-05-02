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
 * Exception raised when change cannot be applied to the running
 * subscription, and the subscription is unchanged. It represents an
 * situation that the change cannot applied to the the subscription and the
 * subscription is not affected and thus does not need to terminate.
 *
 * @since 19.3
 */
public class SubscriptionChangeNotAppliedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * The max number of pending subscription changes is 32.
     */
    public final static long MAX_NUM_PENDING_CHANGES = 32;

    /* id of the failed subscriber */
    private final NoSQLSubscriberId sid;

    /* reason that the change not applied */
    private final Reason reason;

    /**
     * @hidden
     *
     * Constructs an instance of exception
     *
     * @param sid    subscriber id
     * @param reason the reason for the exception
     * @param error  error message
     */
    public SubscriptionChangeNotAppliedException(NoSQLSubscriberId sid,
                                                 Reason reason,
                                                 String error) {
        super(error);
        this.reason = reason;
        this.sid = sid;
    }

    /**
     * Gets id of of the subscriber
     *
     * @return id of of the subscriber
     */
    public NoSQLSubscriberId getSubscriberId() {
        return sid;
    }

    /**
     * Returns the reason that the change was not applied.
     *
     * @return the reason that the change was not applied
     */
    public Reason getReason() {
        return reason;
    }

    /**
     * Describes the reason that a change was not applied.
     */
    public enum Reason {

        /**
         * The number of pending concurrent changes has reached {@link
         * #MAX_NUM_PENDING_CHANGES}, and application should try again
         * later.
         */
        TOO_MANY_PENDING_CHANGES,

        /**
         * The subscription has been canceled.
         */
        SUBSCRIPTION_CANCELED,

        /**
         * The change was requested for a subscription that is configured to
         * stream all tables, and subscribing or unsubscribing from individual
         * tables for a subscription configured to stream all tables is not
         * supported.
         */
        SUBSCRIPTION_ALL_TABLES,

        /**
         * The change is trying to add a table which exists in subscription.
         */
        TABLE_ALREADY_SUBSCRIBED,

        /**
         * The change is trying to remove a table which does not exist in
         * subscription.
         */
        TABLE_NOT_SUBSCRIBED,

        /**
         * Change timeout
         */
        CHANGE_TIMEOUT
    }
}
