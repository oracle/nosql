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

package oracle.kv.impl.tif;

/**
 * Object representing subscription state
 */
enum SubscriptionState {
    /**
     * Subscription manager is ready to stream data from source.
     */
    READY,

    /**
     * Uses partition transfer to receive data from source. Multiple
     * partition readers may be running concurrently depending on parameter
     * setting.
     */
    PARTITION_TRANSFER,

    /**
     * Uses replication stream from feeder to receive data from source. There
     * is no concurrent partition transfer at this stage.
     */
    REPLICATION_STREAM,

    /**
     * Subscription shut down by client
     */
    SHUTDOWN,

    /**
     * Subscription shuts down due to error
     */
    ERROR
}
