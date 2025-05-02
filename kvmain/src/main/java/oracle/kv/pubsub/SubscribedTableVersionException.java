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

import oracle.kv.impl.api.table.TableVersionException;
import oracle.kv.impl.topo.RepGroupId;

/**
 * @hidden
 *
 * Object represents an exception that version a subscribed table mismatch
 * with the required version.
 */
public class SubscribedTableVersionException extends TableVersionException {

    private static final long serialVersionUID = 1L;

    private final NoSQLSubscriberId si;
    private final RepGroupId repGroupId;
    private final String table;
    private final int currentVer;

    /**
     * Constructs a table version exception during subscription
     *
     * @param subscriptionId  subscription id
     * @param repGroupId      shard where incompatible version is detected
     * @param table           name of table with incompatible version
     * @param requiredVer     required table version
     * @param currentVer      current table version in metadata
     */
    public SubscribedTableVersionException(NoSQLSubscriberId subscriptionId,
                                           RepGroupId repGroupId,
                                           String table,
                                           int requiredVer,
                                           int currentVer) {

        super(requiredVer);
        this.si = subscriptionId;
        this.repGroupId = repGroupId;
        this.table = table;
        this.currentVer = currentVer;
    }

    /**
     * Gets the subscription id
     *
     * @return the subscription id
     */
    public NoSQLSubscriberId getSubscriberId() {
        return si;
    }

    /**
     * Gets the replication group id
     *
     * @return the replication group id
     */
    public RepGroupId getRepGroupId() {
        return repGroupId;
    }

    /**
     * Gets the table name
     *
     * @return the table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Gets the current version of table
     *
     * @return the current version of table
     */
    public int getCurrentVer() {
        return currentVer;
    }
}
