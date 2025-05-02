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

package oracle.kv.impl.xregion.agent;

/**
 * Region Agent status
 */
public enum RegionAgentStatus {

    /* IDLE */
    IDLE,

    /* in streaming, all tables are available to read and write */
    STREAMING,

    /*
     * in table initialization, all tables are not available until the
     * initialization is done and status switched to {@link #STREAMING}
     */
    INITIALIZING_TABLES,

    /*
     * changing streams by adding tables, tables are available except
     * the tables being added which would be ready after adding is
     * done and status switched to {@link #STREAMING}
     * */
    ADDING_TABLES,

    /*
     * changing streams by removing tables, tables are available except
     * that users may see entries from tables being removed till the
     * removal is done and status switched to {@link #STREAMING}
     */
    REMOVING_TABLES,

    /*
     * changing table parameters, tables are available and the parameter
     * will be effective after changing is done and status switched to
     * {@link #STREAMING}
     */
    CHANGING_PARAMETER,

    /* agent is canceled and all tables are out-of-sync */
    CANCELED
}
