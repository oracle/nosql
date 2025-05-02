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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Exception raised when one or more tables are not found in kvstore when
 * performing an operation on a subscription.
 *
 * @since 19.3
 */
public class SubscriptionTableNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /* name of not found tables */
    private final Set<String> tableNames;

    /**
     * @hidden
     *
     * Constructs a SubscriptionTableNotFoundException instance from
     * an array of table names that are not found.
     */
    public SubscriptionTableNotFoundException(String... tables) {
        super(((tables.length == 1) ? "Table" : "Tables") +
              " were not found: " + Arrays.toString(tables));
        tableNames = new HashSet<>(Arrays.asList(tables));
    }

    /**
     * Gets the set of tables that are not found
     *
     * @return set of tables that are not found
     */
    public Set<String> getTables() {
        return tableNames;
    }
}
