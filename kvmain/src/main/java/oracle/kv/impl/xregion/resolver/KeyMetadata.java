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

package oracle.kv.impl.xregion.resolver;

/**
 * Interface of primary key metadata for conflict resolver
 */
public interface KeyMetadata {
 
    /**
     * Returns the timestamp when the key is inserted, updated or deleted.
     *
     * @return the timestamp when the key is inserted, updated or deleted.
     */
    long getTimestamp();
 
    /**
     * Returns the id of region where the key inserted, updated or deleted.
     *
     * @return id of region where the key inserted, updated or deleted.
     */
    int getRegionId();
}
