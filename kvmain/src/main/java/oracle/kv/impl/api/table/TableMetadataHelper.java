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

package oracle.kv.impl.api.table;

/**
 * TableMetadataHelper
 *
 * Interface used by code that needs to acquire TableImpl instances. It will
 * be extended to also allow callbacks to manipulate table and namespace names.
 */

public interface TableMetadataHelper {

    /**
     * Returns the table object, if it exists, null if not.
     * @param tableName the table name, which may be a child table name of the
     * format parent.child
     * @return the table object, or null if it does not exist
     */
    TableImpl getTable(String namespace, String tableName);

    /**
     * Gets the specified table with an optional resource cost. If the table
     * is not found, null is returned. The specified cost will be charged
     * against the table's resource limits. If the cost is greater than 0
     * and the table has resource limits and those limits have been exceeded,
     * either by this call, or by other table activity a ResourceLimitException
     * will be thrown.
     *
     * @param namespace the table namespace
     * @param tablePath the table name in component form where each component
     * @param cost the cost to be applied to the table resources
     * @return the table object or null
     */
    TableImpl getTable(String namespace, String[] tablePath, int cost);

    /**
     * Gets a region mapper.
     *
     * @return a region mapper
     */
    default RegionMapper getRegionMapper() {
        return null;
    }
}
