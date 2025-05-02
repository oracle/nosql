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

import java.util.Map;

/**
 * Object which maps region names to IDs and back.
 */
public interface RegionMapper {

    /**
     * Gets the region name for the specified ID. Returns null if the
     * ID does not correspond to a known region. Throws IllegalArgumentException
     * if the ID is not a valid region ID;
     *
     * @param id region ID
     * @return region name or null
     */
    String getRegionName(int id);

    /**
     * Gets the region ID for the specified name. Returns -1 if the
     * name is not an active region. Region names are case-insensitive.
     *
     * @param regionName the region name
     * @return region ID or -1
     */
    int getRegionId(String regionName);

    /**
     * Returns a map of known regions where the key is region ID and
     * the value is the region name. Returns null if there are no known regions.
     *
     * @return known region map or null
     */
    Map<Integer, String> getKnownRegions();

    /**
     * Gets the sequence number of the mapper. The mapper sequence number is the
     * sequence number of the table metadata at the time the mapper was created.
     *
     * @return the sequence number of the mapper
     */
    int getSequenceNumber();

    /**
     * Returns true if there are no known regions in this mapper.
     *
     * @return true if are no known regions in this mapper
     */
    boolean isEmpty();
}
