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

package oracle.kv.impl.xregion.service;

import java.util.Map;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RegionMapper;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.pubsub.NoSQLSubscriberId;

/**
 * Object that manages the mapping of region id and region name. It is a
 * wrapper over {@link RegionMapper} in {@link TableMetadata}.
 */
class RegionIdMapping {

    /** private logger */
    private final Logger logger;

    /** kvstore handle of the table */
    private final KVStore kvs;

    /** region name */
    private final String rgName;

    /** parent service id */
    private final NoSQLSubscriberId parentId;

    /**
     * region mapper from table api, initialized in startup and refreshed when
     * a region cannot be found
     */
    private RegionMapper regionMapper;

    /**
     * Creates an instance of region id table
     *
     * @param rgName   region name
     * @param kvs      kvstore of the table
     * @param parentId parent service id
     * @param logger   private logger
     */
    RegionIdMapping(String rgName,
                    KVStore kvs,
                    NoSQLSubscriberId parentId,
                    Logger logger) {
        this.rgName = rgName;
        this.kvs = kvs;
        this.parentId = parentId;
        this.logger = logger;
        refreshRegionMapper();
    }

    /** Unit test only, use store name as region name */
    RegionIdMapping(KVStore kvs,
                    NoSQLSubscriberId parentId,
                    Logger logger) {
        this.rgName = ((KVStoreImpl) kvs).getTopology().getKVStoreName();
        this.kvs = kvs;
        this.parentId = parentId;
        this.logger = logger;
        refreshRegionMapper();
    }

    /**
     * Returns the region name with the region id, or null if unknown region id
     *
     * @param rid region id
     * @return the region name with the region id, or null
     */
    String getRegionName(int rid) {
        final String region = regionMapper.getRegionName(rid);
        if (region != null) {
            return region;
        }
        refreshRegionMapper();
        return regionMapper.getRegionName(rid);
    }

    /**
     * Returns a region id by region rname or -1 if the region is unknown
     *
     * @param rname region name
     * @return Returns a region id, or -1
     */
    int getRegionIdByName(String rname) {
        final int id = regionMapper.getRegionId(rname);
        if (id != Region.UNKNOWN_REGION_ID) {
            return id;
        }
        refreshRegionMapper();
        return regionMapper.getRegionId(rname);
    }

    /**
     * Returns id region map for all known regions
     *
     * @return id region map
     */
    Map<Integer, String> getKnownRegions() {
        return regionMapper.getKnownRegions();
    }

    /**
     * Unit test only
     */
    String dumpMapping() {
        final StringBuilder sb = new StringBuilder();
        final Map<Integer, String> known = getKnownRegions();
        final String rgs = (known == null) ? "none" : known.values().toString();
        sb.append("Known regions=").append(rgs);
        /* start from the first non-local region */
        int i = Region.REGION_ID_START;
        while (true) {
            final String rname = getRegionName(i);
            if (rname == null) {
                break;
            }
            sb.append("\nregion id=").append(i).append(" -> ").append(rname);
            i++;
        }
        return sb.toString();
    }


    /*--------------------*
     * Private functions  *
     *--------------------*/

    /* logger header */
    private String lm(String msg) {
        return "[RIT-" + parentId + "-" + rgName + "] " + msg;
    }

    /**
     * Updates the region mapper.
     */
    private synchronized void refreshRegionMapper() {
        final int seq =
            (regionMapper == null) ? 0 : regionMapper.getSequenceNumber();
        final TableAPIImpl api = (TableAPIImpl) kvs.getTableAPI();
        regionMapper = api.getRegionMapper();
        logger.info(lm("Region mapper refreshed from seq #=" + seq +
                       " to #=" + regionMapper.getSequenceNumber()));
    }
}
