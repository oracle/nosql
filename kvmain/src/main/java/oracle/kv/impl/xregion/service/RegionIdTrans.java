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


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.util.Pair;
import oracle.kv.pubsub.NoSQLSubscriberId;

/**
 * Object that manages translation of the region id in a row from a source
 * region to the region id in the map to the target region.
 */
public class RegionIdTrans {

    /**
     * For each source region known to the service, maintain a translation
     * table (a mapping from region id to region name) to translate the
     * region id in all operations streamed from the source to the mapping of
     * target region.
     *
     * The mapping for a source region is a transient table that is
     * constructed from source region when the service starts up, and goes
     * away when the service shuts down.
     *
     * <p>
     * The key of the mapping is the region id from the source region,
     * and the value of the mapping is 1) its region name and 2) the
     * region id in target region mapping.
     *
     * For example, suppose source region is BOS and target region is JFK, an
     * operation from BOS with region id 2 represents an operation from
     * region SFO. At JFK, region SFO is represented as region id 4. The
     * translation mapping of SFO would be like
     *
     * RID at source  |   RID at target  |  Region Name
     * -------------------------------------------------
     *     2          |        4         |   SFO
     *
     */

    /* private logger */
    private final Logger logger;

    /* translation tables indexed by store name in source region */
    private final ConcurrentMap<String, TransTable> tables;

    /* region id mapper in local region */
    private final RegionIdMapping localMapper;

    /* subscriber id */
    private final NoSQLSubscriberId sid;

    /* all source regions indexed by name */
    private final Map<String, RegionInfo> srcRegions;

    /* parent metadata manager */
    private final ServiceMDMan parent;

    /* true if the translation table is ready to use */
    private volatile boolean ready;

    /**
     * KVStore of all source regions, unit test only
     * normally the kvstore can be pulled from parent metadata manager. In
     * some unit test without parent metadata manager, the kvstore of source
     * regions are stored in the map
     */
    private Map<RegionInfo, KVStore> srcRegionKVS;

    /**
     * Constructs region is translation tables for parent service
     *
     * @param parent  parent service
     * @param logger  private logger
     */
    RegionIdTrans(ServiceMDMan parent, Logger logger) {

        this.parent = parent;
        this.logger = logger;

        sid = parent.getSid();
        tables = new ConcurrentHashMap<>();

        /* fetch region id table for target */
        final KVStore tgtKVS = parent.getServRegionKVS();
        final String tgtRegionName = parent.getServRegion().getName();
        localMapper = new RegionIdMapping(tgtRegionName, tgtKVS, sid, logger);
        logger.fine(() -> lm("local region id mapper ready, " +
                             "local region=" + tgtRegionName));
        srcRegions = new HashMap<>();
        ready = true;
    }

    /**
     * Unit test only, without parent
     */
    RegionIdTrans(NoSQLSubscriberId sid,
                  Map<RegionInfo, KVStore> srcRegionKVS,
                  KVStore tgtKVS,
                  Logger logger) {

        this.logger = logger;
        this.sid = sid;
        this.srcRegionKVS = srcRegionKVS;

        parent = null;
        srcRegions = new HashMap<>();
        srcRegionKVS.keySet().forEach(r -> srcRegions.put(r.getName(), r));
        tables = new ConcurrentHashMap<>();

        /* fetch region id table for target */
        localMapper = new RegionIdMapping(tgtKVS, sid, logger);
        /* initialize the mapping for each source */
        srcRegions.values().forEach(this::initTransTable);
        logger.fine(() -> lm("RegionId translation ready"));
        ready = true;
    }

    /**
     * Clears all translation tables
     */
    public void close() {
        ready = false;
        tables.keySet().forEach(k -> {
            tables.get(k).clear();
            tables.remove(k);
        });
        tables.clear();
        logger.fine(() -> lm("Translation table has been cleared"));
    }

    /**
     * Returns true if the table is ready
     *
     * @return true if the table is ready
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * Translate the region id from a source region to that of the target
     * region.
     *
     * @param source name of region from which the operation is streamed
     * @param rid    region id in the streamed operation
     *
     * @return region id from local map, or null if the remote
     * region is unknown.
     */
    public int translate(String source, int rid) {

        if (!isReady()) {
            throw new IllegalStateException("Translation is not available");
        }

        final TransTable transTable = getTransTable(source);
        if (transTable == null) {
            final String err = "No translation table for source region " +
                               source + ", treat it as all originated from " +
                               "the source, original rid: " + rid;
            logger.finest(() -> lm(err));
            return localMapper.getRegionIdByName(source);
        }

        final Entry entry = transTable.getTargetMapping(rid);
        if (entry != null) {
            /* we can translate */
            logger.finest(() -> lm("RegionId=" + rid + " from source=" +
                                   source + " translated to target region " +
                                   "id=" + entry.getRegionId() + ", name= " +
                                   entry.getRegionName()));
            return entry.getRegionId();
        }

        /* cannot translate, new region id */
        logger.fine(() -> lm("RegionId=" + rid + " from source=" + source +
                             " in not in translation table"));
        final RegionInfo srcRegion =
            srcRegions.values()
                      .stream()
                      .filter(r -> r.getName().equals(source))
                      .collect(Collectors.toList()).iterator().next();
        final KVStore srcKVS = getKVS(srcRegion);
        final RegionIdMapping srcMapping =
            new RegionIdMapping(srcRegion.getName(), srcKVS, sid, logger);
        final String regionName = srcMapping.getRegionName(rid);
        if (regionName == null) {
            final String err = "Region=" + source + " does not have the " +
                               "region id=" + rid + " in its table md";
            logger.warning(err);
            throw new IllegalStateException(err);
        }

        /* check if target region has this region in its region id table */
        final int regionid = localMapper.getRegionIdByName(regionName);
        /* add a new translation entry */
        transTable.addMapping(rid, new Entry(regionid, regionName));
        logger.fine(() -> lm("Translation added for region name=" + regionName +
                             " from region id at source=" + rid +
                             " to region id at target=" + regionid));

        return regionid;
    }

    /**
     * initialize the mapping for each source
     */
    void initRegionTransTable(Set<RegionInfo> regions) {
        regions.forEach(r -> srcRegions.put(r.getName(), r));
        srcRegions.values().forEach(this::initTransTable);
        logger.info(lm("Region id translation ready, source regions=" +
                       srcRegions.keySet()));
    }

    /**
     * Unit test only
     */
    Map<String, TransTable> getTransTables() {
        return tables;
    }

    /*----------- PRIVATE FUNCTIONS --------------*/

    private String lm(String msg) {
        return "[TRANS-" + sid + "] " + msg;
    }

    private KVStore getKVS(RegionInfo region) {
        if (parent == null) {
            /* unit test only */
            return srcRegionKVS.get(region);
        }
        return parent.getRegionKVS(region);
    }

    /**
     * Initializes the translation table for a region
     *
     * @param src         source region
     */
    private void initTransTable(RegionInfo src) {

        final TransTable ret = new TransTable(src.getName());
        final KVStore srcKVS = getKVS(src);
        final RegionIdMapping srcMap =
            new RegionIdMapping(src.getName(), srcKVS, sid, logger);

        /* add mapping for source region */
        final String name = src.getName();
        final int id = localMapper.getRegionIdByName(name);
        ret.addMapping(Region.LOCAL_REGION_ID, new Entry(id, name));

        /* add mapping for other region ids */
        if (srcMap.getKnownRegions() != null) {
            srcMap.getKnownRegions().forEach((srcId, rname) -> {
                final int tgtId = localMapper.getRegionIdByName(rname);
                if (tgtId == Region.UNKNOWN_REGION_ID) {
                    final String err = "Region=" + rname +
                                       " (region id=" + srcId + ") is" +
                                       " unknown in target store, ignore";
                    logger.info(lm(err));
                } else {
                    /* build translate entry */
                    ret.addMapping(srcId, new Entry(tgtId, rname));
                }
            });
        }
        tables.put(src.getName(), ret);
        logger.info(lm(ret.toString()));
    }

    /**
     * Returns the translation table for a source region, initialize if the
     * table does not exist
     *
     * @param source source region name
     * @return translation table
     */
    private TransTable getTransTable(String source) {
        final TransTable transTable = tables.get(source);
        if (transTable == null) {
            if (parent == null) {
                /* in unit test */
                return null;
            }
            final String err = "Initialize trans table for source=" + source;
            logger.fine(() -> lm(err));
            initTransTable(parent.getRegion(source));
        }
        return tables.get(source);
    }

    /**
     * Entry in the translation table. It records mapping of region id to
     * region name at the target region.
     */
    static class Entry extends Pair<Integer, String> {

        Entry(int rid, String rname) {

            super(rid, rname);
            if (rid <= 0) {
                throw new IllegalArgumentException(
                    "Invalid region id=" + rid + ", name=" + rname);
            }
            if (rname == null || rname.isEmpty()) {
                throw new IllegalArgumentException("Invalid region name");
            }
        }

        int getRegionId() {
            return first();
        }

        String getRegionName() {
            return second();
        }

        @Override
        public String toString() {
            return getRegionId() + "(" + getRegionName() + ")";
        }
    }

    /**
     * RegionId translation table. For each source region, maintains a
     * translation table from region id of source region to the region id and
     * region name of target region.
     */
    static class TransTable {

        /* name of source region */
        private final String srcRegion;

        /*  mapping from source rid to (rid, name)  pair at target */
        private final Map<Integer, Entry> trans;

        TransTable(String srcRegion) {
            this.srcRegion = srcRegion;
            trans = new HashMap<>();
        }

        /**
         * Adds a mapping entry.
         *
         * @param rid    region id from source region
         * @param entry  mapping entry at target region
         */
        void addMapping(int rid, Entry entry) {
            trans.put(rid, entry);
        }

        /**
         * Gets a mapping entry
         *
         * @param rid region id from source region
         * @return mapping entry at target region
         */
        Entry getTargetMapping(int rid) {
            return trans.get(rid);
        }

        void clear() {
            trans.clear();
        }

        @Override
        public String toString() {
            return "Region Id translation for region=" + srcRegion + ":\n" +
                   trans.keySet().stream()
                        .map(rid -> rid + " -> " + trans.get(rid))
                        .collect(Collectors.joining("\n"));
        }
    }
}
