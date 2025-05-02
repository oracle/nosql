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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.pubsub.NoSQLSubscriberId;

/**
 * Object represents exception raised from {@link XRegionService}. The agent
 * has to shutdown on the exception.
 */
public class ServiceException extends RuntimeException {

    private static final long serialVersionUID = 1;

    /* Max # of table names recorded in failure to avoid overhead  */
    private static final int MAX_NUM_TABLE_NAME = 16;

    /* subscriber id of service agent */
    private final NoSQLSubscriberId sid;

    /* region failures indexed by source region */
    private final Map<RegionInfo, RegionFailure> failures;

    /**
     * Constructs an instance of exception
     *
     * @param sid       nosql subscriber id
     */
    ServiceException(NoSQLSubscriberId sid, String err) {
        super(err);
        this.sid = sid;
        failures = new HashMap<>();
    }

    /**
     * Gets id of of the failed inbound stream
     *
     * @return id of of the failed inbound stream
     */
    public NoSQLSubscriberId getSubscriberId() {
        return sid;
    }

    /**
     * Adds a failure to exception
     *
     * @param srcRegion source region
     * @param tables    tables that affected by the failure
     * @param cause     exception being wrapped
     */
    void addRegionFailure(RegionInfo srcRegion,
                          Set<String> tables,
                          Throwable cause) {
        if (tables == null || tables.isEmpty()) {
            failures.put(srcRegion,
                         new RegionFailure(new ArrayList<>(), cause));
            return;
        }

        final int sz = Math.min(MAX_NUM_TABLE_NAME, tables.size());
        final List<String> tbs =
            new ArrayList<>(tables).subList(0, sz);
        failures.put(srcRegion, new RegionFailure(tbs, cause));
    }

    /**
     * Gets set of failed regions
     *
     * @return set of failed regions
     */
    Set<RegionInfo> getRegions() {
        return failures.keySet();
    }

    /**
     * Returns affected tables because of failure in given region
     *
     * @param region given region
     *
     * @return affected tables because of failure in given region
     */
    List<String> getAffectdTables(RegionInfo region) {
        if (!failures.containsKey(region)) {
            return null;
        }
        return failures.get(region).tables;
    }

    /**
     * Returns cause of failure
     *
     * @param region given region
     *
     * @return cause of failure
     */
    public Throwable getCause(RegionInfo region) {
        if (!failures.containsKey(region)) {
            return null;
        }
        return failures.get(region).getCause();
    }

    /* Region exception */
    private static class RegionFailure {

        /* tables involves in the failure if any */
        private final List<String> tables;

        /* cause of failure */
        private final Throwable cause;

        RegionFailure(List<String> tables, Throwable cause) {
            this.tables = tables;
            this.cause = cause;
        }

        public Throwable getCause() {
            return cause;
        }
    }
}
