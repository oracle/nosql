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

package oracle.kv.impl.xregion.stat;

import java.io.Serializable;
import java.util.Objects;

import oracle.kv.impl.xregion.service.XRegionServiceMetrics;
import oracle.nosql.common.json.JsonUtils;

/**
 * Object represents the header part shared by
 * {@link XRegionServiceMetrics} and
 * {@link oracle.kv.impl.xregion.service.MRTableMetrics}
 */
abstract public class JsonMetricsHeader implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * agent id
     */
    private String agentId;
    /**
     * Local region name
     */
    private volatile String localRegion;
    /**
     * Beginning timestamp of the stat interval
     */
    private volatile long beginMs;
    /**
     * End timestamp of the stat interval
     */
    private volatile long endMs;
    /**
     * Stat collection interval in ms
     */
    private volatile long intervalMs;

    /**
     * No args constructor for use in serialization,
     * used when constructing instance from JSON.
     */
    public JsonMetricsHeader() {

    }

    public JsonMetricsHeader(String agentId, String localRegion) {
        if (agentId == null || agentId.isEmpty()) {
            throw new IllegalArgumentException("Null agent id");
        }
        this.agentId = agentId;
        this.localRegion = localRegion;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getLocalRegion() {
        return localRegion;
    }

    public long getBeginMs() {
        return beginMs;
    }

    public long getEndMs() {
        return endMs;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setAgentId(String val) {
        agentId = val;
    }

    public void setBeginMs(long val) {
        beginMs = val;
    }

    public void setEndMs(long val) {
        endMs = val;
        intervalMs = Math.max(0, endMs - beginMs);
    }

    @Override
    public String toString() {
        return JsonUtils.prettyPrint(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof JsonMetricsHeader)) {
            return false;
        }
        final JsonMetricsHeader other = (JsonMetricsHeader) obj;
        return agentId.equals(other.getAgentId()) &&
               Objects.equals(localRegion, other.getLocalRegion()) &&
               beginMs == other.beginMs &&
               endMs == other.endMs &&
               intervalMs == other.intervalMs;
    }

    @Override
    public int hashCode() {
        return agentId.hashCode() +
               Objects.hashCode(localRegion) +
               Long.hashCode(beginMs) +
               Long.hashCode(beginMs) +
               Long.hashCode(intervalMs);
    }
}
