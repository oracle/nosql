/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.http;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.contextlogger.CorrelationId;
import oracle.nosql.common.contextlogger.LogContext;

public class LogControl {

    public static final String NULL_TENANT_ID = "nullTenantId";
    /*
     * For the time being, stuff both entrypoints and tenants into the same
     * map.  Later, perhaps this will be refined.
     */
    private final Map<String, String> ctlMap = new ConcurrentHashMap<>();

    /*
     * Partial path matches. Note: LinkedHashMap used because it's faster to
     * iterate over keys, and thread safety/concurrency is not an issue since
     * this will have extremely rare updates.
     */
    private final Map<String, String> partialMap = new LinkedHashMap<>();

    /**
     * Get the log level for a tenantId.
     * Note that a valid Level may be set for a null tenantId, so
     * that requests that haven't yet been given a valid tentantId (before
     * access context check) can still be logged.
     */
    Level getTenantLogLevel(String tenantId) {
        if (tenantId == null) {
            tenantId = NULL_TENANT_ID;
        }
        final String level = ctlMap.get(tenantId);
        if (level != null) {
            return Level.parse(level);
        }
        return Level.OFF;
    }

    Level getEntrypointLogLevel(String entrypoint) {
        if (entrypoint != null) {
            String level = ctlMap.get(entrypoint);
            if (level == null) {
                level = findPartialLevel(entrypoint);
            }
            if (level != null) {
                return Level.parse(level);
            }
        }
        return Level.OFF;
    }

    public void setTenantLogLevel(String tenantId, Level level) {
        ctlMap.put(tenantId, level.toString());
    }

    public void setEntrypointLogLevel(String entrypoint, Level level) {
        ctlMap.put(entrypoint, level.toString());
    }

    public void setPartialEntrypointLogLevel(String partial, Level level) {
        partialMap.put(partial, level.toString());
    }

    public void removeTenantLogLevel(String tenantId) {
        ctlMap.remove(tenantId);
    }

    public void removeEntrypointLogLevel(String entrypoint) {
        ctlMap.remove(entrypoint);
    }

    public void removePartialEntrypointLogLevel(String partial) {
        partialMap.remove(partial);
    }

    public boolean isEmpty() {
        return (ctlMap.isEmpty() && partialMap.isEmpty());
    }

    public String toJsonString() {
        if (isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder()
            .append("{\n \"exact\":")
            .append(JsonUtils.toJson(ctlMap))
            .append(",\n \"partial\":")
            .append(JsonUtils.toJson(partialMap))
            .append("\n}");
        return sb.toString();
    }

    /**
     * Produce a new LogContext with a tentative log level based on path.
     * Later, if a tenantId is discovered, the LogContext's tenantId and level
     * might be modified by updateLogLevel, below.
     */
    public LogContext generateLogContext(String path) {

        final Level lt = getTenantLogLevel(null);
        final Level le = getEntrypointLogLevel(path);
        Level level = le.intValue() < lt.intValue() ? le : lt;

        return new LogContext(CorrelationId.getNext(), path, null, level);
    }

    /**
     * An extant LogContext object can be updated with new information when the
     * tenantId is discovered.
     */
    public void updateLogContext(LogContext ctx, String tenantId) {
        ctx.putOrigin(tenantId);

        final Level lt = getTenantLogLevel(tenantId);
        final Level le = getEntrypointLogLevel(ctx.getEntry());
        Level newLevel = le.intValue() < lt.intValue() ? le : lt;
        ctx.putLogLevel(newLevel);
    }

    /**
     * Find a level for a partial path match.
     */
    private String findPartialLevel(String entrypoint) {
        if (partialMap.isEmpty()) {
            return null;
        }
        for (String s: partialMap.keySet()) {
            if (entrypoint.contains(s)) {
                return partialMap.get(s);
            }
        }
        return null;
    }
}
