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

package oracle.kv.impl.measurement;

import static oracle.kv.impl.util.FormatUtils.formatDateTimeMillis;

import java.io.Serializable;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService;

import com.google.gson.JsonObject;

/**
 * A notification that a {@link ConfigurableService} has seen logging entries
 * at levels SEVERE, WARNING (or SEC_WARNING), or INFO (or SEC_INFO).
 */
public class LoggingStats implements ConciseStats, Serializable {

    private static final long serialVersionUID = 1L;
    private final ResourceId serviceId;
    private final long severe;
    private final long warning;
    private final long info;
    private final long startTimeMillis;
    private final long endTimeMillis;

    /**
     * Creates an instance of this class.
     *
     * @param serviceId the resource ID of the service
     * @param severe the number of SEVERE log entries
     * @param warning the number of WARNING (or SEC_WARNING) entries
     * @param info the number of INFO (or SEC_INFO) entries
     * @param startTimeMillis the start time of the period in which the logging
     * was seen
     * @param endTimeMillis the end time of the period in which the logging was
     * seen
     */
    public LoggingStats(ResourceId serviceId,
                        long severe,
                        long warning,
                        long info,
                        long startTimeMillis,
                        long endTimeMillis) {
        this.serviceId = serviceId;
        this.severe = severe;
        this.warning = warning;
        this.info = info;
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
    }

    /**
     * Returns the resource ID of the service which had the log entries.
     *
     * @return the resource ID of the service
     */
    public ResourceId getServiceId() {
        return serviceId;
    }

    /**
     * Returns the number of SEVERE log entries logged by the service during
     * the time period.
     *
     * @return the number of SEVERE log entries
     */
    public long getSevere() {
        return severe;
    }

    /**
     * Returns the number of WARNING (or SEC_WARNING) log entries logged by the
     * service during the time period.
     *
     * @return the number of WARNING (or SEC_WARNING) log entries
     */
    public long getWarning() {
        return warning;
    }

    /**
     * Returns the number of INFO (or SEC_INFO) log entries logged by the
     * service during the time period, or zero if not known.
     *
     * @return the number of INFO (or SEC_INFo) log entries or zero
     * @since 21.2
     */
    public long getInfo() {
        return info;
    }

    public JsonObject toJson() {
        final JsonObject result = new JsonObject();
        result.addProperty("serviceId", serviceId.toString());
        result.addProperty("severe", severe);
        result.addProperty("warning", warning);
        result.addProperty("info", info);

        /*
         * Include the start and end times in the JSON format because these
         * entries appear by themselves in the collector output files
         */
        result.addProperty("startTimeMillis", startTimeMillis);
        result.addProperty("startTimeHuman",
                           formatDateTimeMillis(startTimeMillis));
        result.addProperty("endTimeMillis", endTimeMillis);
        result.addProperty("endTimeHuman",
                           formatDateTimeMillis(endTimeMillis));

        return result;
    }

    /* ConciseStats */

    @Override
    public long getStart() {
        return startTimeMillis;
    }

    @Override
    public long getEnd() {
        return endTimeMillis;
    }

    @Override
    public String getFormattedStats() {
        return "Service Log Stats" +
            "\n\tsevere=" + severe +
            "\n\twarning=" + warning +
            "\n\tinfo=" + info +
            "\n";
    }

    /* Object */

    @Override
    public String toString() {
        return "ServiceLogStats[" + getFormattedStats() + "]";
    }
}
