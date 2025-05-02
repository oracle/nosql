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

package oracle.kv.impl.admin;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.client.DdlJsonFormat;
import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;

/**
 * Defines the display and formatting of status info and results for Ddl
 * operation output.
 */
public class DdlResultsReport {

    private static final int CURRENT_OUTPUT_VERSION = 2;

    /**
     * {
     *   "version" : "2",
     *   "comment" : "Statement did not require execution"
     * }
     */
    public static final String NOOP_STATUS_JSON =
        "{\n  \"" + DdlJsonFormat.VERSION_TAG + "\" : \"" +
        CURRENT_OUTPUT_VERSION + "\",\n  \"comment\" : \"" +
        DdlJsonFormat.NOOP_STATUS + "\"\n}";

    /**
     * {
     *   "version" : "2",
     *   "comment" : "Statement completed"
     * }
     */
    public static final String STATEMENT_COMPLETED = "Statement completed.";
    public static final String STATEMENT_COMPLETED_JSON =
        "{\n  \"" + DdlJsonFormat.VERSION_TAG + "\" : \"" +
        CURRENT_OUTPUT_VERSION + "\",\n  \"comment\" : \"" +
        STATEMENT_COMPLETED + "\"\n}";

    private final String status;
    private final String statusAsJson;
    private final String result;

    /**
     * Returns the human-readable format.
     */
    String getStatus() {
        return status;
    }

    /**
     * Returns the JSON format.
     */
    String getStatusAsJson() {
        return statusAsJson;
    }

    /**
     * Generate status and save results for non-plan DDL operations -- noops,
     * show, describe
     * @param serialVersion
     */
    DdlResultsReport(DdlHandler handler, short serialVersion) {
        result = handler.getResultString();

        if (result == null) {
            status = DdlJsonFormat.NOOP_STATUS; /* this is text, not json */
            statusAsJson = NOOP_STATUS_JSON;
        } else {
            status = STATEMENT_COMPLETED;
            statusAsJson = STATEMENT_COMPLETED_JSON;
        }
    }

    /**
     * Generate results for plan DDL operations - the status is a plan history,
     * formatted either for human readability, or as JSON.
     */
    DdlResultsReport(Plan p, @SuppressWarnings("unused") short serialVersion) {
        /*
         * The planRun has information about the last execution of this plan.
         * Hang onto this planRun in case another run starts.
         */
        StatusReport statusReport =
            new StatusReport(p, StatusReport.VERBOSE_BIT);
        status = statusReport.display();

        /**
         * {
         *   "version" : "2",
         *   "info" : JSON from plan status report
         * }
         */
        ObjectNode o = JsonUtils.createObjectNode();

        o.put(DdlJsonFormat.VERSION_TAG, CURRENT_OUTPUT_VERSION);

        JsonNode reportAsJson = statusReport.displayAsJson();
        o.set("planInfo", reportAsJson);
        statusAsJson = JsonUtils.toJsonString(o, true);
        result = null;
    }

    String getResult() {
        return result;
    }
}
