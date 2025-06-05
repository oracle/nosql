/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.ph;

/**
 * TODO merge with oracle.nosql.health.HealthStatus
 * HealthStatus indicates the health of a service.
 */
public enum HealthStatus {

    GREEN(0, "Good"),
    YELLOW(1, "Warning"),
    RED(2, "Error");

    /*
     * A int code to represent the service status, is mostly used by tools.
     */
    private int code;

    /*
     * Human readable String message to know the service status.
     */
    private String message;

    private HealthStatus(int statusCode, String statusMessage) {
        this.code = statusCode;
        this.message = statusMessage;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
