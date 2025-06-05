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

package oracle.nosql.util.ph.record;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.util.ph.HealthStatus;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

/**
 * Common health record reported by all targets.
 */
public abstract class AbstractHealthRecord {

    private long reportTime;
    private HealthStatus status;
    private List<String> errors;
    private String customVersion;
    private Object customData;

    /**
     * No args constructor for use in serialization.
     */
    protected AbstractHealthRecord() {
    }

    protected AbstractHealthRecord(long reportTime,
                                   HealthStatus status,
                                   List<String> errors,
                                   String customVersion,
                                   Object customData) {
        this.reportTime = reportTime;
        this.status = status;
        this.errors = (errors == null) ?
                      new ArrayList<>() :
                      new ArrayList<>(errors);
        this.customVersion = customVersion;
        this.customData = customData;
    }

    public long getReportTime() {
        return reportTime;
    }

    public HealthStatus getStatus() {
        return status;
    }

    public int getStatusCode() {
        return status.getCode();
    }

    public String getStatusMessage() {
        return status.getMessage();
    }

    /**
     * For use in deserialization: convert status code to HealthStatus Enum.
     */
    public void setStatusCode(int statusCode) {
        if (statusCode == HealthStatus.GREEN.getCode()) {
            status = HealthStatus.GREEN;
        } else if (statusCode == HealthStatus.YELLOW.getCode()) {
            status = HealthStatus.YELLOW;
        } else if (statusCode == HealthStatus.RED.getCode()) {
            status = HealthStatus.RED;
        } else {
            throw new IllegalArgumentException(
                statusCode + " is an unknown code for HealthStatus");
        }
    }

    public String getCustomVersion() {
        return customVersion;
    }

    public Object getCustomData() {
        return customData;
    }

    public List<String> getErrors() {
        return errors;
    }

    public boolean isExpired(long expiredTime) {
        return reportTime < expiredTime;
    }

    public abstract String getId();

    protected static void serialize(JsonObject json,
                                    AbstractHealthRecord record,
                                    JsonSerializationContext ctx) {
        json.add("statusMessage", ctx.serialize(record.getStatusMessage()));
        json.add("errors", ctx.serialize(record.getErrors()));
        json.add("reportTime", ctx.serialize(record.getReportTime()));
        json.add("customData", ctx.serialize(record.getCustomData()));
        json.add("customVersion", ctx.serialize(record.getCustomVersion()));
        json.add("statusCode", ctx.serialize(record.getStatusCode()));
    }

    protected static void deserialize(JsonObject json,
                                      AbstractHealthRecord record)
        throws JsonParseException {
        record.setStatusCode(json.get("statusCode").getAsInt());
    }
}
