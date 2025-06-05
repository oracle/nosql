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

import java.lang.reflect.Type;
import java.util.List;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.ph.HealthStatus;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Component instance health record, reported by both webtier and global
 * component.
 */
public class InstanceHealthRecord extends AbstractHealthRecord {

    private String componentName;
    private String componentId;
    private String hostName;

    /**
     * No args constructor for use in serialization.
     */
    public InstanceHealthRecord() {
    }

    public InstanceHealthRecord(long reportTime,
                                HealthStatus status,
                                List<String> errors,
                                String customVersion,
                                Object customData,
                                String componentName,
                                String componentId,
                                String hostName) {
        super(reportTime, status, errors, customVersion, customData);
        this.componentName = componentName;
        this.componentId = componentId;
        this.hostName = hostName;
    }

    public String getComponentName() {
        return componentName;
    }

    public String getComponentId() {
        return componentId;
    }

    public String getHostName() {
        return hostName;
    }

    @Override
    public String getId() {
        return hostName + "_" + componentId;
    }

    public static class SerDe
        implements JsonSerializer<InstanceHealthRecord>,
                   JsonDeserializer<InstanceHealthRecord> {

        @Override
        public JsonElement serialize(InstanceHealthRecord record,
                                     Type type,
                                     JsonSerializationContext ctx) {
            JsonObject json = new JsonObject();
            AbstractHealthRecord.serialize(json, record, ctx);
            json.add("componentName", ctx.serialize(record.getComponentName()));
            json.add("componentId", ctx.serialize(record.getComponentId()));
            json.add("hostName", ctx.serialize(record.getHostName()));
            json.add("id", ctx.serialize(record.getId()));
            return json;
        }

        @Override
        public InstanceHealthRecord deserialize(JsonElement json,
                                                Type type,
                                                JsonDeserializationContext ctx)
            throws JsonParseException {

            JsonObject jsonObject = json.getAsJsonObject();
            InstanceHealthRecord record = JsonUtils.fromJson(
                jsonObject.toString(),
                InstanceHealthRecord.class);

            AbstractHealthRecord.deserialize(jsonObject, record);
            return record;
        }
    }
}
