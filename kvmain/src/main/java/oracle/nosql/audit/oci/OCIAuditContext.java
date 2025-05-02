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
package oracle.nosql.audit.oci;

import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.UUID;

import oracle.nosql.audit.AuditContext;
import oracle.nosql.common.JsonBuilder;

/**
 * OCI Audit model.
 * To record OCI audit context which will be used to generate audit content.
 * Details: https://confluence.oci.oraclecorp.com/display/CLEV/Audit+v2+Schemas
 */
public class OCIAuditContext implements AuditContext {

    public static final String CREATE_TABLE = "CREATETABLE";
    public static final String ALTER_TABLE = "ALTERTABLE";
    public static final String DROP_TABLE = "DROPTABLE";
    public static final String CHANGE_COMPARTMENT = "CHANGECOMPARTMENT";
    public static final String CREATE_INDEX = "CREATEINDEX";
    public static final String DROP_INDEX = "DROPINDEX";
    public static final String ADD_REPLICA = "ADDREPLICA";
    public static final String DROP_REPLICA = "DROPREPLICA";

    public static final String EVENT_TYPE_PATTERN = "com.oraclecloud.%s.%s%s";
    public static final String EVENT_TYPE_SUFFIX_BEGIN = ".begin";
    public static final String EVENT_TYPE_SUFFIX_END = ".end";

    public static final String SERVICE_NAME = "NoSQL";
    public static final String SOURCE = "NoSQLApi";
    public static final String CLOUD_EVENTS_VERSION = "0.1";
    public static final String EVENT_TYPE_VERSION = "2.0";
    public static final String CONTENT_TYPE = "application/json";

    private static final DateFormat dateFormat;
    static {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static final String CLOUDEVENTSVERSION_FIELD = "cloudEventsVersion";
    public final String cloudEventsVersion;

    private static final String EVENTTYPEVERSION_FIELD = "eventTypeVersion";
    public final String eventTypeVersion;

    private static final String SOURCE_FIELD = "source";
    public final String source;

    private static final String SERVICENAME_FIELD = "serviceName";
    public final String serviceName;

    private static final String CONTENTTYPE_FIELD = "contentType";
    public final String contentType;

    private static final String EVENTID_FIELD = "eventId";
    public String eventId;

    private static final String EVENTTIME_FIELD = "eventTime";
    public Date eventTime;

    private static final String EVENTTYPE_FIELD = "eventType";
    public String eventType;

    private static final String DATA_FIELD = "data";
    public Data data;

    public OCIAuditContext() {
        this.cloudEventsVersion = CLOUD_EVENTS_VERSION;
        this.eventTypeVersion = EVENT_TYPE_VERSION;
        this.serviceName = SERVICE_NAME;
        this.contentType = CONTENT_TYPE;
        this.source = SOURCE;
        this.data = new Data();
        reset();
    }

    public void reset() {
        eventId = createEventId();
        eventTime = Date.from(Instant.now());
        eventType = null;
        data.reset();
    }

    public String getPartitionId() {
      if (data == null) {
          return null;
      }
      final String partitionId = data.compartmentId;
      if (partitionId != null && !partitionId.isEmpty()) {
          return partitionId;
      }
      if (data.identity != null) {
          return data.identity.tenantId;
      }
      return null;
    }

    private String createEventId() {
        return UUID.randomUUID().toString();
    }

    public static class Data {
        private static final String EVENTGROUPINGID_FIELD = "eventGroupingId";
        public String eventGroupingId;

        private static final String EVENTNAME_FIELD = "eventName";
        public String eventName;

        private static final String COMPARTMENTID_FIELD = "compartmentId";
        public String compartmentId;

        private static final String COMPARTMENTNAME_FIELD = "compartmentName";
        public String compartmentName;

        private static final String RESOURCENAME_FIELD = "resourceName";
        public String resourceName;

        private static final String RESOURCEID_FIELD = "resourceId";
        public String resourceId;

        private static final String RESOURCEVERSION_FIELD = "resourceVersion";
        public String resourceVersion;

        private static final String AVAILABILITYDOMAIN_FIELD = "availabilityDomain";
        public String availabilityDomain;

        private static final String TAGSLUG_FIELD = "tagSlug";
        public byte[] tagSlug;

        private static final String IDENTITY_FIELD = "identity";
        public OCIIdentity identity;

        private static final String REQUEST_FIELD = "request";
        public Request request;

        private static final String RESPONSE_FIELD = "response";
        public Response response;

        private static final String STATECHANGE_FIELD = "stateChange";
        public StateChange stateChange;

        private static final String ADDITIONALDETAILS_FIELD = "additionalDetails";
        public Map<String, Object> additionalDetails;

        private static final String INTERNALDETAILS_FIELD = "internalDetails";
        public InternalDetails internalDetails;

        public Data() {
            this.identity = new OCIIdentity();
            this.request = new Request();
            this.response = new Response();
            this.stateChange = new StateChange();
            this.additionalDetails = new HashMap<>();
            this.internalDetails = new InternalDetails();
            reset();
        }

        public void reset() {
            this.eventGroupingId = null;
            this.eventName = null;
            this.compartmentId = null;
            this.compartmentName = null;
            this.resourceName = null;
            this.resourceId = null;
            this.resourceVersion = null;
            this.availabilityDomain = null;
            this.tagSlug = null;
            this.identity.reset();
            this.request.reset();
            this.response.reset();
            this.stateChange.reset();
            this.additionalDetails.clear();
            this.internalDetails.reset();
        }

        @Override
        public String toString() {
            return "Data [eventGroupingId=" + eventGroupingId +
                    ", eventName=" + eventName +
                    ", compartmentId=" + compartmentId +
                    ", compartmentName=" + compartmentName +
                    ", resourceName=" + resourceName +
                    ", resourceId=" + resourceId +
                    ", resourceVersion=" + resourceVersion +
                    ", availabilityDomain=" + availabilityDomain +
                    ", tagSlug=" + Arrays.toString(tagSlug) +
                    ", identity=" + identity +
                    ", request=" + request +
                    ", response=" + response +
                    ", stateChange=" + stateChange +
                    ", additionalDetails=" + additionalDetails +
                    ", internalDetails=" + internalDetails + "]";
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            if (eventGroupingId != null) {
                jb.append(EVENTGROUPINGID_FIELD, eventGroupingId);
            }
            if (eventName != null) {
                jb.append(EVENTNAME_FIELD, eventName);
            }
            if (compartmentId != null) {
                jb.append(COMPARTMENTID_FIELD, compartmentId);
            }
            if (compartmentName != null) {
                jb.append(COMPARTMENTNAME_FIELD, compartmentName);
            }
            if (resourceName != null) {
                jb.append(RESOURCENAME_FIELD, resourceName);
            }
            if (resourceId != null) {
                jb.append(RESOURCEID_FIELD, resourceId);
            }
            if (resourceVersion != null) {
                jb.append(RESOURCEVERSION_FIELD, resourceVersion);
            }
            if (availabilityDomain != null) {
                jb.append(AVAILABILITYDOMAIN_FIELD, availabilityDomain);
            }
            if (tagSlug != null) {
                byte[] encoded = Base64.getEncoder().encode(tagSlug);
                jb.append(TAGSLUG_FIELD, new String(encoded));
            }
            jb.appendJson(IDENTITY_FIELD, identity.toJsonString());
            jb.appendJson(REQUEST_FIELD, request.toJsonString());
            jb.appendJson(RESPONSE_FIELD, response.toJsonString());
            jb.appendJson(STATECHANGE_FIELD, stateChange.toJsonString());
            jb.appendJson(INTERNALDETAILS_FIELD, internalDetails.toJsonString());
            putObject(jb, ADDITIONALDETAILS_FIELD, additionalDetails);
            return jb.toString();
        }
    }

    public static class OCIIdentity {
        private static final String PRINCIPALNAME_FIELD = "principalName";
        public String principalName;

        private static final String PRINCIPALID_FIELD = "principalId";
        public String principalId;

        private static final String AUTHTYPE_FIELD = "authType";
        public String authType;

        private static final String CALLERNAME_FIELD = "callerName";
        public String callerName;

        private static final String CALLERID_FIELD = "callerId";
        public String callerId;

        private static final String TENANTID_FIELD = "tenantId";
        public String tenantId;

        private static final String IPADDRESS_FIELD = "ipAddress";
        public String ipAddress;

        private static final String CREDENTIALS_FIELD = "credentials";
        public String credentials;

        private static final String AUTHZPOLICIES_FIELD = "authZPolicies";
        public Object authZPolicies;

        private static final String USERGROUPS_FIELD = "userGroups";
        public Object userGroups;

        private static final String USERAGENT_FIELD = "userAgent";
        public String userAgent;

        private static final String CONSOLESESSIONID_FIELD = "consoleSessionId";
        public String consoleSessionId;

        public void reset() {
            principalName = null;
            principalId = null;
            authType = null;
            callerName = null;
            callerId = null;
            tenantId = null;
            ipAddress = null;
            credentials = null;
            authZPolicies = null;
            userGroups = null;
            userAgent = null;
            consoleSessionId = null;
        }

        @Override
        public String toString() {
            return "OCIIdentity [principalName=" + principalName +
                   ", principalId=" + principalId +
                   ", authType=" + authType +
                   ", callerName=" + callerName +
                   ", callerId=" + callerId +
                   ", tenantId=" + tenantId +
                   ", ipAddress=" + ipAddress +
                   ", credentials=" + credentials +
                   ", authZPolicies=" + authZPolicies +
                   ", userGroups=" + userGroups +
                   ", userAgent=" + userAgent +
                   ", consoleSessionId=" + consoleSessionId + "]";
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            if (principalName != null) {
                jb.append(PRINCIPALNAME_FIELD, principalName);
            }
            if (principalId != null) {
                jb.append(PRINCIPALID_FIELD, principalId);
            }
            if (authType != null) {
                jb.append(AUTHTYPE_FIELD, authType);
            }
            if (callerName != null) {
                jb.append(CALLERNAME_FIELD, callerName);
            }
            if (callerId != null) {
                jb.append(CALLERID_FIELD, callerId);
            }
            if (tenantId != null) {
                jb.append(TENANTID_FIELD, tenantId);
            }
            if (ipAddress != null) {
                jb.append(IPADDRESS_FIELD, ipAddress);
            }
            if (credentials != null) {
                jb.append(CREDENTIALS_FIELD, credentials);
            }
            if (authZPolicies != null) {
                putObject(jb, AUTHZPOLICIES_FIELD, authZPolicies);
            }
            if (userGroups != null) {
                putObject(jb, USERGROUPS_FIELD, userGroups);
            }
            if (userAgent != null) {
                jb.append(USERAGENT_FIELD, userAgent);
            }
            if (consoleSessionId != null) {
                jb.append(CONSOLESESSIONID_FIELD, consoleSessionId);
            }
            return jb.toString();
        }
    }

    public static class Request {
        private static final String ID_FIELD = "id";
        public String id;

        private static final String PATH_FIELD = "path";
        public String path;

        private static final String ACTION_FIELD = "action";
        public String action;

        private static final String PARAMETERS_FIELD = "parameters";
        public Map<String, List<String>> parameters;

        private static final String HEADERS_FIELD = "headers";
        public Map<String, List<String>> headers;

        public Request() {
            this.parameters = new HashMap<>();
            this.headers = new HashMap<>();
        }

        public void reset() {
            this.id = null;
            this.path = null;
            this.action = null;
            this.parameters.clear();
            this.headers.clear();
        }

        @Override
        public String toString() {
            return "Request [id=" + id + ", path=" + path +
                   ", action=" + action + ", parameters=" + parameters +
                   ", headers=" + headers + "]";
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            if (id != null) {
                jb.append(ID_FIELD, id);
            }
            if (path != null) {
                jb.append(PATH_FIELD, path);
            }
            if (action != null) {
                jb.append(ACTION_FIELD, action);
            }
            putObject(jb, PARAMETERS_FIELD, parameters);
            putObject(jb, HEADERS_FIELD, headers);
            return jb.toString();
        }
    }

    public static class Response {
        private static final String STATUS_FIELD = "status";
        public String status;

        private static final String RESPONSETIME_FIELD = "responseTime";
        public Date responseTime;

        private static final String HEADERS_FIELD = "headers";
        public Map<String, List<String>> headers;

        private static final String PAYLOAD_FIELD = "payload";
        public Map<String, Object> payload;

        private static final String MESSAGE_FIELD = "message";
        public String message;

        public Response() {
            this.headers = new HashMap<>();
            this.payload = new HashMap<>();
        }

        public void reset() {
            this.status = null;
            this.responseTime = null;
            this.headers.clear();
            this.payload.clear();
            this.message = null;
        }

        @Override
        public String toString() {
            return "Response [status=" + status +
                   ", responseTime=" + responseTime +
                   ", headers=" + headers +
                   ", payload=" + payload +
                   ", message=" + message + "]";
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            if (status != null) {
                jb.append(STATUS_FIELD, status);
            }
            if (responseTime != null) {
                jb.append(RESPONSETIME_FIELD, getDateStr(responseTime));
            }
            if (message != null) {
                jb.append(MESSAGE_FIELD, message);
            }
            putObject(jb, HEADERS_FIELD, headers);
            putObject(jb, PAYLOAD_FIELD, payload);
            return jb.toString();
        }
    }

    public static class StateChange {
        private static final String PREVIOUS_FIELD = "previous";
        public Map<String, Object> previous;

        private static final String CURRENT_FIELD = "current";
        public Map<String, Object> current;

        public StateChange() {
            this.previous = new HashMap<>();
            this.current = new HashMap<>();
        }

        public void reset() {
            this.previous.clear();
            this.current.clear();
        }

        @Override
        public String toString() {
            return "StateChange [previous=" + previous +
                   ", current=" + current + "]";
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            putObject(jb, PREVIOUS_FIELD, previous);
            putObject(jb, CURRENT_FIELD, current);
            return jb.toString();
        }
    }

    public static class InternalDetails {
        private static final String ATTRIBUTES_FIELD = "attributes";
        public Map<String, Object> attributes;

        public InternalDetails() {
            this.attributes = new HashMap<>();
        }

        public void reset() {
            this.attributes.clear();
        }

        @Override
        public String toString() {
            return "InternalDetails [attributes=" + attributes + "]";
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            putObject(jb, ATTRIBUTES_FIELD, attributes);
            return jb.toString();
        }
    }

    @Override
    public String toString() {
        return "OCIAuditContext [cloudEventsVersion=" + cloudEventsVersion +
               ", eventTypeVersion=" + eventTypeVersion +
               ", source=" + source +
               ", serviceName=" + serviceName +
               ", contentType=" + contentType +
               ", eventId=" + eventId +
               ", eventTime=" + eventTime +
               ", eventType=" + eventType +
               ", data=" + data + "]";
    }

    public String toJsonString() {
        final JsonBuilder jb = JsonBuilder.create();
        if (cloudEventsVersion != null) {
            jb.append(CLOUDEVENTSVERSION_FIELD, cloudEventsVersion);
        }
        if (eventTypeVersion != null) {
            jb.append(EVENTTYPEVERSION_FIELD, eventTypeVersion);
        }
        if (source != null) {
            jb.append(SOURCE_FIELD, source);
        }
        if (serviceName != null) {
            jb.append(SERVICENAME_FIELD, serviceName);
        }
        if (contentType != null) {
            jb.append(CONTENTTYPE_FIELD, contentType);
        }
        if (eventId != null) {
            jb.append(EVENTID_FIELD, eventId);
        }
        if (eventTime != null) {
            jb.append(EVENTTIME_FIELD, getDateStr(eventTime));
        }
        if (eventType != null) {
            jb.append(EVENTTYPE_FIELD, eventType);
        }
        if (data != null) {
            jb.appendJson(DATA_FIELD, data.toJsonString());
        }
        return jb.toString();
    }

    /**
     * For change compartment operation, we need also generate an Audit context
     * for destination compartment.
     */
    public OCIAuditContext getDestContext(String destCompartmentId) {
        final OCIAuditContext context = new OCIAuditContext();
        context.eventTime = eventTime;
        context.eventType = eventType;
        context.data.eventGroupingId = data.eventGroupingId;
        context.data.eventName = data.eventName;
        context.data.compartmentId = destCompartmentId;
        context.data.resourceName = data.resourceName;
        context.data.resourceId = data.resourceId;
        context.data.resourceVersion = data.resourceVersion;
        context.data.availabilityDomain = data.availabilityDomain;
        context.data.tagSlug = data.tagSlug;
        context.data.identity = data.identity;
        context.data.request = data.request;
        context.data.response = data.response;
        context.data.stateChange = data.stateChange;
        context.data.additionalDetails = data.additionalDetails;
        context.data.internalDetails = data.internalDetails;
        return context;
    }

    /**
     * Generate am Audit context for operation end.
     */
    public OCIAuditContext getOpEndContext() {
        if (data.eventName == null) {
            return null;
        }
        final OCIAuditContext context = new OCIAuditContext();
        context.eventTime = eventTime;
        context.eventType = String.format(EVENT_TYPE_PATTERN,
                                          serviceName,
                                          data.eventName,
                                          EVENT_TYPE_SUFFIX_END);
        context.data.eventGroupingId = data.eventGroupingId;
        context.data.eventName = data.eventName;
        context.data.compartmentId = data.compartmentId;
        context.data.resourceName = data.resourceName;
        context.data.resourceId = data.resourceId;
        context.data.resourceVersion = data.resourceVersion;
        context.data.availabilityDomain = data.availabilityDomain;
        context.data.tagSlug = data.tagSlug;
        context.data.identity = data.identity;
        context.data.request = data.request;
        context.data.response = data.response;
        context.data.stateChange = data.stateChange;
        context.data.additionalDetails = data.additionalDetails;
        context.data.internalDetails = data.internalDetails;
        return context;
    }

    /* The date and formatter are not thread safe. */
    private static String getDateStr(Date date) {
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    private static JsonBuilder putObject(JsonBuilder jb,
                                         String name,
                                         Object val) {
        if (val == null) {
            jb.append(name, val);
            return jb;
        }
        if (val instanceof Number || val instanceof Boolean) {
            jb.append(name, val);
        } else if (val instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) val;
            jb.appendJson(name, getJsonFromMap(map));
        } else if (val instanceof List) {
            List<?> list = (List<?>) val;
            jb.appendJson(name, getJsonFromList(list));
        } else if (val.getClass().isArray()) {
            jb.appendJson(name, getJsonFromArray(val));
        } else {
            jb.append(name, val.toString());
        }
        return jb;
    }

    private static String getJsonFromMap(Map<?, ?> map) {
        final JsonBuilder jb = JsonBuilder.create();
        if (map == null) {
            return jb.toString();
        }
        for (Entry<?, ?> entry : map.entrySet()) {
            String name = entry.getKey().toString();
            putObject(jb, name, entry.getValue());
        }
        return jb.toString();
    }

    private static String getJsonFromList(List<?> list) {
        final JsonBuilder jb = JsonBuilder.create(false);
        if (list == null) {
            return jb.toString();
        }
        for (Object entry : list) {
            putObject(jb, null, entry);
        }
        return jb.toString();
    }

    private static String getJsonFromArray(Object array) {
        final JsonBuilder jb = JsonBuilder.create(false);
        if (array == null) {
            return jb.toString();
        }
        for (int i = 0; i < Array.getLength(array); i++) {
            putObject(jb, null, Array.get(array, i));
        }
        return jb.toString();
    }
}
