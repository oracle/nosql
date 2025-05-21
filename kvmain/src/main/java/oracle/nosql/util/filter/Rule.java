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

package oracle.nosql.util.filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;

/*
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and SC filters service.
 *
 * To serialize a Java object into a Json string:
 *   Rule rule;
 *   String jsonPayload = rule.toJson();
 *
 * To deserialize a Json string into this object:
 *   Rule rule = Rule.fromJson(<JsonString> | <InputStream>);
 *
 * The Rule class represents the filter rule which has below information:
 *   o name, the rule name, required.
 *   o action, the action of the rule, default to DROP_REQUEST.
 *   o tenant, the principal tenant ocid.
 *   o user, the principal ocid.
 *   o table, the target table ocid.
 *   o operations, a list of operations, required.
 *   o createTime, the create time in milliseconds.
 */
public class Rule {

    private static final Gson gson = new GsonBuilder()
        .registerTypeAdapter(Action.class, new ActionSerializer())
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        .create();

    public static final Action DROP_REQUEST = new DropRequestAction();
    private static final Action DEFAULT_ACTION = DROP_REQUEST;

    public enum OpType {
        ALL,            /* all ops */
        DDL,            /* ddl ops */
        WRITE,          /* dml write */
        READ,           /* dml read, read metadata, work-request, usage */
        CONFIG_READ,    /* read configuration */
        CONFIG_UPDATE   /* update configuration */
    }

    public enum ActionType {
        DROP_REQUEST,
        RETURN_ERROR
    };

    /* The name of the rule */
    private String name;
    /* The action to take if match the rule */
    private Action action;

    /* The principal tenant ocid */
    private String tenant;
    /* The principal ocid */
    private String user;
    /* The target table ocid */
    private String table;
    /* The operations */
    private String[] operations;
    /* The time stamp the rule was created */
    private Timestamp createTime;

    private transient Set<OpType> opTypes;

    public static Rule createRule(String name,
                                  Action action,
                                  String tenantId,
                                  String userId,
                                  String tableOcid,
                                  String[] operations) {
        return new Rule(name,  action, tenantId, userId, tableOcid,
                        operations, null /* createTime */);
    }

    public static Rule createRule(String name,
                                  Action action,
                                  String tenantId,
                                  String userId,
                                  String tableOcid,
                                  String[] operations,
                                  Timestamp createTime) {
        return new Rule(name, action, tenantId, userId, tableOcid,
                        operations, createTime);
    }

    /* Needed for serialization */
    public Rule() {
    }

    private Rule(String name,
                 Action action,
                 String tenantOcid,
                 String userOcid,
                 String tableOcid,
                 String[] ops,
                 Timestamp createTime) {

        this.name = name;
        this.action = action;
        tenant = tenantOcid;
        user = userOcid;
        table = tableOcid;
        this.createTime = createTime;
        operations = ops;
        validate();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Action getAction() {
        return action;
    }

    public ActionType getActionType() {
        return getAction().getType();
    }

    public String getTenant() {
        return tenant;
    }

    public String getUser() {
        return user;
    }

    public String getTable() {
        return table;
    }

    public String[] getOperations() {
        return operations;
    }

    public Set<OpType> getOpTypes() {
        if (opTypes == null) {
            opTypes = new HashSet<>();
            for (String op : getOperations()) {
                opTypes.add(parseOpType(op));
            }
        }
        return opTypes;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    /*
     * Checks if the rule's attributes are equal with that of another rule,
     * the operations can be not exactly same but equivalent.
     */
    public boolean attributesEqual(Rule o) {
        return stringsEqual(getTenant(), o.getTenant()) &&
               stringsEqual(getUser(), o.getUser()) &&
               stringsEqual(getTable(), o.getTable()) &&
               operationsEqual(getOpTypes(), o.getOpTypes()) &&
               getAction().equals(o.getAction());
    }

    public boolean operationsEqual(Set<OpType> ops) {
        return operationsEqual(getOpTypes(), ops);
    }

    public String toJson() {
        return gson.toJson(this);
    }

    /*
     * Constructs Rule from JSON stream
     */
    public static Rule fromJson(InputStream in) {
        try (InputStreamReader reader = new InputStreamReader(in)) {
            Rule rule = gson.fromJson(reader, Rule.class);
            if (rule == null) {
                throw new IllegalArgumentException(
                    "Failed to deserailize JSON to Rule object: JSON is empty");
            }
            rule.validate();
            return rule;
        } catch (JsonSyntaxException | IOException ex) {
            throw new IllegalArgumentException(
                "Failed to deserailize JSON to Rule object: " + ex.getMessage());
        }
    }

    /*
     * Constructs Rule from JSON string
     */
    public static Rule fromJson(String json) {
        try {
            Rule rule = gson.fromJson(json, Rule.class);
            if (rule == null) {
                throw new IllegalArgumentException(
                    "Failed to deserailize JSON to Rule object: " + json);
            }
            rule.validate();
            return rule;
        } catch (JsonSyntaxException jse) {
            throw new IllegalArgumentException(
                "Failed to deserailize JSON to Rule object: " +
                jse.getMessage() + ", json=" + json);
        }
    }

    public static Gson getGson() {
        return gson;
    }

    @Override
    public String toString() {
        return toJson();
    }

    private void validate() {
        if (name == null) {
            throw new IllegalArgumentException("Rule name should not be null");
        }

        if (action != null) {
            action.validate();
        } else {
            action = DEFAULT_ACTION;
        }

        if (operations == null || operations.length == 0) {
            throw new IllegalArgumentException(
                "Rule operations should not be null or empty");
        }
        for (String op : operations) {
            parseOpType(op);
        }

        if (createTime == null) {
            createTime = new Timestamp(System.currentTimeMillis());
        }
    }

    private static OpType parseOpType(String name) {
        try {
            return OpType.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Invalid operation type '" +
                name + "', not one of the values accepted for Enum class: " +
                Arrays.toString(OpType.values()));
        }
    }

    /*
     * Checks if the given OpType set represents the all the operation types if
     * match any of below 2 conditions:
     *  1. contain OpType.ALL
     *  2. contain all the other OpType except OpType.ALL
     */
    public static boolean isAllOpType(Set<OpType> ops) {
        for (OpType op : OpType.values()) {
            if (op == OpType.ALL) {
                if (ops.contains(OpType.ALL)) {
                    return true;
                }
            } else {
                if (!ops.contains(op)) {
                    return false;
                }
            }
        }
        return true;
    }

    /*
     * Compares if 2 strings are equal ignoring case.
     */
    private static boolean stringsEqual(String s1, String s2) {
        return (s1 == s2) || (s1 != null && s1.equalsIgnoreCase(s2));
    }

    /*
     * Compares if the 2 OpType sets are equal.
     *
     * Operations contain "ALL" or all the other operations are also considered
     * as equal, e.g. [ALL] vs [DDL, READ, WRITE].
     */
    public static boolean operationsEqual(Set<OpType> ops1,
                                          Set<OpType> ops2) {
        if (Objects.equals(ops1, ops2)) {
            return true;
        }
        if (ops1 != null && ops2 != null &&
            isAllOpType(ops1) && isAllOpType(ops2)) {
            return true;
        }
        return false;
    }

    /*
     * Action to take when the rule is matched.
     */
    public static class Action {
        private ActionType type;

        private Action(ActionType type) {
            this.type = type;
        }

        public ActionType getType() {
            return type;
        }

        public void validate() {
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Action)) {
                return false;
            }
            return type == ((Action)obj).getType();
        }
    }

    /*
     * Drops request
     */
    public static class DropRequestAction extends Action {
        public DropRequestAction() {
            super(ActionType.DROP_REQUEST);
        }
    }

    /*
     * Returns the specified error
     *   - errorCode: the response error code (refer to Response error codes
     *     in httpproxy oracle.nosql.proxy.protocol.Protocol class)
     *   - errorMessage: the returned error message.
     */
    public static class ReturnErrorAction extends Action {
        private int errorCode;
        private String errorMessage;

        public ReturnErrorAction(int errorCode, String errorMessage) {
            super(ActionType.RETURN_ERROR);
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;

            validate();
        }

        public int getErrorCode() {
            return errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public void validate() {
            if (errorCode <= 0) {
                throw new IllegalArgumentException(
                    "The errorCode must be positive int, see error " +
                    "codes in oracle.nosql.proxy.protocol class");
            }

            if (errorMessage == null) {
                throw new IllegalArgumentException(
                    "The errorMessage must be not null");
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ReturnErrorAction)) {
                return false;
            }

            ReturnErrorAction o1 = (ReturnErrorAction)obj;
            return super.equals(obj) &&
                   (getErrorCode() == o1.getErrorCode()) &&
                   Objects.equals(getErrorMessage(), o1.getErrorMessage());
        }
    }

    /* Customized Json Serializer/Deserialize for Action */
    private static class ActionSerializer
        implements JsonSerializer<Action>, JsonDeserializer<Action> {

        @Override
        public JsonElement serialize(Action action,
                                     Type typeOfSrc,
                                     JsonSerializationContext context) {
            switch(action.getType()) {
            case DROP_REQUEST:
                return context.serialize(action, DropRequestAction.class);
            case RETURN_ERROR:
                return context.serialize(action, ReturnErrorAction.class);
            default:
                throw new JsonParseException("Unknown action: " + action);
            }
        }

        @Override
        public Action deserialize(JsonElement json,
                                  Type typeOfT,
                                  JsonDeserializationContext context)
            throws JsonParseException {

            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get("type").getAsString();
            ActionType actionType;

            try {
                actionType = ActionType.valueOf(type);
            } catch (IllegalArgumentException ex) {
                throw new JsonParseException("Unknown action type: " + type);
            }

            switch(actionType) {
            case DROP_REQUEST:
                return context.deserialize(json, DropRequestAction.class);
            case RETURN_ERROR:
                return context.deserialize(json, ReturnErrorAction.class);
            default:
                throw new JsonParseException("Unknown action type: " + type);
            }
        }
    }
}
