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

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import oracle.nosql.common.JsonBuilder;

/*
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and SC filters service.
 *
 * To serialize a Java object into a Json string:
 *   Foo foo;
 *   String jsonPayload = JsonUtils.toJson(foo);
 *
 * To deserialize a Json string into this object:
 *   Foo foo = JsonUtils.fromJsont(<jsonstring>, Foo.class);
 *
 * The Rule class represents the filter rule which has below information:
 *   o name, the rule name, required.
 *   o action, the action type of the rule, default to DROP_REQUEST.
 *   o tenant, the principal tenant ocid.
 *   o user, the principal ocid.
 *   o table, the target table ocid.
 *   o operations, a list of operations, required.
 *   o createTime, the create time in milliseconds.
 */
public class Rule {

    private static final ActionType DEF_ACTION = ActionType.DROP_REQUEST;

    public enum OpType {
        ALL,
        DDL,
        WRITE,
        READ
    }

    public enum ActionType {
        DROP_REQUEST
    };

    private String name;
    private ActionType action;

    /* The principal tenant ocid */
    private String tenant;
    /* The principal ocid */
    private String user;
    /* The target table ocid */
    private String table;
    private String[] operations;
    private long createTimeMs;

    private transient Set<OpType> opTypes;

    public static Rule createRule(String name,
                                  ActionType action,
                                  String tenantOcid,
                                  String userOcid,
                                  String tableOcid,
                                  String[] operations) {
        return createRule(name, action, tenantOcid, userOcid, tableOcid,
                          operations, 0);
    }

    public static Rule createRule(String name,
                                  ActionType action,
                                  String tenantOcid,
                                  String userOcid,
                                  String tableOcid,
                                  String[] operations,
                                  long createTimeMs) {
        return new Rule(name, action, tenantOcid, userOcid, tableOcid,
                        operations, createTimeMs);
    }

    /* Needed for serialization */
    public Rule() {
    }

    private Rule(String name,
                 ActionType action,
                 String tenantOcid,
                 String userOcid,
                 String tableOcid,
                 String[] ops,
                 long createTime) {

        this.name = name;
        this.action = action;
        tenant = tenantOcid;
        user = userOcid;
        table = tableOcid;
        createTimeMs = createTime;
        operations = ops;
        validate();
        setOpTypes();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public ActionType getAction() {
        return (action != null) ? action : DEF_ACTION;
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
        if (opTypes != null) {
            return opTypes;
        }
        setOpTypes();
        return opTypes;
    }

    private void setOpTypes() {
        opTypes = new HashSet<>();
        if (getOperations() != null) {
            for (String op : getOperations()) {
                opTypes.add(parseOpType(op));
            }
        }
    }

    public String getCreateTime() {
        if (createTimeMs > 0) {
            return Instant.ofEpochMilli(createTimeMs).toString();
        }
        return null;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    /*
     * Checks if the rule's attributes are equal with that of another rule,
     * the operations can be not exactly same but equivalent.
     */
    public boolean attributesEqual(Rule o) {
        return stringsEqual(getTenant(), o.getTenant()) &&
               stringsEqual(getUser(), o.getUser()) &&
               stringsEqual(getTable(), o.getTable()) &&
               operationsEqual(getOpTypes(), o.getOpTypes());
    }

    public boolean operationsEqual(Set<OpType> ops) {
        return operationsEqual(getOpTypes(), ops);
    }

    public String toJson() {
        JsonBuilder jb = JsonBuilder.create();
        jb.append("name", getName());
        jb.append("action", getAction().name());
        if (getTenant() != null) {
            jb.append("tenant", getTenant());
        }
        if (getUser() != null) {
            jb.append("user", getUser());
        }
        if (getTable() != null) {
            jb.append("table", getTable());
        }
        if (getOperations() != null) {
            jb.startArray("operations");
            for (String op : getOperations()) {
                jb.append(op);
            }
            jb.endArray();
        }

        if (getCreateTimeMs() > 0) {
            jb.append("createTimeMs", getCreateTimeMs());
            jb.append("createTime", getCreateTime());
        }
        return jb.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    public void validate() {
        if (name == null) {
            throw new IllegalArgumentException("Rule name should not be null");
        }
        if (operations == null || operations.length == 0) {
            throw new IllegalArgumentException(
                "Rule operations should not be null or empty");
        }
    }

    private static OpType parseOpType(String name) {
        try {
            return OpType.valueOf(name.toUpperCase());
        } catch(IllegalArgumentException iae) {
            throw new IllegalArgumentException("Invalid operation type '" +
                name + "', not one of the values accepted for Enum class: " +
                Arrays.toString(OpType.values()));
        }
    }

    /*
     * Checks if the given OpType set represents the all the operation types if
     * match any of below 2 conditions:
     *  1. contain OpType.ALL
     *  2. contain all the other OpType except OpType.ALL.s
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
}
