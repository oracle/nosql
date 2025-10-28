/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.protocol;

/**
 * Protocol constants that are part of the Proxy binary protocol
 */
public final class BinaryProtocol extends Protocol {

    /**
     * Consistency
     */
    public static final int ABSOLUTE = 0;
    public static final int EVENTUAL = 1;

    /**
     * Durability.
     * note 1-offset is to distinguish between 0 (not set)
     * and a purposefully set value
     */
    /* sync policy */
    public static final int DURABILITY_SYNC = 1;
    public static final int DURABILITY_NO_SYNC = 2;
    public static final int DURABILITY_WRITE_NO_SYNC = 3;
    /* ack policy */
    public static final int DURABILITY_ALL = 1;
    public static final int DURABILITY_NONE = 2;
    public static final int DURABILITY_SIMPLE_MAJORITY = 3;

    /**
     * Direction
     */
    public static final int UNORDERED = 0;
    public static final int FORWARD = 1;
    public static final int REVERSE = 2;

    /**
     * Table state
     */
    public static final int ACTIVE = 0;
    public static final int CREATING = 1;
    public static final int DROPPED = 2;
    public static final int DROPPING = 3;
    public static final int UPDATING = 4;

    /**
     * Table Limits mode
     */
    public static final int PROVISIONED = 1;
    public static final int ON_DEMAND = 2;

    /**
     * FieldValue type
     */
    public static final int TYPE_ARRAY = 0;
    public static final int TYPE_BINARY = 1;
    public static final int TYPE_BOOLEAN = 2;
    public static final int TYPE_DOUBLE = 3;
    public static final int TYPE_INTEGER = 4;
    public static final int TYPE_LONG = 5;
    public static final int TYPE_MAP = 6;
    public static final int TYPE_STRING = 7;
    public static final int TYPE_TIMESTAMP = 8;
    public static final int TYPE_NUMBER = 9;
    public static final int TYPE_JSON_NULL = 10;
    public static final int TYPE_NULL = 11;
    public static final int TYPE_EMPTY = 12;

    /**
     * String representations of types. This is tied to the values
     * above.
     */
    public static final String[] TYPE_NAMES =
    {
        "ARRAY",
        "BINARY",
        "BOOLEAN",
        "DOUBLE",
        "INTEGER",
        "LONG",
        "MAP",
        "STRING",
        "TIMESTAMP",
        "NUMBER",
        "JSON_NULL",
        "NULL",
        "EMPTY"
    };

    public static String getTypeName(int type) {
        return TYPE_NAMES[type];
    }

    public static enum Test{};
    /*
     * TTL units
     */
    public static final int TTL_HOURS = 1;
    public static final int TTL_DAYS = 2;

    /*
     * If we could get better discrimination of exceptions from DDL
     * queries this wouldn't be necessary. TBD -- do that.
     * Infer the appropriate exception based on the error message.
     *  - exists (index or table or general resource)
     *  - not found (index or table or general resource)
     */
    public static int mapDDLError(String mixedMsg) {
        String msg = mixedMsg.toLowerCase();
        if (msg.contains("exists") && !msg.contains("not")) {
            if (msg.contains("table failed")) {
                return TABLE_EXISTS;
            } else if (msg.contains("index failed")) {
                return INDEX_EXISTS;
            } else {
                return RESOURCE_EXISTS;
            }
        } else if (msg.contains("not exist") || msg.contains("not found")) {
            if (msg.contains("table does not exist")) {
                return TABLE_NOT_FOUND;
            } else if (msg.contains("does not exists in table")) {
                return INDEX_NOT_FOUND;
            } else {
                return RESOURCE_NOT_FOUND;
            }
        }
        return 0;
    }
}
