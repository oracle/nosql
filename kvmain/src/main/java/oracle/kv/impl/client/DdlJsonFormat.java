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

package oracle.kv.impl.client;

/**
 * Tags and values for the JSON format of DDL results. Gather in this
 * standalone file so as to be available to both client and server.
 */
public class DdlJsonFormat {

    /* Tags used when parsing the JSON output for DDL operations */
    public static final String VERSION_TAG = "version";
    public static final String NOOP_STATUS = "Statement did not require execution";
}
