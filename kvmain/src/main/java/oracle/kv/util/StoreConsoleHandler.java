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

package oracle.kv.util;

/**
 * This handler accepts input from the monitor logging view. It is
 * distinguished from oracle.kv.util.ConsoleHandler only because a
 * single process kvstore unit test can view the monitoring output without
 * seeing the component level output. Using a single console handler would
 * cause such a single process kvstore run to see all console output in
 * duplicate.
 *
 * To see KVStore wide view of all monitoring, enable this handler, i.e:
 *
 *      oracle.kv.util.StoreConsoleHandler.level=ALL
 */
public class StoreConsoleHandler extends oracle.kv.util.ConsoleHandler {
}

