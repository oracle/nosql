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

package oracle.kv.impl.monitor;

import java.util.logging.LogRecord;

import oracle.kv.impl.measurement.LoggerMessage;

/**
 * Takes logging output on the Admin and sends it directly to the Monitor.
 */
public class AdminDirectHandler extends LogToMonitorHandler {

    private final MonitorKeeper admin;
    public AdminDirectHandler(MonitorKeeper admin) {
        super();
        this.admin = admin;
    }

    @Override
    public void publish(LogRecord record) {
        final Monitor monitor = admin.getMonitor();
        if (monitor != null) {
            monitor.publish(new LoggerMessage(record));
        }
    }
}
