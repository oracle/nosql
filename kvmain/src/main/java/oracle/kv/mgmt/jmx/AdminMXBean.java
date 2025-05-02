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

package oracle.kv.mgmt.jmx;

/**
 * This MBean represents an Admin's operational parameters.
 *
 * @since 2.0
 */
public interface AdminMXBean {

    /**
     * Returns the AdminId number of an Admin instance.
     */
    int getAdminId();

    /**
     * Returns the reported service status of the Admin.
     */
    String getServiceStatus();

    /**
     * Returns the maximum size of log files.
     */
    int getLogFileLimit();

    /**
     * Returns number of log files that are kept.
     */
    int getLogFileCount();

    /**
     * Returns The polling period for collecting metrics.
     */
    long getPollPeriodMillis();

    /**
     * Returns how long to keep critical event records.
     */
    long getEventExpiryAge();

    /**
     * Tells whether this Admin is the master among Admin instances.
     */
    boolean isMaster();

    /**
     * Returns a JSON string containing a bundle of JE environment-related
     * metrics.  These metrics are also reported by the notification
     * oracle.kv.admin.envmetric.
     */
    String getEnvMetric();
}
