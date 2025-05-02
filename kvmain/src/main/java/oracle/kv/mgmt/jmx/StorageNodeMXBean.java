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
 * This MBean represents the Storage Node's operational parameters.
 * 
 * <p>The information that is published to the
 * &lt;kvroot&gt;/log/&lt;storename&gt;.stats and
 * &lt;kvroot&gt;/log/&lt;storename&gt;.perf files is also available through
 * this MBean, via the standard javax.management.Notification mechanism. An
 * application may subscribe to notifications from a given Storage Node by
 * adding a notification listener with the name
 * "Oracle NoSQL Database:type=StorageNode"</p>
 * <ol>
 * <li>Notifications of type "oracle.kv.repnode.opmetric" contain a user data
 * payload that is the full listing of performance metrics for a given RN. The
 * stats are a string in JSON form, and are obtained via
 * Notification.getUserData().</li>
 * <li>Notifications of type "oracle.kv.repnode.envmetric" contain a user data
 * payload that is the full listing of detailed stats for a given RN. The stats
 * are a string in JSON form and are obtained via Notification.getUserData().
 * </li>
 * </ol>
 *
 * @since 2.0
 */
public interface StorageNodeMXBean {

    /**
     * Returns the StorageNodeId number of this Storage Node.
     */
    int getSnId();

    /**
     * Returns the reported service status of the Storage Node.
     */
    String getServiceStatus();

    /**
     * Returns true if this Storage Node hosts an Admin instance.
     */
    boolean isHostingAdmin();

    /**
     * Returns the pathname of the store's root directory.
     */
    String getRootDirPath();

    /**
     * Returns the configured name of the store to which this
     * Storage Node belongs.
     */
    String getStoreName();

    /**
     * Returns the range of port numbers available for assigning to Replication
     * Nodes that are hosted on this Storage Node.  A port is allocated
     * automatically from this range when a Replication Node is deployed.
     */
    String getHaPortRange();

    /**
     * Returns the name of the network interface used for communication between
     * Replication Nodes
     */
    String getHAHostname();

    /**
     * Returns the port number of the Storage Node's RMI registry.
     */
    int getRegistryPort();

    /**
     * Returns the number of Replication Nodes that can be hosted
     * on this Storage Node.
     */
    int getCapacity();

    /**
     * Returns the name associated with the network interface on which this
     * Storage Node's registry listens.
     */
    String getHostname();

    /**
     * Returns the maximum size of log files.
     */
    int getLogFileLimit();

    /**
     * Returns the number of log files that are kept.
     */
    int getLogFileCount();

    /**
     * Returns the amount of memory known to be available on this Storage Node,
     * in megabytes.
     */
    int getMemoryMB();

    /**
     * Returns the number of CPUs known to be available on this Storage Node.
     */
    int getNumCPUs();

    /**
     * Returns a list of file system mount points on which Replication Nodes
     * can be deployed
     */
    String getMountPoints();

    /**
     * Returns a list of RN log mount points on which Replication Nodes
     * logging  can be done
     */
    String getRNLogMountPoints();

    /**
     * Returns Admin mount points on this Storage node.
     */
    String getAdminMountPoints();

    /**
     * Returns the collector service interval
     */
    long getCollectorInterval();
}
