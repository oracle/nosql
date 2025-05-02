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
 * This MBean represents an Arbiter node's operational parameters.
 *
 */
public interface ArbNodeMXBean {

    /**
     * Returns the ArbNodeId number of an Arbiter instance.
     */
    String getArbNodeId();

    /**
     * Returns the reported service status of the Arbiter
     */
    String getServiceStatus();

    /**
     * Returns Non-default BDB-JE configuration properties.
     */
    String getConfigProperties();
    /**
     * Returns a string that is added to the command line when the Replication
     * Node process is started.
     */
    String getJavaMiscParams();
    /**
     * Returns property settings for the Logging subsystem.
     */
    String getLoggingConfigProps();
    /**
     * If true, then the underlying BDB-JE subsystem will dump statistics into
     * a local .stat file.
     */
    boolean getCollectEnvStats();
    /**
     * Returns the collection period for performance statistics, in sec.
     */
    int getStatsInterval();
    /**
     * Returns the size of the Java heap for this Replication Node, in MB.
     */
    int getHeapMB();

    /**
     * Returns the number of transactions acked.
     */
    long getAcks();

    /**
     * Returns the current master.
     */
    String getMaster();

    /**
     * Returns the current node State.
     */
    String getState();

    /*
     * Returns the current acked VLSN
     */
    long getVLSN();

    /*
     * Returns the current replayQueueOverflow value.
     */
    long getReplayQueueOverflow();
}
