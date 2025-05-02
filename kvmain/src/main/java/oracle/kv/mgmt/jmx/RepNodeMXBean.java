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

import java.util.Date;

/**
 * This MBean represents the RepNode's status, performance metrics, and
 * operational parameters.
 *
 * <p>The performance metrics are organized into five groups, four of
 * which are characterized by whether they refer to single-operation or
 * multi-operation client activities; as well as whether they reference
 * measurements over the most recent collection interval, or measurements
 * accumulated since the RepNode started. The fifth group consists of
 * measurements that don't fall into one of the first four groups.
 * <ol>
 * <li>Single-operation interval metrics.  The accessors for these items begin
 * with the prefix {@code getInterval}.</li>
 * <li>Multi-operation interval metrics.  The accessors for these items begin
 * with the prefix  {@code getMultiInterval}.</li>
 * <li>Single-operation cumulative metrics.  The accessors for these items
 * begin with the prefix {@code getCumulative}.</li>
 * <li>Multi-operation cumulative metrics.  The accessors for these items begin
 * with the prefix  {@code getMultiCumulative}.</li>
 * <li>Metrics that don't fall into one of the above groups.  The accessors
 * for these items begin with the prefix  {@code get}, followed by the
 * metric's name.</li>
 * </ol>
 *
 * <p> Within the first four groups, the accessor's suffix indicates which
 * of the following items are reported.</p>
 * <ol>
 * <li>LatAvg is the average latency recorded during the interval.</li>
 * <li>LatMax is the maximum latency</li>
 * <li>LatMin is the minimum latency</li>
 * <li>Pct99 is the 99th percentile latency</li>
 * <li>Pct95 is the 95th percentile latency</li>
 * <li>TotalOps is the number of operations</li>
 * <li>Throughput is number of operations per second</li>
 * <li>End is the timestamp at the end of the measured interval</li>
 * <li>Start is the timestamp at the beginning of the measured interval</li>
 * </ol>
 *
 * <p> Similarly, within the last group of metrics, the accessor's suffix
 * also indicates which of the items are reported.
 *
 * @since 2.0
 */
public interface RepNodeMXBean {

    /**
     * Returns the RepNodeId, in its String form.
     */
    String getRepNodeId();

    /**
     * Returns the reported service status of the Replication Node.
     */
    String getServiceStatus();

    /* Single Operation Interval Latency Info */

    /**
     * The average latency for single operations during a measured interval.
     */
    float getIntervalLatAvg();
    /**
     * Returns the highest latency for single operations measured during an
     * interval.
     */
    int getIntervalLatMax();
    /**
     * Returns the lowest latency for singleton operations measured during an
     * interval.
     */
    int getIntervalLatMin();
    /**
     * Returns the 95th percentile latency for single operations during a
     * measured interval.
     */
    int getIntervalPct95();
    /**
     * Returns the 99th percentile latency for single operations during a
     * measured interval.
     */
    int getIntervalPct99();
    /**
     * Returns the total number of singleton operations during a measured
     * interval as an int.
     * @deprecated as of 19.2, see {@link #getIntervalTotalOpsLong}
     */
    @Deprecated
    int getIntervalTotalOps();
    /**
     * Returns the total number of singleton operations during a measured
     * interval.
     * @since 19.2
     */
    long getIntervalTotalOpsLong();
    /**
     * Returns the timestamp at the end of a measured interval.
     */
    Date getIntervalEnd();
    /**
     * Returns the timestamp at the beginning of a measured interval.
     */
    Date getIntervalStart();
    /**
     * Returns singleton operations per second during a measured interval.
     */
    long getIntervalThroughput();

    /* Single Operation Cumulative Latency Info */

    /**
     * Returns the average latency for single operations since service startup.
     */
    float getCumulativeLatAvg();
    /**
     * Returns the highest latency measured for single operations since service
     * startup.
     */
    int getCumulativeLatMax();
    /**
     * Returns the lowest latency measured for single operations since service
     * startup.
     */
    int getCumulativeLatMin();
    /**
     * Returns the 95th percentile latency for single operations since service
     * startup.
     */
    int getCumulativePct95();
    /**
     * Returns the 99th percentile latency for single operations since service
     * startup.
     */
    int getCumulativePct99();
    /**
     * Returns the total number of single operations since service startup as
     * an int.
     * @deprecated as of 19.2, see {@link #getCumulativeTotalOpsLong}
     */
    @Deprecated
    int getCumulativeTotalOps();
    /**
     * Returns the total number of single operations since service startup.
     * @since 19.2
     */
    long getCumulativeTotalOpsLong();
    /**
     * Returns the timestamp at the end of a cumulative measurement period.
     */
    Date getCumulativeEnd();
    /**
     * Returns the timestamp at the beginning of a cumulative measurement
     * period.
     */
    Date getCumulativeStart();
    /**
     * Returns single operations per second since service startup.
     */
    long getCumulativeThroughput();

    /* Multiple Operation Interval Latency Info */

    /**
     * Returns the average latency for multi-operation sequences during a
     * measured interval.
     */
    float getMultiIntervalLatAvg();
    /**
     * Returns the highest latency measured for multi-operation sequences
     * during a measured interval.
     */
    int getMultiIntervalLatMax();
    /**
     * Returns the lowest latency measured for multi-operation sequences during
     * a measured interval.
     */
    int getMultiIntervalLatMin();
    /**
     * Returns the 95th percentile latency for multi-operation sequences during
     * a measured interval.
     */
    int getMultiIntervalPct95();
    /**
     * Returns the 99th percentile latency for multi-operation sequences during
     * a measured interval.
     */
    int getMultiIntervalPct99();
    /**
     * Returns the total number of single operations performed in
     * multi-operation sequences during a measured interval as an int.
     * @deprecated as of 19.2, see {@link #getMultiIntervalTotalOpsLong}
     */
    @Deprecated
    int getMultiIntervalTotalOps();
    /**
     * Returns the total number of single operations performed in
     * multi-operation sequences during a measured interval.
     * @since 19.2
     */
    long getMultiIntervalTotalOpsLong();
    /**
     * Returns the total number of multi-operation sequences during a measured
     * interval as an int.
     * @deprecated as of 19.2, see {@link #getMultiIntervalTotalRequestsLong}
     */
    @Deprecated
    int getMultiIntervalTotalRequests();
    /**
     * Returns the total number of multi-operation sequences during a measured
     * interval.
     * @since 19.2
     */
    long getMultiIntervalTotalRequestsLong();
    /**
     * Returns the timestamp at the end of a measured interval.
     */
    Date getMultiIntervalEnd();
    /**
     * Returns the timestamp at the beginning of a measured interval.
     */
    Date getMultiIntervalStart();
    /**
     * Returns multi-operations sequences per second during a measured interval.
     */
    long getMultiIntervalThroughput();

    /* Multiple Operation Cumulative Latency Info */

    /**
     * Returns the average latency for multi-operation sequences since service
     * startup.
     */
    float getMultiCumulativeLatAvg();
    /**
     * Returns the highest latency measured for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativeLatMax();
    /**
     * Returns the lowest latency measured for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativeLatMin();
    /**
     * Returns the 95th percentile latency for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativePct95();
    /**
     * Returns the 99th percentile latency for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativePct99();
    /**
     * Returns the total number of single operations performed in
     * multi-operation sequences since service startup as an int.
     * @deprecated as of 19.2, see {@link #getMultiCumulativeTotalOpsLong}
     */
    @Deprecated
    int getMultiCumulativeTotalOps();
    /**
     * Returns the total number of single operations performed in
     * multi-operation sequences since service startup.
     * @since 19.2
     */
    long getMultiCumulativeTotalOpsLong();
    /**
     * Returns the total number of multi operation sequences since service
     * startup as an int.
     * @deprecated as of 19.2, see {@link #getMultiCumulativeTotalRequestsLong}
     */
    @Deprecated
    int getMultiCumulativeTotalRequests();
    /**
     * Returns the total number of multi operation sequences since service
     * startup.
     * @since 19.2
     */
    long getMultiCumulativeTotalRequestsLong();
    /**
     * Returns the timestamp at the end of a cumulative measurement period.
     */
    Date getMultiCumulativeEnd();
    /**
     * Returns Timestamp of service startup; the start time of a cumulative
     * measurement.
     */
    Date getMultiCumulativeStart();
    /**
     * Returns Multi-operations sequences per second since service startup.
     */
    long getMultiCumulativeThroughput();

    /**
     * @deprecated as of 18.3. The values returned by this method were
     * undependable because they could be influenced by clock skew. Use the JE
     * replicaAvgDelayMsMap stat instead.
     */
    @Deprecated
    long getCommitLag();

    /* RepNode parameters */

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
     * Returns the size of the BDB-JE cache, in MBytes.
     */
    int getCacheSize();
    /**
     * @deprecated as of 20.3. There is no longer a cap on the latency for
     * tracking.
     */
    @Deprecated
    int getMaxTrackedLatency();
    /**
     * Returns the collection period for latency statistics, in sec.
     */
    int getStatsInterval();
    /**
     * Returns the size of the Java heap for this Replication Node, in MB.
     */
    int getHeapMB();
    /**
     * Returns the path to the file system mount point where this Replication
     * Node's files are stored.
     */
    String getMountPoint();
    /**
     * Returns the size of the file system mount point where this Replication
     * Node's files are stored.
     */
    long getMountPointSize();
    /**
     * Returns the path to the RN log mount point where this Replication
     * Node's logging files are stored.
     */
    String getLogMountPoint();
    /**
     * Returns the size of the RN log mount point where this Replication
     * Node's logging files are stored.
     */
    long getLogMountPointSize();
    /**
     * If the Replication Node's latency exceeds this value, a latency ceiling
     * notification will be sent.
     */
    int getLatencyCeiling();
    /**
     * Returns the lower bound on Replication Node throughput.  Lower
     * throughput reports will cause a throughput floor notification to be sent.
     */
    int getThroughputFloor();

    /**
     * @deprecated as of 18.3. No longer supported now that getCommitLag is
     * not supported.
     */
    @Deprecated
    long getCommitLagThreshold();

    /**
     * Returns the replication state of the node, as of the most recent report.
     *
     * @see com.sleepycat.je.rep.ReplicatedEnvironment.State
     */
    String getReplicationState();

    /**
     * Returns a JSON string containing a bundle of operation-related metrics.
     * These metrics are also reported by the notification
     * oracle.kv.repnode.opmetric, which is described in the Run Book.
     * @see <a href="oradoc-lookup:/NSRUN-GUID-0BE1DD69-A4AD-4E2C-8C70-27B9C58B453B/Software+Monitoring" target="_top">
     * Software Monitoring</a>
     */
    String getOpMetric();

    /**
     * Returns a JSON string containing a bundle of JE environment-related
     * metrics.  These metrics are also reported by the notification
     * oracle.kv.repnode.envmetric, which is described in the RunBook.
     * @see <a href="oradoc-lookup:/NSRUN-GUID-0BE1DD69-A4AD-4E2C-8C70-27B9C58B453B/Software+Monitoring" target="_top">
     * Software Monitoring</a>
     */
    String getEnvMetric();
}
