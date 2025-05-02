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

package oracle.kv.impl.mgmt.jmx;

import static oracle.kv.impl.util.NumberUtil.longToIntOrLimit;

import java.util.Date;

import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.measurement.ConciseStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.ReplicationState;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.mgmt.jmx.RepNodeMXBean;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.StateChangeEvent;

import oracle.nosql.common.json.ObjectNode;


public class RepNode
    extends NotificationBroadcasterSupport
    implements RepNodeMXBean {

    private final RepNodeId rnId;
    private final MBeanServer server;
    private final StorageNode sn;
    private ServiceStatus status;
    private LatencyInfo singleInterval;
    private LatencyInfo singleCumulative;
    private LatencyInfo multiInterval;
    private LatencyInfo multiCumulative;
    private RepNodeParams parameters;
    private State replicationState;
    private String opMetricString;
    private String envMetricString;
    private ObjectName oName;
    long notifySequence = 1L;

    public static final String
        NOTIFY_RN_STATUS_CHANGE = "oracle.kv.repnode.status";
    public static final String
        NOTIFY_SINGLE_TFLOOR = "oracle.kv.singleop.throughputfloor";
    public static final String
        NOTIFY_SINGLE_LCEILING = "oracle.kv.singleop.latencyceiling";
    public static final String
        NOTIFY_MULTI_TFLOOR = "oracle.kv.multiop.throughputfloor";
    public static final String
        NOTIFY_MULTI_LCEILING = "oracle.kv.multiop.latencyceiling";
    public static final String
        NOTIFY_RN_OP_METRIC = "oracle.kv.repnode.opmetric";
    public static final String
        NOTIFY_RN_EXCEPTION_METRIC = "oracle.kv.repnode.exceptionmetric";
    public static final String
        NOTIFY_RN_ENV_METRIC = "oracle.kv.repnode.envmetric";
    public static final String
        NOTIFY_RN_REPLICATION_STATE = "oracle.kv.repnode.replicationstate";
    public static final String
        NOTIFY_RN_TABLE_METRIC = "oracle.kv.repnode.tablemetric";
    public static final String
        NOTIFY_RN_JVM_STATS = "oracle.kv.repnode.jvmstats";
    public static final String
        NOTIFY_RN_ENDPOINT_GROUP_STATS =
        "oracle.kv.repnode.endpointgroupstats";
    public static final String
        NOTIFY_RN_LOGGING_STATS = "oracle.kv.repnode.loggingstats";

    public RepNode(RepNodeParams rnp, MBeanServer server, StorageNode sn) {
        this.server = server;
        this.rnId = rnp.getRepNodeId();
        this.sn = sn;
        status = ServiceStatus.UNREACHABLE;

        resetMetrics();

        setParameters(rnp);

        register();
    }

    private void resetMetrics() {
        /*
         * Create a fake LatencyInfo to report when no metrics are available.
         */
        final LatencyInfo li = new LatencyInfo
            (PerfStatType.PUT_IF_ABSENT_INT,
             System.currentTimeMillis(), System.currentTimeMillis(),
             new LatencyResult());

        singleInterval = li;
        singleCumulative = li;
        multiInterval = li;
        multiCumulative = li;

        replicationState = State.UNKNOWN;

        final String emptyJson = "{}";
        opMetricString = emptyJson;
        envMetricString = emptyJson;
    }

    private void register() {

        final StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=RepNode");
        buf.append(",id=");
        buf.append(getRepNodeId());
        try {
            oName = new ObjectName(buf.toString());
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException
                ("Unexpected exception creating JMX ObjectName " +
                 buf.toString(), e);
        }

        try {
            server.registerMBean(this, oName);
        } catch (Exception e) {
            throw new IllegalStateException
                ("Unexpected exception registring MBean " + oName.toString(),
                 e);
        }
    }

    public void unregister() {
        if (oName != null) {
            try {
                server.unregisterMBean(oName);
            } catch (Exception e) {
                throw new IllegalStateException
                    ("Unexpected exception while unregistring MBean " +
                     oName.toString(), e);
            }
        }
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return new MBeanNotificationInfo[]
        {
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this RepNode's service status"),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_TFLOOR},
                 Notification.class.getName(),
                 "Single-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_LCEILING},
                 Notification.class.getName(),
                 "Single-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_TFLOOR},
                 Notification.class.getName(),
                 "Multi-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_LCEILING},
                 Notification.class.getName(),
                 "Multi-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_OP_METRIC},
                 Notification.class.getName(),
                 "New operation performance metrics are available."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_ENV_METRIC},
                 Notification.class.getName(),
                 "New statistics are available."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_EXCEPTION_METRIC},
                 Notification.class.getName(),
                 "New RepNode exception metrics are available"),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_TABLE_METRIC},
                 Notification.class.getName(),
                 "New RepNode table metrics are available"),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_REPLICATION_STATE},
                 Notification.class.getName(),
                 "Announce a change in this RepNode's replication state"),
            new MBeanNotificationInfo(
                new String[]{RepNode.NOTIFY_RN_JVM_STATS},
                Notification.class.getName(),
                "New RepNode JVM stats are available"),
            new MBeanNotificationInfo(
                new String[]{RepNode.NOTIFY_RN_ENDPOINT_GROUP_STATS},
                Notification.class.getName(),
                "New RepNode EndpointGroup stats are available"),
            new MBeanNotificationInfo(
                new String[]{NOTIFY_RN_LOGGING_STATS},
                Notification.class.getName(),
                "Announce RepNode SEVERE and WARNING logging entries"),
        };
    }

    public void setParameters(RepNodeParams rnp) {
        parameters = rnp;
    }

    public synchronized void setStats(StatsPacket packet) {

        singleInterval =
            packet.get(PerfStatType.USER_SINGLE_OP_INT);
        singleCumulative =
            packet.get(PerfStatType.USER_SINGLE_OP_CUM);
        multiInterval =
            packet.get(PerfStatType.USER_MULTI_OP_INT);
        multiCumulative =
            packet.get(PerfStatType.USER_MULTI_OP_CUM);

        /* Extract the replication state from the otherStats. */
        if (packet.getOtherStats() != null) {
            for (ConciseStats cs : packet.getOtherStats()) {
                if (cs instanceof ReplicationState) {
                    replicationState = ((ReplicationState) cs).getState();
                }
            }
        }

        final int ceiling = parameters.getLatencyCeiling();
        final int floor = parameters.getThroughputFloor();

        Notification notification = null;

        /*
         * Check the interval measurements against their limits to determine
         * whether an trap should be issued.
         */
        if (singleInterval.getLatency().getOperationCount() != 0) {
            if (PerfEvent.latencyCeilingExceeded(ceiling, singleInterval)) {
                notification = new Notification
                    (NOTIFY_SINGLE_LCEILING, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The latency ceiling limit for single operations " +
                     "of " + ceiling + "ms was violated.");
                notification.setUserData(Float.valueOf(getIntervalLatAvg()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }

            if (PerfEvent.throughputFloorExceeded(floor, singleInterval)) {
                notification = new Notification
                    (NOTIFY_SINGLE_TFLOOR, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The throughput floor limit for single operations " +
                     "of " + floor + " ops/sec was violated.");
                notification.setUserData
                    (Long.valueOf(getIntervalThroughput()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }
        }

        if (multiInterval.getLatency().getOperationCount() != 0) {
            if (PerfEvent.latencyCeilingExceeded(ceiling, multiInterval)) {
                notification = new Notification
                    (NOTIFY_MULTI_LCEILING, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The latency ceiling limit for multi operations " +
                     "of " + ceiling + "ms was violated.");
                notification.setUserData
                    (Float.valueOf(getMultiIntervalLatAvg()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }
            if (PerfEvent.throughputFloorExceeded(floor, multiInterval)) {
                notification = new Notification
                    (NOTIFY_MULTI_TFLOOR, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The throughput floor limit for multi operations " +
                     "of " + floor + " ops/sec was violated.");
                notification.setUserData
                    (Long.valueOf(getMultiIntervalThroughput()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }
        }

        final String rnOpStr = packet.toOpJsonString();
        if (rnOpStr != null && !rnOpStr.isEmpty()) {
            opMetricString = rnOpStr;
            notification = new Notification
                (NOTIFY_RN_OP_METRIC, oName, notifySequence++,
                 System.currentTimeMillis(),
                 "New operation metrics for this RepNode.");
            notification.setUserData(rnOpStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String rnExceptionStr = packet.toExceptionsJsonString();
        if (rnExceptionStr != null && !rnExceptionStr.isEmpty()) {
            notification = new Notification
                (NOTIFY_RN_EXCEPTION_METRIC, oName, notifySequence++,
                 System.currentTimeMillis(),
                 "New exception metric of this RepNode.");
            notification.setUserData(rnExceptionStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String rnEnvStr = packet.toEnvJsonString();
        if (rnEnvStr != null && !rnEnvStr.isEmpty()) {
            envMetricString = rnEnvStr;
            notification = new Notification
                (NOTIFY_RN_ENV_METRIC, oName, notifySequence++,
                 System.currentTimeMillis(),
                 "New statistics for this RepNode.");
            notification.setUserData(rnEnvStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String rnTableStr = packet.toTableJsonString();
        if (rnTableStr != null && !rnTableStr.isEmpty()) {
            notification = new Notification
                (NOTIFY_RN_TABLE_METRIC, oName, notifySequence++,
                 System.currentTimeMillis(),
                 "New table metrics for this RepNode.");
            notification.setUserData(rnTableStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String rnJVMStatsStr = packet.toJVMStatsJsonString();
        if ((rnJVMStatsStr != null) && !rnJVMStatsStr.isEmpty()) {
            notification = new Notification(
                NOTIFY_RN_JVM_STATS, oName, notifySequence++,
                System.currentTimeMillis(),
                "New JVM stats for this RepNode.");
            notification.setUserData(rnJVMStatsStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String rnEndpointGroupStatsStr =
            packet.toEndpointGroupStatsJsonString();
        if ((rnEndpointGroupStatsStr != null) &&
            !rnEndpointGroupStatsStr.isEmpty()) {
            notification = new Notification(
                NOTIFY_RN_ENDPOINT_GROUP_STATS, oName, notifySequence++,
                System.currentTimeMillis(),
                "New EndpointGroup stats for this RepNode.");
            notification.setUserData(rnEndpointGroupStatsStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String loggingStatsStr = packet.toLoggingStatsJsonString();
        if ((loggingStatsStr != null) && !loggingStatsStr.isEmpty()) {
            notification = new Notification(
                NOTIFY_RN_LOGGING_STATS, oName, notifySequence++,
                System.currentTimeMillis(),
                "New logging stats for this RepNode.");
            notification.setUserData(loggingStatsStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }
    }

    public synchronized void setServiceStatus(ServiceStatus newStatus) {
        if (status.equals(newStatus)) {
            return;
        }

        final Notification n = new Notification
            (NOTIFY_RN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for RepNode " + getRepNodeId() +
             " changed to " + newStatus.toString() + ".");

        String statusInfo = "";
        try {
            ObjectNode jsonRoot = JsonUtils.createObjectNode();
            jsonRoot.put("resource", getRepNodeId());
            jsonRoot.put("shard", rnId.getGroupName());
            jsonRoot.put("reportTime", System.currentTimeMillis());
            jsonRoot.put("service_status", newStatus.toString());
            statusInfo = JsonUtils.toJsonString(jsonRoot, false);
        } catch (Exception e) {
        }
        n.setUserData(statusInfo);

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this RepNode.
         */
        sn.sendProxyNotification(n);

        status = newStatus;

        /*
         * Whenever there is a service status change, reset the metrics so that
         * we don't report stale information.
         */
        resetMetrics();
    }

    public synchronized void updateReplicationState(StateChangeEvent sce) {
        if (sce == null) {
            return;
        }
        State newState = sce.getState();
        if (newState == null || newState.equals(replicationState)) {
            return;
        }

        final Notification n = new Notification
            (NOTIFY_RN_REPLICATION_STATE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The replication state for RepNode " + getRepNodeId() +
             " changed to " + newState.toString() + ".");

        String stateInfo = "";
        try {
            ObjectNode jsonRoot = JsonUtils.createObjectNode();
            jsonRoot.put("resource", getRepNodeId());
            jsonRoot.put("shard", rnId.getGroupName());
            jsonRoot.put("reportTime", sce.getEventTime());
            jsonRoot.put("replication_state", newState.toString());
            stateInfo = JsonUtils.toJsonString(jsonRoot, false);
        } catch (Exception e) {
        }
        n.setUserData(stateInfo);

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this RepNode.
         */
        sn.sendProxyNotification(n);

        replicationState = newState;
    }

    @Override
    public String getRepNodeId() {
        return rnId.getFullName();
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
    }

    @Override
    public float getIntervalLatAvg() {
        return singleInterval.getLatency().getAverage();
    }

    @Override
    public int getIntervalLatMax() {
        return longToIntOrLimit(singleInterval.getLatency().getMax());
    }

    @Override
    public int getIntervalLatMin() {
        return longToIntOrLimit(singleInterval.getLatency().getMin());
    }

    @Override
    public int getIntervalPct95() {
        return longToIntOrLimit(singleInterval.getLatency().getPercent95());
    }

    @Override
    public int getIntervalPct99() {
        return longToIntOrLimit(singleInterval.getLatency().getPercent99());
    }

    @Deprecated
    @Override
    public int getIntervalTotalOps() {
        return longToIntOrLimit(getIntervalTotalOpsLong());
    }

    @Override
    public long getIntervalTotalOpsLong() {
        return singleInterval.getLatency().getOperationCount();
    }

    @Override
    public Date getIntervalEnd() {
        return new Date(singleInterval.getEnd());
    }

    @Override
    public Date getIntervalStart() {
        return new Date(singleInterval.getStart());
    }

    @Override
    public long getIntervalThroughput() {
        return singleInterval.getThroughputPerSec();
    }

    @Override
    public float getCumulativeLatAvg() {
        return singleCumulative.getLatency().getAverage();
    }

    @Override
    public int getCumulativeLatMax() {
        return longToIntOrLimit(singleCumulative.getLatency().getMax());
    }

    @Override
    public int getCumulativeLatMin() {
        return longToIntOrLimit(singleCumulative.getLatency().getMin());
    }

    @Override
    public int getCumulativePct95() {
        return longToIntOrLimit(singleCumulative.getLatency().getPercent95());
    }

    @Override
    public int getCumulativePct99() {
        return longToIntOrLimit(singleCumulative.getLatency().getPercent99());
    }

    @Deprecated
    @Override
    public int getCumulativeTotalOps() {
        return longToIntOrLimit(getCumulativeTotalOpsLong());
    }

    @Override
    public long getCumulativeTotalOpsLong() {
        return singleCumulative.getLatency().getOperationCount();
    }

    @Override
    public Date getCumulativeEnd() {
        return new Date(singleCumulative.getEnd());
    }

    @Override
    public Date getCumulativeStart() {
        return new Date(singleCumulative.getStart());
    }

    @Override
    public long getCumulativeThroughput() {
        return singleCumulative.getThroughputPerSec();
    }

    @Override
    public float getMultiIntervalLatAvg() {
        return multiInterval.getLatency().getAverage();
    }

    @Override
    public int getMultiIntervalLatMax() {
        return longToIntOrLimit(multiInterval.getLatency().getMax());
    }

    @Override
    public int getMultiIntervalLatMin() {
        return longToIntOrLimit(multiInterval.getLatency().getMin());
    }

    @Override
    public int getMultiIntervalPct95() {
        return longToIntOrLimit(multiInterval.getLatency().getPercent95());
    }

    @Override
    public int getMultiIntervalPct99() {
        return longToIntOrLimit(multiInterval.getLatency().getPercent99());
    }

    @Deprecated
    @Override
    public int getMultiIntervalTotalOps() {
        return longToIntOrLimit(getMultiIntervalTotalOpsLong());
    }

    @Override
    public long getMultiIntervalTotalOpsLong() {
        return multiInterval.getLatency().getOperationCount();
    }

    @Deprecated
    @Override
    public int getMultiIntervalTotalRequests() {
        return longToIntOrLimit(getMultiIntervalTotalRequestsLong());
    }

    @Override
    public long getMultiIntervalTotalRequestsLong() {
        return multiInterval.getLatency().getRequestCount();
    }

    @Override
    public Date getMultiIntervalEnd() {
        return new Date(multiInterval.getEnd());
    }

    @Override
    public Date getMultiIntervalStart() {
        return new Date(multiInterval.getStart());
    }

    @Override
    public long getMultiIntervalThroughput() {
        return multiInterval.getThroughputPerSec();
    }

    @Override
    public float getMultiCumulativeLatAvg() {
        return multiCumulative.getLatency().getAverage();
    }

    @Override
    public int getMultiCumulativeLatMax() {
        return longToIntOrLimit(multiCumulative.getLatency().getMax());
    }

    @Override
    public int getMultiCumulativeLatMin() {
        return longToIntOrLimit(multiCumulative.getLatency().getMin());
    }

    @Override
    public int getMultiCumulativePct95() {
        return longToIntOrLimit(multiCumulative.getLatency().getPercent95());
    }

    @Override
    public int getMultiCumulativePct99() {
        return longToIntOrLimit(multiCumulative.getLatency().getPercent99());
    }

    @Deprecated
    @Override
    public int getMultiCumulativeTotalOps() {
        return longToIntOrLimit(getMultiCumulativeTotalOpsLong());
    }

    @Override
    public long getMultiCumulativeTotalOpsLong() {
        return multiCumulative.getLatency().getOperationCount();
    }

    @Deprecated
    @Override
    public int getMultiCumulativeTotalRequests() {
        return longToIntOrLimit(getMultiCumulativeTotalRequestsLong());
    }

    @Override
    public long getMultiCumulativeTotalRequestsLong() {
        return multiCumulative.getLatency().getRequestCount();
    }

    @Override
    public Date getMultiCumulativeEnd() {
        return new Date(multiCumulative.getEnd());
    }

    @Override
    public Date getMultiCumulativeStart() {
        return new Date(multiCumulative.getStart());
    }

    @Override
    public long getMultiCumulativeThroughput() {
        return multiCumulative.getThroughputPerSec();
    }

    @Deprecated
    @Override
    public long getCommitLag() {
        /* Stub value for deprecated method */
        return 0;
    }

    @Override
    public String getConfigProperties() {
        return parameters.getConfigProperties();
    }

    @Override
    public String getJavaMiscParams() {
        return parameters.getJavaMiscParams();
    }

    @Override
    public String getLoggingConfigProps() {
        return parameters.getLoggingConfigProps();
    }

    @Override
    public boolean getCollectEnvStats() {
        return parameters.getCollectEnvStats();
    }

    @Override
    public int getCacheSize() {
        return (int) (parameters.getJECacheSize() / (1024 * 1024));
    }

    @Deprecated
    @Override
    public int getMaxTrackedLatency() {
        return -1;
    }

    @Override
    public int getStatsInterval() {
        return (int) sn.getCollectorInterval() / 1000; /* In seconds. */
    }

    @Override
    public int getHeapMB() {
        return (int) parameters.getMaxHeapMB();
    }

    @Override
    public String getMountPoint() {
        return parameters.getStorageDirectoryPath();
    }

    @Override
    public long getMountPointSize() {
        return parameters.getStorageDirectorySize();
    }

    @Override
    public String getLogMountPoint() {
        return parameters.getLogDirectoryPath();
    }

    @Override
    public long getLogMountPointSize() {
        return parameters.getLogDirectorySize();
    }

    @Override
    public int getLatencyCeiling() {
        return parameters.getLatencyCeiling();
    }

    @Override
    public int getThroughputFloor() {
        return parameters.getThroughputFloor();
    }

    @Deprecated
    @Override
    public long getCommitLagThreshold() {
        /* Stub value for deprecated method */
        return 0;
    }

    @Override
    public String getReplicationState() {
        return replicationState.toString();
    }

    @Override
    public String getOpMetric() {
        return opMetricString;
    }

    @Override
    public String getEnvMetric() {
        return envMetricString;
    }
}
