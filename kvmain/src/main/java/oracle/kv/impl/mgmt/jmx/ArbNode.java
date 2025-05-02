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

import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.measurement.ArbiterNodeStats;
import oracle.kv.impl.measurement.ConciseStats;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.mgmt.jmx.ArbNodeMXBean;

import com.sleepycat.je.rep.arbiter.ArbiterStats;

public class ArbNode
    extends NotificationBroadcasterSupport
    implements ArbNodeMXBean {

    private final ArbNodeId anId;
    private final MBeanServer server;
    private final StorageNode sn;
    private ServiceStatus status;
    private ArbiterStats arbStats;
    private ArbNodeParams parameters;
    private ObjectName oName;
    long notifySequence = 1L;

    static final String
        NOTIFY_AN_STATUS_CHANGE = "oracle.kv.arbnode.status";
    public static final String
        NOTIFY_AN_LOGGING_STATS = "oracle.kv.arbnode.loggingstats";

    public ArbNode(ArbNodeParams anp, MBeanServer server, StorageNode sn) {
        this.server = server;
        this.anId = anp.getArbNodeId();
        this.sn = sn;
        status = ServiceStatus.UNREACHABLE;

        resetMetrics();

        setParameters(anp);

        register();
    }

    private void resetMetrics() {
        arbStats = null;
    }

    private void register() {

        final StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=ArbNode");
        buf.append(",id=");
        buf.append(getArbNodeId());
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
                (new String[]{NOTIFY_AN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this ArbNode's service status"),
            new MBeanNotificationInfo(
                new String[]{NOTIFY_AN_LOGGING_STATS},
                Notification.class.getName(),
                "Announce ArbNode SEVERE and WARNING logging entries"),
        };
    }

    public void setParameters(ArbNodeParams anp) {
        parameters = anp;
    }

    public synchronized void setStats(StatsPacket packet) {

        for (ConciseStats cs : packet.getOtherStats()) {
            if (cs instanceof ArbiterNodeStats) {
                arbStats = ((ArbiterNodeStats)cs).getArbiterStats();
            }
        }

        final String loggingStatsStr = packet.toLoggingStatsJsonString();
        if ((loggingStatsStr != null) && !loggingStatsStr.isEmpty()) {
            final Notification notification = new Notification(
                NOTIFY_AN_LOGGING_STATS, oName, notifySequence++,
                System.currentTimeMillis(),
                "New logging stats for this ArbNode.");
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
            (NOTIFY_AN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for ArbNode " + getArbNodeId() +
             " changed to " + newStatus.toString() + ".");

        n.setUserData(newStatus.toString());

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this ArbNode.
         */
        sn.sendProxyNotification(n);

        status = newStatus;

        /*
         * Whenever there is a service status change, reset the metrics so that
         * we don't report stale information.
         */
        resetMetrics();
    }

    @Override
    public String getArbNodeId() {
        return anId.getFullName();
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
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
    public int getStatsInterval() {
        return (int) sn.getCollectorInterval() / 1000; /* In seconds. */
    }

    @Override
    public int getHeapMB() {
        return (int) parameters.getMaxHeapMB();
    }

    @Override
    public long getAcks() {
        return arbStats != null ? arbStats.getAcks() : -1;
    }

    @Override
    public String getMaster() {
        return arbStats != null ? arbStats.getMaster() : null;
    }

    @Override
    public long getReplayQueueOverflow() {
        return arbStats != null ? arbStats.getReplayQueueOverflow() : -1;
    }

    @Override
    public String getState() {
        return arbStats != null ? arbStats.getState() : null;
    }

    @Override
    public long getVLSN() {
        return arbStats != null ? arbStats.getVLSN() : -1;
    }
}
