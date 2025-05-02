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

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.mgmt.jmx.AdminMXBean;

public class Admin
    extends NotificationBroadcasterSupport
    implements AdminMXBean {

    private AdminId aid;
    final private MBeanServer server;
    final private StorageNode sn;

    private ServiceStatus status;
    private AdminParams parameters;
    private boolean isMaster;
    private ObjectName oName;
    long notifySequence = 1L;
    private String envMetricString;

    public final static String
        NOTIFY_ADMIN_STATUS_CHANGE = "oracle.kv.admin.status";
    public final static String
        NOTIFY_PLAN_STATUS_CHANGE = "oracle.kv.plan.status";
    public static final String
        NOTIFY_ADMIN_ENV_METRIC = "oracle.kv.admin.envmetric";
    public static final String
        NOTIFY_ADMIN_LOGGING_STATS = "oracle.kv.admin.loggingstats";

    public Admin(AdminParams ap, MBeanServer server, StorageNode sn) {
        this.server = server;
        /* If ap is null, then this is a bootstrap Admin. */
        this.aid = (ap == null ? new AdminId(0) : ap.getAdminId());
        this.sn = sn;
        status = ServiceStatus.UNREACHABLE;
        isMaster = false;
        setParameters(ap);
        resetMetrics();
        register();
    }

    private void register() {
        StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=Admin");
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
                (new String[]{NOTIFY_ADMIN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this Admin's service status"),
            new MBeanNotificationInfo
                (new String[]{NOTIFY_PLAN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a plan status change"),
            new MBeanNotificationInfo
                (new String[]{NOTIFY_ADMIN_ENV_METRIC},
                 Notification.class.getName(),
                 "New statistics are available."),
            new MBeanNotificationInfo(
                new String[]{NOTIFY_ADMIN_LOGGING_STATS},
                Notification.class.getName(),
                "Announce Admin SEVERE and WARNING logging entries"),
        };
    }

    public void setParameters(AdminParams ap) {
        parameters = ap;
        if (ap == null  || ap.getAdminId().equals(aid)) {
            return;
        }
        aid = ap.getAdminId();
    }

    public synchronized void setServiceStatus(ServiceStatus newStatus,
                                              boolean master) {

        if (status.equals(newStatus) && master == isMaster) {
            return;
        }

        Notification n = new Notification
            (NOTIFY_ADMIN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for Admin " + getAdminId() +
             " changed to " + newStatus.toString() +
             (master ? " master" : ""));

        n.setUserData(newStatus.toString());

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this Admin.
         */
        sn.sendProxyNotification(n);

        status = newStatus;
        isMaster = master;
        resetMetrics();
    }

    public synchronized void updatePlanStatus(String planStatus) {
        Notification n = new Notification
            (NOTIFY_PLAN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "Announce a plan status change");

        n.setUserData(planStatus);

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this Admin.
         */
        sn.sendProxyNotification(n);
    }

    @Override
    public int getAdminId() {
        return aid.getAdminInstanceId();
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
    }

    @Override
    public int getLogFileLimit() {
        return parameters == null ? 0 : parameters.getLogFileLimit();
    }

    @Override
    public int getLogFileCount() {
        return parameters == null ? 0 : parameters.getLogFileCount();
    }

    @Override
    public long getPollPeriodMillis() {
        return parameters == null ? 0L : parameters.getPollPeriodMillis();
    }

    @Override
    public long getEventExpiryAge() {
        return parameters == null ? 0L : parameters.getEventExpiryAge();
    }

    @Override
    public boolean isMaster() {
        return isMaster;
    }

    @Override
    public String getEnvMetric() {
        return envMetricString;
    }

    public synchronized void setStats(StatsPacket packet) {

        Notification notification = null;

        final String envStr = packet.toEnvJsonString();
        if (envStr != null && !envStr.isEmpty()) {
            envMetricString = envStr;
            notification = new Notification
                (NOTIFY_ADMIN_ENV_METRIC, oName, notifySequence++,
                 System.currentTimeMillis(),
                 "New statistics for this Admin.");
            notification.setUserData(envStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }

        final String loggingStatsStr = packet.toLoggingStatsJsonString();
        if ((loggingStatsStr != null) && !loggingStatsStr.isEmpty()) {
            notification = new Notification(
                NOTIFY_ADMIN_LOGGING_STATS, oName, notifySequence++,
                System.currentTimeMillis(),
                "New logging stats for this Admin.");
            notification.setUserData(loggingStatsStr);
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }
    }

    private void resetMetrics() {
        final String emptyJson = "{}";
        envMetricString = emptyJson;
    }
}
