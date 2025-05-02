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

package oracle.kv.impl.sna.collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.mgmt.jmx.Admin;
import oracle.kv.impl.mgmt.jmx.ArbNode;
import oracle.kv.impl.mgmt.jmx.RepNode;
import oracle.kv.impl.mgmt.jmx.StorageNode;
import oracle.kv.impl.sna.collector.CollectorRecorder.MetricType;
import oracle.kv.util.ErrorMessage;

/**
 * This agent will collect metric data from JMX. It is not allowed to
 * enable JMXCollectorAgent if the user doesn't enable JMX in KVStore.
 *
 * 1) It can add a listener to receive SN notification.
 * 2) It can schedule a task to poll component Mbean data at a giving interval.
 *
 * Poll vs. notification
 * ---------------------
 * The notification carries the StatsPacket, but the JMX Mbean has getters that
 * provide info that is not in the statsPacket, which come from other sources
 * like the parameters. An example: RepNode.getHeapMB(). If we want those types
 * of info, we have to poll (call the JMXMbean) and not just rely on the
 * notification.
 *
 * Getting the stats as a string in the notification vs.
 * the JMX getters (MetricListener.handleNotification).
 * ----------------------------------------------------
 * notification has a payload for these reasons
 * 1) it's more efficient.
 * 2) more important reason - prevents loss of stats, prevents a separation of
 * the notification and the information. Otherwise, if the listener doesn't
 * call the JMX bean in time, the information the caller gets may not match the
 * notification.
 * 3) Using a string insulates the JMXCollectorAgent from a JMX upgrade
 */
public class JMXCollectorAgent implements CollectorAgent{

    static final Map<String, MetricType> notificationMap
        = createNotificationMap();

    private final CollectorRecorder fileRecorder;
    /* Note that the optional recorder may be null */
    private CollectorRecorder optionalRecorder;

    /*
     * The recorderReference gives us a way to obtain a new optional
     * recorder, which might be updated if params change.
     */
    private final AtomicReference<CollectorRecorder> recorderReference;

    /* to log JMXCollectorAgent execution process information */
    private Logger snLogger;

    /* JMX information */
    private JMXServiceURL url;
    private Map<String, Object> env = null;
    private ObjectName storageNodeMBeanName;
    private JMXConnector jmxc;
    private MBeanServerConnection mbsc;

    /* receive SN notification*/
    private NotificationListener metricListener;
    private NotificationFilter metricFilter;

    /*
     * Increase sequence if start or stop JMXCollectorAgent. Then the executing
     * AddSNMetricsListener or RemoveSNMetricsListener thread will know its
     * sequence is invalid and stop.
     */
    private volatile int agentSequence;

    public JMXCollectorAgent(StorageNodeParams snp,
                             SecurityParams sp,
                             CollectorRecorder fileRecorder,
                             AtomicReference<CollectorRecorder>
                             recorderReference,
                             Logger snLogger) {
        this.snLogger = snLogger;
        this.fileRecorder = fileRecorder;
        this.recorderReference = recorderReference;
        this.optionalRecorder = recorderReference.get();

        /* If using SSL, set up the appropriate SocketFactories. */
        if (sp.isSecure() && !sp.allTransportSSLDisabled()) {
            SslRMIClientSocketFactory csf = new SslRMIClientSocketFactory();
            env = new HashMap<String, Object>();
            env.put("com.sun.jndi.rmi.factory.socket", csf);
        }

        final String host = snp.getHostname();
        final int port = snp.getRegistryPort();
        try {
            /* This URL is the standard JMX service descriptor. */
            url = new JMXServiceURL
                ("service:jmx:rmi:///jndi/rmi://" + host +":" +
                 port + "/jmxrmi");
            storageNodeMBeanName =
                new ObjectName("Oracle NoSQL Database:type=StorageNode");
        } catch (Exception e) {
            throw new CommandFaultException(e.getMessage(), e,
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        metricListener = new SNMetricListener();
        metricFilter = new SNMetricFilter();
        agentSequence = 0;
    }

    @Override
    public void start() {
        ++agentSequence;
        startupNotificationListener();
    }

    @Override
    public void stop() {
        snLogger.info("CollectorService: JMXCollectorAgent stop.");
        ++agentSequence;
        shutdownNotificationListener();
    }

    @Override
    public void updateParams(GlobalParams newGlobalParams,
                             StorageNodeParams newSNParams) {
        fileRecorder.updateParams(newGlobalParams, newSNParams);
        optionalRecorder = recorderReference.get();

        if (newGlobalParams != null) {
            if (newGlobalParams.getCollectorEnabled()) {
                start();
            } else {
                stop();
            }
        }
    }

    private void startupNotificationListener() {
        /* Create a thread to add jmx listener and retry if fail to add. */
        Thread t = new Thread(new AddSNMetricsListener(agentSequence));
        t.setDaemon(true);
        t.start();
    }

    private void shutdownNotificationListener() {
        /*
         * JMXConnector.close() is slow, so create a thread to close it.
         * This new thread will keep running in SN process. If SN process exit
         * before the new thread finish JMXConnector.close(), it is fine as JMX
         * server will also exit. It and executor shutdown don't rely on each
         * other.
         */
        Thread t = new Thread(new RemoveSNMetricsListener(agentSequence));
        t.setDaemon(true);
        t.start();
    }

    private class AddSNMetricsListener implements Runnable {

        private int executorSequence;

        private AddSNMetricsListener(int taskSequence) {
            this.executorSequence = taskSequence;
        }

        @Override
        public void run() {
            while(true) {
                synchronized (JMXCollectorAgent.this) {
                    if (executorSequence != agentSequence) {
                        /*
                         * We have new sequence now, either agent is stopped or
                         * started again.
                         */
                        return;
                    }
                    try {
                        if (jmxc == null) {
                            jmxc = JMXConnectorFactory.connect(url, env);
                            snLogger.info("CollectorService: JMXCollectorAgent"
                                + " connected to JMX.");
                        }
                        if (mbsc == null) {
                            mbsc = jmxc.getMBeanServerConnection();
                            mbsc.addNotificationListener(storageNodeMBeanName,
                                                         metricListener,
                                                         metricFilter,
                                                         null);
                            snLogger.info("CollectorService: JMXCollectorAgent"
                                + " registered listener to JMX.");
                        }
                        return;
                    } catch (Exception e) {
                        /* If hit any Exception, retry next interval. */
                        mbsc = null;
                        jmxc = null;
                        snLogger.warning("CollectorService: "
                            + "JMXCollectorAgent add listener error:"
                            + e.toString());
                    }
                }
                try {
                    Thread.sleep(60000);
                } catch(Exception e) {}
            }
        }
    }

    private class RemoveSNMetricsListener implements Runnable {

        private int executorSequence;

        private RemoveSNMetricsListener(int taskSequence) {
            this.executorSequence = taskSequence;
        }

        @Override
        public void run() {
            synchronized (JMXCollectorAgent.this) {
                if (executorSequence != agentSequence) {
                    /*
                     * We have new sequence now, either agent is started or
                     * stopped again.
                     */
                    return;
                }
                if (mbsc != null) {
                    try {
                        mbsc.removeNotificationListener(storageNodeMBeanName,
                                                        metricListener,
                                                        metricFilter,
                                                        null);
                        snLogger.info("CollectorService: JMXCollectorAgent"
                            + " unregistered listener to JMX.");
                    } catch (Exception e) {
                        snLogger.warning("CollectorService: "
                            + "JMXCollectorAgent remove listener error:"
                            + e.toString());
                    } finally {
                        mbsc = null;
                    }
                }
                if (jmxc != null) {
                    try {
                        jmxc.close();
                        snLogger.info("CollectorService: JMXCollectorAgent"
                            + " disconnected to JMX.");
                    } catch (Exception e) {
                        snLogger.warning("CollectorService: "
                            + "JMXCollectorAgent close connection error:"
                            + e.toString());
                    } finally {
                        jmxc = null;
                    }
                }
            }
        }
    }

    /**
     */
    private static
        Map<String, CollectorRecorder.MetricType> createNotificationMap()
    {
        final Map<String, CollectorRecorder.MetricType> map = new HashMap<>();
        map.put(RepNode.NOTIFY_RN_OP_METRIC, CollectorRecorder.MetricType.RNOP);
        map.put(RepNode.NOTIFY_RN_TABLE_METRIC,
                CollectorRecorder.MetricType.RNTABLE);
        map.put(RepNode.NOTIFY_RN_JVM_STATS,
                CollectorRecorder.MetricType.RNJVM);
        map.put(RepNode.NOTIFY_RN_ENDPOINT_GROUP_STATS,
                CollectorRecorder.MetricType.RNENDPOINTGROUP);
        map.put(RepNode.NOTIFY_RN_EXCEPTION_METRIC,
                CollectorRecorder.MetricType.RNEXCEPTION);
        map.put(RepNode.NOTIFY_RN_ENV_METRIC,
                CollectorRecorder.MetricType.RNENV);
        map.put(RepNode.NOTIFY_RN_STATUS_CHANGE,
                CollectorRecorder.MetricType.RNEVENT);
        map.put(RepNode.NOTIFY_RN_REPLICATION_STATE,
                CollectorRecorder.MetricType.RNEVENT);
        map.put(RepNode.NOTIFY_RN_LOGGING_STATS,
                CollectorRecorder.MetricType.LOGGINGSTAT);
        map.put(Admin.NOTIFY_PLAN_STATUS_CHANGE,
                CollectorRecorder.MetricType.PLAN);
        map.put(Admin.NOTIFY_ADMIN_LOGGING_STATS,
                CollectorRecorder.MetricType.LOGGINGSTAT);
        map.put(StorageNode.NOTIFY_SN_LOGGING_STATS,
                CollectorRecorder.MetricType.LOGGINGSTAT);
        map.put(ArbNode.NOTIFY_AN_LOGGING_STATS,
                CollectorRecorder.MetricType.LOGGINGSTAT);
        return map;
    }

    private class SNMetricListener implements NotificationListener {

        @Override
        public void handleNotification(Notification notif,
                                       Object handback) {

            try {
                snLogger.fine("CollectorService: JMXCollectorAgent "
                    + "Notification received: " + notif.getMessage());
                String metrics = (String) notif.getUserData();
                if (metrics != null && !metrics.isEmpty()) {
                    MetricType metricType = notificationMap.get(notif.getType());
                    if (metricType == null) {
                        /* Ignore unsupported notification type */
                        return;
                    }
                    fileRecorder.record(metricType, metrics);
                    if (optionalRecorder != null) {
                        optionalRecorder.record(metricType, metrics);
                    }
                }
            } catch (Exception e) {
                snLogger.severe("CollectorService: JMXCollectorAgent "
                    + "listener error: " + e.toString());
            }
        }
    }
}

/*
 * SNMetricFilter can't be inner class as it need be serializable.
 * Make sure all fields in the filter are serializable.
 */
class SNMetricFilter implements NotificationFilter {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isNotificationEnabled(Notification notification) {
        return JMXCollectorAgent.notificationMap.containsKey(
            notification.getType());
    }
}
