/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.mgmt.jmx;

import static java.util.Arrays.asList;
import static java.util.logging.Level.SEVERE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.mgmt.MgmtSystemTestBase;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.mgmt.jmx.AdminMXBean;
import oracle.kv.mgmt.jmx.ArbNodeMXBean;
import oracle.kv.mgmt.jmx.RepNodeMXBean;
import oracle.kv.mgmt.jmx.StorageNodeMXBean;
import oracle.kv.util.CreateStore;

import com.sleepycat.je.rep.ReplicatedEnvironment;

import org.junit.Test;

/**
 * System-level test for JMX polling and notification.
 */
public class JmxSystemTest extends MgmtSystemTestBase {
    private static final double DELTA = 1e-15;

    private static final Set<String> REP_NODE_NOTIFICATION_TYPES = set(
        RepNode.NOTIFY_RN_STATUS_CHANGE,
        RepNode.NOTIFY_SINGLE_TFLOOR,
        RepNode.NOTIFY_SINGLE_LCEILING,
        RepNode.NOTIFY_MULTI_TFLOOR,
        RepNode.NOTIFY_MULTI_LCEILING,
        RepNode.NOTIFY_RN_OP_METRIC,
        RepNode.NOTIFY_RN_EXCEPTION_METRIC,
        RepNode.NOTIFY_RN_ENV_METRIC,
        RepNode.NOTIFY_RN_REPLICATION_STATE,
        RepNode.NOTIFY_RN_TABLE_METRIC,
        RepNode.NOTIFY_RN_JVM_STATS,
        RepNode.NOTIFY_RN_ENDPOINT_GROUP_STATS,
        RepNode.NOTIFY_RN_LOGGING_STATS);

    private static final Set<String> ADMIN_NOTIFICATION_TYPES = set(
        Admin.NOTIFY_ADMIN_STATUS_CHANGE,
        Admin.NOTIFY_PLAN_STATUS_CHANGE,
        Admin.NOTIFY_ADMIN_ENV_METRIC,
        Admin.NOTIFY_ADMIN_LOGGING_STATS);

    private static final Set<String> ARB_NODE_NOTIFICATION_TYPES = set(
        ArbNode.NOTIFY_AN_STATUS_CHANGE,
        ArbNode.NOTIFY_AN_LOGGING_STATS);

    private static final Set<String> STORAGE_NODE_NOTIFICATION_TYPES = set(
        StorageNode.NOTIFY_SN_STATUS_CHANGE,
        StorageNode.NOTIFY_SN_LOGGING_STATS);
    static {
        STORAGE_NODE_NOTIFICATION_TYPES.addAll(REP_NODE_NOTIFICATION_TYPES);
        STORAGE_NODE_NOTIFICATION_TYPES.addAll(ADMIN_NOTIFICATION_TYPES);
        STORAGE_NODE_NOTIFICATION_TYPES.addAll(ARB_NODE_NOTIFICATION_TYPES);
    }

    int registryPort[];
    MBeanServerConnection[] mbsc;
    JMXConnector[] jmxc;
    int connections = 0;
    NotificationListener stListener;
    ObjectName storageNodeMBeanName;
    ObjectName adminMBeanName;
    ObjectName arbNodeMBeanName;

    /* Received notifications are added to this list. */
    List<Notification> componentStatusNotificationsReceived =
        new ArrayList<Notification>();
    List<Notification> planStatusNotificationsReceived =
        new ArrayList<Notification>();
    List<Notification> otherNotificationsReceived =
        new ArrayList<Notification>();

    List<Notification> startupNotifications;
    List<Notification> metricsNotifications;
    List<Notification> stopRN1Notifications;
    List<Notification> startRN1Notifications;
    List<Notification> stopSN1Notifications;
    List<Notification> shutdownLeftNotifications;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
        storageNodeMBeanName =
            new ObjectName("Oracle NoSQL Database:type=StorageNode");

        adminMBeanName =
            new ObjectName("Oracle NoSQL Database:type=Admin");

        arbNodeMBeanName =
            new ObjectName("Oracle NoSQL Database:type=ArbNode,id=rg1-an1");

        populate();

        TestUtils.clearTestDirectory();

    }

    /**
     * Give Notification a useful constructor that includes userData.
     */
    private class MyNotification extends Notification {

        private static final long serialVersionUID = 1L;

        public MyNotification(String type,
                              Object source,
                              Object userData) {
            super(type, source, 0L);
            this.setUserData(userData);
        }

        /** Include more information, for debugging */
        @Override
        public String toString() {
            return String.format("%s[userData=%s][timestamp=%s]",
                                 super.toString(),
                                 getUserData(),
                                 FormatUtils.formatDateTimeMillis(
                                     getTimeStamp()));
        }
    }

    private ObjectName getRepNodeMBeanName(RepNodeId rnId) {
        try {
            return  new ObjectName("Oracle NoSQL Database:type=RepNode,id=" +
                                   rnId.getFullName());
        } catch (Exception e) {
            fail("Internal test error forming mbean name");
            return null;
        }
    }

    private void populate() {
        startupNotifications = asList(
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STARTING"), /* sn1 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "RUNNING"), /* sn1 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STARTING"), /* sn2 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "RUNNING"), /* sn2 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STARTING"), /* sn3 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "RUNNING"), /* sn3 */
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "STARTING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "STARTING"),
            new MyNotification("oracle.kv.arbnode.status",
                               arbNodeMBeanName,
                               "STARTING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "RUNNING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "RUNNING"),
            new MyNotification("oracle.kv.repnode.replicationstate",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "MASTER"),
            new MyNotification("oracle.kv.repnode.replicationstate",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "REPLICA"),
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               ""), /* admin2 WAITING_FOR_DEPLOY */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               ""), /* admin3 WAITING_FOR_DEPLOY */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "RUNNING"), /* admin1 */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "RUNNING"), /* admin2 */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "RUNNING"), /* admin3 */
            new MyNotification("oracle.kv.arbnode.status",
                               arbNodeMBeanName,
                               "RUNNING"));

        metricsNotifications = new ArrayList<>();
        metricsNotifications.addAll(asList(
            new MyNotification("oracle.kv.singleop.throughputfloor",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               ""),
            new MyNotification("oracle.kv.singleop.throughputfloor",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               ""),
            new MyNotification("oracle.kv.repnode.opmetric",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "rg1-rn1"),
            new MyNotification("oracle.kv.repnode.opmetric",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "rg1-rn2"),
            new MyNotification("oracle.kv.repnode.envmetric",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "rg1-rn1"),
            new MyNotification("oracle.kv.repnode.envmetric",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "rg1-rn2"),
            new MyNotification("oracle.kv.repnode.jvmstats",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "rg1-rn1"),
            new MyNotification("oracle.kv.repnode.jvmstats",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "rg1-rn2"),
            new MyNotification("oracle.kv.admin.envmetric",
                               adminMBeanName,
                               "admin"),
            new MyNotification("oracle.kv.repnode.loggingstats",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "severe"),
            new MyNotification("oracle.kv.repnode.loggingstats",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "severe"),
            new MyNotification("oracle.kv.admin.loggingstats",
                               adminMBeanName,
                               "severe"),
            new MyNotification("oracle.kv.storagenode.loggingstats",
                               storageNodeMBeanName,
                               "severe"),
            new MyNotification("oracle.kv.arbnode.loggingstats",
                               arbNodeMBeanName,
                               "severe")));

        stopRN1Notifications = asList(
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "STOPPING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "STOPPED"),
            new MyNotification("oracle.kv.repnode.replicationstate",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "UNKNOWN"),
            new MyNotification("oracle.kv.repnode.replicationstate",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "MASTER"));

        startRN1Notifications = asList(
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "STARTING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "RUNNING"),
            new MyNotification("oracle.kv.repnode.replicationstate",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "REPLICA")
            );

        stopSN1Notifications = asList(
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "STOPPING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,1)),
                               "STOPPED"),
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STOPPING"),
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STOPPED"),
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "STOPPING"),
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "STOPPED"),
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               /* admin3 change to master running */
                               "RUNNING"));

        shutdownLeftNotifications = asList(
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STOPPING"), /* sn2 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STOPPING"), /* sn3 */
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "STOPPING"),
            new MyNotification("oracle.kv.repnode.status",
                               getRepNodeMBeanName(new RepNodeId(1,2)),
                               "STOPPED"),
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STOPPED"), /* sn2 */
            new MyNotification("oracle.kv.storagenode.status",
                               storageNodeMBeanName,
                               "STOPPED"), /* sn3 */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "STOPPING"), /* admin2 */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "STOPPED"), /* admin2 */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "STOPPING"), /* admin3 */
            new MyNotification("oracle.kv.admin.status",
                               adminMBeanName,
                               "STOPPED"), /* admin3 */
            new MyNotification("oracle.kv.arbnode.status",
                               arbNodeMBeanName,
                               "STOPPING"),
            new MyNotification("oracle.kv.arbnode.status",
                               arbNodeMBeanName,
                               "STOPPED"));
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testAll ()
        throws Exception {

        store = new CreateStore(kvstoreName,
                                startPort,
                                3,     /* Storage nodes */
                                2,     /* Replication factor */
                                100,   /* Partitions */
                                1,     /* Capacity */
                                CreateStore.MB_PER_SN,
                                false, /* Use threads */
                                "oracle.kv.impl.mgmt.jmx.JmxAgent");
        if (SECURITY_ENABLE) {
            System.setProperty("javax.net.ssl.trustStore",
                               store.getTrustStore().getPath());
        }
        store.setAllowArbiters(true);
        /*
         * In secure mode, if the SN is not deployed, the secure connection
         * policy for JMX will not work. So we only attempt to test whether
         * the JMX connection can work after secure SN is deployed.
         * In non-secure, the JMX can be connected for unregistered SN, and
         * verify the notification after deployment.
         */
        if (SECURITY_ENABLE) {
            store.start();
        } else {
            store.initStorageNodes();
        }

        PortFinder[] pfs = store.getPortFinders();
        registryPort = new int[pfs.length];
        jmxc = new JMXConnector[pfs.length];
        mbsc = new MBeanServerConnection[pfs.length];
        for (int i = 0; i < pfs.length; i++) {
            registryPort[i] = pfs[i].getRegistryPort();
            openJmxConnection(registryPort[i]);
        }
        resetNotificationsReceived();
        /*
         * No need to deploy again in secure mode.
         */
        if (!SECURITY_ENABLE) {
            store.start();
        }

        /*
         * No stage changes notification received for secure store, because
         * to make the secure JMX work, the store need to deploy first. After
         * deployment, we connect the secure JMX to test whether the connection
         * can work. At that time the expected notification for stage change
         * "STARTING", "RUNNING" already passed by without capture. So no need
         * to verify here in secure mode.
         * TODO modify the expectation to make the verification work in
         * secure mode.
         */
        if (!SECURITY_ENABLE) {
            verifyNotificationsReceived(componentStatusNotificationsReceived,
                                        startupNotifications,
                                        true);
        }
        CommandServiceAPI cs = store.getAdmin();

        /* Disable the  KVAdminMetadata and KVVersion thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "false") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "false") ;
        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        /* Check that MBeans advertise the expected notifications */
        checkAllMBeanNotificationInfo();

        /* Poll for service statuses and such. */
        doStatusPolling();

        /*
         * Poll for metrics. During this operation, we will get at least one
         * perf limit notification.
         */
        resetNotificationsReceived();
        doMetricPolling();
        verifyNotificationsReceived(otherNotificationsReceived,
                                    metricsNotifications,
                                    true /* executedPlan */);

        checkReplicationState();

        /* Poll for parameters. */
        doParameterPolling();

        /* execute stop rg1-rn1 plan */
        resetNotificationsReceived();
        executeStopRNPlan("rg1-rn1");
        verifyNotificationsReceived(componentStatusNotificationsReceived,
                                    stopRN1Notifications,
                                    true /* executedPlan */);

        /* execute start rg1-rn1 plan */
        resetNotificationsReceived();
        executeStartRNPlan("rg1-rn1");

        verifyNotificationsReceived(componentStatusNotificationsReceived,
                                    startRN1Notifications,
                                    true);

        /* Stop one SN and verify */
        resetNotificationsReceived();
        store.shutdownSNA(0, false);
        verifyNotificationsReceived(componentStatusNotificationsReceived,
                                    stopSN1Notifications,
                                    false /* executedPlan */);

        /* Shutdown and verify the traps received during shutdown. */
        resetNotificationsReceived();
        store.shutdownSNA(1, false);
        store.shutdownSNA(2, false);
        store = null;

        verifyNotificationsReceived(componentStatusNotificationsReceived,
                                    shutdownLeftNotifications,
                                    false /* executedPlan */);
    }

    /**
     * Check that all MBeans list the expected set of notifications.
     */
    private void checkAllMBeanNotificationInfo() throws Exception {
        for (final MBeanServerConnection connection : mbsc) {
            checkMBeanNotificationInfo(connection, storageNodeMBeanName,
                                       STORAGE_NODE_NOTIFICATION_TYPES);

            final Set<ObjectName> allNames = connection.queryNames(null, null);

            if (allNames.contains(adminMBeanName)) {
                checkMBeanNotificationInfo(connection, adminMBeanName,
                                           ADMIN_NOTIFICATION_TYPES);
            }

            if (allNames.contains(arbNodeMBeanName)) {
                checkMBeanNotificationInfo(connection, arbNodeMBeanName,
                                           ARB_NODE_NOTIFICATION_TYPES);
            }

            for (final ObjectName objectName : allNames) {
                if (objectName.toString().contains(
                        "Oracle NoSQL Database:type=RepNode")) {
                    checkMBeanNotificationInfo(connection, objectName,
                                               REP_NODE_NOTIFICATION_TYPES);
                }
            }
        }
    }

    private void checkMBeanNotificationInfo(MBeanServerConnection connection,
                                            ObjectName mbeanName,
                                            Set<String> expectedTypes)
        throws Exception
    {
        final MBeanInfo mbeanInfo = connection.getMBeanInfo(mbeanName);
        final Set<String> allTypes = new HashSet<>();
        for (final MBeanNotificationInfo notifInfo :
                 mbeanInfo.getNotifications()) {
            final String[] types = notifInfo.getNotifTypes();
            assertEquals("Our notifications advertise a single type",
                         1, types.length);
            allTypes.add(types[0]);
        }
        checkSet(expectedTypes, allTypes);
    }

    @SafeVarargs
    private static <E> Set<E> set(E... elements) {
        final Set<E> set = new HashSet<>();
        for (final E e : elements) {
            set.add(e);
        }
        return set;
    }

    /** Check that the set contains the expected elements. */
    private static <E> void checkSet(Set<E> expected, Set<E> set) {
        final Set<E> missing = unexpected(set, expected);
        final Set<E> unexpected = unexpected(expected, set);
        if (!missing.isEmpty() || !unexpected.isEmpty()) {
            fail((!missing.isEmpty() ?
                  "Missing elements: " + missing : "") +
                 (!missing.isEmpty() && !unexpected.isEmpty() ? ", " : "") +
                 (!unexpected.isEmpty() ?
                  "Unexpected elements: " + unexpected : ""));
        }
    }

    /** Return the elements of set that are not present in expected. */
    private static <E> Set<E> unexpected(Set<E> expected, Set<E> set) {
        final Set<E> result = new HashSet<>(set);
        result.removeAll(expected);
        return result;
    }

    private void executeStopRNPlan(String rnidStr) throws RemoteException {
        RepNodeId rnid = RepNodeId.parse(rnidStr);
        Set<ResourceId> serviceIds= new HashSet<>();
        serviceIds.add(rnid);
        CommandServiceAPI cs = store.getAdmin();
        int p = cs.createStopServicesPlan("stop rg1-rn1",
                                          serviceIds);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
    }

    private void executeStartRNPlan(String rnidStr) throws RemoteException {
        RepNodeId rnid = RepNodeId.parse(rnidStr);
        Set<ResourceId> serviceIds= new HashSet<>();
        serviceIds.add(rnid);
        CommandServiceAPI cs = store.getAdmin();
        int p = cs.createStartServicesPlan("start rg1-rn1",
                                           serviceIds);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
     }

    private void openJmxConnection(int port) throws Exception {
        JMXServiceURL url =
            new JMXServiceURL
            ("service:jmx:rmi:///jndi/rmi://localhost:" +
             port + "/jmxrmi");
        Map<String, Object> env = null;
        if (SECURITY_ENABLE) {
            SslRMIClientSocketFactory csf = new SslRMIClientSocketFactory();
            env = new HashMap<String, Object>();
            env.put("com.sun.jndi.rmi.factory.socket", csf);
        }

        jmxc[connections] = JMXConnectorFactory.connect(url, env);
        mbsc[connections] = jmxc[connections].getMBeanServerConnection();

        /* Crank up the notification listener now. */
        stListener = new STListener();
        mbsc[connections].addNotificationListener
            (storageNodeMBeanName, stListener, null, null);
        connections++;
    }

    private void doStatusPolling()
        throws Exception {

        int snIdx = 0;
        for (StorageNodeId snId : store.getStorageNodeIds()) {
            StorageNodeMXBean snmb =
                JMX.newMBeanProxy(mbsc[snIdx], storageNodeMBeanName,
                                  StorageNodeMXBean.class, true);

            String snServiceStatus = snmb.getServiceStatus();

            assertEquals("RUNNING", snServiceStatus);
            if (store.numRNs(snId) > 0) {
                for (RepNodeId rnId : store.getRNs(snId)) {
                    RepNodeMXBean rnmb =
                        JMX.newMBeanProxy(mbsc[snIdx],
                                          getRepNodeMBeanName(rnId),
                                          RepNodeMXBean.class, true);

                    String rnServiceStatus = rnmb.getServiceStatus();

                    assertEquals("RUNNING", rnServiceStatus);
                }
            }

            if (store.numANs(snId) > 0) {
                ArbNodeMXBean anmb =
                    JMX.newMBeanProxy(mbsc[snIdx], arbNodeMBeanName,
                                      ArbNodeMXBean.class, true);

                String anServiceStatus = anmb.getServiceStatus();

                assertEquals("RUNNING", anServiceStatus);
            }
            snIdx++;
        }
    }

    private synchronized void resetNotificationsReceived() {
        planStatusNotificationsReceived.clear();
        componentStatusNotificationsReceived.clear();
        otherNotificationsReceived.clear();
    }

    public class STListener implements NotificationListener {
        @Override
        public void handleNotification(Notification notif,
                                       Object handback) {

            synchronized (JmxSystemTest.this) {

                if (isPlanStatusNotification(notif)) {
                    planStatusNotificationsReceived.add(notif);
                } else if (isComponentStatusNotification(notif)) {
                    componentStatusNotificationsReceived.add(notif);
                } else if (isExceptionMetricNotification(notif)) {
                    /*
                     * Ignore exception notifications because they can happen
                     * as a matter of course when RNs are starting up
                     */
                } else {
                    if (SECURITY_ENABLE) {
                        /* filter security scan operations */
                        if ("oracle.kv.multiop.throughputfloor".equals(
                            notif.getType())) {
                            return;
                        }
                    }
                    if ("oracle.kv.repnode.endpointgroupstats".equals(
                            notif.getType())) {
                        /*
                         * Filter out async endpoint group stats since they are
                         * only generated when there is activity, and this test
                         * may not generate that activity
                         */
                        return;
                    }
                    otherNotificationsReceived.add(notif);
                }
                JmxSystemTest.this.notify();
            }
        }
    }

    private boolean isComponentStatusNotification(Notification notif) {
        return notif.getType().contains("repnode.status") ||
               notif.getType().contains("arbnode.status") ||
               notif.getType().contains("storagenode.status") ||
               notif.getType().contains("admin.status") ||
               notif.getType().equals("oracle.kv.repnode.replicationstate");
    }

    private boolean isPlanStatusNotification(Notification notif) {
        return notif.getType().contains("plan.status");
    }

    private boolean isExceptionMetricNotification(Notification notif) {
        return notif.getType().equals("oracle.kv.repnode.exceptionmetric");
    }

    private synchronized void
        verifyNotificationsReceived(List<Notification> receivedNotifications,
                                    List<Notification> expectedNotifications,
                                    boolean executedPlan)
    {
        /* Wait up to 20 seconds for expected notifications */
        long limit = System.currentTimeMillis() + 20000;
        while (true) {
            final List<Notification> missing =
                unexpected(receivedNotifications, expectedNotifications);
            final long wait = limit - System.currentTimeMillis();
            if (missing.isEmpty()) {
                logger.info("Remaining time: " + wait + " ms");
                break;
            }

            if (wait <= 0) {
                final List<Notification> unexpected =
                    unexpected(expectedNotifications, receivedNotifications);
                fail(FormatUtils.formatDateTimeMillis(
                         System.currentTimeMillis()) +
                     ": Missing notifications:\n" + missing +
                     (!unexpected.isEmpty() ?
                      "\nUnexpected notifications:\n" + unexpected :
                      ""));
            }
            try {
                wait(wait);
            } catch (InterruptedException e) {
                limit = 0;
            }
        }

        /* Wait an additional 10 seconds for unexpected notifications */
        limit = System.currentTimeMillis() + 10000;
        while (true) {
            final List<Notification> unexpected =
                unexpected(expectedNotifications, receivedNotifications);
            final long wait = limit - System.currentTimeMillis();
            if (wait <= 0) {
                if (unexpected.isEmpty()) {
                    break;
                }
                fail("Unexpected notifications: " + unexpected);
            }
            try {
                wait(wait);
            } catch (InterruptedException e) {
                limit = 0;
            }
        }

        if (executedPlan) {
            assertTrue("miss plan status change event",
                       !planStatusNotificationsReceived.isEmpty());
        } else {
            assertTrue("found unexpected plan status change event: " +
                           planStatusNotificationsReceived.toString(),
                       planStatusNotificationsReceived.isEmpty());
        }
    }

    /**
     * Returns whether the two notifications have the same type and source.
     * Also checks that the user data for one is contained in the other, in one
     * direction or another. That allows an expected notification to specify a
     * subset of the expected data, and this method doesn't need to know which
     * of the two notifications is the expected notification versus received
     * notification being checked.
     */
    private boolean equivalentNotifications(Notification n1, Notification n2) {
        return n1.getType().equals(n2.getType()) &&
            n1.getSource().equals(n2.getSource()) &&
            (n1.getUserData().toString().contains(
                n2.getUserData().toString()) ||
             n2.getUserData().toString().contains(
                 n1.getUserData().toString()));
    }

    /**
     * Return a list of notifications in checkList that are not present in
     * expectedList. Note that it is OK if a notification appears multiple
     * times in the check list with only a single match in the expected list.
     */
    private List<Notification> unexpected(List<Notification> expectedList,
                                          List<Notification> checkList) {
        final List<Notification> unexpected = new ArrayList<>();
        for (final Notification check : checkList) {
            boolean found = false;
            for (final Notification expected : expectedList) {
                if (equivalentNotifications(check, expected)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                unexpected.add(check);
            }
        }
        return unexpected;
    }

    private void doMetricPolling()
        throws Exception {

        /*
         * We want the admin to fetch perf events frequently, so it will be up
         * to date with the information we are getting directly from the SN.
         */
        setAdminPollingInterval();

        /*
         * Set the throughput limit to an unattainable values.
         *
         */
        setRepNodeThroughputLimit(10000000);

        /*
         * Choose one of the two RN's to check.
         */
        RepNodeId rnToInspect = null;
        int snIdx = 0;
        for (StorageNodeId snId : store.getStorageNodeIds()) {
            for (RepNodeId rnId : store.getRNs(snId)) {
                if (rnToInspect == null) {
                    rnToInspect = rnId;
                }
            }
            if (rnToInspect != null) {
                break;
            }
            snIdx++;
        }

        for (int i = 0; i < 5; i++) {
            /*
             * Set the stats interval on the repnode.
             */
            setRepNodeStatsInterval("15 SECONDS");

            /*
             * Do logging first to make sure it happens within the stats
             * interval
             */
            doSevereLogging();

            /*
             * We need to do some client operations to get metrics generated.
             * Do this for a period that is slightly greater than the stats
             * interval.
             */
            doClientOps(20L);

            /*
             * Now set the stats interval to be very long, so that another
             * interval does not pass while we are fetching and comparing
             * metrics.
             */
            setRepNodeStatsInterval("100 SECONDS");
            RepNodeMXBean rnmb =
                JMX.newMBeanProxy(mbsc[snIdx], getRepNodeMBeanName(rnToInspect),
                                  RepNodeMXBean.class, true);

            /* Now fetch the same metrics from the Admin and compare them. */
            CommandServiceAPI cs = store.getAdmin();
            Map<ResourceId, PerfEvent> perfMap = cs.getPerfMap();
            PerfEvent rnperf = perfMap.get(rnToInspect);
            if (rnperf != null) {
                if (compareMetrics(rnmb, rnperf)) {
                    return;
                }
                System.err.println("retrying, to fetch same interval");
            } else {
                System.err.println("null rnperf");
            }

        }

        fail("Unable to get the same interval through MBean and Admin " +
             "-- due to timing problem");
    }

    /** Log a SEVERE entry to each service. */
    private void doSevereLogging() throws Exception {
        final Topology topo = store.getAdminMaster().getTopology();
        final StorageNodeId[] snIds = store.getStorageNodeIds();
        final RegistryUtils registryUtils =
            new RegistryUtils(topo, store.getSNALoginManager(snIds[0]),
                              logger);

        registryUtils.getAdminTest(snIds[0]).logMessage(SEVERE, "Test", false);

        for (final RepNodeId rnId : topo.getRepNodeIds()) {
            registryUtils.getRepNodeTest(rnId)
                .logMessage(SEVERE, "Test", false);
        }

        for (final ArbNodeId anId : topo.getArbNodeIds()) {
            registryUtils.getArbNodeTest(anId)
                .logMessage(SEVERE, "Test", false);
        }

        for (final StorageNodeId snId : snIds) {
            registryUtils.getStorageNodeAgentTest(snId)
                .logMessage(SEVERE, "Test", false);
        }
    }

    private void checkReplicationState()
        throws Exception {
        int masterCount = 0;
        int snIdx = 0;
        for (StorageNodeId snId : store.getStorageNodeIds()) {
            for (RepNodeId rnId : store.getRNs(snId)) {
                RepNodeMXBean rnmb =
                    JMX.newMBeanProxy(mbsc[snIdx], getRepNodeMBeanName(rnId),
                                      RepNodeMXBean.class, true);
                if (rnmb.getReplicationState().equals("MASTER")) {
                    masterCount++;
                }
            }
            snIdx++;
        }
        assertEquals(masterCount, 1);
    }

    /* Suppress warnings for calls to deprecated JMX accessor methods */
    @SuppressWarnings("deprecation")
    private boolean compareMetrics(RepNodeMXBean rnmb, PerfEvent rnperf) {

        /* Interval, single */
        if (rnmb.getIntervalEnd().getTime() != rnperf.getSingleInt().getEnd()) {
            /*
             * If the timestamps differ, then we are not looking at the same
             * interval.
             */
            return false;
        }

        assertEquals
            (rnmb.getIntervalLatAvg(),
             rnperf.getSingleInt().getLatency().getAverage(), DELTA);

        assertEquals
            (rnmb.getIntervalLatMax(),
             rnperf.getSingleInt().getLatency().getMax());

        assertEquals
            (rnmb.getIntervalLatMin(),
             rnperf.getSingleInt().getLatency().getMin());

        assertEquals
            (rnmb.getIntervalPct95(),
             rnperf.getSingleInt().getLatency().getPercent95());

        assertEquals
            (rnmb.getIntervalPct99(),
             (rnperf.getSingleInt().getLatency().getPercent99()));

        assertEquals
            (rnmb.getIntervalStart().getTime(),
             rnperf.getSingleInt().getStart());

        assertEquals
            (rnmb.getIntervalThroughput(),
             (rnperf.getSingleInt().getThroughputPerSec()));

        assertEquals
            (rnmb.getIntervalTotalOps(),
             (rnperf.getSingleInt().getLatency().getOperationCount()));

        assertEquals
            (rnmb.getIntervalTotalOpsLong(),
             (rnperf.getSingleInt().getLatency().getOperationCount()));

        /* Cumulative, single */
        assertEquals
            (rnmb.getCumulativeEnd().getTime(),
             rnperf.getSingleCum().getEnd());

        assertEquals
            (rnmb.getCumulativeLatAvg(),
             rnperf.getSingleCum().getLatency().getAverage(), DELTA);

        assertEquals
            (rnmb.getCumulativeLatMax(),
             (rnperf.getSingleCum().getLatency().getMax()));

        assertEquals
            (rnmb.getCumulativeLatMin(),
             (rnperf.getSingleCum().getLatency().getMin()));

        assertEquals
            (rnmb.getCumulativePct95(),
             (rnperf.getSingleCum().getLatency().getPercent95()));

        assertEquals
            (rnmb.getCumulativePct99(),
             (rnperf.getSingleCum().getLatency().getPercent99()));

        assertEquals
            (rnmb.getCumulativeStart().getTime(),
             rnperf.getSingleCum().getStart());

        assertEquals
            (rnmb.getCumulativeThroughput(),
             (rnperf.getSingleCum().getThroughputPerSec()));

        assertEquals
            (rnmb.getCumulativeTotalOps(),
             (rnperf.getSingleCum().getLatency().getOperationCount()));

        assertEquals
            (rnmb.getCumulativeTotalOpsLong(),
             (rnperf.getSingleCum().getLatency().getOperationCount()));

        /* Interval, multi */
        assertEquals
            (rnmb.getMultiIntervalEnd().getTime(),
             rnperf.getMultiInt().getEnd());

        assertEquals
            (rnmb.getMultiIntervalLatAvg(),
             rnperf.getMultiInt().getLatency().getAverage(), DELTA);

        assertEquals
            (rnmb.getMultiIntervalLatMax(),
             (rnperf.getMultiInt().getLatency().getMax()));

        assertEquals
            (rnmb.getMultiIntervalLatMin(),
             (rnperf.getMultiInt().getLatency().getMin()));

        assertEquals
            (rnmb.getMultiIntervalPct95(),
             (rnperf.getMultiInt().getLatency().getPercent95()));

        assertEquals
            (rnmb.getMultiIntervalPct99(),
             (rnperf.getMultiInt().getLatency().getPercent99()));

        assertEquals
            (rnmb.getMultiIntervalStart().getTime(),
             rnperf.getMultiInt().getStart());

        assertEquals
            (rnmb.getMultiIntervalThroughput(),
             (rnperf.getMultiInt().getThroughputPerSec()));

        assertEquals
            (rnmb.getMultiIntervalTotalOps(),
             (rnperf.getMultiInt().getLatency().getOperationCount()));

        assertEquals
            (rnmb.getMultiIntervalTotalOpsLong(),
             (rnperf.getMultiInt().getLatency().getOperationCount()));

        assertEquals
            (rnmb.getMultiIntervalTotalRequests(),
             (rnperf.getMultiInt().getLatency().getRequestCount()));

        assertEquals
            (rnmb.getMultiIntervalTotalRequestsLong(),
             (rnperf.getMultiInt().getLatency().getRequestCount()));

        /* Cumulative, multi */
        assertEquals
            (rnmb.getMultiCumulativeEnd().getTime(),
             rnperf.getMultiCum().getEnd());

        assertEquals
            (rnmb.getMultiCumulativeLatAvg(),
             rnperf.getMultiCum().getLatency().getAverage(), DELTA);

        assertEquals
            (rnmb.getMultiCumulativeLatMax(),
             (rnperf.getMultiCum().getLatency().getMax()));

        assertEquals
            (rnmb.getMultiCumulativeLatMin(),
             (rnperf.getMultiCum().getLatency().getMin()));

        assertEquals
            (rnmb.getMultiCumulativePct95(),
             (rnperf.getMultiCum().getLatency().getPercent95()));

        assertEquals
            (rnmb.getMultiCumulativePct99(),
             (rnperf.getMultiCum().getLatency().getPercent99()));

        assertEquals
            (rnmb.getMultiCumulativeStart().getTime(),
             rnperf.getMultiCum().getStart());

        assertEquals
            (rnmb.getMultiCumulativeThroughput(),
             (rnperf.getMultiCum().getThroughputPerSec()));

        assertEquals
            (rnmb.getMultiCumulativeTotalOps(),
             (rnperf.getMultiCum().getLatency().getOperationCount()));

        assertEquals
            (rnmb.getMultiCumulativeTotalOpsLong(),
             (rnperf.getMultiCum().getLatency().getOperationCount()));

        assertEquals
            (rnmb.getMultiCumulativeTotalRequests(),
             (rnperf.getMultiCum().getLatency().getRequestCount()));

        assertEquals
            (rnmb.getMultiCumulativeTotalRequestsLong(),
             (rnperf.getMultiCum().getLatency().getRequestCount()));

        return true;
    }

    @SuppressWarnings("deprecation")
    private void doParameterPolling() throws Exception {

        int snIdx = 0;

        /*
         * 100 seconds is the value of statsInterval left over from
         * doMetricPolling.
         */
        int expectedInterval = 100;

        for (StorageNodeId snId : store.getStorageNodeIds()) {
            StorageNodeMXBean snmb =
                JMX.newMBeanProxy(mbsc[snIdx], storageNodeMBeanName,
                                  StorageNodeMXBean.class, true);

            assertTrue("isHostingAdmin" ,
                       snId.equals(new StorageNodeId(1)) ?
                       snmb.isHostingAdmin() :
                       !snmb.isHostingAdmin());

            assertEquals("getRootDirPath",
                         store.getRootDir(), snmb.getRootDirPath());

            assertEquals("getStoreName",
                         store.getStoreName(), snmb.getStoreName());

            assertEquals("getHostname",
                         store.getHostname(), snmb.getHostname());

            assertEquals("getRegistryPort",
                         store.getRegistryPort(snId), snmb.getRegistryPort());

            assertEquals("getHAHostname",
                         store.getHostname(), snmb.getHAHostname());

            assertEquals("getCapacity",
                         1, snmb.getCapacity());

            assertTrue("getLogFileLimit()",
                       0 < snmb.getLogFileLimit());

            assertTrue("getLogFileCount",
                       0 < snmb.getLogFileCount());

            assertTrue("getHaPortRange",
                       snmb.getHaPortRange().contains(","));

            assertEquals("getSnId",
                         snId, new StorageNodeId(snmb.getSnId()));

            assertTrue("getMemoryMB",
                       0 < snmb.getMemoryMB());

            assertTrue("getNumCPUs",
                       0 < snmb.getNumCPUs());

            assertEquals("getMountPoints",
                         "", snmb.getMountPoints());

            assertEquals("getRNlogMountPoints",
                    "", snmb.getRNLogMountPoints());

            assertEquals("getAdminMountPoints",
                    "", snmb.getAdminMountPoints());

            AdminMXBean amb =
                JMX.newMBeanProxy(mbsc[snIdx], adminMBeanName,
                                  AdminMXBean.class, true);

            assertEquals("status", "RUNNING", amb.getServiceStatus());

            assertTrue("logFileLimit", 0 < amb.getLogFileLimit());

            assertTrue("logFileCount", 0 < amb.getLogFileCount());

            assertTrue("pollPeriod", 0 < amb.getPollPeriodMillis());

            assertTrue("eventExpiryAge", 0 < amb.getEventExpiryAge());

            assertTrue("isMaster" ,
                       snId.equals(new StorageNodeId(1)) ?
                       amb.isMaster() :
                       !amb.isMaster());

            String adminStats = amb.getEnvMetric();
            assertTrue("getEnvMetric", adminStats.contains("admin"));

            if (store.numANs(snId) > 0) {
                ArbNodeMXBean anmb =
                    JMX.newMBeanProxy(mbsc[snIdx], arbNodeMBeanName,
                                      ArbNodeMXBean.class, true);

                /*
                 * Setting the stats interval to 10 seconds. Internally,
                 * the value is rounded up to the nearest minute.
                 */
                setArbNodeStatsInterval("10 SECONDS");
                expectedInterval = 10;

                assertEquals("parameter test", expectedInterval,
                             anmb.getStatsInterval());

                assertEquals("an getConfigProperties",
                             CreateStore.JE_DEFAULT_PROPERTIES,
                             anmb.getConfigProperties());
                assertTrue("an getJavaMiscParams",
                           anmb.getJavaMiscParams().contains("-Xmx"));

                assertEquals("an getLoggingConfigProps",
                             ParameterState.JVM_LOGGING_DEFAULT,
                             anmb.getLoggingConfigProps());

                assertEquals("an getCollectEnvStats", true,
                             anmb.getCollectEnvStats());

                assertTrue("an getHeapMB",
                           0 < anmb.getHeapMB());

                /*
                 * The following stats are set by the stats polling thread.
                 * The poller is set for one minute so you may need to wait.
                 */
                if (anmb.getMaster() == null) {
                    int tries = 0;
                    while (anmb.getMaster() == null && tries < 7) {
                        Thread.sleep(10 * 1000);
                        tries++;
                    }
                }

                assertTrue("an getMaster",
                        null != anmb.getMaster());

                assertTrue("an getAcks",
                           -1 <= anmb.getAcks());

                assertTrue("an getReplayQueueOverflow",
                           -1 <= anmb.getReplayQueueOverflow());

                String repState =
                    ReplicatedEnvironment.State.REPLICA.toString();
                assertTrue("an getState",
                           repState.equals(anmb.getState()));

                assertTrue("an getVLSN",
                           -1 <= anmb.getVLSN());
            }

            for (RepNodeId rnId : store.getRNs(snId)) {
                RepNodeMXBean rnmb =
                    JMX.newMBeanProxy(mbsc[snIdx], getRepNodeMBeanName(rnId),
                                      RepNodeMXBean.class, true);

                assertEquals("parameter test", expectedInterval,
                             rnmb.getStatsInterval());

                setRepNodeStatsInterval("10 SECONDS");
                expectedInterval = 10;

                assertEquals("parameter test", expectedInterval,
                             rnmb.getStatsInterval());

                /*
                 * For the rest of the parameters, just expect a default
                 * value or range.
                 */
                assertEquals("getConfigProperties",
                             CreateStore.JE_DEFAULT_PROPERTIES,
                             rnmb.getConfigProperties());
                assertTrue("getJavaMiscParams",
                           rnmb.getJavaMiscParams().contains("-Xmx"));
                assertEquals("getLoggingConfigProps",
                             ParameterState.JVM_LOGGING_DEFAULT,
                             rnmb.getLoggingConfigProps());
                assertEquals("getCollectEnvStats", true,
                             rnmb.getCollectEnvStats());
                assertTrue("getCacheSize",
                           0 <= rnmb.getCacheSize());
                assertTrue("getMaxTrackedLatency",
                           -1 == rnmb.getMaxTrackedLatency());
                assertTrue("getHeapMB",
                           0 < rnmb.getHeapMB());
                assertEquals("getMountPoint", null,
                             rnmb.getMountPoint());
                assertEquals("getRNlogMountPoint", null,
                             rnmb.getLogMountPoint());
                assertEquals("getLatencyCeiling", 0,
                             rnmb.getLatencyCeiling());
                assertTrue("getThroughputFloor",
                           0 < rnmb.getThroughputFloor());
                assertEquals("getCommitLagThreshold", 0,
                             rnmb.getCommitLagThreshold());
                assertEquals("getCommitLag", 0, rnmb.getCommitLag());
            }

            snIdx++;
        }
    }
}
