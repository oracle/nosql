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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.mgmt.AgentInternal;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.sna.ServiceManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.StateChangeEvent;

public class JmxAgent extends AgentInternal {

    final static String DOMAIN = "Oracle NoSQL Database";
    /* JMX service name */
    private static final String JMX_SERVICE_NAME = "jmxrmi";

    private final MBeanServer server;
    private final JMXConnectorServer connector;
    private final StorageNode snMBean;
    private final Map<RepNodeId, RepNode> rnMap = new HashMap<>();
    private final Map<ArbNodeId, ArbNode> anMap = new HashMap<>();
    private Admin admin;

    /**
     * The constructor is found by reflection and must match this signature.
     * However, the port and hostname arguments are not used by this
     * implementation.
     */
    public JmxAgent(StorageNodeAgent sna,
                    @SuppressWarnings("unused") int pollingPort,
                    @SuppressWarnings("unused") String trapHostName,
                    @SuppressWarnings("unused") int trapPort,
                    ServiceStatusTracker tracker) {

        super(sna);

        server = MBeanServerFactory.createMBeanServer();

        int freePort = restrictPortRange() ? getFreePort() : 0;
        JMXServiceURL url = JmxConnectorUtil.makeUrl(
            getHostname(), getRegistryPort(), freePort, JMX_SERVICE_NAME);
        SecurityParams sp = sna.getSecurityParams();
        connector = JmxConnectorUtil.createAndStartConnector(url, server, sp);

        addPlatformMBeans();

        snMBean = new StorageNode(this, server);

        setSnaStatusTracker(tracker);
        setSnaLoggingStatsTracker(sna.getSnaLoggingStatsTracker());

        /* Log JMX service name and port */
        final Logger logger = sna.getLogger();
        if (logger != null && logger.isLoggable(Level.INFO)) {
            if (url != null && url.getPort() != 0) {
                logger.info(JMX_SERVICE_NAME + " service port: " +
                            url.getPort());
            } else {
                /*
                 * If servicerange is not specified, JMX service uses an
                 * anonymous port, which is not contained in the url, we have
                 * to parse the port number from the string that represents the
                 * remote object that bound to JMX service name.
                 */
                try {
                    RegistryUtils.logServiceNameAndPort(
                        JMX_SERVICE_NAME, getHostname(), getRegistryPort(),
                        logger);
                } catch (Exception ignore) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }
    }

    @Override
    public boolean checkParametersEqual(int pollp, String traph, int trapp) {
        /* JMX doesn't use these parameters, so always return true. */
        return true;
    }

    /**
     * Return the connector. So that other components can reuse the same connector.
     */
    public JMXConnectorServer getConnector() {
        return connector;
    }

    /**
     * Add the platform MBeans as proxies.  See [#22267].
     */
    private void addPlatformMBeans() {
        MBeanServer platformServer = ManagementFactory.getPlatformMBeanServer();

        Set<ObjectInstance> beans =
            platformServer.queryMBeans(null, null);

        final java.util.logging.Logger snaLogger = sna.getLogger();
        for (ObjectInstance oi : beans) {

            try {
                Class<?> c =
                    getMBeanInterfaceClass(Class.forName(oi.getClassName()));
                /*
                 * If no complying MBean interface was found, skip this one.
                 * This is unexpected, but we should be able to carry on
                 * without it.
                 */
                if (c == null) {
                    snaLogger.warning
                        ("Unexpected non-compliant platform MBean impl " +
                         oi.getClassName() +
                         " found.  Forgoing proxy creation.");
                    continue;
                }

                /*
                 * If it is the MBeanServerDelegate, just skip it; we already
                 * have one of those. And if we try to register it here, a
                 * javax.naming.NameAlreadyBoundException will be thrown
                 * from the RMI registry.
                 *
                 * If it is a DiagnosticCommandMBean, attempting to create
                 * its proxy will result in a NotCompliantMBeanException
                 * because it is not an open type. So skip it as well.
                 */
                final String mbeanInterfaceName = c.getName();
                if (mbeanInterfaceName.equals
                        ("javax.management.MBeanServerDelegateMBean") ||
                    mbeanInterfaceName.equals
                        ("com.sun.management.DiagnosticCommandMBean")) {
                    continue;
                }

                Object o = ManagementFactory.newPlatformMXBeanProxy
                    (platformServer, oi.getObjectName().toString(), c);

                server.registerMBean(o, oi.getObjectName());

            } catch (Exception e) {
                /*
                 * If the current MBean is not compliant, then skip it but
                 * log an informative message indicating what happened.
                 * Although unexpected, we should still be able to carry on.
                 *
                 * If any other problem with the current MBean is encountered,
                 * then log the exception and skip the problem MBean. For
                 * most purposes this will do.
                 */
                if (e.getCause() instanceof NotCompliantMBeanException) {
                    snaLogger.warning
                        ("Non-compliant platform MBean encountered [" +
                         oi.getClassName() + "]: skip registration.");
                } else {
                    snaLogger.log
                        (Level.WARNING, e + ": Unexpected error creating " +
                         "platform MBean proxy for " + oi.getClassName());
                }
            }
        }
    }

    /**
     * Find an MBean interface in this class's ancestry.
     */
    private static Class<?> getMBeanInterfaceClass(Class<?> c) {
        while (c != null) {
            String name = c.getName();
            if (name.endsWith("MBean") || name.endsWith("MXBean")) {
                return c;
            }
            Class<?>[] interfaces = c.getInterfaces();
            for (Class<?> i : interfaces) {
                Class<?> j = getMBeanInterfaceClass(i);
                if (j != null) {
                    return j;
                }
            }
            c = c.getSuperclass();
        }
        return null;
    }

    @Override
    public void addRepNode(RepNodeParams rnp, ServiceManager mgr)
        throws Exception {

        final RepNodeId rnId = rnp.getRepNodeId();
        RepNode rn = new RepNode(rnp, server, snMBean);

        rnMap.put(rnId, rn);
    }

    @Override
    public void removeRepNode(RepNodeId rnid) {
        RepNode rn = rnMap.get(rnid);
        if (rn != null) {
            rn.unregister();
        }
        rnMap.remove(rnid);
    }

    @Override
    public void addAdmin(AdminParams ap, ServiceManager mgr)
        throws Exception {

        admin = new Admin(ap, server, snMBean);
    }

    @Override
    public void removeAdmin() {
        if (admin != null) {
            admin.unregister();
        }
        admin = null;
    }

    @Override
    public void shutdown() {

        super.shutdown();

        snMBean.unregister();

        for (RepNode rn : rnMap.values()) {
            rn.unregister();
        }
        rnMap.clear();

        if (admin != null) {
            admin.unregister();
            admin = null;
        }

        try {
            connector.stop();
        } catch (IOException e) {

            /*
             * This exception occurs when shutting down the StorageNodeAgent,
             * and here's why: Connector.stop() attempts to unregister
             * the connector from the RMI registry, but the registry has
             * already been cleaned up and unexported by the time we reach
             * here.
             *
             * The beneficial effect of calling stop() is to terminate the
             * connector's listener thread, which is all we are interested in
             * at this point.  The thread termination occurs before the
             * exception is thrown, so all is hunky dory.
             */
        }

        MBeanServerFactory.releaseMBeanServer(server);
    }

    @Override
    public void updateSNStatus(ServiceStatusChange p, ServiceStatusChange n) {
        snMBean.setServiceStatus(n.getStatus());
    }

    @Override
    public void updateStorageNodeStats(StatsPacket packet) {
        snMBean.setStorageNodeStats(packet);
    }

    @Override
    public void updateNodeStatus(ResourceId serviceId,
                                 ServiceStatusChange newStatus) {
        if (serviceId instanceof RepNodeId) {
            final RepNodeId rnId = (RepNodeId) serviceId;
            final RepNode rn = rnMap.get(rnId);
            if (rn == null) {
                sna.getLogger().warning
                    ("Updating service status, RepNode MBean not found for " +
                     rnId.getFullName());
            } else {
                sna.getLogger().info
                    ("Updating service status, node: " + rnId.getFullName() +
                     ", service status: " + newStatus.getStatus());
                rn.setServiceStatus(newStatus.getStatus());
            }
        } else if (serviceId instanceof ArbNodeId) {
            final ArbNodeId anId = (ArbNodeId) serviceId;
            final ArbNode an = anMap.get(anId);
            if (an == null) {
                sna.getLogger().warning
                    ("Updating service status, ArbNode MBean not found for " +
                     anId.getFullName());
            } else {
                an.setServiceStatus(newStatus.getStatus());
            }
        } else {
            sna.getLogger().warning("Updating service status for wrong" +
                                    " service ID type: " + serviceId);
        }
    }

    @Override
    public void updateReplicationState(RepNodeId rnId,
                                       StateChangeEvent changeEvent) {
        final RepNode rn = rnMap.get(rnId);
        if (rn == null) {
            sna.getLogger().warning
                ("Updating replication state, RepNode MBean not found for " +
                 rnId.getFullName());
        } else {
            sna.getLogger().info
                ("Updating replication state, node: " + rnId.getFullName() +
                 ", replication state: " + changeEvent.getState());
            rn.updateReplicationState(changeEvent);
        }
    }

    @Override
    public void receiveStats(ResourceId serviceId, StatsPacket packet) {
        if (serviceId instanceof RepNodeId) {
            final RepNodeId rnId = (RepNodeId) serviceId;
            final RepNode rn = rnMap.get(rnId);
            if (rn == null) {
                sna.getLogger().warning
                    ("Updating stats, RepNode MBean not found for " +
                     rnId.getFullName());
            } else {
                rn.setStats(packet);
            }
        } else if (serviceId instanceof ArbNodeId) {
            final ArbNodeId anId = (ArbNodeId) serviceId;
            final ArbNode an = anMap.get(anId);
            if (an == null) {
                sna.getLogger().warning
                    ("Updating stats, ArbNode MBean not found for " +
                     anId.getFullName());
            } else {
                an.setStats(packet);
            }
        } else {
            sna.getLogger().warning("Updating stats for wrong service ID" +
                                    " type: " + serviceId);
        }
    }

    @Override
    public void receiveAdminStats(StatsPacket packet) {
        if (admin == null) {
            sna.getLogger().warning
                ("Updating Admin stats, Admin MBean not found.");
        } else {
            admin.setStats(packet);
        }
    }

    @Override
    public void receiveNewParams(ResourceId serviceId, ParameterMap map) {
        if (serviceId instanceof RepNodeId) {
            final RepNodeId rnId = (RepNodeId) serviceId;
            final RepNode rn = rnMap.get(rnId);
            if (rn == null) {
                sna.getLogger().warning
                    ("Updating parameters, RepNode MBean not found for " +
                     rnId.getFullName());
            } else {
                rn.setParameters(new RepNodeParams(map));
            }
        } else if (serviceId instanceof ArbNodeId) {
            final ArbNodeId anId = (ArbNodeId) serviceId;
            final ArbNode an = anMap.get(anId);
            if (an == null) {
                sna.getLogger().warning
                    ("Updating parameters, ArbNode MBean not found for " +
                     anId.getFullName());
            } else {
                an.setParameters(new ArbNodeParams(map));
            }
        } else if (serviceId instanceof AdminId) {
            if (admin == null) {
                sna.getLogger().warning("Updating parameters, Admin MBean" +
                                        " not found");
            } else {
                admin.setParameters(new AdminParams(map));
            }
        } else {
            sna.getLogger().warning("Updating parameters for wrong service" +
                                    " ID type: " + serviceId);
        }
    }

    @Override
    public void updateAdminStatus(ServiceStatusChange newStatus,
                                  boolean isMaster) {
        if (admin == null) {
            sna.getLogger().warning("Updating admin status, Admin MBean" +
                                    " not found");
        } else {
            admin.setServiceStatus(newStatus.getStatus(), isMaster);
        }
    }

    @Override
    public void updatePlanStatus(String planStatus) {
        if (admin == null) {
            sna.getLogger().warning("Updating plan status, Admin MBean" +
                                    " not found");
        } else {
            admin.updatePlanStatus(planStatus);
        }
    }

    @Override
    public void addArbNode(ArbNodeParams anp, ServiceManager mgr)
        throws Exception {

        final ArbNodeId anId = anp.getArbNodeId();
        ArbNode an = new ArbNode(anp, server, snMBean);
        anMap.put(anId, an);
    }

    @Override
    public void removeArbNode(ArbNodeId anid) {
        ArbNode an = anMap.get(anid);
        if (an != null) {
            an.unregister();
        }
        anMap.remove(anid);
    }
}
