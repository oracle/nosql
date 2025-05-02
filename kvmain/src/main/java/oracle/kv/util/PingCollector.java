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
package oracle.kv.util;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.Ping.Problem;

/**
 * PingCollector visits each service in the store and requests ping status. The
 * collector is created with a topology instance to direct its collection
 * efforts. It is meant for one time use; detected problems are accumulated and
 * are not cleared between calls to
 *
 * - Since admin services are not in the topology, a Parameters instance
 * is used to find admins. The Parameters instance may be null, in which case
 * the collector omits admins.
 *
 * - The collector will use a login manager if one is supplied to contact
 * admin services.
 *
 * The collector is meant to be robust and to continue on past any component
 * failure. If one component of the store can't be reached, the collector
 * saves information about the problem and carries on.
 */
public class PingCollector {

    private static final int MAX_N_THREADS = 10;

    /*
     * The action to take when the collector visits a storage node
     */
    private interface StorageNodeCallback {
        void nodeCallback(StorageNode sn, StorageNodeStatus status);
    }

    /*
     * The action to take when the collector visits a replication node
     */
    private interface RepNodeCallback {
        void nodeCallback(RepNode rn, RepNodeStatus status);
    }

    /*
     * The action to take when the collector visits an Admin service.
     */
    private interface AdminCallback {
        void nodeCallback(AdminId aId, AdminInfo info);
    }

    /*
     * The action to take when the collector visits an arbiter node
     */
    private interface ArbNodeCallback {
        void nodeCallback(ArbNode an, ArbNodeStatus status);
    }

    /* A struct for packaging information returned from an Admin service. */
    class AdminInfo {
        final StorageNodeId snId;
        AdminStatus adminStatus;

        AdminInfo(StorageNodeId snId) {
            this.snId = snId;
        }
    }

    /* Collections of status information */
    private Map<StorageNode, StorageNodeStatus> snMap;
    private Map<RepNode, RepNodeStatus> rnMap;
    private Map<ArbNode, ArbNodeStatus> anMap;

    /*
     * AdminMap and monitoredChanges are collected at the same time. AdminMap *
     * holds admin statuses obtained by pinging each Admin service, while *
     * monitoredChanges holds the collection of service changes held by the
     * admin monitoring system.
     */
    private Map<AdminId, AdminInfo> adminMap;
    private Map<ResourceId, ServiceChange> monitoredChanges;

    /* Map of problems encountered when pinging components of the store. */
    private final List<Problem> problems =
        Collections.synchronizedList(new ArrayList<Problem>());

    private final Topology topo;
    private final Logger logger;
    private final Parameters params;
    private final LoginManager adminLoginManager;

    PingCollector(Topology topo,
                  Logger logger,
                  Parameters params,
                  LoginManager adminLoginManager) {

        this.topo = topo;
        this.params = params;
        this.adminLoginManager = adminLoginManager;
        this.logger = logger;
    }

    public PingCollector(Topology topo, Logger logger) {
        this(topo, logger, null, null);
    }

    Map<StorageNode, StorageNodeStatus> getSNMap() {
        if (snMap == null) {
            /*
             * Note that a synchronized HashMap is used rather than
             * ConcurrentHashMap because we need to be able to support null
             * status values, and ConcurrentHashMap does not support null
             * values.
             */
            snMap = Collections.synchronizedMap(
                new HashMap<StorageNode, StorageNodeStatus>());
            forEachStorageNode(new StorageNodeCallback() {
                    @Override
                    public void nodeCallback(StorageNode sn,
                                             StorageNodeStatus status) {
                        snMap.put(sn, status);
                    }
                });
        }
        return snMap;
    }

    Map<RepNode, RepNodeStatus> getRNMap() {
        if (rnMap == null) {
            /*
             * Note that a synchronized TreeMap is used rather than
             * ConcurrentHashMap because we need to be able to support null
             * status values, and ConcurrentHashMap does not support null
             * values.
             */
            rnMap = Collections.synchronizedMap(
                new TreeMap<RepNode, RepNodeStatus>());
            forEachRepNode(new RepNodeCallback() {
                    @Override
                    public void nodeCallback(RepNode rn, RepNodeStatus status) {
                        rnMap.put(rn, status);
                    }
                });
        }
        return rnMap;
    }

    Map<AdminId, AdminInfo> getAdminMap() {
        if (adminMap == null) {
            /*
             * Note that a synchronized HashMap is used rather than
             * ConcurrentHashMap because we need to be able to support null
             * status values, and ConcurrentHashMap does not support null
             * values.
             */
            adminMap = Collections.synchronizedMap(
                new HashMap<AdminId, AdminInfo>());
            monitoredChanges = Collections.synchronizedMap(
                new HashMap<ResourceId, ServiceChange>());
            forEachAdmin(new AdminCallback() {
                    @Override
                    public void nodeCallback(AdminId aId,
                                             AdminInfo info) {
                        adminMap.put(aId, info);
                    }
                });
        }
        return adminMap;
    }

    Map<ArbNode, ArbNodeStatus> getANMap() {
        if (anMap == null) {
            anMap = new TreeMap<ArbNode, ArbNodeStatus>();
            forEachArbNode(new ArbNodeCallback() {
                    @Override
                    public void nodeCallback(ArbNode an, ArbNodeStatus status) {
                        anMap.put(an, status);
                    }
                });
        }
        return anMap;
    }

    Map<ResourceId, ServiceChange> getMonitoredChanges() {
        if (monitoredChanges == null) {
            getAdminMap();
        }
        return monitoredChanges;
    }

    List<Problem> getProblems() {
        return problems;
    }

    /**
     * Collect all statuses, and then return a service status map of all the
     * SNs, RNs, and optionally admins, that make up the topology.
     * @return a map of all node resource ids to their statuses.
     */
    public Map<ResourceId, ServiceStatus> getTopologyStatus() {

        final Map<ResourceId, ServiceStatus> ret =
            new HashMap<ResourceId, ServiceStatus>();

        for (Map.Entry<StorageNode, StorageNodeStatus> e :
                 getSNMap().entrySet()) {
            ServiceStatus status =
                e.getValue() == null ? ServiceStatus.UNREACHABLE:
                e.getValue().getServiceStatus();

            ret.put(e.getKey().getResourceId(), status);
        }

        for (Map.Entry<RepNode, RepNodeStatus> e :
                 getRNMap().entrySet()) {
            ServiceStatus status =
                e.getValue() == null ? ServiceStatus.UNREACHABLE:
                e.getValue().getServiceStatus();

            ret.put(e.getKey().getResourceId(), status);
        }

        for (Map.Entry<AdminId, AdminInfo> e :
                 getAdminMap().entrySet()) {
            ServiceStatus status = null;
            if (e.getValue() == null) {
                status = ServiceStatus.UNREACHABLE;
            } else {
                AdminStatus aStatus = e.getValue().adminStatus;
                status = (aStatus == null) ? ServiceStatus.UNREACHABLE:
                    aStatus.getServiceStatus();
            }
            ret.put(e.getKey(), status);
        }

        return ret;
    }

    /**
     * Collects status for a given replication group and returns the
     * replication node which is the master for the replication group. If the
     * master is not found or there are more than one node that thinks it's
     * master, @code null is returned.
     *
     * @return a replication node or @code null
     */
    public RepNode getMaster(RepGroupId rgId) {

        final List<RepNode> master =
            Collections.synchronizedList(new ArrayList<RepNode>());

        forEachRepNodeInShard
            (rgId,
             new RepNodeCallback() {
                 @Override
                 public void nodeCallback(RepNode rn, RepNodeStatus status) {
                     if ((status != null) &&
                         status.getReplicationState().isMaster()) {
                         master.add(rn);
                     }
                 }
             });
        return (master.size() == 1) ? master.get(0) : null;
    }

    /**
     * Find the master of this shard and return its full name and its
     * haport. Returns null if no master found.
     */
    public RNNameHAPort getMasterNamePort(RepGroupId rgId) {

        final List<RNNameHAPort> namePort =
            Collections.synchronizedList(new ArrayList<RNNameHAPort>());

        forEachRepNodeInShard
            (rgId, new RepNodeCallback() {
                    @Override
                    public void nodeCallback(RepNode rn, RepNodeStatus status) {
                        if ((status != null) &&
                            status.getReplicationState().isMaster()) {
                            String rnName = rn.getResourceId().getFullName();
                            namePort.add(new RNNameHAPort
                                         (rnName, status.getHAHostPort()));
                        }
                    }
                });
        return (namePort.size() == 1) ? namePort.get(0) : null;
    }

    /* A struct for returning information about a master of a shard */
    public class RNNameHAPort {
        private final String fullName;
        private final String haHostPort;

        RNNameHAPort(String fullName, String haHostPort) {
            this.fullName = fullName;
            this.haHostPort = haHostPort;
        }

        public String getFullName() {
            return fullName;
        }

        public String getHAHostPort() {
            return haHostPort;
        }
    }

    /**
     * Return a map of replication node status for each RN in the shard. Note
     * that the status value may be null if the RN is not responsive.
     */
    public Map<RepNodeId, RepNodeStatus> getRepNodeStatus(RepGroupId rgId) {

        /*
         * Get status for each node in the shard
         * Note that a synchronized HashMap is used rather than
         * ConcurrentHashMap because we need to be able to support null
         * status values, and ConcurrentHashMap does not support null
         * values.
         */
        final Map<RepNodeId, RepNodeStatus> statusMap =
            Collections.synchronizedMap(
                new HashMap<RepNodeId, RepNodeStatus>());

        forEachRepNodeInShard
            (rgId, new RepNodeCallback() {
                    @Override
                    public void nodeCallback(RepNode rn, RepNodeStatus status) {
                        statusMap.put(rn.getResourceId(), status);
                    }
                });
        return statusMap;
    }

    /**
     * Return a map of node status for each AN in the shard. Note
     * that the status value may be null if the AN is not responsive.
     */
    public Map<ArbNodeId, ArbNodeStatus> getArbNodeStatus(RepGroupId rgId) {

        /* Get status for each node in the shard */
        final Map<ArbNodeId, ArbNodeStatus> statusMap =
            new HashMap<ArbNodeId, ArbNodeStatus>();

        forEachArbNodeInShard
            (rgId, new ArbNodeCallback() {
                    @Override
                    public void nodeCallback(ArbNode an, ArbNodeStatus status) {
                        statusMap.put(an.getResourceId(), status);
                    }
                });
        return statusMap;
    }

    private void forEachStorageNode(final StorageNodeCallback callback) {

        ExecutorService executor = Executors.newFixedThreadPool(MAX_N_THREADS);
        Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        /* LoginManager not needed for ping */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, (LoginManager) null, logger);

        for (final StorageNode sn : topo.getStorageNodeMap().getAll()) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    StorageNodeStatus status = null;
                    try {
                        StorageNodeAgentAPI sna =
                            regUtils.getStorageNodeAgent(sn.getResourceId());
                        status = sna.ping();
                    } catch (RemoteException re) {
                        problems.add(new Problem(sn.getResourceId(),
                                                 sn.getHostname(),
                                                 sn.getRegistryPort(),
                                                 "Can't call ping for SN: ",
                                                 re));
                    } catch (NotBoundException e) {
                        problems.add(new Problem(sn.getResourceId(),
                                                 sn.getHostname(),
                                                 sn.getRegistryPort(),
                                                 "No RMI service for SN",
                                                 e));
                    } finally {
                        callback.nodeCallback(sn, status);
                    }
                    return null;
                }
            });
        }

        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            /* ignore */
        } finally {
            executor.shutdown();
        }
    }

    private void forEachRepNode(RepNodeCallback callback) {

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            forEachRepNodeInShard(rg.getResourceId(), callback);
        }
    }

    private void forEachRepNodeInShard(RepGroupId rgId,
                                       final RepNodeCallback callback) {

        final RepGroup group = topo.get(rgId);

        if (group == null) {
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(MAX_N_THREADS);
        Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        /* LoginManager not needed for ping */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, (LoginManager) null, logger);

        for (final RepNode rn : group.getRepNodes()) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    RepNodeStatus status = null;
                    StorageNode sn = topo.get(rn.getStorageNodeId());
                    try {
                        RepNodeAdminAPI rna =
                            regUtils.getRepNodeAdmin(rn.getResourceId());
                        status = rna.ping();
                    } catch (RemoteException re) {
                        problems.add(new Problem(rn.getResourceId(),
                                                 sn.getHostname(),
                                                 sn.getRegistryPort(),
                                                 "Can't call ping for RN:" +
                                                 re));
                    } catch (NotBoundException e) {
                        problems.add(new Problem(rn.getResourceId(),
                                                 sn.getHostname(),
                                                 sn.getRegistryPort(),
                                                 "No RMI service for RN: " +
                                                 e));
                    } finally {
                        callback.nodeCallback(rn, status);
                    }
                    return null;
                }
            });
        }

        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            /* ignore */
        } finally {
            executor.shutdown();
        }
    }


    private void forEachAdmin(final AdminCallback callback) {
        if (params == null) {
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(MAX_N_THREADS);
        Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        for (final AdminId aId : params.getAdminIds()) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    final StorageNodeId snId =
                        params.get(aId).getStorageNodeId();
                    final StorageNodeParams snp = params.get(snId);
                    String hostname = snp.getHostname();
                    int port = snp.getRegistryPort();
                    final AdminInfo info = new AdminInfo(snId);
                    try {
                        final CommandServiceAPI admin =
                            RegistryUtils.getAdmin(hostname, port,
                                                   adminLoginManager, logger);
                        info.adminStatus = admin.getAdminStatus();

                        /*
                         * Ask the Admin master for the latest service status
                         * map, which is maintained as part of the Admin's
                         * monitoring infrastructure.
                         */
                        if (info.adminStatus.getIsAuthoritativeMaster()) {
                            Map<ResourceId, ServiceChange> monitorMap =
                                admin.getStatusMap();
                            monitoredChanges.putAll(monitorMap);
                        }
                    } catch (AdminFaultException afe) {
                        /*
                         * Note that admin.getAdminStatus() may throw an AFE
                         * wrapping a SAE in case of network issues, which is
                         * caused by RE or NBE.
                         */
                        problems.add(new Problem(aId, hostname, port,
                                                 "Can't get status for Admin:",
                                                 afe));
                    } catch (RemoteException re) {
                        problems.add(new Problem(aId, hostname, port,
                                                 "Can't get status for Admin:",
                                                 re));
                    } catch (NotBoundException e) {
                        problems.add(new Problem(aId, hostname, port,
                                                 "No RMI Service for Admin:",
                                                 e));
                    } finally {
                        callback.nodeCallback(aId, info);
                    }
                    return null;
                }
            });
        }

        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            /* ignore */
        } finally {
            executor.shutdown();
        }
    }

    private void forEachArbNodeInShard(RepGroupId rgId,
                                       ArbNodeCallback callback) {

        final RepGroup group = topo.get(rgId);

        if (group == null) {
            return;
        }

        /* LoginManager not needed for ping */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, (LoginManager) null, logger);

        for (ArbNode an : group.getArbNodes()) {
            ArbNodeStatus status = null;
            StorageNode sn = topo.get(an.getStorageNodeId());
            try {
                ArbNodeAdminAPI ana =
                    regUtils.getArbNodeAdmin(an.getResourceId());
                status = ana.ping();
            } catch (RemoteException re) {
                problems.add(new Problem(an.getResourceId(),
                                         sn.getHostname(),
                                         sn.getRegistryPort(),
                                         "Can't call ping for AN:" + re));
            } catch (NotBoundException e) {
                problems.add(new Problem(an.getResourceId(),
                                         sn.getHostname(),
                                         sn.getRegistryPort(),
                                         "No RMI service for AN: " + e));
            }
            callback.nodeCallback(an, status);
        }
    }

    private void forEachArbNode(ArbNodeCallback callback) {

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            forEachArbNodeInShard(rg.getResourceId(), callback);
        }
    }
}
