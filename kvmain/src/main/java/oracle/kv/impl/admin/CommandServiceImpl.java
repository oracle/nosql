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

package oracle.kv.impl.admin;

import static oracle.kv.impl.security.AccessCheckUtils.checkPermission;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Snapshot.SnapResultSummary;
import oracle.kv.impl.admin.Snapshot.SnapshotOperation;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.AdminStatusReceiver;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.TrackerListener;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AccessCheckUtils.PlanAccessContext;
import oracle.kv.impl.security.AccessCheckUtils.PlanOperationContext;
import oracle.kv.impl.security.AccessCheckUtils.TableContext;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.table.FieldDef;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.util.DbBackup;

@SecureAPI
public class CommandServiceImpl
    extends VersionedRemoteImpl implements CommandService {

    private final AdminService aservice;

    private RemoteTestInterface rti;
    private static final String TEST_INTERFACE_NAME =
        "oracle.kv.impl.admin.CommandServiceTestInterface";

    /**
     * A cached copy of the Admin.  This is null until configure() is
     * called.
     */
    private Admin admin;

    private DbBackup dbBackup;

    public CommandServiceImpl(AdminService aservice) {
        this.aservice = aservice;
        admin = aservice.getAdmin();
        startTestInterface();
    }

    /**
     * Can the test interface be created?
     */
    private void startTestInterface() {
        try {
            final Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            final Constructor<?> c = cl.getConstructor(aservice.getClass());
            rti = (RemoteTestInterface) c.newInstance(aservice);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
    }


    /**
     * Return the service status associated with the CommandService.  If the
     * service is up, it will return RUNNING.  Otherwise it won't respond
     * at all.  This is just a way for the client to ask if the service is
     * running.
     */
    @Override
    @PublicMethod
    public synchronized ServiceStatus ping(AuthContext authCtx,
                                           short serialVersion) {
        return ServiceStatus.RUNNING;
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<String> getStorageNodePoolNames(AuthContext authCtx,
                                                short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<String>>() {

            @Override
            public List<String> execute() {
                requireConfigured();

                final Parameters p = admin.getCurrentParameters();

                final List<String> names =
                    new ArrayList<>(p.getStorageNodePoolNames());
                Collections.sort(names);
                return names;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void addStorageNodePool(final String name,
                                   AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
            @Override

            public void execute() {
                requireConfigured();
                admin.addStorageNodePool(name);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void cloneStorageNodePool(final String name,
                                     final String source,
                                     AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
            @Override

            public void execute() {
                requireConfigured();
                admin.cloneStorageNodePool(name, source);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void removeStorageNodePool(final String name, AuthContext authCtx,
                                      short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.removeStorageNodePool(name);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<StorageNodeId> getStorageNodePoolIds(final String name,
                                                     AuthContext authCtx,
                                                     short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<StorageNodeId>>() {
            @Override

            public List<StorageNodeId> execute() {
                requireConfigured();

                final Parameters p = admin.getCurrentParameters();

                final StorageNodePool pool = p.getStorageNodePool(name);
                if (pool == null) {
                    throw new IllegalCommandException
                        ("No such Storage Node Pool: " + name);
                }

                return pool.getList();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void addStorageNodeToPool(final String name,
                                     final StorageNodeId snId,
                                     AuthContext authCtx,
                                     short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.addStorageNodeToPool(name, snId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void removeStorageNodeFromPool(final String name,
                                          final StorageNodeId snId,
                                          AuthContext authCtx,
                                          short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.removeStorageNodeFromPool(name, snId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void replaceStorageNodePool(final String name,
                                       final List<StorageNodeId> ids,
                                       AuthContext authCtx,
                                       final short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.replaceStorageNodePool(name, ids);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String createTopology(final String candidateName,
                                 final String snPoolName,
                                 final int numPartitions,
                                 final boolean json,
                                 final short jsonVersion,
                                 AuthContext authCtx,
                                 short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.createTopoCandidate(candidateName, snPoolName,
                                                 numPartitions, json,
                                                 jsonVersion);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String copyCurrentTopology(final String candidateName,
                                      AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                admin.addTopoCandidate(candidateName,
                                       admin.getCurrentTopology());
                return "Created " + candidateName;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String copyTopology(final String sourceCandidateName,
                               final String targetCandidateName,
                               AuthContext authCtx,
                               short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                admin.addTopoCandidate(targetCandidateName,
                                       sourceCandidateName);
                return "Created " + targetCandidateName;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public String validateTopology(final String candidateName,
                                   final short jsonVersion,
                                   AuthContext authCtx,
                                   short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.validateTopology(candidateName, jsonVersion);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<String> listTopologies(AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<String>>() {

            @Override
            public List<String> execute() {
                requireConfigured();
                return admin.listTopoCandidates();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String deleteTopology(final String candidateName,
                                 AuthContext authCtx,
                                 short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                admin.deleteTopoCandidate(candidateName);
                return "Removed " + candidateName;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String rebalanceTopology(final String candidateName,
                                    final String snPoolName,
                                    final DatacenterId dcId,
                                    AuthContext authCtx,
                                    short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.rebalanceTopology(candidateName, snPoolName,
                                               dcId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String changeRepFactor(final String candidateName,
                                  final String snPoolName,
                                  final DatacenterId dcId,
                                  final int repFactor,
                                  AuthContext authCtx,
                                  short serialVersion) {
        return aservice.getFaultHandler().execute
        (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.changeRepFactor(candidateName, snPoolName,
                                             dcId, repFactor);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String changeZoneType(final String candidateName,
                                 final DatacenterId dcId,
                                 final DatacenterType type,
                                 AuthContext authCtx,
                                 short serialVersion) {
        return aservice.getFaultHandler().execute
        (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.changeZoneType(candidateName, dcId, type);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String changeZoneMasterAffinity(final String candidateName,
                                           final DatacenterId dcId,
                                           final boolean masterAffinity,
                                           AuthContext authCtx,
                                           short serialVersion) {
        return aservice.getFaultHandler().execute
        (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.changeZoneMasterAffinity(candidateName,
                                                      dcId,
                                                      masterAffinity);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String changeZoneArbiters(final String candidateName,
                                     final DatacenterId dcId,
                                     final boolean allowArbiters,
                                     AuthContext authCtx,
                                     short serialVersion) {
        return aservice.getFaultHandler().execute
        (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.changeZoneArbiters(candidateName,
                                                dcId,
                                                allowArbiters);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String redistributeTopology(final String candidateName,
                                       final String snPoolName,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.redistributeTopology(candidateName, snPoolName);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String contractTopology(final String candidateName,
                                   final String snPoolName,
                                   AuthContext authCtx,
                                   short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.contractTopology(candidateName, snPoolName);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String removeFailedShard(final RepGroupId failedShard,
                                    final String candidateName,
                                    AuthContext authCtx,
                                    short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.removeFailedShard(failedShard, candidateName);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String movePartition(String candidateName, PartitionId partId,
                                RepGroupId rgId, AuthContext NULL_CTX,
                                short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.movePartition(candidateName, partId, rgId);
            }
       });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String moveRN(final String candidateName,
                         final RepNodeId rnId,
                         final StorageNodeId snId,
                         AuthContext authCtx,
                         short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.moveRN(candidateName, rnId, snId);
            }
       });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String preview(final String targetTopoName,
                          final String startTopoName,
                          final boolean verbose,
                          final short jsonVersion,
                          AuthContext authCtx,
                          short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.previewTopology(targetTopoName, startTopoName,
                                             verbose, jsonVersion);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<ParameterMap> getAdmins(AuthContext authCtx,
                                        short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<ParameterMap>>() {

            @Override
            public List<ParameterMap> execute() {
                requireConfigured();
                final Parameters p = admin.getCurrentParameters();
                final ArrayList<ParameterMap> list = new ArrayList<>();
                for (AdminId id : p.getAdminIds()) {
                    list.add(p.get(id).getMap());
                }
                return list;
            }
        });
    }

    @Override
    @SecureInternalMethod
    public Plan getPlanById(final int planId,
                            AuthContext authCtx, final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Plan>() {

            @Override
            public Plan execute() {
                requireConfigured();

                final Plan plan = admin.getPlanById(planId);
                if (plan != null) {
                    checkPermission(aservice,
                                    new PlanAccessContext(plan, "Get plan"));
                }
                return plan;
            }
        });
    }

    @Override
    @SecureInternalMethod
    public void approvePlan(final int planId,
                            AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission(aservice,
                                new PlanOperationContext(plan, "Approve plan"));
                admin.approvePlan(planId);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public void executePlan(final int planId, final boolean force,
                            AuthContext authCtx, short serialVersion) {

         aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission(aservice,
                                new PlanOperationContext(plan, "Execute plan"));
                admin.executePlan(planId, force);
            }
        });
    }

   /**
     * Wait for the plan to finish. If a timeout period is specified, return
     * either when the plan finishes or the timeout occurs.
     * @return the current plan status when the call returns. If the call timed
     * out, the plan may still be running.
     */
    @Override
    @SecureInternalMethod
    public Plan.State awaitPlan(final int planId, final int timeout,
                                final TimeUnit timeUnit,
                                AuthContext authCtx, short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Plan.State>() {

            @Override
            public Plan.State execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission(aservice,
                                new PlanOperationContext(plan, "Await plan"));
                return admin.awaitPlan(planId, timeout, timeUnit);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public void cancelPlan(final int planId,
                           AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission(aservice,
                                new PlanOperationContext(plan, "Cancel plan"));
                admin.cancelPlan(planId);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public void interruptPlan(final int planId,
                              AuthContext authCtx,
                              short serialVersion) {

        aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission(aservice,
                                new PlanOperationContext(plan,
                                                         "Interrupt plan"));
                admin.interruptPlan(planId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createDeployDatacenterPlan(final String planName,
                                          final String datacenterName,
                                          final int repFactor,
                                          final DatacenterType datacenterType,
                                          final boolean allowArbiters,
                                          final boolean masterAffinity,
                                          AuthContext authCtx,
                                          final short serialVersion) {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().createDeployDatacenterPlan
                        (planName, datacenterName, repFactor,
                         datacenterType, allowArbiters, masterAffinity);
                }
            });
    }

    /**
     * Creates a plan to deploy a storage node, and stores it by id.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createDeploySNPlan(final String planName,
                                  final DatacenterId datacenterId,
                                  final String hostName,
                                  final int registryPort,
                                  final String comment,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                final ParameterMap pMap = admin.copyPolicy();
                final StorageNodeParams snParams =
                    new StorageNodeParams
                    (pMap, null, hostName, registryPort, comment);
                return admin.getPlanner().createDeploySNPlan
                    (planName, datacenterId, snParams);
            }
        });
    }

    /**
     * Creates a plan to deploy an Admin, and stores it by its plan id.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createDeployAdminPlan(final String planName,
                                     final StorageNodeId snid,
                                     final int httpPort /* unused*/,
                                     final AdminType adminType,
                                     AuthContext authCtx,
                                     short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().createDeployAdminPlan
                    (planName, snid, adminType);
            }
        });
    }

    /**
     * @since 18.1
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createRemoveAdminPlan(final String planName,
                                     final DatacenterId dcid,
                                     final AdminId aid,
                                     final boolean failedSN,
                                     AuthContext authCtx,
                                     short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().createRemoveAdminPlan
                    (planName, dcid, aid, failedSN);
            }
        });
    }

    /**
     * @since 18.1
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createDeployTopologyPlan(final String planName,
                                        final String candidateName,
                                        final RepGroupId failedShard,
                                        AuthContext authCtx,
                                        short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().createDeployTopoPlan
                    (planName, candidateName, failedShard);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createFailoverPlan(final String planName,
                                  final Set<DatacenterId> newPrimaryZones,
                                  final Set<DatacenterId> offlineZones,
                                  AuthContext authCtx,
                                  short serialVersion) {
        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().createFailoverPlan(
                        planName, newPrimaryZones, offlineZones);
                }
            });
    }

    /**
     * Creates a plan to stop all RepNodes in a kvstore.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createStopAllRepNodesPlan(final String planName,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createStopAllRepNodesPlan(planName);
            }
        });
    }

    /**
     * Creates a plan to start all RepNodes in a kvstore.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createStartAllRepNodesPlan(final String planName,
                                          AuthContext authCtx,
                                          short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createStartAllRepNodesPlan(planName);
            }
        });
    }

    /**
     * Creates a plan to stop the given services.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createStopServicesPlan(final String planName,
                                      final Set<? extends ResourceId> ids,
                                      AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().createStopServicesPlan(planName, ids);
            }
        });
    }

    /**
     * Creates a plan to start the given services.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createStartServicesPlan(final String planName,
                                       final Set<? extends ResourceId> ids,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createStartServicesPlan(planName, ids);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createChangeParamsPlan(final String planName,
                                      final ResourceId rid,
                                      final ParameterMap newParams,
                                      AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createChangeParamsPlan(planName, rid, newParams);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createChangeAllParamsPlan(final String planName,
                                         final DatacenterId dcid,
                                         final ParameterMap newParams,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createChangeAllParamsPlan(planName, dcid, newParams);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createChangeAllANParamsPlan(final String planName,
                                           final DatacenterId dcid,
                                           final ParameterMap newParams,
                                           AuthContext authCtx,
                                           short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createChangeAllANParamsPlan(planName, dcid, newParams);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createChangeAllAdminsPlan(final String planName,
                                         final DatacenterId dcid,
                                         final ParameterMap newParams,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createChangeAllAdminsPlan(planName, dcid, newParams);
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int
        createChangeGlobalComponentsParamsPlan(final String planName,
                                               final ParameterMap newParams,
                                               AuthContext authCtx,
                                               short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createChangeGlobalComponentsParamsPlan(planName,
                                                           newParams,
                                                           null);
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int
        createChangeGlobalSecurityParamsPlan(final String planName,
                                             final ParameterMap newParams,
                                             AuthContext authCtx,
                                             short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createChangeGlobalSecurityParamsPlan(planName, newParams);
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createCreateUserPlan(final String planName,
                                    final String userName,
                                    final boolean isEnabled,
                                    final boolean isAdmin,
                                    final char[] plainPassword,
                                    AuthContext authCtx,
                                    short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createCreateUserPlan(planName, userName, isEnabled,
                                             isAdmin, plainPassword,
                                             null /* pwdLifetime*/);
                }
            });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createDropUserPlan(final String planName,
                                  final String userName,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createDropUserPlan(planName, userName,
                                           false /* cascade */);
                }
            });
    }

    /* no R2-compatible version */
    @Override
    /*
     * Minimum privilege is USRVIEW to verify the user has successfully logged
     * in. SYSOPER is required if the modified user is different from the
     * current user. Such check will be conducted in the plan.
     */
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public int createChangeUserPlan(final String planName,
                                    final String userName,
                                    final Boolean isEnabled,
                                    final char[] plainPassword,
                                    final boolean retainPassword,
                                    final boolean clearRetainedPassword,
                                    AuthContext authCtx,
                                    short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createChangeUserPlan(planName, userName,
                                             isEnabled, plainPassword,
                                             retainPassword,
                                             clearRetainedPassword,
                                              null /* pwdLifetime*/);
                }
            });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createGrantPlan(final String planName,
                               final String grantee,
                               final Set<String> roles,
                               AuthContext authCtx,
                               short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createGrantPlan(planName, grantee, roles);
                }
            });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createRevokePlan(final String planName,
                                final String userName,
                                final Set<String> roles,
                                AuthContext authCtx,
                                short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createRevokePlan(planName, userName, roles);
                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createMigrateSNPlan(final String planName,
                                   final StorageNodeId oldNode,
                                   final StorageNodeId newNode,
                                   final int newHttpPort,
                                   AuthContext authCtx,
                                   short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createMigrateSNPlan(planName, oldNode,
                                        newNode);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createRemoveSNPlan(final String planName,
                                  final StorageNodeId targetNode,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createRemoveSNPlan(planName, targetNode);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createRemoveDatacenterPlan(final String planName,
                                          final DatacenterId targetId,
                                          AuthContext authCtx,
                                          short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                    createRemoveDatacenterPlan(planName, targetId);
            }
        });
    }

    /** Create a plan that will address mismatches in the topology */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createRepairPlan(final String planName,
                                AuthContext authCtx,
                                short serialVersion) {
      return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().createRepairPlan(planName);
            }
        });
    }

    /*
     * Note on table names and namespaces.
     * In order to keep the on-the-wire signatures compatible and not duplicate
     * interfaces for compatibility, table names with namespaces are passed as
     * a string string of the format:
     * [namespace:]full-table-name
     *
     * Validation of the namespace and names will have been done before getting
     * here.
     */

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.CREATE_ANY_TABLE })
    @Deprecated
    public int createAddTablePlan(final String planName,
                                  final String namespace,
                                  final String tableName,
                                  final String parentName,
                                  final FieldMap fieldMap,
                                  final List<String> primaryKey,
                                  final List<Integer> primaryKeySizes,
                                  final List<String> shardKey,
                                  final TimeToLive ttl,
                                  final boolean r2compat,
                                  final int schemaId,
                                  final String description,
                                  AuthContext authCtx,
                                  final short serialVersion) {
        return createAddTablePlan(planName, namespace, tableName, parentName,
                                  fieldMap, primaryKey, primaryKeySizes,
                                  shardKey, ttl, null, r2compat, schemaId,
                                  false, null, description, authCtx, serialVersion);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.CREATE_ANY_TABLE })
    @Deprecated
    public int createAddTablePlan(final String planName,
                                  final String namespace,
                                  final String tableName,
                                  final String parentName,
                                  final FieldMap fieldMap,
                                  final List<String> primaryKey,
                                  final List<Integer> primaryKeySizes,
                                  final List<String> shardKey,
                                  final TimeToLive ttl,
                                  final TableLimits limits,
                                  final boolean r2compat,
                                  final int schemaId,
                                  final String description,
                                  AuthContext authCtx,
                                  final short serialVersion) {
        return createAddTablePlan(planName, namespace, tableName, parentName,
                                  fieldMap, primaryKey, primaryKeySizes,
                                  shardKey, ttl, limits, r2compat, schemaId,
                                  false, null, description, authCtx, serialVersion);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.CREATE_ANY_TABLE })
    @Deprecated
    public int createAddTablePlan(final String planName,
                                  final String namespace,
                                  final String tableName,
                                  final String parentName,
                                  final FieldMap fieldMap,
                                  final List<String> primaryKey,
                                  final List<Integer> primaryKeySizes,
                                  final List<String> shardKey,
                                  final TimeToLive ttl,
                                  final TableLimits limits,
                                  final boolean r2compat,
                                  final int schemaId,
                                  final boolean jsonCollection,
                                  final String description,
                                  AuthContext authCtx,
                                  final short serialVersion) {

        return createAddTablePlan(planName, namespace, tableName, parentName,
                                  fieldMap, primaryKey, primaryKeySizes,
                                  shardKey, ttl, limits, r2compat, schemaId,
                                  jsonCollection, null, description,
                                  authCtx, serialVersion);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.CREATE_ANY_TABLE })
    public int createAddTablePlan(final String planName,
                                  final String namespace,
                                  final String tableName,
                                  final String parentName,
                                  final FieldMap fieldMap,
                                  final List<String> primaryKey,
                                  final List<Integer> primaryKeySizes,
                                  final List<String> shardKey,
                                  final TimeToLive ttl,
                                  final TableLimits limits,
                                  final boolean r2compat,
                                  final int schemaId,
                                  final boolean jsonCollection,
                                  final Map<String, FieldDef.Type> mrCounters,
                                  final String description,
                                  AuthContext authCtx,
                                  final short serialVersion) {

       return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                if (schemaId != 0) {
                    throw new IllegalArgumentException("schemaId should be 0");
                }

                TableImpl table = new TableImpl(namespace,
                                                tableName,
                                                null,
                                                primaryKey,
                                                primaryKeySizes,
                                                shardKey,
                                                fieldMap,
                                                ttl,
                                                null, /*beforeImageTTL*/
                                                limits,
                                                r2compat,
                                                schemaId,
                                                description,
                                                true,
                                                null,
                                                false,
                                                null, /* identity col info */
                                                null, /* regions */
                                                jsonCollection,
                                                mrCounters);
                return admin.getPlanner().
                    createAddTablePlan(planName, table, parentName, false,
                        null);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public int createRemoveTablePlan(final String planName,
                                     final String namespace,
                                     final String tableName,
                                     final boolean removeData,
                                     AuthContext authCtx,
                                     short serialVersion) {
        if (!removeData) {
            throw new IllegalArgumentException("removeData cannot be false");
        }
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();

                final TableImpl table = getAndCheckTable(namespace, tableName);

                /*
                 * If an non-owner drops a table and to remove the data, the
                 * DROP_INDEX privilege will also be checked if any index exists
                 * in the table.
                 */
                final List<KVStorePrivilege> privsToCheck = new ArrayList<>();
                if (!table.getIndexes().isEmpty()) {
                    privsToCheck.add(
                        new TablePrivilege.DropIndex(table.getId(), namespace));
                }
                privsToCheck.add(SystemPrivilege.DROP_ANY_TABLE);

                checkPermission(aservice, new TableContext("Drop table",
                                                           table,
                                                           privsToCheck));
                return admin.getPlanner().
                    createRemoveTablePlan(planName, namespace, tableName);
            }
        });
    }

    @Deprecated
    @Override
    @SecureInternalMethod
    public int createAddIndexPlan(final String planName,
                                  final String namespace,
                                  final String indexName,
                                  final String tableName,
                                  final String[] indexedFields,
                                  final FieldDef.Type[] indexedTypes,
                                  final String description,
                                  AuthContext authCtx,
                                  short serialVersion) {
        return createAddIndexPlan(planName,
                                  namespace,
                                  indexName,
                                  tableName,
                                  indexedFields,
                                  indexedTypes,
                                  true,
                                  false,
                                  description,
                                  authCtx,
                                  serialVersion);
    }

    @Override
    @SecureInternalMethod
    public int createAddIndexPlan(final String planName,
                                  final String namespace,
                                  final String indexName,
                                  final String tableName,
                                  final String[] indexedFields,
                                  final FieldDef.Type[] indexedTypes,
                                  boolean indexNulls,
                                  boolean isUnique,
                                  final String description,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();

                final TableImpl table = getAndCheckTable(namespace, tableName);
                final TablePrivilege privToCheck =
                    new TablePrivilege.CreateIndex(table.getId(), namespace);

                checkPermission(aservice, new TableContext("Create index",
                                                           table,
                                                           privToCheck));

                return admin.getPlanner().
                    createAddIndexPlan(planName,
                                       namespace,
                                       indexName,
                                       tableName,
                                       indexedFields,
                                       indexedTypes,
                                       indexNulls,
                                       isUnique,
                                       description,
                                       false);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public int createRemoveIndexPlan(final String planName,
                                     final String namespace,
                                     final String indexName,
                                     final String tableName,
                                     AuthContext authCtx,
                                     short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();

                final TableImpl table = getAndCheckTable(namespace, tableName);
                final TablePrivilege privToCheck =
                    new TablePrivilege.DropIndex(table.getId(), namespace);

                checkPermission(aservice, new TableContext("Drop index",
                                                           table,
                                                           privToCheck));

                return admin.getPlanner().
                    createRemoveIndexPlan(planName, namespace, indexName,
                                          tableName, false);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public int createEvolveTablePlan(final String planName,
                                     final String namespace,
                                     final String tableName,
                                     final int tableVersion,
                                     final FieldMap fieldMap,
                                     final TimeToLive ttl,
                                     final Set<Integer> regions,
                                     AuthContext authCtx,
                                     short serialVersion) {
        return createEvolveTablePlan(planName, namespace, tableName,
                                     tableVersion, fieldMap, ttl,
                                     null /* ignore before image ttl */,
                                     regions, authCtx, serialVersion);
    }

    @Override
    @SecureInternalMethod
    public int createEvolveTablePlan(final String planName,
                                     final String namespace,
                                     final String tableName,
                                     final int tableVersion,
                                     final FieldMap fieldMap,
                                     final TimeToLive ttl,
                                     final TimeToLive beforeImgTTL,
                                     final Set<Integer> regions,
                                     AuthContext authCtx,
                                     short serialVersion) {

       return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                final TableImpl table = getAndCheckTable(namespace, tableName);
                final TablePrivilege privToCheck =
                    new TablePrivilege.EvolveTable(table.getId(), namespace);

                checkPermission(aservice,
                                new TableContext("Evolve table",
                                                 table, privToCheck));

                return admin.getPlanner().
                    createEvolveTablePlan(planName,
                                          namespace,
                                          tableName,
                                          tableVersion,
                                          fieldMap,
                                          ttl,
                                          beforeImgTTL,
                                          table.getIdentityColumnInfo(),
                                          regions);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createNetworkRestorePlan(final String planName,
                                        final ResourceId sourceNode,
                                        final ResourceId targetNode,
                                        final boolean retainOrigLog,
                                        AuthContext authCtx,
                                        short serialVersion) {

       return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();

                return admin.getPlanner().createNetworkRestorePlan(
                    planName, sourceNode, targetNode, retainOrigLog);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createEnableRequestsPlan(final String planName,
                                        final String requestType,
                                        final Set<? extends ResourceId> resIds,
                                        final boolean entireStore,
                                        AuthContext authCtx,
                                        short serialVersion) {

       return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();

                return admin.getPlanner().createEnableRequestsPlan(
                    planName, requestType, resIds, entireStore);
            }
        });
    }

    /**
     * This is only used by table code, maybe change it to be table-specific.
     */
    @Override
    @SecureInternalMethod
    public <T extends Metadata<? extends MetadataInfo>> T
                                 getMetadata(final Class<T> returnType,
                                             final MetadataType metadataType,
                                             AuthContext authCtx,
                                             short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<T>() {

                @Override
                public  T execute() {
                    requireConfigured();
                    checkPermission(aservice,
                                    new MetadataAccessContext(metadataType));
                    return admin.getMetadata(returnType, metadataType);
                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void putMetadata(final Metadata<?> metadata,
                            AuthContext authCtx,
                            short serialVersion) {
        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
                    @Override
                    public  void execute() {
                        requireConfigured();
                        admin.saveMetadata(metadata, (Plan)null);
                        MetadataType type = metadata.getType();
                        int planId = 0;
                        switch (type) {
                        case TABLE:
                            planId = admin.getPlanner().
                                createBroadcastTableMDPlan();
                            break;
                        case SECURITY:
                            planId = admin.getPlanner().
                                createBroadcastSecurityMDPlan();
                            break;
                        default:
                            throw new IllegalCommandException
                                ("Invalid metadata type: " + type);
                        }

                        /*
                         * approve, execute, wait
                         */
                        admin.approvePlan(planId);
                        admin.executePlan(planId, false);
                        admin.awaitPlan(planId, 0, null);
                        admin.assertSuccess(planId);
                    }
                });
    }

    /**
     * Since 24.1, this method has been updated to only require USRVIEW from
     * SYSVIEW to make the privilege enforcement of accessing current topology
     * information consistent. [KVSTORE-2183]
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public Topology getTopology(AuthContext authCtx,
                                final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Topology>() {

            @Override
            public Topology execute() {
                requireConfigured();

                return admin.getCurrentTopology();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public Parameters getParameters(AuthContext authCtx,
                                    final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Parameters>() {

            @Override
            public Parameters execute() {
                requireConfigured();
                return admin.getCurrentParameters();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public ParameterMap getRepNodeParameters(final RepNodeId rnid,
                                             AuthContext authCtx,
                                             short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<ParameterMap>() {

            @Override
            public ParameterMap execute() {
                requireConfigured();
                final Parameters p = admin.getCurrentParameters();
                if (p.get(rnid) != null) {
                    return p.get(rnid).getMap();
                }
                throw new IllegalCommandException
                        ("RepNode does not exist: " + rnid);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public ParameterMap getPolicyParameters(AuthContext authCtx,
                                            final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<ParameterMap>() {

            @Override
            public ParameterMap execute() {
                requireConfigured();
                return admin.getCurrentParameters().getPolicies();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void newParameters(AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {

                /**
                 * Allow this call even if not configured.  This call can be
                 * made as a side effect of a storage node being registered and
                 * especially in test environments it can be difficult to know
                 * the configured state.
                 */
                if (admin != null) {
                    admin.newParameters();
                }
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void newGlobalParameters(AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {

                /**
                 * Allow this call even if not configured.  This call can be
                 * made as a side effect of a storage node being registered and
                 * especially in test environments it can be difficult to know
                 * the configured state.
                 */
                if (admin != null) {
                    admin.newGlobalParameters();
                }
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void newSecurityMDChange(AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.newSecurityMDChange();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void configure(final String storeName,
                          AuthContext authCtx,
                          short serialVersion) {
        aservice.getFaultHandler().execute
            (new AdminServiceFaultHandler.SimpleCommandProc() {

            @Override
            protected void perform() {
                if (admin != null) {
                    if (storeName.equals(aservice.getParams().
                                         getGlobalParams().getKVStoreName())) {
                        return;
                    }
                    throw new IllegalCommandException(
                        "Store has been configured with a different name");
                }
                /* Not yet configured */
                aservice.configure(storeName);
                /* Admin should be non-null now */
                admin = aservice.getAdmin();
            }

            @Override
            protected RuntimeException getWrappedException(RuntimeException e) {
                if (e instanceof IllegalStateException) {
                    return new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.STORE_CLEANUP);
                }
                if (e instanceof DatabaseException) {
                    return new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.STORE_CLEANUP);
                }
                return e;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public boolean isStoreReady(boolean forTables, AuthContext authCtx,
                                final short serialVersion) {
        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Boolean>() {
                    @Override
                    public Boolean execute() {
                        requireConfigured();
                        return admin.isStoreReady(forTables);
                    }
                });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public String getStoreName(AuthContext authCtx, short serialVersion) {
        if (admin != null) {
            return admin.getParams().getGlobalParams().getKVStoreName();
        }
        return null;
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public String getRootDir(AuthContext authCtx, short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

             @Override
             public String execute() {
                 if (admin != null) {
                     return admin.getParams().getStorageNodeParams().
                         getRootDirPath();
                 }
                 return null;
             }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    @Deprecated
    public void stop(boolean force,
                     AuthContext authCtx,
                     short serialVersion) {
        stop(force, null /* reason */, authCtx, serialVersion);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void stop(final boolean force,
                     final String reason,
                     AuthContext authCtx,
                     short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                aservice.stop(force, reason);
            }
        });
    }

    /**
     * Stop the remote test interface, if present.  Called from
     * AdminService.stop so that the test interface is stopped whenever the
     * admin stops the command service.
     */
    void stopRemoteTestInterface(Logger logger) {
        if (rti != null) {
            logger.info("Stopping remoteTestInterface");
            try {
                rti.stop(SerialVersion.CURRENT);
            } catch (RemoteException e) {
                /* Ignore */
            }
        }
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void setPolicies(final ParameterMap policyMap,
                            AuthContext authCtx,
                            short serialVersion) {
        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                admin.setPolicy(policyMap);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public Map<ResourceId, ServiceChange> getStatusMap(AuthContext authCtx,
                                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation
             <Map<ResourceId, ServiceChange>>() {

            @Override
            public Map<ResourceId, ServiceChange> execute() {
                requireConfigured();
                return admin.getMonitor().getServiceChangeTracker().getStatus();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public Map<ResourceId, PerfEvent> getPerfMap(AuthContext authCtx,
                                                 short serialVersion) {

        return aservice.getFaultHandler().
        execute(new ProcessFaultHandler.SimpleOperation
                <Map<ResourceId, PerfEvent>>() {

            @Override
            public Map<ResourceId, PerfEvent> execute() {
                requireConfigured();
                return admin.getMonitor().getPerfTracker().getPerf();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public RetrievedEvents<ServiceChange> getStatusSince(final long since,
                                                         AuthContext authCtx,
                                                         short serialVersion) {

        return aservice.getFaultHandler().
        execute(new ProcessFaultHandler.SimpleOperation
                <RetrievedEvents<ServiceChange>>() {

            @Override
            public RetrievedEvents<ServiceChange> execute() {
                requireConfigured();
                return admin.getMonitor().getServiceChangeTracker().
                    retrieveNewEvents(since);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public RetrievedEvents<PerfEvent> getPerfSince(long since,
                                                   AuthContext authCtx,
                                                   short serialVersion) {

        requireConfigured();
        return admin.getMonitor().getPerfTracker().retrieveNewEvents(since);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public RetrievedEvents<LogRecord> getLogSince(long since,
                                                  AuthContext authCtx,
                                                  short serialVersion) {

        requireConfigured();

        return admin.getMonitor().getLogTracker().retrieveNewEvents(since);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public RetrievedEvents<PlanStateChange> getPlanSince(long since,
                                                         AuthContext authCtx,
                                                         short serialVersion) {

        requireConfigured();
        return admin.getMonitor().getPlanStateChangeTracker().
            retrieveNewEvents(since);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void registerLogTrackerListener(TrackerListener tl,
                                           AuthContext authCtx,
                                           short serialVersion) {

        requireConfigured();
        admin.getMonitor().getLogTracker().registerListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void removeLogTrackerListener(TrackerListener tl,
                                         AuthContext authCtx,
                                         short serialVersion) {

        requireConfigured();
        admin.getMonitor().getLogTracker().removeListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void registerStatusTrackerListener(TrackerListener tl,
                                              AuthContext authCtx,
                                              short serialVersion) {

        requireConfigured();
        admin.getMonitor().getServiceChangeTracker().registerListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void removeStatusTrackerListener(TrackerListener tl,
                                            AuthContext authCtx,
                                            short serialVersion) {

        requireConfigured();
        admin.getMonitor().getServiceChangeTracker().removeListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void registerPerfTrackerListener(TrackerListener tl,
                                            AuthContext authCtx,
                                            short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPerfTracker().registerListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void removePerfTrackerListener(TrackerListener tl,
                                          AuthContext authCtx,
                                          short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPerfTracker().removeListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void registerPlanTrackerListener(TrackerListener tl,
                                            AuthContext authCtx,
                                            short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPlanStateChangeTracker().registerListener(tl);
    }

    @Deprecated
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public void removePlanTrackerListener(TrackerListener tl,
                                          AuthContext authCtx,
                                          short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPlanStateChangeTracker().removeListener(tl);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public Map<String, Long> getLogFileNames(final AuthContext authCtx,
                                             final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Map<String, Long>>() {

             @Override
             public Map<String, Long> execute() {

                 requireConfigured();

                 final Map<String, Long> namesToTimes = new HashMap<>();

                 final File logDir = FileNames.getLoggingDir
                    (new File(getRootDir(authCtx, serialVersion)),
                     getStoreName(authCtx, serialVersion));

                 final File[] files = logDir.listFiles(new FilenameFilter() {
                     @Override
                     public boolean accept(File dir, String fname) {
                         return fname.endsWith(FileNames.LOG_FILE_SUFFIX) ||
                             fname.endsWith(FileNames.PERF_FILE_SUFFIX) ||
                             fname.endsWith(FileNames.STAT_FILE_SUFFIX) ||
                             fname.endsWith(FileNames.DETAIL_CSV) ||
                             fname.endsWith(FileNames.SUMMARY_CSV);
                     }
                 });

                 for (File f : files) {
                     final String name = f.getName();
                     final long modTime = f.lastModified();

                     namesToTimes.put(name, Long.valueOf(modTime));
                 }
                 return namesToTimes;
             }
         });
    }

    private void checkReady() {
        if (!admin.isReady()) {
            throw new AdminNotReadyException
                ("The Admin is not ready to serve this request.  " +
                 "Please try again later");
        }
    }

    private void requireConfigured() {
        if (admin == null) {
            throw new IllegalCommandException
                ("This command can't be used until the Admin is configured.",
                 ErrorMessage.NOSQL_5200,
                 CommandResult.NO_CLEANUP_JOBS);
        }
        checkReady();
    }

    /**
     * Throws UnsupportedOperationException if client serialization version is
     * earlier than required one.
     */
    @Override
    protected void checkClientSupported(short clientSerialVersion,
                                        short requiredSerialVersion) {
        try {
            super.checkClientSupported(clientSerialVersion,
                                       requiredSerialVersion);
        } catch (UnsupportedOperationException e) {
            throw new AdminFaultException(
                e, e.getMessage(), ErrorMessage.NOSQL_5200,
                CommandResult.NO_CLEANUP_JOBS);
        }
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public AdminStatus getAdminStatus(
        AuthContext authCtx, final short serialVersion) {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<AdminStatus>() {
                @Override
                public AdminStatus execute() {

                    /*
                     * Say the admin is running if it is not configured, as
                     * done by ping
                     */
                    if (admin == null) {
                        return new AdminStatus(ServiceStatus.RUNNING,
                                               State.UNKNOWN,
                                               false,
                                               null,
                                               0,
                                               0,
                                               0);
                    }

                    return admin.getAdminStatus();
                }
            });
    }

    @Override
    /*
     * Only USRVIEW is required here because we want to allow all authenticated
     * users to be able to perform this operation, since it is required by
     * every operation that needs to locate the master admin node.
     */
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public URI getMasterRmiAddress(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<URI>() {

            @Override
            public URI execute() {
                requireConfigured();
                return admin.getMasterRmiAddress();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public URI getMasterWebServiceAddress(AuthContext authCtx,
                                          short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<URI>() {

            @Override
            public URI execute() {
                requireConfigured();
                return admin.getMasterWebServiceAddress();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<CriticalEvent> getEvents(final long startTime,
                                         final long endTime,
                                         final CriticalEvent.EventType type,
                                         AuthContext authCtx,
                                         final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<CriticalEvent>>() {

            @Override
            public List<CriticalEvent> execute() {
                requireConfigured();

                return admin.getEvents(startTime, endTime, type);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public CriticalEvent getOneEvent(final String eventId,
                                     AuthContext authCtx,
                                     final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<CriticalEvent>() {

            @Override
            public CriticalEvent execute() {
                requireConfigured();

                return admin.getOneEvent(eventId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public String [] startBackup(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String []>() {
            @Override
            public String [] execute() {
                requireConfigured();
                final ReplicatedEnvironment env = admin.getEnv();
                if (dbBackup != null) {
                    dbBackup.endBackup();
                }
                dbBackup = new DbBackup(env);
                dbBackup.startBackup();
                return dbBackup.getLogFilesInBackupSet();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public VerificationInfo
        startAndPollVerify(final VerificationOptions options,
                           final int planId,
                           AuthContext authCtx,
                           short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerificationInfo>() {

                @Override
                public VerificationInfo execute() {

                    requireConfigured();
                    return admin.startAndPollVerify(options, planId);

                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public boolean interruptVerifyData(AuthContext authCtx,
                                       short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {

                @Override
                public Boolean execute() {
                    requireConfigured();
                    return admin.interruptVerify();
                }

            });

    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public long stopBackup(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Long>() {

            @Override
            public Long execute() {
                requireConfigured();
                long lastFile = -1;
                if (dbBackup != null) {
                    lastFile = dbBackup.getLastFileInBackupSet();
                    dbBackup.endBackup();
                    dbBackup = null;
                }
                return lastFile;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public void updateMemberHAAddress(final AdminId targetId,
                                      final String targetHelperHosts,
                                      final String newNodeHostPort,
                                      AuthContext authCtx,
                                      short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                aservice.updateMemberHAAddress(targetId,
                                               targetHelperHosts,
                                               newNodeHostPort);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public VerifyResults verifyConfiguration(final boolean showProgress,
                                             final boolean listAll,
                                             final boolean json,
                                             AuthContext authCtx,
                                             short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerifyResults>() {

            @Override
            public VerifyResults execute() {

                requireConfigured();
                final VerifyConfiguration checker =
                    new VerifyConfiguration(admin,
                                            showProgress,
                                            listAll,
                                            json,
                                            admin.getLogger());
                checker.verifyTopology();
                return checker.getResults();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public VerifyResults verifyUpgrade(final KVVersion targetVersion,
                                       final List<StorageNodeId> snIds,
                                       final boolean showProgress,
                                       final boolean listAll,
                                       final boolean json,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerifyResults>() {

            @Override
            public VerifyResults execute() {

                requireConfigured();
                final VerifyConfiguration checker =
                    new VerifyConfiguration(admin,
                                            showProgress,
                                            listAll,
                                            json,
                                            admin.getLogger());
                checker.verifyUpgrade(targetVersion, snIds);
                return checker.getResults();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public VerifyResults verifyPrerequisite(final KVVersion targetVersion,
                                            final KVVersion prerequisiteVersion,
                                            final List<StorageNodeId> snIds,
                                            final boolean showProgress,
                                            final boolean listAll,
                                            final boolean json,
                                            AuthContext authCtx,
                                            final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerifyResults>() {

            @Override
            public VerifyResults execute() {

                requireConfigured();
                final VerifyConfiguration checker =
                    new VerifyConfiguration(admin,
                                            showProgress,
                                            listAll,
                                            json,
                                            admin.getLogger());
                checker.verifyPrerequisite(targetVersion,
                                           prerequisiteVersion,
                                           snIds);
                return checker.getResults();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public LoadParameters getParams(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                requireConfigured();
                return admin.getAllParams();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public String getStorewideLogName(AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.getStorewideLogName();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW})
    public TopologyCandidate getTopologyCandidate(final String candidateName,
                                                  AuthContext authCtx,
                                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<TopologyCandidate>() {

           @Override
           public TopologyCandidate execute() {
               requireConfigured();
               return admin.getCandidate(candidateName);
           }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<String> getTopologyHistory(final boolean concise,
                                           AuthContext authCtx,
                                           short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<String>>() {

           @Override
           public List<String> execute() {
               requireConfigured();
               return admin.displayRealizedTopologies(concise);
           }
        });
    }

    @Override
    @SecureInternalMethod
    public void assertSuccess(final int planId,
                              final AuthContext authCtx,
                              short serialVersion)
        throws RemoteException {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
            @Override
             public void execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission
                    (aservice,
                     new PlanAccessContext(plan, "Assert success"));
                admin.assertSuccess(planId);
            }
        });
    }

    @Override
    @SecureInternalMethod
    public String getPlanStatus(final int planId,
                                final long options,
                                final boolean json,
                                final AuthContext authCtx,
                                short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                final Plan plan = admin.getAndCheckPlan(planId);
                checkPermission
                    (aservice,
                     new PlanAccessContext(plan, "Get plan status"));
                return admin.getPlanStatus(planId, options, json);
            }
        });
    }

    /**
     * This method is deprecated as of 21.2. It shouldn't be called because
     * calls would have come from the SN on the same host which should have
     * been upgraded at the same time and should no longer call this method.
     * But leave the method as a stub for now just in case.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    @Deprecated
    public void installStatusReceiver(AdminStatusReceiver asr,
                                      AuthContext authCtx,
                                      short serialVersion) {
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public String getUpgradeOrder(final KVVersion targetVersion,
                                  final KVVersion prerequisiteVersion,
                                  final AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return UpgradeUtil.generateUpgradeList(admin,
                                                       targetVersion,
                                                       prerequisiteVersion);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public List<Set<StorageNodeId>>
        getUpgradeOrderList(final KVVersion targetVersion,
                            final KVVersion prerequisiteVersion,
                            final AuthContext authCtx,
                            short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.
                SimpleOperation<List<Set<StorageNodeId>>>() {

            @Override
            public List<Set<StorageNodeId>> execute() {
                requireConfigured();
                return UpgradeUtil.generateUpgradeOrderList(
                    admin, targetVersion, prerequisiteVersion);
            }
        });
    }

    @Override
    /*
     * Minimum required is USRVIEW for users to show plans created by them.
     * SYSVIEW is required to show all other users' plan.
     */
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany,
                                AuthContext authCtx,
                                final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<int[]>() {

            @Override
            public int[] execute() {
                requireConfigured();

                final ExecutionContext currentCtx =
                    ExecutionContext.getCurrent();
                if (currentCtx == null ||
                    currentCtx.hasPrivilege(SystemPrivilege.SYSVIEW)) {
                    return admin.getPlanIdRange(startTime, endTime, howMany,
                                                null /* ownerId */);
                }
                return admin.getPlanIdRange(startTime, endTime, howMany,
                                            getCurrentUserId());
            }
        });
    }

    @Override
    /*
     * Minimum required is USRVIEW for users to show plans created by them.
     * SYSVIEW is required to show all other users' plan.
     */
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany,
                                           AuthContext authCtx,
                                           final short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Map<Integer, Plan>>() {

            @Override
            public Map<Integer, Plan> execute() {
                requireConfigured();

                final ExecutionContext currentCtx =
                    ExecutionContext.getCurrent();
                if (currentCtx == null ||
                    currentCtx.hasPrivilege(SystemPrivilege.SYSVIEW)) {
                    return admin.getPlanRange(firstPlanId, howMany,
                                              null /* ownerId */);
                }
                return admin.getPlanRange(firstPlanId, howMany,
                                          getCurrentUserId());
            }
        });

    }

    @Override
    /*
     * Minimum required is USRVIEW for a user to show its own information.
     * SYSVIEW is required to show all other users' information.
     */
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public Map<String, UserDescription>
        getUsersDescription(final AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.
                SimpleOperation<Map<String, UserDescription>>() {

                @Override
                public Map<String, UserDescription> execute() {
                    requireConfigured();

                    final SecurityMetadata md =
                        admin.getMetadata(SecurityMetadata.class,
                                          MetadataType.SECURITY);

                    if (md == null) {
                        return null;
                    }

                    /*
                     * If security is not enabled, or current user has SYSVIEW
                     * privilege, returns all users' description.
                     */
                    final ExecutionContext currentCtx =
                        ExecutionContext.getCurrent();
                    if (currentCtx == null ||
                        currentCtx.hasPrivilege(SystemPrivilege.SYSVIEW)) {
                        return md.getUsersDescription();
                    }

                    return md.getCurrentUserDescription();
                }
            });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public boolean verifyUserPassword(final String userName,
                                      final char[] password,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Boolean>() {

                @Override
                public Boolean execute() {
                    requireConfigured();

                    final SecurityMetadata md =
                            admin.getMetadata(SecurityMetadata.class,
                                              MetadataType.SECURITY);
                    return md == null ?
                           false : md.verifyUserPassword(userName, password);
                }
            });
    }


    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public SnapResultSummary
        executeSnapshotOperation(final SnapshotOperation sop,
                                 final String sname,
                                 final DatacenterId dcId,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<SnapResultSummary>() {

                @Override
                public SnapResultSummary execute() {
                    requireConfigured();

                    return new SnapshotOperationProxy(admin).
                        executeSnapshotTasks(sop, sname, dcId);
                }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public String[] listSnapshots(final StorageNodeId snid,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<String[]>() {

                @Override
                public String[] execute() {
                    requireConfigured();

                    return new SnapshotOperationProxy(admin).
                        listSnapshots(snid);
                }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public Set<AdminId> repairAdminQuorum(final Set<DatacenterId> zoneIds,
                                          final Set<AdminId> adminIds,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<Set<AdminId>>() {
                @Override
                public Set<AdminId> execute() {
                    requireConfigured();
                    return admin.repairAdminQuorum(zoneIds, adminIds);
                }
            });
    }

    /**
     * Tries to identify current user. An IllegalCommandException will be
     * thrown if failed.
     *
     * @return id of current user
     */
    private static String getCurrentUserId() {
        final KVStoreUserPrincipal currentUser =
                KVStoreUserPrincipal.getCurrentUser();
        if (currentUser == null) {
            throw new IllegalCommandException(
                "Cannot identify current user");
        }
        return currentUser.getUserId();
    }

    private TableImpl getAndCheckTable(String namespace, String tableName) {
        final TableMetadata tableMd =
            aservice.getAdmin().getMetadata(TableMetadata.class,
                                            MetadataType.TABLE);
        final TableImpl table = (tableMd == null) ? null :
            tableMd.getTable(namespace, tableName);
        if (table == null) {
            throw new IllegalCommandException(
                "Could not find table: " +
                NameUtils.makeQualifiedName(namespace, tableName));
        }
        return table;
    }

    /**
     * Provides an implementation of OperationContext for access checking when
     * Metadata is requested.
     */
    private static class MetadataAccessContext implements OperationContext {
        private final MetadataType mdType;

        private MetadataAccessContext(MetadataType type) {
            this.mdType = type;
        }

        @Override
        public String describe() {
            return "Metadata request for type: " + mdType;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /*
             * Table Metadata requires DBVIEW authentication, others require
             * INTLOPER
             */
            if (mdType.equals(MetadataType.TABLE)) {
                return SystemPrivilege.dbviewPrivList;
            }
            return SystemPrivilege.internalPrivList;
        }
    }

    /**
     * Creates a plan to inform the Store of the existence of an ES node, and
     * stores it by its plan id.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createRegisterESClusterPlan(final String planName,
                                           final String clusterName,
                                           final String transportHp,
                                           final boolean secure,
                                           final boolean forceClear,
                                           AuthContext authCtx,
                                           short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                if (VersionUtil.getJavaMajorVersion() < 11) {
                         throw new IllegalCommandException("Can not register" +
                               " ES Cluster, as it requires Java 11 or later," +
                               " but running Java version " +
                               System.getProperty("java.version"));
                }
                if (admin.getParams().getSecurityParams() != null) {
                    if (admin.getParams().getSecurityParams().isSecure() ^
                            secure) {
                        throw new IllegalCommandException("Can not register" +
                                " ES Cluster." +
                                " Please Register secure ES on secure KV" +
                                " and non-secure ES on non-secure KV");
                    }
                }
                return admin.getPlanner().createRegisterESClusterPlan
                    (planName, clusterName, transportHp,secure, forceClear);
            }
        });
    }

    /**
     * Creates a plan to cause the Store to forget about a registered ES
     * cluster.  Only one cluster may be registered, so no identifying
     * information is needed.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createDeregisterESClusterPlan(final String planName,
                                             AuthContext authCtx,
                                             short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                if (VersionUtil.getJavaMajorVersion() < 11) {
                    throw new IllegalCommandException("Can not de register" +
                        " ES Cluster as it was not registered " +
                        "and also registeration  requires Java 11 or later," +
                        " but running Java version " +
                        System.getProperty("java.version"));
                }
                return admin.getPlanner().createDeregisterESClusterPlan
                    (planName);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    @Deprecated
    public void verifyData(final boolean verifyBtree,
                           final boolean verifyLog,
                           final boolean verifyIndex,
                           final boolean verifyRecord,
                           final long btreeDelay,
                           final long logDelay,
                           AuthContext authCtx,
                           short serialVersion)
        throws IOException {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.Procedure<IOException>() {
                @Override
                public void execute() throws IOException {
                    throw new IllegalCommandException(
                        "This API has been deprecated. Please " +
                        "upgrade the store and rerun the plan.");

                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public int createVerifyServicePlan(final String planName,
                                       final ResourceId rid,
                                       final VerificationOptions verifyOptions,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createVerifyServicePlan(planName,
                                                rid, verifyOptions);
                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public int
        createVerifyAllAdminsPlan(final String planName,
                                  final DatacenterId dcid,
                                  final VerificationOptions verifyOptions,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createVerifyAllAdminsPlan(planName,
                                                  dcid, verifyOptions);
                }
            });

    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public int
        createVerifyAllRepNodesPlan(final String planName,
                                    final DatacenterId dcid,
                                    final VerificationOptions verifyOptions,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createVerifyAllRepNodesPlan(planName,
                                                    dcid, verifyOptions);
                }
            });

    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public int
        createVerifyAllServicesPlan(final String planName,
                                    final DatacenterId dcid,
                                    final VerificationOptions verifyOptions,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.getPlanner().
                        createVerifyAllServicesPlan(planName,
                                                    dcid, verifyOptions);
                }
            });

    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    public int createTableLimitPlan(String planName,
                                    String namespace,
                                    String tableName,
                                    TableLimits newLimits,
                                    AuthContext NULL_CTX, short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.getPlanner().
                        createTableLimitPlan(planName,
                                             namespace, tableName,
                                             newLimits);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.SYSOPER})
    public int createUpdateTlsCredentialsPlan(String planName,
                                              boolean retrieve,
                                              boolean install,
                                              AuthContext authCtx,
                                              short serialVersion) {
        return aservice.getFaultHandler().execute(
            (ProcessFaultHandler.SimpleOperation<Integer>) () -> {
                requireConfigured();
                return admin.getPlanner().createUpdateTlsCredentialsPlan(
                    planName, retrieve, install);
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.SYSVIEW})
    public String getTlsCredentialsInfo(AuthContext authCtx,
                                        short serialVersion) {
        return aservice.getFaultHandler().execute(
            (ProcessFaultHandler.SimpleOperation<String>) () -> {
                requireConfigured();
                return admin.getTlsCredentialsInfo();
            });
    }
}
