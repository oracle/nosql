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

package oracle.kv.impl.admin.plan.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.MasterAdminStats;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.MasterRepNodeStats;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

/**
 * Health check utility for admin plans/tasks.
 *
 * Many plans and tasks may have a great impact on the services, particularly,
 * those that will stop services. Before the execution of those operations, we
 * want to make sure we do not start those operations during a time of when the
 * related groups are vulnerable. Hence the health check.
 *
 * Goals:
 * - The very first goal of the class is to provide a centralized place to
 *   share common health check code. The health check applies to RepNode,
 *   ArbNode and Admin, which shares similar code for status collection and
 *   health verification.
 * - The second goal of the class is to have clear but flexible healthiness
 *   definitions. We need to define what healthiness means when we do the
 *   check. Most plans/tasks can share the same healthiness definition, and the
 *   rest have their own variation. This is achieved by using the Requirements
 *   and Requirement class to specify the criteria for a health check.
 * - The class also considers the complication of an inconsistent view of node
 *   type among admin database and the node. It is possible that admin sees a
 *   node of one type but the node itself, either in its memory or je database
 *   sees a different type. This class try to detect such inconsistency and
 *   handle it.
 *
 * No-goals:
 * - The class cannot solve the concurrency problem of multiple plans changing
 *   node status and checking health in parallel. In this case, the health
 *   check results may be inaccurate. To solve this, a locking mechanism is
 *   needed among plans and is thus not solved in this class.
 *
 * Usage:
 * - Use HealthCheck.create(...).await(...) to wait for the groups to be
 *   healthy enough for a default healthiness requirement.
 * - Use HealthCheck.create(....).await(Requirements, ...) to wait for a
 *   customized healthiness requirement.
 * - Use HealthCheck.create(....).ping(ResourceId).get(...).get*() to obtain
 *   the group size information.
 */
public class HealthCheck {

    /* A rep group Id representing the admin group. */
    private static final RepGroupId ADMIN_GROUP_ID = new RepGroupId(-42);

    /* Timeout to wait for ping. */
    private static final long PING_TIMEOUT_MILLIS = 25000;

    /* Polling interval for await. */
    private static final long AWAIT_POLL_INTERVAL_MILLIS = 500;

    /* Logging prefix. */
    private final String loggingPrefix;

    /* Admin and other variables. */
    private final Admin admin;
    private final LoginManager loginManager;
    private final Topology topology;
    private final Parameters parameters;
    private RegistryUtils regUtils = null;

    /*
     * The mapping of rep group Id to its node set. The groups are those we will
     * ping to check status. It always contains groups in the nodesToStop.
     */
    private final Map<RepGroupId, Set<ResourceId>> nodesByGroup =
        new HashMap<RepGroupId, Set<ResourceId>>();

    /* Node sets that will be stopped. */
    private final Map<RepGroupId, Set<ResourceId>> nodesToStop =
        new HashMap<RepGroupId, Set<ResourceId>>();

    /*
     * A mapping of resource Id to node type. The mapping is generated from
     * admin database (Admin.getCurrentParameters()). The view might not
     * represent what the nodes believe they are due to possible inconsistency
     * between admin database, je database and the node local parameters. The
     * health check may verify the consistency among this view, the JE view
     * (see Result#nodeTypeJEView) and the in-memory view (see
     * Result#nodeTypeInMemView).
     */
    private final Map<ResourceId, NodeType> nodeTypeAdminView =
        new HashMap<ResourceId, NodeType>();

    /*
     * A mapping of resource Id to node type. The mapping is the nodes the
     * types of which will be updated in the future. The transition decides
     * which kind of health check is needed. The recommended wait behavior is:
     *
     * Node Type
     * Current      Future      Action
     * ---------    ---------   ---------------------------
     * Primary      -           Wait for group health
     * Secondary    Secondary   Don't wait
     * Secondary    Primary     Wait for node catch up (not a failure)
     */
    private final Map<ResourceId, NodeType> nodesToUpdate =
        new HashMap<ResourceId, NodeType>();

    /*
     * A set of nodes that are already occupying resource for contacting it.
     */
    private final Set<ResourceId> blacklist =
        Collections.newSetFromMap(
                new ConcurrentHashMap<ResourceId, Boolean>());

    /**
     * Creates a health check for a plan that stops one node and does not
     * change node type.
     *
     * @param admin the admin
     * @param loggingPrefix the logging prefix
     * @param resId the resource Id for the service that will be stopped
     */
    public static HealthCheck create(Admin admin,
                                     String loggingPrefix,
                                     ResourceId resId) {
        final Set<ResourceId> stopSet = new HashSet<ResourceId>();
        stopSet.add(resId);
        final Map<ResourceId, NodeType> updateMap =
            new HashMap<ResourceId, NodeType>();
        return create(admin, loggingPrefix, stopSet, updateMap);
    }

    /**
     * Creates a health check for a plan that stops one node and changes its
     * node type.
     *
     * @param admin the admin
     * @param loggingPrefix the logging prefix
     * @param resId the resource Id for the service that will be stopped
     * @param futureNodeType the future type for the node
     */
    public static HealthCheck create(Admin admin,
                                     String loggingPrefix,
                                     ResourceId resId,
                                     NodeType futureNodeType) {
        final Set<ResourceId> stopSet = new HashSet<ResourceId>();
        stopSet.add(resId);
        final Map<ResourceId, NodeType> updateMap =
            new HashMap<ResourceId, NodeType>();
        updateMap.put(resId, futureNodeType);
        return create(admin, loggingPrefix, stopSet, updateMap);
    }

    /**
     * Creates a health check for a plan that stops a set of nodes and does not
     * change node types.
     *
     * @param admin the admin
     * @param loggingPrefix the logging prefix
     * @param stopSet the set of services that will be stopped
     */
    public static HealthCheck create(Admin admin,
                                     String loggingPrefix,
                                     Set<? extends ResourceId> stopSet) {
        final Map<ResourceId, NodeType> updateMap =
            new HashMap<ResourceId, NodeType>();
        return create(admin, loggingPrefix, stopSet, updateMap);
    }

    /**
     * Creates a health check for a plan that stops a set of nodes and
     * specifies a update map for future node type.
     *
     * @param admin the admin
     * @param loggingPrefix the logging prefix
     * @param stopSet the set of services that will be stopped
     * @param updateMap the mapping of resource Id to the future node type
     */
    public static HealthCheck create(Admin admin,
                                     String loggingPrefix,
                                     Set<? extends ResourceId> stopSet,
                                     Map<ResourceId, NodeType> updateMap) {
        final Map<RepGroupId, Set<ResourceId>> nodesByGroup =
            getNodesByGroups(stopSet);
        return new HealthCheck(
                admin, loggingPrefix,
                nodesByGroup.keySet(), nodesByGroup, updateMap);
    }

    /**
     * Creates a health check for a plan that does not stop nodes and does not
     * change node types.
     *
     * @param admin the admin
     * @param loggingPrefix the logging prefix
     * @param groups the set of groups to check
     */
    public static HealthCheck createForGroups(Admin admin,
                                              String loggingPrefix,
                                              Set<RepGroupId> groups) {
        final Map<RepGroupId, Set<ResourceId>> nodesToStop =
            new HashMap<RepGroupId, Set<ResourceId>>();
        for (RepGroupId rgId : groups) {
            nodesToStop.put(rgId, Collections.emptySet());
        }
        return new HealthCheck(
                admin, loggingPrefix,
                groups, nodesToStop, Collections.emptyMap());
    }

    /**
     * Awaits for all the groups to meet the default health requirement which
     * is NODE_TYPE_VIEW_CONSISTENT, SIMPMAJ_WRITE_IF_STOP_ELECTABLE and
     * PROMOTING_CAUGHT_UP.
     *
     * Note that the method will try to obtain admin object lock in the calling
     * thread if stopSet has an admin node. This may cause deadlock if another
     * thread that acquired admin object lock waits for the calling thread.
     */
    public void await() {
        await((new Requirements()).
                and(NODE_TYPE_VIEW_CONSISTENT).
                and(SIMPMAJ_WRITE_IF_STOP_ELECTABLE).
                and(PROMOTING_CAUGHT_UP));
    }

    /**
     * Awaits for all the groups to meet a health requirement.
     *
     * Note that the method will try to obtain admin object lock in the calling
     * thread if stopSet has an admin node. This may cause deadlock if another
     * thread that acquired admin object lock waits for the calling thread.
     *
     * @param requirements the health requirements
     */
    public void await(Requirements requirements) {
        await(requirements,
              getAdminParams().getWaitTimeoutUnit().
              toMillis(getAdminParams().getWaitTimeout()),
              AWAIT_POLL_INTERVAL_MILLIS);
    }

    /**
     * Awaits for all the groups to meet a health requirement.
     *
     * Note that the method will try to obtain admin object lock in the calling
     * thread if stopSet has an admin node. This may cause deadlock if another
     * thread that acquired admin object lock waits for the calling thread.
     *
     * @param requirements the health requirements
     * @param awaitTimeout the timeout for the wait in milli-seconds
     * @param pollInterval the interval for status poll in milli-seconds
     */
    public void await(Requirements requirements,
                      long awaitTimeout,
                      long pollInterval) {

        final long startTimeMs = System.currentTimeMillis();
        final long deadlineMs = startTimeMs + awaitTimeout;
        final ExecutorService executor = Executors.newCachedThreadPool(
                /*
                 * Use daemon thread so that hanging RMI calls will not prevent
                 * the admin from termination.
                 */
                r -> {
                    Thread thread = Executors.
                        defaultThreadFactory().newThread(r);
                    thread.setDaemon(true);
                    return thread;
                });
        log(Level.FINE, String.format(
                    "Awaits for passing health check, " +
                    "req=%s, timeout=%d, interval=%d, " +
                    "groups=%s, nodesToStop=%s",
                    requirements, awaitTimeout, pollInterval,
                    nodesByGroup.keySet(), nodesToStop));
        try {
            while (true) {

                if (admin.isClosing()) {
                    throw new RuntimeException(
                            "Admin closing during health check.");
                }

                Violations violations =
                    checkViolations(executor, requirements);

                if (violations.isEmpty()) {
                    log(Level.FINE,
                            String.format(
                                "No violations, all check passed after %d ms",
                                System.currentTimeMillis() - startTimeMs));
                    break;
                }

                if (System.currentTimeMillis() > deadlineMs) {
                    log(Level.INFO, violations.toString());
                    if (!violations.shouldThrow()) {
                        return;
                    }
                    throw new OperationFaultException(violations.toString());
                }

                log(Level.FINEST,
                       String.format("Health check failed with violations: %s",
                           violations));

                try {
                    Thread.sleep(pollInterval);
                } catch (InterruptedException e) {
                    /*
                     * Do nothing when we are interrupted. If admin is closing,
                     * we will detect that during next iteration.
                     */
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Verifies if all the groups meet the default health requirement.
     */
    public void verify(Requirements requirements) {
        final ExecutorService executor = Executors.newCachedThreadPool(
                /*
                 * Use daemon thread so that hanging RMI calls will not prevent
                 * the admin from termination.
                 */
                r -> {
                    Thread thread = Executors.
                        defaultThreadFactory().newThread(r);
                    thread.setDaemon(true);
                    return thread;
                });
        log(Level.FINE,
            String.format("Verifies for health, " +
                          "req=%s, groups=%s, nodesToStop=%s",
                          requirements, nodesByGroup.keySet(), nodesToStop));
        try {
            Violations violations = checkViolations(executor, requirements);

            if (violations.isEmpty()) {
                log(Level.FINE, "No violations, all check passed");
                return;
            }

            log(Level.FINE,
                String.format("Health check failed with violations: %s",
                              violations));

            if (violations.shouldThrow()) {
                throw new OperationFaultException(violations.toString());
            }
        } finally {
            executor.shutdownNow();
        }
    }

     /**
     * Checks violations.
     */
    private Violations checkViolations(ExecutorService executor,
                                       Requirements requirements) {
        final Map<RepGroupId, Future<?>> futures =
            new HashMap<RepGroupId, Future<?>>();
        final Violations violations = new Violations();

        for (final Map.Entry<RepGroupId, Set<ResourceId>> entry :
                nodesByGroup.entrySet()) {
            final RepGroupId rgId = entry.getKey();
            if (rgId.equals(ADMIN_GROUP_ID)) {
                /* Do admin later in this thread. */
                continue;
            }
            futures.put(
                    rgId,
                    executor.submit(new Callable<Violations>() {
                        @Override
                        public Violations call() {
                            return ping(rgId, executor).
                                verify(requirements);
                        }
                    }));
        }

        /* Do admin */
        if (nodesByGroup.containsKey(ADMIN_GROUP_ID)) {
            violations.add(
                ping(ADMIN_GROUP_ID, executor).verify(requirements));
        }

        /*
         * Wait until all the tasks are finished. The durations of the
         * tasks are all bounded, see Collector methods.
         */
        awaitFuturesOrCancel(futures.values(), Integer.MAX_VALUE);

        /* Get the result */
        for (Map.Entry<RepGroupId, Future<?>> entry :
                futures.entrySet()) {
            final RepGroupId rgId = entry.getKey();
            final Future<?> future = entry.getValue();
            try {
                violations.add((Violations) future.get(
                            0, TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                violations.add(
                        new Violation(
                            String.format("\t[%s] %s\n",
                                rgId,
                                CommonLoggerUtils.getStackTrace(t)),
                            true /* throw exception */));
            }
        }

        return violations;
    }


    /**
     * Pings all the groups specified by the health check and returns a list of
     * health check results.
     *
     * Note that the method will try to obtain admin object lock in the calling
     * thread if stopSet has an admin node. This may cause deadlock if another
     * thread that acquired admin object lock waits for the calling thread.
     *
     * @return the list of results, which can be used to check group
     * properties, such as number of electables, etc.
     */
    public List<Result> ping() {
        List<Result> results = new ArrayList<Result>();
        List<RepGroupId> groups =
            new ArrayList<RepGroupId>(nodesByGroup.keySet());
        Collections.sort(groups);
        for (RepGroupId rgId : groups) {
            results.add(ping(rgId));
        }
        return results;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HealthCheck: ");
        sb.append("groups=").append(nodesByGroup).append(" ");
        sb.append("toStop=").append(nodesToStop).append(" ");
        sb.append("type.admin=").append(nodeTypeAdminView).append(" ");
        sb.append("toUpdate=").append(nodesToUpdate).append(" ");
        return sb.toString();
    }

    /**
     * Requirements of healthiness of the group.
     */
    public static class Requirements {

        private final Set<Requirement> requirements =
            new HashSet<Requirement>();

        /**
         * Adds a requirement.
         */
        public Requirements and(Requirement r) {
            requirements.add(r);
            return this;
        }

        /**
         * Returns the violations of a health check for the requirements, null
         * if no violation.
         */
        private Violations verify(Result result) {
            final Violations violations = new Violations();
            for (Requirement r : requirements) {
                violations.add(r.verify(result));
            }
            return violations;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (Requirement r : requirements) {
                sb.append(r).append(" ");
            }
            sb.append(")");
            return sb.toString();
        }
    }

    /* A list of common requirements */

    /**
     * Requires the node type view of the group to be consistent between admin
     * and the node.
     */
    public static final Requirement NODE_TYPE_VIEW_CONSISTENT =
        new Requirement() {
            @Override
            public Violation verify(Result result) {
                final String mesg = result.verifyView();
                return (mesg == null) ? null : new Violation(mesg, false);
            }

            @Override
            public String toString() {
                return "NODE_TYPE_VIEW_CONSISTENT";
            }
        };

    /**
     * Requires the group of a stopping node to be healthy enough to have a
     * simple majority of electors running after the node is stopped.
     */
    public static final Requirement SIMPMAJ_RUNNING =
        new Requirement() {
            @Override
            public Violation verify(Result result) {
                final String mesg =
                    result.verifyMajorityElectorsRunning();
                return (mesg == null) ? null : new Violation(mesg, true);
            }

            @Override
            public String toString() {
                return "SIMPMAJ_RUNNING";
            }
        };


    /**
     * Requires the group of a stopping node to be healthy enough to server
     * writes with simple majority consistency after the node is stopped.
     */
    public static final Requirement SIMPMAJ_WRITE =
        new Requirement() {
            @Override
            public Violation verify(Result result) {
                final String mesg =
                    result.verifySimpleMajorityWrite();
                return (mesg == null) ? null : new Violation(mesg, true);
            }

            @Override
            public String toString() {
                return "SIMPMAJ_WRITE";
            }
        };


    /**
     * Requires the group of a stopping electable node to be healthy enough to
     * server writes with simple majority consistency after the node is
     * stopped.
     */
    public static final Requirement SIMPMAJ_WRITE_IF_STOP_ELECTABLE =
        new Requirement() {
            @Override
            public Violation verify(Result result) {
                final String mesg =
                    result.verifySimpleMajorityWriteIfStopElectable();
                return (mesg == null) ? null : new Violation(mesg, true);
            }

            @Override
            public String toString() {
                return "SIMPMAJ_WRITE_IF_STOP_ELECTABLE";
            }
        };

    /**
     * Requires a stopping node to catch up to the master if the node is
     * promoting from a SECONDARY type to PRIMARY/ELECTABLE.
     */
    public static final Requirement PROMOTING_CAUGHT_UP =
        new Requirement() {
            @Override
            public Violation verify(Result result) {
                final String mesg =
                    result.verifyPromotingNodeCaughtUp();
                return (mesg == null) ? null : new Violation(mesg, false);
            }

            @Override
            public String toString() {
                return "PROMOTING_CAUGHT_UP";
            }
        };

    /**
     * A requirement of heathiness of the group.
     */
    private interface Requirement {
        Violation verify(Result result);
    }

    /**
     * Violations of requirements.
     */
    private static class Violations {

        /* Violations message prefix. */
        private static final String PREFIX =
            "One of the groups is not healthy enough for the operation:\n";
        /* The list of violations. */
        private final List<Violation> violations =
            new ArrayList<Violation>();
        /* Whether the violations should throw exception. */
        private boolean shouldThrow = false;

        /**
         * Adds a violation.
         */
        private void add(Violation v) {
            if (v == null) {
                return;
            }
            violations.add(v);
            if (v.shouldThrow()) {
                shouldThrow = true;
            }
        }

        /**
         * Adds violations.
         */
        private void add(Violations other) {
            for (Violation v : other.violations) {
                add(v);
            }
        }

        /**
         * Returns if the violations should throw OperationFaultException.
         */
        private boolean shouldThrow() {
            return shouldThrow;
        }

        /**
         * Returns true if no violations.
         */
        private boolean isEmpty() {
            return violations.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(PREFIX);
            for (Violation v : violations) {
                sb.append("\t").append(v).append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * Violation of a requirement.
     */
    private static class Violation {

        private final String message;
        private final boolean shouldThrow;

        /**
         * Constructor.
         */
        private Violation(String message, boolean shouldThrow) {
            this.message = message;
            this.shouldThrow = shouldThrow;
        }

        private boolean shouldThrow() {
            return shouldThrow;
        }

        @Override
        public String toString() {
            return message;
        }
    }

    /**
     * Gets the nodes by group based on the provided set.
     */
    private static Map<RepGroupId, Set<ResourceId>>
        getNodesByGroups(Set<? extends ResourceId> resIds) {

        final Map<RepGroupId, Set<ResourceId>> result =
            new HashMap<RepGroupId, Set<ResourceId>>();
        for (ResourceId resId : resIds) {
            RepGroupId rgId;
            switch(resId.getType()) {
            case ADMIN:
                rgId = ADMIN_GROUP_ID;
                break;
            case ARB_NODE:
                rgId = new RepGroupId(((ArbNodeId) resId).getGroupId());
                break;
            case REP_NODE:
                rgId = new RepGroupId(((RepNodeId) resId).getGroupId());
                break;
            default:
                throw new IllegalStateException();
            }
            if (!result.containsKey(rgId)) {
                result.put(rgId, new HashSet<ResourceId>());
            }
            result.get(rgId).add(resId);
        }
        return result;
    }

    /**
     * Constructs a health check profile for a rep node group.
     */
    private HealthCheck(Admin admin,
                        String loggingPrefix,
                        Set<RepGroupId> groups,
                        Map<RepGroupId, Set<ResourceId>> nodesToStop,
                        Map<ResourceId, NodeType> nodesToUpdate) {
        this.admin = admin;
        this.loggingPrefix = loggingPrefix;
        this.loginManager = admin.getLoginManager();
        this.topology = admin.getCurrentTopology();
        this.parameters = admin.getCurrentParameters();
        this.nodesToStop.putAll(nodesToStop);
        this.nodesToUpdate.putAll(nodesToUpdate);
        /* Updates the nodesByGroup */
        putNodesByGroups(groups);
        /* Updates the admin node type view */
        putNodeTypeAdminView();
    }

    /**
     * Updates the nodes by groups according to the view.
     */
    private void putNodesByGroups(Set<RepGroupId> groups) {
        /* Adds group Ids. */
        for (RepGroupId rgId : groups) {
            nodesByGroup.put(rgId, new HashSet<ResourceId>());
        }
        for (RepGroupId rgId : nodesToStop.keySet()) {
            if (!nodesByGroup.containsKey(rgId)) {
                nodesByGroup.put(rgId, new HashSet<ResourceId>());
            }
        }
        /* Finds out the node set of each group using the admin parameters. */
        if (nodesByGroup.containsKey(ADMIN_GROUP_ID)) {
            /* Admin */
            for (AdminParams ap : parameters.getAdminParams()) {
                AdminId id = ap.getAdminId();
                nodesByGroup.get(ADMIN_GROUP_ID).add(id);
            }
        }
        for (RepNodeParams rnp : parameters.getRepNodeParams()) {
            /* Rep nodes */
            final RepGroupId rgId =
                new RepGroupId(rnp.getRepNodeId().getGroupId());
            final ResourceId resId = rnp.getRepNodeId();
            if (nodesByGroup.containsKey(rgId)) {
                nodesByGroup.get(rgId).add(resId);
            }
        }
        for (ArbNodeParams anp : parameters.getArbNodeParams()) {
            /* Arb nodes */
            final RepGroupId rgId =
                new RepGroupId(anp.getArbNodeId().getGroupId());
            final ResourceId resId = anp.getArbNodeId();
            if (nodesByGroup.containsKey(rgId)) {
                nodesByGroup.get(rgId).add(resId);
            }
        }
    }

    /**
     * Puts the node type admin view.
     */
    private void putNodeTypeAdminView() {
        if (nodesByGroup.containsKey(ADMIN_GROUP_ID)) {
            putAdminGroupNodeType();
        }
        putRepGroupNodeType();
    }

    /**
     * Updates the admin group node type.
     */
    private void putAdminGroupNodeType() {
        for (AdminParams ap : parameters.getAdminParams()) {
            AdminId id = ap.getAdminId();
            switch (ap.getType()) {
            case PRIMARY:
                nodeTypeAdminView.put(id, NodeType.ELECTABLE);
                break;
            case SECONDARY:
                nodeTypeAdminView.put(id, NodeType.SECONDARY);
                break;
            default:
                throw new IllegalStateException(
                        "Unknown Admin type: " + ap.getType());
            }
        }
    }

    /**
     * Updates the rep group node type.
     */
    private void putRepGroupNodeType() {
        for (RepNodeParams rnp : parameters.getRepNodeParams()) {
            final RepGroupId rgId =
                new RepGroupId(rnp.getRepNodeId().getGroupId());
            final ResourceId resId = rnp.getRepNodeId();
            if (nodesByGroup.containsKey(rgId)) {
                nodeTypeAdminView.put(resId, rnp.getNodeType());
            }
        }
        for (ArbNodeParams anp : parameters.getArbNodeParams()) {
            final RepGroupId rgId =
                new RepGroupId(anp.getArbNodeId().getGroupId());
            if (nodesByGroup.containsKey(rgId)) {
                nodeTypeAdminView.put(anp.getArbNodeId(), NodeType.ARBITER);
            }
        }
    }

    /**
     * Pings the rep nodes of the group and returns a health check result.
     */
    private Result ping(RepGroupId rgId) {
        return ping(rgId, Executors.newCachedThreadPool(
                /*
                 * Use daemon thread so that hanging RMI calls will not prevent
                 * the admin from termination.
                 */
                r -> {
                    Thread thread = Executors.
                        defaultThreadFactory().newThread(r);
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    /**
     * Pings the rep nodes of the group and returns a health check.
     */
    private Result ping(RepGroupId rgId,
                        ExecutorService executor) {
        /* Creates a new RegistryUtils just in case. */
        regUtils = new RegistryUtils(topology, loginManager,
                                     admin.getLogger());
        /* Creates the check result. */
        Collector collector = new Collector(executor);
        if (rgId.equals(ADMIN_GROUP_ID)) {
            return new Result(
                    rgId,
                    collector.fetchAdminJEView(),
                    collector.fetchAdminInMemView(),
                    collector.pingAdmins());
        }
        return new Result(
                rgId,
                collector.fetchJEView(rgId),
                collector.fetchInMemView(rgId),
                collector.pingRNs(rgId),
                collector.pingANs(rgId));
    }

    /**
     * Logs a message.
     */
    private void log(Level level, String message) {
        admin.getLogger().log(
            level, loggingPrefix + " healthcheck: " + message);
    }

    /**
     * The result of one ping check.
     */
    public class Result {

        /*
         * The rep group Id.
         */
        private final RepGroupId rgId;

        /*
         * A mapping of the node type view of the rep group. The mapping is
         * generated through JE ReplicationGroupAdmin#getGroup which contacts
         * the master. The view always represents what the nodes believe they
         * are, but might not be available due to master unavailability. The
         * mapping is used as the ground truth to decide the node types for
         * requirement verification, if available.
         */
        private final Map<ResourceId, NodeType> nodeTypeJEView;

        /*
         * A mapping of the node type view of the rep group. The mapping is
         * generated from the in memory params of a service through RMI
         * #getParams call. The view is less susceptible to failure than JE
         * view and is the preferred view for node types when JE view is not
         * available.
         */
        private final Map<ResourceId, NodeType> nodeTypeInMemView;

        /*
         * A mapping of the service status.
         */
        private final Map<ResourceId, ServiceStatus> statusOfServices =
            new HashMap<ResourceId, ServiceStatus>();

        /*
         * A mapping of the ack delay between a replica and the master in JE.
         */
        private final Map<ResourceId, Long> jeAckTimeMillis =
            new HashMap<ResourceId, Long>();

        /* Master resource Id */
        private ResourceId masterId = null;

        /**
         * Returns the number of nodes that satisfy NodeType#isElectable.
         *
         * @return the number
         */
        public int getNumElectors() {
            int result = 0;
            for (ResourceId resId : nodesByGroup.get(rgId)) {
                final NodeType nodeType = getNodeType(resId);
                if ((nodeType != null) && nodeType.isElectable()) {
                    result ++;
                }
            }
            return result;
        }

        /**
         * Returns the number of nodes that satisfy NodeType#isElectable and
         * NodeType#isDataNode.
         *
         * @return the number
         */
        public int getNumDataElectables() {
            int result = 0;
            for (ResourceId resId : nodesByGroup.get(rgId)) {
                final NodeType nodeType = getNodeType(resId);
                if ((nodeType != null) &&
                    (nodeType.isElectable()) &&
                    (nodeType.isDataNode())) {
                    result ++;
                }
            }
            return result;
        }

        /**
         * Returns the group size override.
         *
         * See je.rep.ReplicationMutableConfig#ELECTABLE_GROUP_SIZE_OVERRIDE.
         *
         * @return the number
         */
        public int getElectableGroupSizeOverride() {
            for (ResourceId resId : nodesByGroup.get(rgId)) {
                if (resId.getType().isRepNode()) {
                    final int overrideSize =
                        parameters.get(((RepNodeId) resId)).
                        getElectableGroupSizeOverride();
                    if (overrideSize != 0) {
                        return overrideSize;
                    }
                }
            }
            return 0;
        }

        /**
         * Returns the size of simple majority for the group.
         *
         * @return the size
         */
        public int getSizeOfSimpleMajority() {
            final int n = getNumElectors();
            if (n < 3) {
                assert n >= 1;
                /*
                 * We do not have 3 nodes (even perhaps already included
                 * arbiter nodes)
                 */
                return 1;
            }
            return n / 2 + 1;
        }

        /**
         * Returns the size of simple majority for the group, but taking group
         * size override into considertion.
         *
         * @return the size
         */
        public int getSizeOfSimpleMajorityWithOverride() {
            final int n = getElectableGroupSizeOverride();
            if (n != 0) {
                return n / 2 + 1;
            }
            return getSizeOfSimpleMajority();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("HealthCheck ping result: ");
            sb.append("rgId=").append(rgId).append(" ");
            sb.append("type.je=").append(nodeTypeJEView).append(" ");
            sb.append("type.inmem=").append(nodeTypeInMemView).append(" ");
            sb.append("status=").append(statusOfServices).append(" ");
            sb.append("ackTime=").append(jeAckTimeMillis).append(" ");
            sb.append("master=").append(masterId).append(" ");
            return sb.toString();
        }

        /**
         * Constructs the result with rn and an ping results.
         */
        private Result(RepGroupId rgId,
                       Map<ResourceId, NodeType> nodeTypeJEView,
                       Map<ResourceId, NodeType> nodeTypeInMemView,
                       Map<RepNodeId, RepNodeStatus> rnStatus,
                       Map<ArbNodeId, ArbNodeStatus> anStatus) {
            this.rgId = rgId;
            this.nodeTypeJEView = nodeTypeJEView;
            this.nodeTypeInMemView = nodeTypeInMemView;
            putRepNodeStatus(rnStatus);
            putArbNodeStatus(anStatus);
        }

        /**
         * Constructs the result with admin ping results.
         */
        private Result(RepGroupId rgId,
                       Map<ResourceId, NodeType> nodeTypeJEView,
                       Map<ResourceId, NodeType> nodeTypeInMemView,
                       Map<AdminId, AdminStatus> adminStatus) {
            this.rgId = rgId;
            this.nodeTypeJEView = nodeTypeJEView;
            this.nodeTypeInMemView = nodeTypeInMemView;
            putAdminStatus(adminStatus);
        }

        /**
         * Puts the status and the ack time for rep nodes.
         */
        private void putRepNodeStatus(Map<RepNodeId, RepNodeStatus> rnStatus) {

            for (Map.Entry<RepNodeId, RepNodeStatus> entry :
                 rnStatus.entrySet()) {

                final RepNodeId statusId = entry.getKey();
                final RepNodeStatus status = entry.getValue();
                statusOfServices.put(statusId, status.getServiceStatus());
                if (!status.getReplicationState().isMaster()) {
                    continue;
                }
                masterId = statusId;
                jeAckTimeMillis.put(statusId, 0L);
                final MasterRepNodeStats mstats =
                    status.getMasterRepNodeStats();
                if (mstats == null) {
                    /*
                     * We have the master, but the stats is null, this usually
                     * means the master is of an older version that does not
                     * have this field. Assuming everything is fine.
                     */
                    for (final ResourceId resId : nodesByGroup.get(rgId)) {
                        jeAckTimeMillis.put(resId, 0L);
                    }
                } else {
                    final Map<String, Long> delayMap =
                        mstats.getReplicaDelayMillisMap();
                    for (Map.Entry<String, Long> e : delayMap.entrySet()) {
                        final ResourceId resId = parseRepGroupNode(e.getKey());
                        if (resId == null) {
                            continue;
                        }
                        final Long delay = e.getValue();
                        jeAckTimeMillis.put(resId, delay);
                    }
                }
             }
        }

        /**
         * Puts the status and the ack time for arb nodes.
         */
        private void putArbNodeStatus(Map<ArbNodeId, ArbNodeStatus> anStatus) {
            for (Map.Entry<ArbNodeId, ArbNodeStatus> entry :
                 anStatus.entrySet()) {
                statusOfServices.put(
                        entry.getKey(), entry.getValue().getServiceStatus());
            }
        }

        /**
         * Puts the status and the ack time for admin.
         */
        private void putAdminStatus(Map<AdminId, AdminStatus> adminStatus) {

            for (Map.Entry<AdminId, AdminStatus> entry :
                 adminStatus.entrySet()) {

                final AdminId statusId = entry.getKey();
                final AdminStatus status = entry.getValue();
                statusOfServices.put(statusId, status.getServiceStatus());
                if (!status.getReplicationState().isMaster()) {
                    continue;
                }
                masterId = statusId;
                jeAckTimeMillis.put(statusId, 0L);
                final MasterAdminStats mstats = status.getMasterAdminStats();
                if (mstats == null) {
                    /*
                     * We have the master, but the stats is null, this usually
                     * means the master is of an older version that does not
                     * have this field. Assuming everything is fine.
                     */
                    for (ResourceId resId : nodesByGroup.keySet()) {
                        jeAckTimeMillis.put(resId, 0L);
                    }
                } else {
                    final Map<String, Long> delayMap =
                        mstats.getReplicaDelayMillisMap();
                    for (Map.Entry<String, Long> e : delayMap.entrySet()) {
                        final ResourceId resId = parseAdminNode(e.getKey());
                        final Long delay = e.getValue();
                        jeAckTimeMillis.put(resId, delay);
                    }
                }
             }
        }


        /**
         * Returns the reason for violation of a requirement.
         */
        private Violations verify(Requirements requirements) {
            return requirements.verify(this);
        }

        /**
         * Returns the node type based on views. The priority of views for
         * checking is JE, in memory then admin. JE and in memory views should
         * be accurate as for the node type. Admin view might be outdated in
         * some unusual cases.
         *
         * The method could return null if the node has already been removed
         * from our views, e.g., in RemoveAdminRefs.
         */
        private NodeType getNodeType(ResourceId resId) {
            NodeType nodeType;
            nodeType = nodeTypeJEView.get(resId);
            if (nodeType != null) {
                return nodeType;
            }
            nodeType = nodeTypeInMemView.get(resId);
            if (nodeType != null) {
                return nodeType;
            }
            nodeType = nodeTypeAdminView.get(resId);
            return nodeType;
        }

        /**
         * Verifies that the admin, je and in memory views of the node type is
         * the same. If a node type of a view is not available, we assume it is
         * the same with the admin view.
         */
        private String verifyView() {
            final StringBuilder sb =
                new StringBuilder(String.format(
                            "[%s] Some nodes has a type that is " +
                            "inconsistent with the admin, including",
                            rgIdToString(rgId)));
            boolean inconsistent = false;
            for (final ResourceId resId : nodesByGroup.get(rgId)) {
                if (nodeTypeJEView.containsKey(resId)) {
                    if (!nodeTypeJEView.get(resId).
                            equals(nodeTypeAdminView.get(resId))) {
                        sb.append(String.format(", (admin=%s, node=%s)",
                            nodeTypeAdminView.get(resId),
                            nodeTypeJEView.get(resId)));
                        inconsistent = true;
                    }
                }
                if (nodeTypeInMemView.containsKey(resId)) {
                    if (!nodeTypeInMemView.get(resId).
                            equals(nodeTypeAdminView.get(resId))) {
                        sb.append(String.format(", (admin=%s, node=%s)",
                            nodeTypeAdminView.get(resId),
                            nodeTypeInMemView.get(resId)));
                        inconsistent = true;
                    }
                }
            }
            if (inconsistent) {
                return sb.toString();
            }
            return null;
        }

        /**
         * Verfies that the group is healthy enough for simple majority write
         * if we are stopping a electable node.
         */
        private String verifySimpleMajorityWriteIfStopElectable() {
            if (!isStoppingElectable()) {
                return null;
            }
            return verifySimpleMajorityWrite();
        }

        /**
         * Verfies that the group is healthy enough for simple majority write.
         */
        private String verifySimpleMajorityWrite() {
            String mesg;
            if ((mesg = verifyMaster()) != null) {
                return mesg;
            }
            if ((mesg = verifyMajorityElectorsRunning()) != null) {
                return mesg;
            }
            if ((mesg = verifyMajorityElectorsCaughtUp()) != null) {
                return mesg;
            }
            if ((mesg = verifyOneDataElectableCaughtUp()) != null) {
                return mesg;
            }
            return null;
        }

        /**
         * Returns true if stopping isElectable nodes.
         */
        private boolean isStoppingElectable() {
            for (ResourceId resId : nodesByGroup.get(rgId)) {
                NodeType nodeType = getNodeType(resId);
                if (nodeType == null) {
                    return false;
                }
                if (nodeType.isElectable()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Verifies that master is healthy. We contacted master multiple times when
         * we tried to obtain our result. If any of the contact fails, we think
         * the master is not healthy.
         */
        private String verifyMaster() {
            final StringBuilder sb = new StringBuilder(String.format(
                        "[%s] Master might have problem",
                        rgIdToString(rgId)));
            boolean failed = false;
            if ((masterId == null) ||
                (!statusOfServices.containsKey(masterId)) ||
                (!nodeTypeInMemView.containsKey(masterId))){
                sb.append(", failed to obtain master service status");
                failed = true;
            }
            if (nodeTypeJEView.isEmpty()) {
                sb.append(", failed to contact master JE database");
                failed = true;
            }
            sb.append(". ");
            if (failed) {
                return sb.append(this).toString();
            }
            return null;
        }

        /**
         * Verifies if a simple majority of electors are running.
         */
        private String verifyMajorityElectorsRunning() {
            final Set<ResourceId> nodesNotRunning =
                new HashSet<ResourceId>();
            int numRunning = 0;
            for (final ResourceId resId : nodesByGroup.get(rgId)) {
                ServiceStatus status = statusOfServices.get(resId);
                final NodeType nodeType = getNodeType(resId);
                if ((nodeType == null) || (!nodeType.isElectable())) {
                    continue;
                }
                if (nodesToStop.get(rgId).contains(resId)) {
                    continue;
                }

                if ((status == null) || (status != ServiceStatus.RUNNING)) {
                    nodesNotRunning.add(resId);
                } else {
                    numRunning ++;
                }
            }
            final int required = getSizeOfSimpleMajorityWithOverride();
            if (numRunning < required) {
                return String.format(
                        "[%s] Only %d primary nodes are running " +
                        "such that a simple majority cannot be formed " +
                        "which requires %d primary nodes. " +
                        "The shard is vulnerable and " +
                        "will not be able to elect a new master. " +
                        "Nodes not running: %s. " +
                        "Nodes to stop: %s. " +
                        "%s",
                        rgIdToString(rgId),
                        numRunning, required,
                        nodesNotRunning, nodesToStop, this);
            }
            return null;
        }

        /**
         * Verifies if a simple majority of electors are caught up.
         */
        private String verifyMajorityElectorsCaughtUp() {
            final Map<ResourceId, Long> nodesNotCaughtUp =
                new HashMap<ResourceId, Long>();
            final long ackTimeoutMillis =
                getAdminParams().getAckTimeoutMillis();
            int numCaughtUp = 0;
            for (final ResourceId resId : nodesByGroup.get(rgId)) {
                final NodeType nodeType = getNodeType(resId);
                if ((nodeType == null) || (!nodeType.isElectable())) {
                    continue;
                }
                if (nodesToStop.get(rgId).contains(resId)) {
                    continue;
                }

                Long ackTimeMillis = jeAckTimeMillis.get(resId);

                if (nodeType.isArbiter()) {
                    if ((ackTimeMillis == null) ||
                            (ackTimeMillis <= ackTimeMillis)) {
                        /*
                         * If the value is null or less than 0, the arbiter has
                         * never acked before. The arbiter is usually not queried
                         * for ack unless not enough data nodes can ack in time.
                         * Once queried, the arbiter does not need to catch up for
                         * previous transactions and thus should be considered
                         * caught up.
                         */
                        numCaughtUp ++;
                    } else {
                        nodesNotCaughtUp.put(resId, ackTimeMillis);
                    }
                    continue;
                }

                if ((ackTimeMillis != null) &&
                        (ackTimeMillis >= 0) &&
                        (ackTimeMillis < ackTimeoutMillis)) {
                    numCaughtUp ++;
                } else {
                    nodesNotCaughtUp.put(resId, ackTimeMillis);
                }
            }
            final int required = getSizeOfSimpleMajorityWithOverride();
            if (numCaughtUp < required) {
                return String.format(
                        "[%s] Only %d primary nodes " +
                        "are caught up to the master " +
                        "such that a simple majority cannot be formed " +
                        "which requires %d primary nodes. " +
                        "The shard may not serve write requests properly. " +
                        "Nodes not caught up: %s. " +
                        "Nodes to stop: %s. " +
                        "%s",
                        rgIdToString(rgId),
                        numCaughtUp, required,
                        nodesNotCaughtUp, nodesToStop, this);
            }
            return null;
        }

        /**
         * Verifies if at least one data electable is caught up.
         */
        private String verifyOneDataElectableCaughtUp() {
            final long ackTimeoutMillis =
                getAdminParams().getAckTimeoutMillis();
            for (final ResourceId resId : nodesByGroup.get(rgId)) {
                final NodeType nodeType = getNodeType(resId);
                if ((nodeType == null) ||
                    (!nodeType.equals(NodeType.ELECTABLE))) {
                    continue;
                }
                if (nodesToStop.get(rgId).contains(resId)) {
                    continue;
                }

                Long ackTimeMillis = jeAckTimeMillis.get(resId);

                if ((ackTimeMillis != null) &&
                        (ackTimeMillis >= 0) &&
                        (ackTimeMillis < ackTimeoutMillis)) {
                    return null;
                        }
            }
            return String.format(
                    "[%s] No primary data node is caught up to the master " +
                    "such that the shard may not be able to " +
                    "serve write requests properly. " +
                    "Nodes to stop: %s. " +
                    "%s",
                    rgIdToString(rgId), nodesToStop, this);
        }

        /**
         * Verifies if the stop nodes are caught up if they are promoting from
         * SECONDARY to ELECTABLE.
         */
        private String verifyPromotingNodeCaughtUp() {
            final long ackTimeoutMillis =
                getAdminParams().getAckTimeoutMillis();
            for (final ResourceId resId : nodesToStop.get(rgId)) {
                final NodeType nodeType = getNodeType(resId);
                if  ((nodeType == null) || (nodeType.isElectable())) {
                    /* isElectable in the first place. */
                    continue;
                }
                final NodeType updatedType = nodesToUpdate.get(resId);
                if  ((updatedType == null) || updatedType.isSecondary()) {
                    /* Not promoting. */
                    continue;
                }

                Long ackTimeMillis = jeAckTimeMillis.get(resId);

                if ((ackTimeMillis != null) &&
                        (ackTimeMillis >= 0) &&
                        (ackTimeMillis < ackTimeoutMillis)) {
                    continue;
                        }

                return String.format(
                        "[%s] The node to stop [%s] " +
                        "does not caught up to the master. " +
                        "%s",
                        rgIdToString(rgId), resId, this);
            }
            return null;
        }
    }

    /**
     * A health info collector.
     */
    private class Collector {

        private final ExecutorService executor;

        private Collector(ExecutorService executor) {
            this.executor = executor;
        }

        /*
         * TODO: the following ping methods have a lot of duplicated code among
         * themselves and with the PingCollector. Yet I cannot find an easy way
         * to simplify them.
         */

        /**
         * Fetches the admin nodes JE view.
         */
        private Map<ResourceId, NodeType> fetchAdminJEView() {
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            final Map<ResourceId, NodeType> result =
                new HashMap<ResourceId, NodeType>();
            futures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final AdminId self =
                            admin.getParams().getAdminParams().getAdminId();
                        final ReplicationGroupAdmin repGroupAdmin =
                            admin.getReplicationGroupAdmin(self);
                        final ReplicationGroup jeRepGroup =
                            Admin.getReplicationGroup(repGroupAdmin);
                        for (ReplicationNode node : jeRepGroup.getNodes()) {
                            result.put(
                                    parseAdminNode(node.getName()),
                                    node.getType());
                        }
                    } catch (Throwable t) {
                        /* do nothing */
                    }
                }
            }));
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            return result;
        }

        /**
         * Fetches the admin node in memory view.
         */
        private Map<ResourceId, NodeType> fetchAdminInMemView() {
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            final Map<ResourceId, NodeType> result =
                new ConcurrentHashMap<ResourceId, NodeType>();
            final AdminId self =
                admin.getParams().getAdminParams().getAdminId();
            for (final AdminId adminId : parameters.getAdminIds()) {
                if (adminId.equals(self)) {
                    final AdminType adminType =
                        new AdminParams(
                                admin.getAllParams().
                                getMap(adminId.getFullName(),
                                    ParameterState.ADMIN_TYPE)).
                        getType();
                    result.put(adminId, adminTypeToNodeType(adminType));

                }
                /* Run a task to get the admin params */
                final StorageNodeId snId =
                    parameters.get(adminId).getStorageNodeId();
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final AdminType adminType =
                                new AdminParams(
                                        regUtils.getAdmin(snId).getParams().
                                        getMap(adminId.getFullName(),
                                            ParameterState.ADMIN_TYPE)).
                                getType();
                            result.put(
                                    adminId, adminTypeToNodeType(adminType));
                        } catch (Throwable t) {
                        }
                    }
                }));
            }
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            return result;
        }

        /**
         * Fetches the rep group je node type view.
         */
        private Map<ResourceId, NodeType> fetchJEView(RepGroupId rgId) {
            final Map<ResourceId, NodeType> result =
                new HashMap<ResourceId, NodeType>();
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            futures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final String helpers =
                            getHelpers(parameters, topology, rgId);
                        final ReplicationGroupAdmin repGroupAdmin =
                            admin.getReplicationGroupAdmin(
                                    rgId.getGroupName(), helpers);
                        final ReplicationGroup jeRepGroup =
                            Admin.getReplicationGroup(repGroupAdmin);
                        for (ReplicationNode node : jeRepGroup.getNodes()) {
                            ResourceId resId =
                                parseRepGroupNode(node.getName());
                            if (resId == null) {
                                continue;
                            }
                            result.put(resId, node.getType());
                        }
                    } catch (Throwable t) {
                        /* do nothing */
                    }
                }
            }));
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            return result;
        }

        /**
         * Fetches the rep group node in memory view.
         */
        private Map<ResourceId, NodeType> fetchInMemView(RepGroupId rgId) {
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            final Map<ResourceId, NodeType> result =
                new ConcurrentHashMap<ResourceId, NodeType>();
            final RepGroup group = topology.get(rgId);
            for (final RepNode rn : group.getRepNodes()) {
                final RepNodeId rnId = rn.getResourceId();
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            NodeType nodeType =
                                new RepNodeParams(
                                        regUtils.getRepNodeAdmin(rnId).
                                        getParams().getMap(rnId.getFullName(),
                                            ParameterState.REPNODE_TYPE)).
                                getNodeType();
                            result.put(rnId, nodeType);
                        } catch (Throwable t) {
                        }
                    }
                }));
            }
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            for (final ArbNode an : group.getArbNodes()) {
                final ArbNodeId anId = an.getResourceId();
                result.put(anId, NodeType.ARBITER);
            }
            return result;
        }

        /**
         * Pings the admins to get the status map.
         *
         * Note that this method calls {@link Admin#getAdminStatus} which
         * acquires the admin object lock. Therefore, deadlock may occur if the
         * calling thread acquires the admin object lock, forking a new thread
         * calling this method.
         */
        private Map<AdminId, AdminStatus> pingAdmins() {
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            final Map<AdminId, AdminStatus> result =
                new ConcurrentHashMap<AdminId, AdminStatus>();
            final AdminId self =
                admin.getParams().getAdminParams().getAdminId();
            for (final AdminId adminId : parameters.getAdminIds()) {
                if (adminId.equals(self)) {
                    /*
                     * Call the get method within this thread, otherwise, this
                     * may cause deadlock since the calling thread might hold
                     * the admin object lock already.
                     */
                    try {
                        result.put(self, admin.getAdminStatus());
                    } catch (Throwable t) {
                    }
                    continue;
                }
                /* Run a task to ping the admin */
                final StorageNodeId snId =
                    parameters.get(adminId).getStorageNodeId();
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (blacklist.contains(adminId)) {
                            return;
                        }
                        blacklist.add(adminId);
                        try {
                            AdminStatus status =
                                regUtils.getAdmin(snId).getAdminStatus();
                            result.put(adminId, status);
                        } catch (Throwable t) {
                        } finally {
                            blacklist.remove(adminId);
                        }
                    }
                }));
            }
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            return result;
        }

        /**
         * Pings the rep nodes in the rep group to get the status map.
         */
        private Map<RepNodeId, RepNodeStatus> pingRNs(RepGroupId rgId) {
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            final Map<RepNodeId, RepNodeStatus> result =
                new ConcurrentHashMap<RepNodeId, RepNodeStatus>();
            final RepGroup group = topology.get(rgId);
            for (final RepNode rn : group.getRepNodes()) {
                final RepNodeId rnId = rn.getResourceId();
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (blacklist.contains(rnId)) {
                            return;
                        }
                        blacklist.add(rnId);
                        try {
                            RepNodeStatus status =
                                regUtils.getRepNodeAdmin(rnId).ping();
                            result.put(rnId, status);
                        } catch (Throwable t) {
                        } finally {
                            blacklist.remove(rnId);
                        }
                    }
                }));
            }
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            return result;
        }

        /**
         * Pings the arb nodes in the rep group to get the status map.
         */
        private Map<ArbNodeId, ArbNodeStatus> pingANs(RepGroupId rgId) {
            final List<Future<?>> futures = new ArrayList<Future<?>>();
            final Map<ArbNodeId, ArbNodeStatus> result =
                new ConcurrentHashMap<ArbNodeId, ArbNodeStatus>();
            final RepGroup group = topology.get(rgId);
            for (final ArbNode an : group.getArbNodes()) {
                final ArbNodeId anId = an.getResourceId();
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (blacklist.contains(anId)) {
                            return;
                        }
                        blacklist.add(anId);
                        try {
                            ArbNodeStatus status =
                                regUtils.getArbNodeAdmin(anId).ping();
                            result.put(anId, status);
                        } catch (Throwable t) {
                        } finally {
                            blacklist.remove(anId);
                        }
                    }
                }));
            }
            awaitFuturesOrCancel(futures, PING_TIMEOUT_MILLIS);
            return result;
        }
    }

    /**
     * Gets the admin parameters.
     */
    private AdminParams getAdminParams() {
        return admin.getParams().getAdminParams();
    }

    /**
     * Awaits for the futures to be done.
     *
     * The method will not be able to cancel running tasks and thus can only
     * wait for them to be done.
     */
    private static void awaitFuturesOrCancel(Collection<Future<?>> futures,
                                             long timeout) {
        final long deadlineMs =
            System.currentTimeMillis() + timeout;
        while (true) {
            boolean allDone = true;
            for (Future<?> f : futures) {
                if (f.isDone()) {
                    continue;
                }
                final long curr = System.currentTimeMillis();
                if (curr >= deadlineMs) {
                    f.cancel(true);
                    continue;
                }
                allDone = false;
            }
            if (allDone) {
                break;
            }
            try {
                Thread.sleep(Math.min(timeout, 50));
            } catch (InterruptedException e) {
                throw new OperationFaultException(
                        "Unexpected interrupt while waiting for " +
                        "health check ping results");
            }
        }
    }

    /**
     * Parses the je string Id to AdminId.
     */
    private static AdminId parseAdminNode(String id) {
        return AdminId.parse("admin" + id);
    }

    /**
     * Parses rep group node ID.
     */
    private static ResourceId parseRepGroupNode(String id) {
        try {
            if (id.contains(ArbNodeId.getPrefix())) {
                return ArbNodeId.parse(id);
            }
            return RepNodeId.parse(id);
        } catch (IllegalArgumentException e) {
            /*
             * Cannot parse the name, not something we care about, e.g., a
             * stream node.
             */
            return null;
        }
    }

    /**
     * Gets the helper hosts of a group.
     */
    private static String getHelpers(Parameters params,
                                     Topology topo,
                                     RepGroupId rgId) {

        final StringBuilder helperHosts = new StringBuilder();

        /* Find helper hosts from RNs */
        for (RepNode rn : topo.get(rgId).getRepNodes()) {
            final RepNodeParams rnp = params.get(rn.getResourceId());
            if (rnp == null) {
                continue;
            }
            addHelpers(helperHosts,
                       rnp.getJENodeHostPort(),
                       rnp.getJEHelperHosts());
        }

        /* If still no helper hosts found, try to find from arbiter nodes */
        if (helperHosts.length() == 0) {
            for (ArbNode arb : topo.get(rgId).getArbNodes()) {
                final ArbNodeParams anp = params.get(arb.getResourceId());
                if (anp == null) {
                    continue;
                }
                addHelpers(helperHosts,
                           anp.getJENodeHostPort(),
                           anp.getJEHelperHosts());
            }
        }
        return helperHosts.toString();
    }

    private static void addHelpers(StringBuilder helperHosts,
                                   String nodeHostPort,
                                   String helpers) {
        if (helperHosts.length() != 0) {
            helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
        }

        if (nodeHostPort != null) {
            helperHosts.append(nodeHostPort);
        }

        if (!"".equals(helpers) && (helpers != null)) {
            helperHosts.
            append(ParameterUtils.HELPER_HOST_SEPARATOR).
            append(nodeHostPort);
        }
    }

    private static String rgIdToString(RepGroupId rgId) {
        if (rgId.equals(ADMIN_GROUP_ID)) {
            return "rg-admin";
        }
        return rgId.toString();
    }

    private static NodeType adminTypeToNodeType(AdminType adminType) {
        switch (adminType) {
        case PRIMARY:
            return NodeType.ELECTABLE;
        case SECONDARY:
            return NodeType.SECONDARY;
        default:
            throw new IllegalStateException();
        }
    }
}
