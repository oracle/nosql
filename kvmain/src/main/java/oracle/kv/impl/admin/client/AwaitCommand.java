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

package oracle.kv.impl.admin.client;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.rep.MasterRepNodeStats;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.PingCollector;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/*
 * await-consistent command
 */
@POST
class AwaitCommand extends ShellCommand {

    /* Time to wait between pings */
    private static final long WAIT_MS = 5000;

    /* Default is 5 seconds */
    private static final long DEFAULT_REPLICA_DELAY_THRESHOLD_MILLIS = 5000;

    AwaitCommand() {
        super("await-consistent", 3);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        return new AwaitCommandExecutor<String>() {


            @Override
            public String failureResult(String message) {
                return message;
            }

            @Override
            public String successResult(String message) {
                return message;
            }

            @Override
            public String timeoutResult(Map<String, ReplicaDelayInfo> waiting,
                                        Set<DatacenterId> zones,
                                        long thresholdMillis) {
                final StringBuilder sb = new StringBuilder();
                sb.append("The ").append(zonesOrStore(zones.size()));
                sb.append(" did not become consistent " +
                          "within the timeout period\n");

                for (Entry<String, ReplicaDelayInfo> e : waiting.entrySet()) {
                    sb.append("  ").append(e.getKey());
                    if (e.getValue() == null) {
                        sb.append(" state is not known\n");
                    } else {
                        final ReplicaDelayInfo ri = e.getValue();
                        ri.report(sb, thresholdMillis);
                    }
                }
                return sb.toString();
            }
        }.commonExecute(args, shell);
    }

    private abstract class AwaitCommandExecutor<T>
        implements Executor<T> {
        @Override
        public T commonExecute(String[] args, Shell shell)
            throws ShellException {
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            int timeoutSec = -1;
            long thresholdMillis = DEFAULT_REPLICA_DELAY_THRESHOLD_MILLIS;
            final Set<DatacenterId> zones = new HashSet<>();

            try {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-timeout".equals(arg)) {
                        timeoutSec =
                            parseUnsignedInt(
                                Shell.nextArg(args, i++, AwaitCommand.this));
                    } else if ("-zn".equals(arg)) {
                        zones.add(
                            DatacenterId.parse(
                                Shell.nextArg(args, i++, AwaitCommand.this)));
                    } else if ("-znname".equals(arg)) {
                        final String zoneName =
                            Shell.nextArg(args, i++, AwaitCommand.this);
                        zones.add(
                            CommandUtils.getDatacenterId(zoneName, cs,
                                                         AwaitCommand.this));
                    } else if ("-replica-delay-threshold".equals(arg)) {
                        thresholdMillis =
                            parseUnsignedInt(Shell.nextArg(args, i++,
                                                           AwaitCommand.this));
                    } else {
                        shell.unknownArgument(arg, AwaitCommand.this);
                    }
                }

                if (timeoutSec < 0) {
                    shell.requiredArg("-timeout", AwaitCommand.this);
                }

                final Map<String, ReplicaDelayInfo> waiting;

                try {
                    waiting = waitForZones(cs.getTopology(), zones,
                                           timeoutSec, thresholdMillis);
                } catch (IllegalArgumentException iae) {
                    return failureResult(iae.getMessage());
                } catch (InterruptedException ex) {
                    return failureResult("Unexpected interupt");
                }

                if (waiting == null) {
                    return successResult(
                        "The " + zonesOrStore(zones.size()) +
                        ((zones.size() > 1) ? " are" : " is") +
                        " consistent");
                }

                return timeoutResult(waiting, zones, thresholdMillis);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return null;  /* Not reached */
        }

        public abstract T
            timeoutResult(Map<String, ReplicaDelayInfo> waiting,
                          Set<DatacenterId> zones,
                          long thresholdMillis);

        public abstract T failureResult(String message);

        public abstract T successResult(String message);
    }

    @Override
    public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
        throws ShellException {
        final ShellCommandResult scr =
            ShellCommandResult.getDefault("await consistency");
        return new AwaitCommandExecutor<ShellCommandResult>() {


            @Override
            public ShellCommandResult failureResult(String message) {
                scr.setReturnCode(ErrorMessage.NOSQL_5100.getValue());
                scr.setDescription(message);
                return scr;
            }

            @Override
            public ShellCommandResult successResult(String message) {
                scr.setDescription(message);
                return scr;
            }

            @Override
            public ShellCommandResult
                timeoutResult(Map<String, ReplicaDelayInfo> waiting,
                              Set<DatacenterId> zones,
                              long thresholdMillis) {
                scr.setReturnCode(ErrorMessage.NOSQL_5300.getValue());
                scr.setDescription(
                    "The " + zonesOrStore(zones.size()) +
                    " did not become consistent within the timeout period");

                final ObjectNode top = JsonUtils.createObjectNode();
                final ArrayNode delayArray = top.putArray("replicaDelayInfos");
                for (Entry<String, ReplicaDelayInfo> e : waiting.entrySet()) {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    on.put("replicaName", e.getKey());
                    if (e.getValue() != null) {
                        final ReplicaDelayInfo ri = e.getValue();
                        on.put("replicaDelayMillis", ri.replicaDelayMillis);
                        on.put("replicaCatchupSecs",
                               ri.replicaCatchupTimeSecs);
                        on.put("thresholdMillis", thresholdMillis);
                    }
                    delayArray.add(on);
                }
                scr.setReturnValue(top);
                return scr;
            }
        }.commonExecute(args, shell);
    }

    /**
     * Waits up to the timeout number of seconds for the RNs in the
     * specified zones to become consistent. If all of the RNs are consistent
     * null is returned. Otherwise, returns a map of RNs which are behind
     * when the timeout is reached. The map key is the name of the RN and the
     * value is a ReplicaDelayInfo instance containing replica delay
     * information for the node. If no information about a target node is
     * found, the value will be null;
     *
     * If the set of zones is empty, all of the zones in the store
     * are waited on.
     *
     * @param topo a topology
     * @param zones the set of zones to wait for
     * @param timeoutSec how long to wait
     * @param thresholdMillis replica delay threshold
     * @return the map of lagging nodes or null
     * @throws InterruptedException if the call is interrupted
     * @throws IllegalArgumentException if a specified zone is not found or
     * the store or specified zone(s) contained no nodes
     */
    static Map<String, ReplicaDelayInfo> waitForZones(Topology topo,
                                                      Set<DatacenterId> zones,
                                                      int timeoutSec,
                                                      long thresholdMillis)
        throws InterruptedException {

        /* If no zone is specified, then check all zones */
        final Set<DatacenterId> zonesToCheck =
                    zones.isEmpty() ? topo.getDatacenterMap().getAllIds() :
                                      zones;
        final Set<String> targetRNs = new HashSet<>();

        /* Gather the RNs from each zone */
        for (DatacenterId zoneId : zonesToCheck) {
            final Datacenter dc = topo.get(zoneId);
            if (dc == null) {
                throw new IllegalArgumentException("Unknown zone: " + zoneId);
            }

            for (RepNodeId rnId : topo.getRepNodeIds(zoneId)) {
                targetRNs.add(rnId.getFullName());
            }
        }

        if (targetRNs.isEmpty()) {
            throw new IllegalArgumentException(
                                        "The " + zonesOrStore(zones.size()) +
                                        " does not contain any nodes");
        }

        final long limitMs = System.currentTimeMillis() +
                                            SECONDS.toMillis(timeoutSec);
        while (true) {
            final Map<String, ReplicaDelayInfo> waiting =
                    checkRNs(targetRNs, topo, thresholdMillis);

            if (waiting.isEmpty()) {
                return null;
            }

            /*
             * If waiting would put us over the timeout, return with the
             * failed nodes.
             */
            if (System.currentTimeMillis() + WAIT_MS > limitMs) {
                return waiting;
            }
            Thread.sleep(WAIT_MS);
        }
    }

    /**
     * Checks whether the target replicas delays are under the threshold.
     * If a target replica is behind, its name and the number of milliseconds
     * it is behind is added to the waiting map. If all nodes are caught-up
     * the map will be empty. If no information about a target node is found,
     * its name will have a null value.
     *
     * @param targetRNs the set of RNs to check
     * @param topo a topology
     * @param thresholdMillis the threshold value to use in checking RN delay
     * @return a map of lagging replicas
     */
    private static Map<String, ReplicaDelayInfo> checkRNs(Set<String> targetRNs,
                                                          Topology topo,
                                                          long thresholdMillis){

        /*
         * Create a map with all of the target RNs, setting the value to
         * null. Entries are removed if the RN is found to be a  master,
         * or its delay is under the threshold. If the delay is
         * over the threshold, the value is replaced by a ReplicaDelayInfo
         * containing the delay information for that node.
         */
        final Map<String, ReplicaDelayInfo> waiting =
                        new HashMap<>(targetRNs.size());
        for (String rnId : targetRNs) {
            waiting.put(rnId, null);
        }

        /*
         * For each group, find the master's stats for its nodes.
         */
        PingCollector collector =
            new PingCollector(topo,
                              LoggerUtils.getLogger(AwaitCommand.class,
                                                    "command"));
        for (RepGroupId rgId : topo.getRepGroupIds()) {

            /*
             * Get the RN status for each node in the group. Find  the master
             * and remove it from waiting and use its stats to check for delays.
             */
            MasterRepNodeStats masterStats = null;
            final Map<RepNodeId, RepNodeStatus> statusMap =
                collector.getRepNodeStatus(rgId);
            for (Entry<RepNodeId, RepNodeStatus> e : statusMap.entrySet()) {
                final RepNodeStatus rns = e.getValue();
                if ((rns != null) && rns.getReplicationState().isMaster()) {
                    waiting.remove(e.getKey().getFullName());
                    masterStats = rns.getMasterRepNodeStats();
                    break;
                }
            }

            /* The master's stats for this group was not found */
            if (masterStats == null) {
                continue;
            }

            final Map<String, Long> delayMap =
                                        masterStats.getReplicaDelayMillisMap();
            for (Entry<String, Long> e : delayMap.entrySet()) {
                final String replicaName = e.getKey();
                final Long delayMillis = e.getValue();

                /* Null, we dont know */
                if (delayMillis == null) {
                    continue;
                }
                if (delayMillis > thresholdMillis) {
                    /* Catchup can be null */
                    final Long catchupTimeSecs =
                            masterStats.getReplicaCatchupTimeSecs(replicaName);
                    waiting.put(replicaName,
                                new ReplicaDelayInfo(delayMillis,
                                                     catchupTimeSecs));
                } else {
                    waiting.remove(replicaName);
                }
            }
        }
        return waiting;
    }

    private static String zonesOrStore(int n) {
        return (n == 0) ? "store" :
                          "specified " + ((n == 1) ? "zone" : "zones");
    }

    @Override
    protected String getCommandSyntax() {
        return "await-consistent -timeout <timeout-secs> " +
               "[-zn <id> | -znname <name>]... " +
               "[-replica-delay-threshold <time-millis>] " +
               CommandParser.getJsonUsage();
    }

    @Override
    public String getCommandDescription() {
        return
            "Waits for up to the specified number of seconds for" + eolt +
            "the replicas in one or more zones, or in the entire" + eolt +
            "store, to catch up with the masters in their associated" + eolt +
            "shards. Prints information about whether consistency" + eolt +
            "was achieved or, if not, details about which nodes" + eolt +
            "failed to become consistent.";
    }

    static class ReplicaDelayInfo {
        final Long replicaDelayMillis;
        final Long replicaCatchupTimeSecs;

         private ReplicaDelayInfo(Long replicaDelayMillis,
                                  Long replicaCatchupTimeSecs) {
            assert replicaDelayMillis != null && replicaDelayMillis > 0;

            this.replicaDelayMillis = replicaDelayMillis;
            this.replicaCatchupTimeSecs = replicaCatchupTimeSecs;
        }

        @Override
        public String toString() {
            return "ReplicaDelayInfo[" +
                "replicaDelayMillis=" + replicaDelayMillis +
                " replicaCatchupTimeSecs=" + replicaCatchupTimeSecs +
                "]";
        }

        private StringBuilder report(StringBuilder sb, long thresholdMillis) {
            sb.append(" is ").append(replicaDelayMillis);
            sb.append(" milliseconds behind");

            if (replicaCatchupTimeSecs == null) {
                sb.append(", time to catchup is unknown");
            } else if (replicaCatchupTimeSecs == Long.MAX_VALUE) {
                sb.append(" and is not catching up");
            } else if (replicaCatchupTimeSecs < 0) {
                sb.append(" and is falling further behind");
            } else {
                /*
                 * If catching up, adjust the time to reflect the fact that
                 * we are waiting to get under the threshold, not completely
                 * caught up.
                 */
                final long adjustedCatchup =
                    replicaDelayMillis > thresholdMillis ?
                       (long)(replicaCatchupTimeSecs *
                            (float)(replicaDelayMillis - thresholdMillis) /
                                                           replicaDelayMillis) :
                       1;
                sb.append(", expected to catchup in ");
                sb.append(adjustedCatchup).append(" second");
                if (adjustedCatchup > 1) {
                    sb.append("s");
                }
            }
            sb.append("\n");
            return sb;
        }
    }
}
