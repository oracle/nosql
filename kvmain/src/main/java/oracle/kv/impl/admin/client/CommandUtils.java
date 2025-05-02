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

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.ObjectNode;

/*
 * Some useful utilities for command validation and other functions
 */
public class CommandUtils {
    protected final static String eol = System.getProperty("line.separator");

    static void ensureTopoExists(String topoName, CommandServiceAPI cs,
                                 ShellCommand command)
        throws ShellException, RemoteException {

        List<String> topos = cs.listTopologies();
        if (topos.indexOf(topoName) == -1) {
            throw new ShellUsageException
                ("Topology " + topoName + " does not exist. " +
                 "Use topology list to see existing candidates.",
                 command);
        }
    }

    private static Topology.Component<?> get(CommandServiceAPI cs,
                                             ResourceId rid)
        throws RemoteException {

        Topology t = cs.getTopology();
        return t.get(rid);
    }

    static void ensureDatacenterExists(DatacenterId dcid, CommandServiceAPI cs,
                                       ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, dcid) == null)  {
            throw new ShellUsageException("Zone does not exist: " +
                                          dcid, command);
        }
    }

    public static void ensureShardExists(RepGroupId rgid, CommandServiceAPI cs,
                                         ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, rgid) == null)  {
            throw new ShellUsageException("Shard does not exist: " +
                                          rgid, command);
        }
    }

    public static void ensurePartitionExists(PartitionId partId,
                                             CommandServiceAPI cs,
                                             ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, partId) == null)  {
            throw new ShellUsageException("Partition does not exist: " +
                                          partId.getPartitionId(), command);
        }
    }

    public static void ensureRepNodeExists(RepNodeId rnid, CommandServiceAPI cs,
                                           ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, rnid) == null)  {
            throw new ShellUsageException("RepNode does not exist: " +
                                          rnid, command);
        }
    }

    public static void ensureArbNodeExists(ArbNodeId anid, CommandServiceAPI cs,
                                           ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, anid) == null)  {
            throw new ShellUsageException("ArbNode does not exist: " +
                                          anid, command);
        }
    }

    static void ensureStorageNodeExists(StorageNodeId snid,
                                        CommandServiceAPI cs,
                                        ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, snid) == null)  {
            throw new ShellUsageException("StorageNode does not exist: " +
                                          snid, command);
        }
    }

    static void ensurePlanExists(int planId, CommandServiceAPI cs,
                                 ShellCommand command)
        throws ShellException, RemoteException {

        if (cs.getPlanById(planId) == null)  {
            throw new ShellUsageException
                ("Plan does not exist: " + planId, command);
        }
    }

    static void validatePool(String poolName, CommandServiceAPI cs,
                             ShellCommand command)
        throws ShellException, RemoteException {

        List<String> poolNames = cs.getStorageNodePoolNames();
        if (poolNames.indexOf(poolName) == -1) {
            throw new ShellUsageException("Pool does not exist: " +
                                          poolName, command);
        }
    }

    static DatacenterId getDatacenterId(String name, CommandServiceAPI cs,
                                        ShellCommand command)
        throws ShellException, RemoteException {

        Topology t = cs.getTopology();
        DatacenterMap dcMap = t.getDatacenterMap();
        for (Datacenter dc : dcMap.getAll()) {
            if (name.equals(dc.getName())) {
                return dc.getResourceId();
            }
        }
        throw new ShellUsageException("Zone does not exist: " + name,
                                      command);
    }

    /** Return whether the command flag specifies a datacenter ID. */
    static boolean isDatacenterIdFlag(final String flag) {
        return "-zn".equals(flag) || "-dc".equals(flag);
    }

    /** Return whether the command flag specifies a datacenter name. */
    static boolean isDatacenterNameFlag(final String flag) {
        return "-znname".equals(flag) || "-dcname".equals(flag);
    }

    /** Return whether the zone ID flag or value is deprecated. */
    static boolean isDeprecatedDatacenterId(final String flag,
                                            final String value) {
        return "-dc".equals(flag) || value.startsWith("dc");
    }

    /** Return whether the zone name flag is deprecated. */
    static boolean isDeprecatedDatacenterName(final String flag) {
        return "-dcname".equals(flag);
    }

    /*
     * Functions shared by change-policy and plan change-parameters
     */
    static void assignParam(ParameterMap map, String name, String value,
                            ParameterState.Info info,
                            ParameterState.Scope scope,
                            boolean showHidden,
                            ShellCommand command)
        throws ShellException {

        ParameterState pstate = ParameterState.lookup(name);
        String errorMessage = null;
        if (pstate != null) {
            if (pstate.getReadOnly()) {
                errorMessage = "Parameter is read-only: " + name;
            }
            if (!showHidden && pstate.isHidden()) {
                errorMessage = "Parameter can only be set using -hidden " +
                    "flag: " + name;
            }
            if (scope != null && scope != pstate.getScope()) {
                errorMessage = "Parameter cannot be used as a store-wide " +
                    "parameter: " + name;
            }
            if (info != null && !pstate.appliesTo(info)) {
                errorMessage = "Parameter is not valid for the service: " +
                    name;
            }
            if (errorMessage == null) {
                /* This method will validate the value if necessary */
                try {
                    map.setParameter(name, value);
                } catch (IllegalArgumentException iae) {
                    throw new ShellUsageException
                        ("Illegal parameter value:" + Shell.eolt +
                         iae.getMessage(), command);
                }
                return;
            }
        } else {
            errorMessage = "Unknown parameter field: " + name;
        }
        throw new ShellUsageException(errorMessage, command);
    }

    static void parseParams(ParameterMap map, String[] args, int i,
                            ParameterState.Info info,
                            ParameterState.Scope scope,
                            boolean showHidden,
                            ShellCommand command)
        throws ShellException {

        for (; i < args.length; i++) {
            String param = args[i];
            if (param.startsWith("-")) {
                throw new ShellUsageException
                    ("No flags are permitted after the -params flag",
                     command);
            }

            String splitArgs[] = null;

            /*
             * name="value with embedded spaces" will turn up as 2 args:
             * 1. name=
             * 2. value with embedded spaces
             */
            if (param.endsWith("=")) {
                if (++i >= args.length) {
                    throw new ShellUsageException
                        ("Parameters require a value after =", command);
                }
                splitArgs = new String[] {param.split("=")[0], args[i]};
            } else {
                splitArgs = param.split("=", 2);
            }

            if (splitArgs.length != 2) {
                throw new ShellUsageException
                    ("Unable to parse parameter assignment: " + param,
                     command);
            }
            assignParam(map, splitArgs[0].trim(), splitArgs[1].trim(),
                        info, scope, showHidden, command);
        }
    }

    /*
     * Format and sort the ParameterMap.  This is similar to
     * ParameterMap.showContents() except that it deals with hidden
     * parameters.
     */
    public static String formatParams(ParameterMap map,
                                      boolean showHidden,
                                      ParameterState.Info info) {
        StringBuilder sb = new StringBuilder();
        for (Parameter p : map) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if ((pstate != null) && (info == null || pstate.appliesTo(info)) &&
                (showHidden || !pstate.isHidden())) {
                sb.append(p.getName()).append("=").append(p.asString());
                sb.append(eol);
            }
        }
        return sb.toString();
    }

    /* Return JSON represents parameters */
    public static ObjectNode formatParamsJson(ParameterMap map,
                                              boolean showHidden,
                                              ParameterState.Info info) {
        final ObjectNode top = JsonUtils.createObjectNode();
        for (Parameter p : map) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if ((pstate != null) && (info == null || pstate.appliesTo(info)) &&
                (showHidden || !pstate.isHidden())) {
                top.put(p.getName(), p.asString());
            }
        }
        return top;
    }

    /*
     * Get new password from user input
     */
    static char[] getPasswordFromInput(final PasswordReader passReader,
                                       final ShellCommand command)
        throws ShellException {

        try {
            final char[] newPassword =
                    passReader.readPassword("Enter the new password: ");
            final String errorMsg = verifyPassword(passReader, newPassword);
            if (errorMsg != null) {
                throw new ShellUsageException(errorMsg, command);
            }
            return newPassword;
        } catch (IOException ioe) {
            throw new ShellUsageException(
                "Could not read password from console: " + ioe, command);
        }
    }

    /**
     * Read the password again from user input for verification.  The
     * verification succeeds if and only if the new input is not null, not
     * empty, is identical to the specified password and no exception occurs.
     * Otherwise, an error message is returned.
     *
     * @param passReader
     * @param newPassword the password to be verified
     * @return an error message if any problem happens in verification,
     * otherwise null is returned
     */
    static String verifyPassword(final PasswordReader passReader,
                                 final char[] newPassword) {

        String errorMessage = null;
        try {
            if (newPassword == null || newPassword.length == 0) {
                errorMessage = "Empty password is unacceptable.";
            }
            final char[] rePasswd =
                passReader.readPassword("Re-enter the new password: ");
            if (!Arrays.equals(newPassword, rePasswd)) {
                errorMessage = "Sorry, passwords do not match.";
            }
            SecurityUtils.clearPassword(rePasswd);
        } catch (IOException ioe) {
            errorMessage = "Could not read password from console: " + ioe;
        }
        return errorMessage;
    }

    /*
     * Ensure that RNs associated with failed shard during deploy topology
     * are not running
     */
    public static void ensureRNNotRunning(RepGroupId failedShard,
                                          Topology currentTopo,
                                          Map<ResourceId, ServiceChange> statusMap,
                                          ShellCommand command)
        throws ShellUsageException {
        List<RepNodeId> repIds = currentTopo.getSortedRepNodeIds(failedShard);
        List<RepNodeId> runningRepNodes = new ArrayList<RepNodeId>();
        for (RepNodeId repId : repIds) {
            final ServiceChange change = statusMap.get(repId);
            if (ServiceStatus.RUNNING.equals(change.getStatus())) {
                /* RepNode is running */
                runningRepNodes.add(repId);
            }
        }

        if (!runningRepNodes.isEmpty()) {
            throw new ShellUsageException("Error removing failed RepNodes: " +
                "some nodes were found running: " + runningRepNodes,
                command);
        }
    }
}
