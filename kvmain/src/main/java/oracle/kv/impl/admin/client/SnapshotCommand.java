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

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.Snapshot.SnapResult;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.ObjectNode;

/*
 * Subcommands of snapshot
 *   create
 *   remove
 */
class SnapshotCommand extends CommandWithSubs {
    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new CreateSnapshotSub(),
                                     new RemoveSnapshotSub());

    SnapshotCommand() {
        super(subs, "snapshot", 3, 2);
    }

    @Override
    protected String getCommandOverview() {
        return "The snapshot command encapsulates commands that create and " +
            "delete snapshots," + eol + "which are used for backup and " +
            "restore.";
    }

    @POST
    static class CreateSnapshotSub extends SnapshotSub {

        CreateSnapshotSub() {
            super("create", 3, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "snapshot create -name <name> " +
                   "[-zn <id> | -znname <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Creates a new snapshot using the specified name as " +
                "the prefix. If a zone with the specified id or name is " +
                "specified then the command applies to all the SNs " +
                "executing in that zone. Snapshot of configurations will " +
                "backup for related SNs in zones";
        }
    }

    @POST
    static class RemoveSnapshotSub extends SnapshotSub {

        RemoveSnapshotSub() {
            super("remove", 3, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "snapshot remove {-name <name> | -all} [-zn <id> |" +
                   " -znname <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Removes the named snapshot.  If -all is specified " +
                "remove all snapshots. If a zone with the specified id or " +
                "name is specified then the command applies to all the SNs " +
                "executing in that zone. Snapshot of configurations will be " +
                "removed for related SNs in zones";
        }
    }

    abstract static class SnapshotSub extends SubCommand {
        final boolean isCreate;
        protected SnapshotSub(String name, int prefixMatchLength,
                              boolean isCreate) {
            super(name, prefixMatchLength);
            this.isCreate = isCreate;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new SnapshotCommandExecutor<String>() {

                @Override
                public String createSnapshotResult(Snapshot snapshot,
                                                   String newSnapName,
                                                   String zoneInfo,
                                                   DatacenterId dcId) {
                    String output = "Created data snapshot named " +
                                    newSnapName;
                    int numSuccess = snapshot.getSuccesses().size();
                    int numFailure = snapshot.getFailures().size();

                    if (dcId != null) {
                        output += " in zone " + zoneInfo;
                    }

                    /* Report snapshot data results */
                    if (snapshot.succeeded()) {
                        output += " on " + "all " + numSuccess +
                                  " components";
                    } else if (snapshot.getQuorumSucceeded()) {
                        output += " on " + numSuccess + " " +
                                  "components but failed to create snapshot " +
                                  "on " + numFailure + " components (" +
                                  "snapshot created on quorum RNs)";
                    } else {
                        output += " on " + numSuccess + " " +
                                  "components but failed to create snapshot " +
                                  "on " + numFailure + " components (" +
                                  "snapshot not created on quorum RNs)";
                    }

                    /* Report snapshot data success results */
                    if (!snapshot.getSuccesses().isEmpty()) {
                        output += eol;
                        final StringBuffer compList = new StringBuffer();
                        for (SnapResult sr : snapshot.getSuccesses()) {
                            compList.append(", ");
                            compList.append(sr.getService().toString());
                        }
                        output += "Successfully created snapshot on RNs: " +
                            compList.substring(2);
                    }

                    /* Report snapshot config success results */
                    if (!snapshot.getConfigSuccesses().isEmpty()) {
                        output += eol;
                        final StringBuffer compList = new StringBuffer();
                        for (SnapResult sr : snapshot.getConfigSuccesses()) {
                            compList.append(", ");
                            compList.append(sr.getService().toString());
                        }
                        output += "Successfully backed up configurations on: " +
                            compList.substring(2);
                    }

                    /* Report snapshot data failure results */
                    if (!snapshot.getFailures().isEmpty()) {
                        output += eol;
                        final StringBuffer compList = new StringBuffer();
                        for (SnapResult sr : snapshot.getFailures()) {
                            compList.append(", ");
                            compList.append(sr.getService().toString());
                            if (sr.getException() != null) {
                                final String message =
                                    sr.getException().getMessage();
                                if (message != null) {
                                    compList.append(" (Reason: ");
                                    compList.append(message.trim() + ")");
                                }
                            }
                            compList.append(eol);
                        }
                        output += "Failed to create snapshot on RNs: " +
                            compList.substring(2);
                    }

                    /* Report snapshot config failure results */
                    if (!snapshot.getConfigFailures().isEmpty()) {
                        output += eol;
                        final StringBuffer compList = new StringBuffer();
                        for (SnapResult sr : snapshot.getConfigFailures()) {
                            compList.append(", ");
                            compList.append(sr.getService().toString());
                        }
                        output += "Failed to backup configurations on: " +
                            compList.substring(2);
                    }

                    return output;
                }

                @Override
                public String removeAllSnapshotResult(Snapshot snapshot,
                                                      String zoneInfo,
                                                      DatacenterId dcId) {
                    String output = "";
                    if (snapshot.succeeded()) {
                        output = "Removed all snapshots";
                        if (dcId != null) {
                            output += " in zone " + zoneInfo;
                        }
                    }
                    return output;
                }

                @Override
                public String removeOneSnapshotResult(Snapshot snapshot,
                                                      String zoneInfo,
                                                      DatacenterId dcId,
                                                      String snapName) {
                    String output = "";
                    if (snapshot.succeeded()) {
                        output = "Removed snapshot " + snapName;
                        if (dcId != null) {
                            output += " in zone " + zoneInfo;
                        }
                    }
                    return output;
                }
            }.commonExecute(args, shell);
        }

        private abstract class
            SnapshotCommandExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, SnapshotSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                final String cannotMixMsg = "Use either -zn or -znname";
                String snapName = null;
                String zoneId = null;
                String zoneName = null;
                boolean removeAll = false;
                String zoneInfo = "";

                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        snapName = Shell.nextArg(args, i++, SnapshotSub.this);
                    } else if ("-all".equals(arg)) {
                        removeAll = true;
                    } else if ("-zn".equals(arg)) {
                        zoneId = Shell.nextArg(args, i++, SnapshotSub.this);
                        if (zoneName != null) {
                            throw new ShellUsageException(
                                cannotMixMsg, SnapshotSub.this);
                        }
                    /* Parse -zname because it was released by accident */
                    } else if ("-zname".equals(arg) || "-znname".equals(arg)) {
                        zoneName = Shell.nextArg(args, i++, SnapshotSub.this);
                        if (zoneId != null) {
                            throw new ShellUsageException(
                                cannotMixMsg, SnapshotSub.this);
                        }
                    }
                    else {
                        shell.unknownArgument(arg, SnapshotSub.this);
                    }
                }

                if (snapName == null && !removeAll) {
                    shell.requiredArg("-name", SnapshotSub.this);
                }

                try {
                    /* do not internally print in JSON mode */
                    Snapshot snapshot =
                        new Snapshot(cs,
                                     shell.getJson() ?
                                         false : shell.getVerbose(),
                                     shell.getJson() ?
                                         null : shell.getOutput());
                    final Topology topology = cs.getTopology();
                    DatacenterId dcId = null;
                    if (zoneId != null) {
                        dcId = DatacenterId.parse(zoneId);
                        Datacenter dc = topology.get(dcId);
                        if (dc == null) {
                            throw new IllegalArgumentException(
                                "The specified zone id does not exist");
                        }
                        zoneInfo +=
                            "zn:[id=" + zoneId + " name=" + dc.getName() +
                            "]";
                    }
                    if (zoneName != null) {
                        Datacenter zone = topology.getDatacenter(zoneName);
                        if (zone == null) {
                            throw new IllegalArgumentException(
                                "The specified zone name does not exist");
                        }
                        dcId = zone.getResourceId();
                        zoneInfo += "zn:[id=" + dcId.getDatacenterId() +
                                    " name=" + zoneName + "]";
                    }
                    if (isCreate) {
                        if (removeAll) {
                            invalidArgument("-all");
                        }
                        String newSnapName = null;
                        if (dcId != null) {
                            newSnapName =
                                snapshot.createSnapshot(snapName, dcId);
                        } else {
                            newSnapName = snapshot.createSnapshot(snapName);
                        }

                        return createSnapshotResult(
                            snapshot, newSnapName, zoneInfo, dcId);
                    }
                    if (removeAll) {
                        if (snapName != null) {
                            invalidArgument("-all");
                        }
                        if (dcId != null) {
                            snapshot.removeAllSnapshots(dcId);
                        } else {
                            snapshot.removeAllSnapshots();
                        }
                        return removeAllSnapshotResult(
                            snapshot, zoneInfo, dcId);
                    }
                    if (dcId != null) {
                        snapshot.removeSnapshot(snapName, dcId);
                    } else {
                        snapshot.removeSnapshot(snapName);
                    }
                    return removeOneSnapshotResult(
                        snapshot, zoneInfo, dcId, snapName);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                } catch (IllegalArgumentException iae) {
                    throw new ShellUsageException(
                        iae.getMessage(), SnapshotSub.this);
                } catch (RuntimeException re) {
                    /* re-throw runtime exception */
                    throw re;
                } catch (Exception e) {
                    /* handle unknown exception */
                    throw new ShellException(
                        "Unknown exception: " + e.toString(),
                        ErrorMessage.NOSQL_5500,
                        CommandResult.NO_CLEANUP_JOBS);
                }
                return null;
            }

            public abstract T createSnapshotResult(Snapshot snapshot,
                                                   String newSnapName,
                                                   String zoneInfo,
                                                   DatacenterId dcId);

            public abstract T removeAllSnapshotResult(Snapshot snapshot,
                                                      String zoneInfo,
                                                      DatacenterId dcId);

            public abstract T removeOneSnapshotResult(Snapshot snapshot,
                                                      String snapName,
                                                      DatacenterId dcId,
                                                      String zoneInfo);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("snapshot operation");
            return new SnapshotCommandExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    createSnapshotResult(Snapshot snapshot,
                                         String newSnapName,
                                         String zoneInfo,
                                         DatacenterId dcId) {
                    final ObjectNode top = JsonUtils.createObjectNode();
                    top.put("snapshotName", newSnapName);
                    final ArrayNode successArray =
                        top.putArray("successSnapshots");
                    for (SnapResult sr : snapshot.getSuccesses()) {
                        successArray.add(sr.getService().toString());
                    }
                    final ArrayNode failureArray =
                        top.putArray("failureSnapshots");
                    for (SnapResult sr : snapshot.getFailures()) {
                        failureArray.add(sr.getService().toString());
                    }

                    final ArrayNode successConfigArray =
                        top.putArray("successSnapshotConfigs");
                    for (SnapResult sr : snapshot.getConfigSuccesses()) {
                        successConfigArray.add(sr.getService().toString());
                    }
                    final ArrayNode failureConfigArray =
                        top.putArray("failureSnapshotConfigs");
                    for (SnapResult sr : snapshot.getConfigFailures()) {
                        failureConfigArray.add(sr.getService().toString());
                    }
                    if (snapshot.succeeded()) {
                        scr.setReturnCode(ErrorMessage.NOSQL_5000.getValue());
                    } else if (snapshot.getQuorumSucceeded()) {
                        scr.setReturnCode(ErrorMessage.NOSQL_5301.getValue());
                        scr.setDescription("Created complete snapshot but " +
                                           "the snapshot operation failed " +
                                           "on some nodes");
                    } else {
                        scr.setReturnCode(ErrorMessage.NOSQL_5300.getValue());
                        scr.setDescription("Snapshot failed");
                    }
                    scr.setReturnValue(top);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    removeAllSnapshotResult(Snapshot snapshot,
                                            String zoneInfo,
                                            DatacenterId dcId) {
                    if (snapshot.succeeded()) {
                        return scr;
                    }
                    scr.setReturnCode(ErrorMessage.NOSQL_5500.getValue());
                    return scr;
                }

                @Override
                public ShellCommandResult
                    removeOneSnapshotResult(Snapshot snapshot,
                                            String zoneInfo,
                                            DatacenterId dcId,
                                            String snapName) {
                    if (snapshot.succeeded()) {
                        return scr;
                    }
                    scr.setReturnCode(ErrorMessage.NOSQL_5500.getValue());
                    return scr;
                }
            }.commonExecute(args, shell);
        }
    }
}
