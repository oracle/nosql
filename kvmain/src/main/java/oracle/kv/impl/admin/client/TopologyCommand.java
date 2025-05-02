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
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandSucceeds;
import oracle.kv.impl.admin.CommandResult.CommandWarns;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/*
 * Subcommands of topology
 */
class TopologyCommand extends CommandWithSubs {

    private static final List<? extends SubCommand> subs = Arrays.asList(
        new TopologyChangeRFSub(),            /* change-repfactor */
        new TopologyChangeZoneArbitersSub(),  /* change-zone-arbiters */
        new TopologyChangeZoneAffinitySub(),  /* change-zone-master-affinity */
        new TopologyChangeZoneTypeSub(),      /* change-zone-type */
        new TopologyCloneSub(),               /* clone */
        new TopologyContractSub(),            /* contract */
        new TopologyCreateSub(),              /* create */
        new TopologyDeleteSub(),              /* delete */
        new TopologyListSub(),                /* list */
        new TopologyMovePartitionSub(),       /* move-partition */
        new TopologyMoveRNSub(),              /* move-repnode */
        new TopologyPreviewSub(),             /* preview */
        new TopologyRebalanceSub(),           /* rebalance */
        new TopologyRedistributeSub(),        /* redistribute */
        new TopologyRemoveShardSub(),         /* remove-shard */
        new TopologyValidateSub(),            /* validate */
        new TopologyViewSub()                 /* view */
        );

    private static final String TOPOLOGY_COMMAND_NAME = "topology";

    /**
     * A warning message to be displayed when commands attempt to create
     * topology candidates whose names contain the substring reserved for
     * system use.
     */
    public static final String RESERVED_CANDIDATE_NAME_WARNING =
        "Warning: The topology candidate name contains '" +
        TopologyCandidate.RESERVED_SUBSTRING +
        "', which is reserved for" + eol + " system use." + eol + eol;

    TopologyCommand() {
        super(subs,
              TOPOLOGY_COMMAND_NAME,
              4,  /* prefix length */
              0); /* min args -- let subs control it */
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates commands that manipulate store topologies." + eol +
            "Examples are " +
            "redistribution/rebalancing of nodes or changing replication" +
            eol + "factor.  Topologies are created and modified using this " +
            "command.  They" + eol + "are then deployed by using the " +
            "\"plan deploy-topology\" command.";
    }

    private static ObjectNode readObjectValue(String input)
        throws ShellException {
        return CommandJsonUtils.handleConversionFailure(
            (CommandJsonUtils.JsonConversionTask<ObjectNode>)() -> {
                return CommandJsonUtils.
                           readObjectValue(input);
        });
    }

    @POST
    static class TopologyChangeRFSub extends SubCommand {

        final static String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        TopologyChangeRFSub() {
            super("change-repfactor", 8);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyChangeRFExecutor<String>() {

                @Override
                public String successMessage(String message,
                                             String deprecatedDcFlagPrefix) {
                    return deprecatedDcFlagPrefix + message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyChangeRFExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyChangeRFSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String poolName = null;
                DatacenterId dcid = null;
                String dcName = null;
                int rf = -1;
                boolean deprecatedDcFlag = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++, TopologyChangeRFSub.this);
                    } else if ("-pool".equals(arg)) {
                        poolName =
                            Shell.nextArg(args, i++, TopologyChangeRFSub.this);
                    } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                        dcid =
                            parseDatacenterId(
                                Shell.nextArg(
                                    args, i++, TopologyChangeRFSub.this));
                        if (CommandUtils.
                                isDeprecatedDatacenterId(arg, args[i])) {
                            deprecatedDcFlag = true;
                        }
                    } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                        dcName =
                            Shell.nextArg(args, i++, TopologyChangeRFSub.this);
                        if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                            deprecatedDcFlag = true;
                        }
                    } else if ("-rf".equals(arg)) {
                        String rfString =
                            Shell.nextArg(args, i++, TopologyChangeRFSub.this);
                        rf = parseUnsignedInt(rfString);
                    } else {
                        shell.unknownArgument(arg, TopologyChangeRFSub.this);
                    }
                }
                if (topoName == null || poolName == null || rf < 0 ||
                    (dcid == null && dcName == null)) {
                    shell.requiredArg(null, TopologyChangeRFSub.this);
                }
                final String deprecatedDcFlagPrefix =
                    !deprecatedDcFlag ? "" : dcFlagsDeprecation;
                try {
                    if (dcid == null) {
                        dcid =
                            CommandUtils.getDatacenterId(
                                dcName, cs, TopologyChangeRFSub.this);
                    } else {
                        CommandUtils.ensureDatacenterExists(
                            dcid, cs, TopologyChangeRFSub.this);
                    }
                    CommandUtils.validatePool(
                        poolName, cs, TopologyChangeRFSub.this);
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyChangeRFSub.this);
                    final Datacenter candidateZone =
                        cs.getTopologyCandidate(topoName)
                        .getTopology().getDatacenterMap().get(dcid);
                    /*
                     * TODO: This RF check is performed on the server as of the
                     * 23.1 release. Remove the check once we know all servers
                     * have been upgraded.
                     */
                    try {
                        Rules.validateChangeReplicationFactor(
                            rf, candidateZone);
                    } catch (IllegalCommandException e) {
                        throw new ShellArgumentException(e.getMessage());
                    }
                    try {
                        return successMessage(
                            cs.changeRepFactor(topoName, poolName, dcid, rf),
                            deprecatedDcFlagPrefix);
                    } catch (AdminFaultException afe) {
                        convertIllegalCommandException(afe);
                        throw afe;
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                successMessage(String message, String deprecatedDcFlagPrefix);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                 ShellCommandResult.getDefault("topology change rf");
            return new TopologyChangeRFExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    successMessage(String message,
                                   String deprecatedDcFlagPrefix) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology change-repfactor -name <name> -pool " +
                "<pool name>" + eolt + "-zn <id> | -znname <name> -rf " +
                "<replication factor> " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to change the replication factor of " +
                "the specified" + eolt +
                "zone to a new value. The replication factor cannot be " +
                "decreased for a" + eolt +
                "primary zone, but can be decreased for a secondary zone.";
        }
    }

    @POST
    static class TopologyChangeZoneTypeSub extends SubCommand {

        TopologyChangeZoneTypeSub() {
            super("change-zone-type", 13);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyChangeZoneTypeExecutor<String>() {
                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyChangeZoneTypeExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyChangeZoneTypeSub.this);
                final CommandShell cmd = (CommandShell)shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                DatacenterId dcid = null;
                DatacenterType type = null;
                String dcName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneTypeSub.this);
                    } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                        dcid = parseDatacenterId(
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneTypeSub.this));
                    } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                        dcName =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneTypeSub.this);
                    } else if ("-type".equals(arg)) {
                        final String typeValue =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneTypeSub.this);
                        type = parseDatacenterType(typeValue);
                    } else {
                        shell.unknownArgument(arg,
                                              TopologyChangeZoneTypeSub.this);
                    }
                }
                if (topoName == null || type == null ||
                    (dcid == null && dcName == null)) {
                    shell.requiredArg(null, TopologyChangeZoneTypeSub.this);
                }

                try {
                    if (dcid == null) {
                        dcid =
                            CommandUtils.getDatacenterId(
                                dcName, cs, TopologyChangeZoneTypeSub.this);
                    } else {
                        CommandUtils.ensureDatacenterExists(
                            dcid, cs, TopologyChangeZoneTypeSub.this);
                    }
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyChangeZoneTypeSub.this);
                    return successMessage(
                        cs.changeZoneType(topoName, dcid, type));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology change zone type");
            return new TopologyChangeZoneTypeExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology change-zone-type -name <name> " + eolt +
                   "{-zn <id> | -znname <name>} " +
                   "-type {primary | secondary} " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to change the type of the specified " +
                "zone to a new type." + eolt +
                "Removes master affinity and removes allowing arbiters for " +
                "the zone if the type of" + eolt +
                "the zone is changed to secondary. Adds allowing arbiters " +
                "if the zone type is" + eolt +
                "changed to primary and the zone has replication factor 0.";
        }
    }

    @SuppressWarnings("null")
    @POST
    static class TopologyChangeZoneAffinitySub extends SubCommand {

        TopologyChangeZoneAffinitySub() {
            super("change-zone-master-affinity", 16);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyChangeZoneAffinityExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyChangeZoneAffinityExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyChangeZoneAffinitySub.this);
                final CommandShell cmd = (CommandShell)shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                DatacenterId dcid = null;
                Boolean masterAffinity = null;
                String dcName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneAffinitySub.this);
                    } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                        dcid =
                            parseDatacenterId(Shell.nextArg(
                                args, i++,
                                TopologyChangeZoneAffinitySub.this));
                    } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                        dcName =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneAffinitySub.this);
                    } else if ("-master-affinity".equals(arg)) {
                        masterAffinity = true;
                    } else if ("-no-master-affinity".equals(arg)) {
                        masterAffinity = false;
                    } else {
                        shell.unknownArgument(
                            arg, TopologyChangeZoneAffinitySub.this);
                    }
                }
                if (topoName == null || masterAffinity == null ||
                    (dcid == null && dcName == null)) {
                    shell.requiredArg(
                        null, TopologyChangeZoneAffinitySub.this);
                }

                try {
                    if (dcid == null) {
                        dcid =
                            CommandUtils.getDatacenterId(
                                dcName, cs,
                                TopologyChangeZoneAffinitySub.this);
                    } else {
                        CommandUtils.ensureDatacenterExists(
                            dcid, cs, TopologyChangeZoneAffinitySub.this);
                    }
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyChangeZoneAffinitySub.this);
                    try {
                        return successMessage(
                            cs.changeZoneMasterAffinity(topoName, dcid,
                                                        masterAffinity));
                    } catch (AdminFaultException afe) {
                        convertIllegalCommandException(afe);
                        throw afe;
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("change zone master affinity");
            return
                new TopologyChangeZoneAffinityExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology change-zone-master-affinity -name <name> " + eolt +
                "{-zn <id> | -znname <name>} " + eolt +
                "{-master-affinity | -no-master-affinity} " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to change the master affinity of the " +
                "specified zone." + eolt +
                "Master affinity is only allowed for primary zones with a " +
                "replication" + eolt +
                "factor greater than 0.";
        }
    }

    @SuppressWarnings("null")
    @POST
    static class TopologyChangeZoneArbitersSub extends SubCommand {

        TopologyChangeZoneArbitersSub() {
            super("change-zone-arbiters", 13);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyChangeZoneArbitersExecutor<String>() {
                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyChangeZoneArbitersExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyChangeZoneArbitersSub.this);
                final CommandShell cmd = (CommandShell)shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                DatacenterId dcid = null;
                Boolean allowArbiters = null;
                String dcName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneArbitersSub.this);
                    } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                        dcid =
                            parseDatacenterId(
                                Shell.nextArg(
                                    args, i++,
                                    TopologyChangeZoneArbitersSub.this));
                    } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                        dcName =
                            Shell.nextArg(args, i++,
                                          TopologyChangeZoneArbitersSub.this);
                    } else if ("-arbiters".equals(arg)) {
                        allowArbiters = true;
                    } else if ("-no-arbiters".equals(arg)) {
                       allowArbiters = false;
                    } else {
                        shell.unknownArgument(
                            arg, TopologyChangeZoneArbitersSub.this);
                    }
                }
                if (topoName == null || allowArbiters == null ||
                    (dcid == null && dcName == null)) {
                    shell.requiredArg(
                        null, TopologyChangeZoneArbitersSub.this);
                }

                try {
                    if (dcid == null) {
                        dcid =
                            CommandUtils.getDatacenterId(
                                dcName, cs,
                                TopologyChangeZoneArbitersSub.this);
                    } else {
                        CommandUtils.
                            ensureDatacenterExists(
                                dcid, cs,
                                TopologyChangeZoneArbitersSub.this);
                    }
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyChangeZoneArbitersSub.this);
                    try {
                        return successMessage(
                            cs.changeZoneArbiters(topoName, dcid,
                                                  allowArbiters));
                    } catch (AdminFaultException afe) {
                        convertIllegalCommandException(afe);
                        throw afe;
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology change zone arbiters");
            return
                new TopologyChangeZoneArbitersExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology change-zone-arbiters -name <name> " + eolt +
                   "{-zn <id> | -znname <name>} " +
                   "{-arbiters | -no-arbiters} " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to change whether the specified zone " +
                "allows arbiters." + eolt +
                "Arbiters are only allowed for primary zones, and are " +
                "required for primary zones" + eolt +
                "with replication factor 0.";
        }
    }

    @POST
    static class TopologyCloneSub extends SubCommand {

        TopologyCloneSub() {
            super("clone", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyCloneExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyCloneExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyCloneSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String fromName = null;
                boolean isCurrent = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++, TopologyCloneSub.this);
                    } else if ("-from".equals(arg)) {
                        fromName =
                            Shell.nextArg(args, i++, TopologyCloneSub.this);
                    } else if ("-current".equals(arg)) {
                        isCurrent = true;
                    } else {
                        shell.unknownArgument(arg, TopologyCloneSub.this);
                    }
                }
                if (topoName == null || (fromName == null && !isCurrent)) {
                    shell.requiredArg(null, TopologyCloneSub.this);
                    throw new AssertionError("Not reached");
                }

                final String reservedWarning =
                    topoName.contains(TopologyCandidate.RESERVED_SUBSTRING) ?
                    RESERVED_CANDIDATE_NAME_WARNING : "";
                try {
                    if (isCurrent) {
                        return successMessage(
                            reservedWarning +
                            cs.copyCurrentTopology(topoName));
                    }
                    CommandUtils.ensureTopoExists(
                        fromName, cs, TopologyCloneSub.this);
                    return successMessage(
                        reservedWarning +
                        cs.copyTopology(fromName, topoName));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                    throw new AssertionError("Not reached");
                }
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology clone");
            return new TopologyCloneExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology clone -from <from topology> -name " +
                "<to topology> or "+
                eolt + "topology clone -current -name <toTopology> " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Clones an existing topology so as to create a new " +
                "candidate topology " + eolt +
                "to be used for topology change operations.";
        }
    }

    @POST
    static class TopologyCreateSub extends SubCommand {

        TopologyCreateSub() {
            super("create", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyCreateExecutor<String>() {

                @Override
                public String topologyCandidateResult(CommandServiceAPI cs,
                                                      String poolName,
                                                      String topoName,
                                                      int numPartitions,
                                                      Shell commandShell)
                    throws ShellException {
                    CommandShell cmd = (CommandShell)commandShell;
                    String returnValue = "";
                    try {
                        CommandUtils.validatePool(poolName, cs,
                                                  TopologyCreateSub.this);
                        returnValue = cs.createTopology(topoName, poolName,
                                                        numPartitions,
                                                        shell.getJson());
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                        throw new AssertionError("Not reached");
                    }
                    final boolean isReservedName =
                        topoName.contains(
                            TopologyCandidate.RESERVED_SUBSTRING);
                    if (shell.getJson()) {
                        String operation = TOPOLOGY_COMMAND_NAME + " " +
                            getCommandName();
                        CommandResult result;
                        if (isReservedName) {
                            result =
                                new CommandWarns(
                                    RESERVED_CANDIDATE_NAME_WARNING,
                                    returnValue);
                        } else {
                            result = new CommandSucceeds(returnValue);
                        }
                        return Shell.toJsonReport(operation, result);
                    }
                    if (isReservedName) {
                        return RESERVED_CANDIDATE_NAME_WARNING + returnValue;
                    }
                    return returnValue;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyCreateExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyCreateSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String poolName = null;
                int numPartitions = 0;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++, TopologyCreateSub.this);
                    } else if ("-pool".equals(arg)) {
                        poolName =
                            Shell.nextArg(args, i++, TopologyCreateSub.this);
                    } else if ("-partitions".equals(arg)) {
                        String partString =
                            Shell.nextArg(args, i++, TopologyCreateSub.this);
                        numPartitions = parseUnsignedInt(partString);
                    } else {
                        shell.unknownArgument(arg, TopologyCreateSub.this);
                    }
                }
                if (topoName == null || poolName == null ||
                    numPartitions == 0) {
                    shell.requiredArg(null, TopologyCreateSub.this);
                    throw new AssertionError("Not reached");
                }

                return topologyCandidateResult(
                    cs, poolName, topoName, numPartitions, shell);
            }

            public abstract T
                topologyCandidateResult(CommandServiceAPI cs,
                                        String poolName,
                                        String topoName,
                                        int numPartitions, Shell shell)
                throws ShellException;
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology create");
            return new TopologyCreateExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult
                    topologyCandidateResult(CommandServiceAPI cs,
                                            String poolName,
                                            String topoName,
                                            int numPartitions,
                                            Shell commandShell)
                    throws ShellException {
                    CommandShell cmd = (CommandShell)commandShell;
                    try {
                        CommandUtils.validatePool(poolName, cs,
                                                  TopologyCreateSub.this);
                        final String serverJson =
                            cs.createTopology(
                                topoName, poolName,
                                numPartitions, true,
                                SerialVersion.ADMIN_CLI_JSON_V2_VERSION);

                        final ObjectNode result = readObjectValue(serverJson);
                        final boolean isReservedName =
                            topoName.contains(
                                TopologyCandidate.RESERVED_SUBSTRING);
                        if (isReservedName) {
                            scr.setDescription(
                                RESERVED_CANDIDATE_NAME_WARNING);
                        }
                        scr.setReturnValue(result);
                        return scr;
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                        throw new AssertionError("Not reached");
                    }
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology create -name <candidate name> -pool " +
                   "<pool name>" + eolt + "-partitions <num> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Creates a new topology with the specified number of " +
                "partitions" + eolt + "using the specified storage pool.";
        }
    }

    @POST
    static class TopologyDeleteSub extends SubCommand {

        TopologyDeleteSub() {
            super("delete", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyDeleteExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyDeleteExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyDeleteSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++, TopologyDeleteSub.this);
                    } else {
                        shell.unknownArgument(arg, TopologyDeleteSub.this);
                    }
                }
                if (topoName == null) {
                    shell.requiredArg("-name", TopologyDeleteSub.this);
                }

                try {
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyDeleteSub.this);
                    return successMessage(cs.deleteTopology(topoName));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology delete");
            return new TopologyDeleteExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology delete -name <name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Deletes a topology.";
        }
    }

    @POST
    static class TopologyListSub extends SubCommand {

        TopologyListSub() {
            super("list", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyListExecutor<String>() {

                @Override
                public String topologyList(List<String> topos,
                                           boolean showHidden) {
                    StringBuilder sb = new StringBuilder();
                    for (String oneTopo : topos) {
                        if (!showHidden &&
                            oneTopo.startsWith(
                                TopologyCandidate.INTERNAL_NAME_PREFIX)) {
                            continue;
                        }
                        sb.append(oneTopo).append(eol);
                    }
                    return sb.toString();
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyListExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyListSub.this);
                if (args.length > 1) {
                    shell.unknownArgument(args[1], TopologyListSub.this);
                }
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                final boolean showHidden = cmd.getHidden();
                try {
                    List<String> topos = cs.listTopologies();
                    Collections.sort(topos);
                    return topologyList(topos, showHidden);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T topologyList(List<String> topos,
                                           boolean showHidden);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology list");
            return new TopologyListExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    topologyList(List<String> topos, boolean showHidden) {
                    final ObjectNode top = JsonUtils.createObjectNode();
                    final ArrayNode topoArray = top.putArray("topologies");
                    for (String oneTopo : topos) {
                        if (!showHidden &&
                            oneTopo.startsWith(
                                TopologyCandidate.INTERNAL_NAME_PREFIX)) {
                            continue;
                        }
                        topoArray.add(oneTopo);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology list " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Lists existing topologies.";
        }
    }

    @POST
    static class TopologyMovePartitionSub extends SubCommand {

        TopologyMovePartitionSub() {
            super("move-partition", 6);
        }

        @Override
        protected boolean isHidden() {
            return true;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyMovePartitionExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyMovePartitionExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                    throws ShellException {
                Shell.checkHelp(args, TopologyMovePartitionSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                RepGroupId rgId = null;
                PartitionId partId = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(
                                    args, i++, TopologyMovePartitionSub.this);
                    } else if ("-partition".equals(arg)) {
                        String partString =
                             Shell.nextArg(
                                     args, i++, TopologyMovePartitionSub.this);
                        try {
                            partId = new PartitionId(
                                                Integer.parseInt(partString));
                        } catch (NumberFormatException nfe) {
                            throw new ShellArgumentException(
                                "Invalid partition id: " + partString);
                        }
                    } else if ("-shard".equals(arg)) {
                        String rgString =
                             Shell.nextArg(
                                     args, i++, TopologyMovePartitionSub.this);
                        try {
                            rgId = RepGroupId.parse(rgString);
                        } catch (IllegalArgumentException iae) {
                            throw new ShellArgumentException(
                                "Invalid shard id: " + rgString);
                        }
                    }
                }
                if (topoName == null || partId == null || rgId == null) {
                    shell.requiredArg(null, TopologyMovePartitionSub.this);
                }

                try {
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyMovePartitionSub.this);
                    CommandUtils.ensurePartitionExists(
                        partId, cs, TopologyMovePartitionSub.this);
                    CommandUtils.ensureShardExists(
                        rgId, cs, TopologyMovePartitionSub.this);

                    return successMessage(cs.movePartition(topoName,
                                                           partId, rgId));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }
            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
                throws ShellException {
            ShellCommandResult scr =
                ShellCommandResult.getDefault("topology move partition");
            return new TopologyMovePartitionExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology move-partition -name <name> -partition <n>" +
                   " -shard <id> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to move the specified partition to " +
                "to the" + eolt + "specified shard.";
        }
    }

    @POST
    static class TopologyMoveRNSub extends SubCommand {

        TopologyMoveRNSub() {
            super("move-repnode", 6);
        }

        @Override
        protected boolean isHidden() {
            return true;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyMoveRNExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyMoveRNExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyMoveRNSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                RepNodeId rnid = null;
                StorageNodeId snid = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(
                                args, i++, TopologyMoveRNSub.this);
                    } else if ("-rn".equals(arg)) {
                        String rnString =
                            Shell.nextArg(args, i++, TopologyMoveRNSub.this);
                        try {
                            rnid = RepNodeId.parse(rnString);
                        } catch (IllegalArgumentException iae) {
                            throw new ShellArgumentException(
                                "Invalid RepNode id: " + rnString);
                        }
                    } else if ("-sn".equals(arg)) {
                        String snString =
                            Shell.nextArg(args, i++, TopologyMoveRNSub.this);
                        try {
                            snid = StorageNodeId.parse(snString);
                        } catch (IllegalArgumentException iae) {
                            throw new ShellArgumentException(
                                "Invalid StorageNode id: " + snString);
                        }
                    } else {
                        shell.unknownArgument(arg, TopologyMoveRNSub.this);
                    }
                }
                if (topoName == null || rnid == null) {
                    shell.requiredArg(null, TopologyMoveRNSub.this);
                }

                try {
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyMoveRNSub.this);
                    CommandUtils.ensureRepNodeExists(
                        rnid, cs, TopologyMoveRNSub.this);
                    if (snid != null) {
                        CommandUtils.ensureStorageNodeExists(
                            snid, cs, TopologyMoveRNSub.this);
                    }
                    return successMessage(cs.moveRN(topoName, rnid, snid));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            ShellCommandResult scr =
                ShellCommandResult.getDefault("topology move repnode");
            return new TopologyMoveRNExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology move-repnode -name <name> -rn <id> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to move the specified RepNode to " +
                "an available" + eolt + "storage node chosen by the system.";
        }
    }

    @POST
    static class TopologyPreviewSub extends SubCommand {

        TopologyPreviewSub() {
            super("preview", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyPreviewExecutor<String>() {

                @Override
                public String topologyPreviewResult(CommandServiceAPI cs,
                                                    String topoName,
                                                    String startName,
                                                    boolean b)
                    throws RemoteException {
                    return cs.preview(topoName, startName, b);
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyPreviewExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyPreviewSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String startName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++, TopologyPreviewSub.this);
                    } else if ("-start".equals(arg)) {
                        startName =
                            Shell.nextArg(args, i++, TopologyPreviewSub.this);
                    } else {
                        shell.unknownArgument(arg, TopologyPreviewSub.this);
                    }
                }
                if (topoName == null) {
                    shell.requiredArg("-name", TopologyPreviewSub.this);
                }

                try {
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyPreviewSub.this);
                    if (startName != null) {
                        CommandUtils.ensureTopoExists(
                            startName, cs, TopologyPreviewSub.this);
                    }
                    return topologyPreviewResult(cs,
                        topoName, startName, shell.getVerbose());
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                topologyPreviewResult(
                    CommandServiceAPI cs, String topoName,
                    String startName, boolean b)
                throws RemoteException, ShellException;
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology preview");
            return new TopologyPreviewExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    topologyPreviewResult(CommandServiceAPI cs,
                                          String topoName,
                                          String startName,
                                          boolean b)
                    throws RemoteException, ShellException {
                    final String serverResult =
                        cs.preview(topoName, startName, b,
                                   SerialVersion.ADMIN_CLI_JSON_V2_VERSION);
                    final ObjectNode result = readObjectValue(serverResult);
                    scr.setReturnValue(result);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology preview -name <name> [-start <from topology>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Describes the actions that would be taken to transition " +
                "from the " + eolt + "starting topology to the named, target " +
                "topology. If -start is not " + eolt + "specified "  +
                "the current topology is used. This command should be used " +
                eolt +  "before deploying a new topology.";
        }
    }

    @POST
    static class TopologyRebalanceSub extends SubCommand {

        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        TopologyRebalanceSub() {
            super("rebalance", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyRebalanceExecutor<String>() {

                @Override
                public String successMessage(String message,
                                             String deprecatedDcFlagPrefix) {
                    return deprecatedDcFlagPrefix + message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyRebalanceExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyRebalanceSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String poolName = null;
                DatacenterId dcid = null;
                String dcName = null;
                boolean deprecatedDcFlag = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args,
                                          i++, TopologyRebalanceSub.this);
                    } else if ("-pool".equals(arg)) {
                        poolName =
                            Shell.nextArg(args,
                                          i++, TopologyRebalanceSub.this);
                    } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                        dcid =
                            parseDatacenterId(
                                Shell.nextArg(args, i++,
                                              TopologyRebalanceSub.this));
                        if (CommandUtils.
                                isDeprecatedDatacenterId(arg, args[i])) {
                            deprecatedDcFlag = true;
                        }
                    } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                        dcName =
                            Shell.nextArg(args, i++,
                                          TopologyRebalanceSub.this);
                        if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                            deprecatedDcFlag = true;
                        }
                    } else {
                        shell.unknownArgument(arg, TopologyRebalanceSub.this);
                    }
                }
                if (topoName == null || poolName == null) {
                    shell.requiredArg(null, TopologyRebalanceSub.this);
                }
                final String deprecatedDcFlagPrefix =
                    !deprecatedDcFlag ? "" : dcFlagsDeprecation;
                try {
                    CommandUtils.validatePool(
                        poolName, cs, TopologyRebalanceSub.this);
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyRebalanceSub.this);
                    if (dcName != null) {
                        dcid = CommandUtils.getDatacenterId(
                            dcName, cs, TopologyRebalanceSub.this);
                    }
                    if (dcid != null) {
                        CommandUtils.ensureDatacenterExists(
                            dcid, cs, TopologyRebalanceSub.this);
                    }
                    return successMessage(
                        cs.rebalanceTopology(topoName, poolName, dcid),
                        deprecatedDcFlagPrefix);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                successMessage(String message, String deprecatedDcFlagPrefix);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology rebalance");
            return new TopologyRebalanceExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    successMessage(String message,
                                   String deprecatedDcFlagPrefix) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology rebalance -name <name> -pool " +
                   "<pool name> [-zn <id> | -znname <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the named topology to create a \"balanced\" " +
                "topology. If the" + eolt + "optional -zn flag is used " +
                "only storage nodes from the specified" + eolt +
                "zone will be used for the operation.";
        }
    }

    @POST
    static class TopologyContractSub extends SubCommand {

        TopologyContractSub() {
            super("contract", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyContractExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyContractExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyContractSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String poolName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++, TopologyContractSub.this);
                    } else if ("-pool".equals(arg)) {
                        poolName =
                            Shell.nextArg(args, i++, TopologyContractSub.this);
                    } else {
                        shell.unknownArgument(arg, TopologyContractSub.this);
                    }
                }
                if (topoName == null || poolName == null) {
                    shell.requiredArg(null, TopologyContractSub.this);
                }

                String contractInfo = "";
                try {
                    CommandUtils.validatePool(
                        poolName, cs, TopologyContractSub.this);
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyContractSub.this);
                    contractInfo = cs.contractTopology(topoName, poolName);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }

                /* Check whether there are snapshots for the topology. */
                try {
                    String [] list = cs.listSnapshots(null);

                    if (list.length == 0) {
                        return successMessage(contractInfo);
                    }

                    String snapshotsList = "";
                    for (String ss : list) {
                        snapshotsList += ss + eol;
                    }
                    String warnMessage = null;
                    if (list.length == 1) {
                        warnMessage =
                            "Warning: the following snapshot will " +
                            "be removed, please backup it:";
                    } else {
                        warnMessage =
                            "Warning: the following snapshots will " +
                            "be removed, please backup them:";
                    }
                    return
                        successMessage(contractInfo + eol + eol +
                                       warnMessage + eol + snapshotsList);

                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology contract");
            return new TopologyContractExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology contract -name <name> -pool " +
                   "<pool name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Modifies the named topology to contract storage nodes.";
        }
    }

    @POST
    static class TopologyRedistributeSub extends SubCommand {

        TopologyRedistributeSub() {
            super("redistribute", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyRedistributeExecutor<String>() {

                @Override
                public String successMessage(String message) {
                    return message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyRedistributeExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyRedistributeSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                String poolName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++,
                                          TopologyRedistributeSub.this);
                    } else if ("-pool".equals(arg)) {
                        poolName =
                            Shell.nextArg(args, i++,
                                          TopologyRedistributeSub.this);
                    } else {
                        shell.unknownArgument(arg,
                                              TopologyRedistributeSub.this);
                    }
                }
                if (topoName == null || poolName == null) {
                    shell.requiredArg(null, TopologyRedistributeSub.this);
                }

                try {
                    CommandUtils.validatePool(
                        poolName, cs, TopologyRedistributeSub.this);
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyRedistributeSub.this);
                    return successMessage(
                        cs.redistributeTopology(topoName, poolName));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successMessage(String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            ShellCommandResult scr =
                ShellCommandResult.getDefault("topology redistribute");
            return new TopologyRedistributeExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult successMessage(String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology redistribute -name <name> -pool " +
                   "<pool name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the named topology to redistribute resources " +
                "to more efficiently" + eolt + "use those available.";
        }
    }

    @POST
    static class TopologyValidateSub extends SubCommand {

        TopologyValidateSub() {
            super("validate", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyValidateExecutor<String>() {

                @Override
                public String validateResult(CommandServiceAPI cs,
                                             String topoName)
                    throws RemoteException {
                    return cs.validateTopology(topoName);
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyValidateExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyValidateSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName =
                            Shell.nextArg(args, i++,
                                          TopologyValidateSub.this);
                    } else {
                        shell.unknownArgument(arg,
                                              TopologyValidateSub.this);
                    }
                }

                try {
                    if (topoName != null) {
                        CommandUtils.ensureTopoExists(
                            topoName, cs, TopologyValidateSub.this);
                    }
                    return validateResult(cs, topoName);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T validateResult(CommandServiceAPI cs,
                                             String topoName)
                throws RemoteException, ShellException;
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology validate");
            return new TopologyValidateExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    validateResult(CommandServiceAPI cs,
                                   String topoName)
                    throws RemoteException, ShellException {
                    final String serverResult =
                        cs.validateTopology(
                            topoName,
                            SerialVersion.ADMIN_CLI_JSON_V2_VERSION);
                    final ObjectNode result = readObjectValue(serverResult);
                    scr.setReturnValue(result);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology validate [-name <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Validates the specified topology. If no name is given, " +
                "the current " + eolt +
                "topology is validated. Validation will generate " +
                "\"violations\" and " + eolt + "\"notes\". Violations are " +
                "issues that can cause problems and should be " + eolt +
                "investigated. Notes are informational and highlight " +
                "configuration " + eolt +
                "oddities that could be potential issues or could be expected.";

        }
    }

    @POST
    static class TopologyViewSub extends SubCommand {

        TopologyViewSub() {
            super("view", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new TopologyViewExecutor<String>() {

                @Override
                public String topologyResult(TopologyCandidate tc,
                                             Parameters params,
                                             boolean verbose) {
                    return TopologyPrinter.printTopology(tc, params, verbose);
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyViewExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyViewSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                String topoName = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-name".equals(arg)) {
                        topoName = Shell.nextArg(
                            args, i++, TopologyViewSub.this);
                    } else {
                        shell.unknownArgument(arg, TopologyViewSub.this);
                    }
                }
                if (topoName == null) {
                    shell.requiredArg("-name", TopologyViewSub.this);
                }

                try {
                    CommandUtils.ensureTopoExists(
                        topoName, cs, TopologyViewSub.this);
                    TopologyCandidate tc = cs.getTopologyCandidate (topoName);
                    Parameters params = cs.getParameters();
                    return topologyResult(tc, params, shell.getVerbose());
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                topologyResult(TopologyCandidate tc,
                               Parameters params,
                               boolean verbose);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology view");
            return new TopologyViewExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    topologyResult(TopologyCandidate tc,
                                   Parameters params,
                                   boolean verbose) {
                    scr.setReturnValue(
                        TopologyPrinter.printTopologyJson(
                            tc.getTopology(), params, TopologyPrinter.all,
                            verbose));
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology view -name <name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays details of the specified topology.";
        }
    }

    @POST
    static class TopologyRemoveShardSub extends SubCommand {

        TopologyRemoveShardSub() {
            super("remove-shard", 9);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new TopologyRemoveShardExecutor<String>() {
                @Override
                public String successResult(String reservedWarning,
                                            String message) {
                    return reservedWarning + message;
                }
            }.commonExecute(args, shell);
        }

        private abstract class TopologyRemoveShardExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, TopologyRemoveShardSub.this);
                CommandShell cmd = (CommandShell)shell;
                CommandServiceAPI cs = cmd.getAdmin();
                RepGroupId failedShard = null;
                String toponame = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-failed-shard".equals(arg)) {
                        failedShard =
                            RepGroupId.parse(
                                Shell.nextArg(args, i++,
                                              TopologyRemoveShardSub.this));
                    } else if ("-name".equals(arg)) {
                        toponame =
                            Shell.nextArg(args, i++,
                                          TopologyRemoveShardSub.this);
                    }
                    else {
                        shell.unknownArgument(arg,
                                              TopologyRemoveShardSub.this);
                    }
                }
                if (toponame == null || failedShard == null) {
                    shell.requiredArg(null,
                                      TopologyRemoveShardSub.this);
                    throw new AssertionError("Not reached");
                }

                try {
                    /*
                     * Need to ensure the failed shardId exists in topology.
                     *
                     * Also raise warning message if user gives reserved topology
                     * candidate name for new topology.
                     */
                    final String reservedWarning =
                        toponame.contains(TopologyCandidate.RESERVED_SUBSTRING) ?
                        RESERVED_CANDIDATE_NAME_WARNING : "";
                    CommandUtils.ensureShardExists(
                        failedShard, cs, TopologyRemoveShardSub.this);
                    return successResult(reservedWarning,
                                         cs.removeFailedShard(
                                             failedShard, toponame));
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                successResult(String reservedWarning, String message);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("topology remove-shard");
            return new TopologyRemoveShardExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult
                    successResult(String reservedWarning,
                                  String message) {
                    scr.setDescription(message);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "topology remove-shard -failed-shard <shardId> -name " +
                   "<topology name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Removes a failed shard from a topology";
        }

        /**
         * Currently we have kept this as hidden command. Returning true.
         * If we decide to make this visible command then will return false.
         */
        @Override
        protected boolean isHidden() {
            return true;
        }
    }
}
