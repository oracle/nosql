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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandResult.CommandSucceeds;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

import oracle.nosql.common.json.ObjectNode;

/*
 * Subcommands of pool
 *   clone
 *   create
 *   remove
 *   join
 *   leave
 */
public class PoolCommand extends CommandWithSubs {
    public static final List<? extends SubCommand> subs =
                                       Arrays.asList(new ClonePool(),
                                                     new CreatePool(),
                                                     new RemovePool(),
                                                     new JoinPool(),
                                                     new LeavePool());
    private static final String POOL_COMMAND_NAME = "pool";

    /**
     * Pool names containing this substring are reserved for system use.
     */
    public static final String RESERVED_SUBSTRING = "$";

    /**
     * Pool names starting with this prefix are reserved for internal
     * system use.  These names are for storage node pools that are not
     * intended to be referred to by users.
     */
    public static final String INTERNAL_NAME_PREFIX = "SYS$";

    /**
     * A warning message to be displayed when commands attempt to create
     * storage node pool whose names contain the substring reserved for
     * system use.
     * 
     * TODO : Further plan is to raise error than warning and have
     * separate code path for user and system use. SR[#26554]
     */
    public static final String RESERVED_CANDIDATE_NAME_WARNING =
        "Warning: The storage node pool name contains '" +
        PoolCommand.RESERVED_SUBSTRING +
        "', which is reserved for" + eol + " system use." + eol + eol;

    PoolCommand() {
        super(subs, POOL_COMMAND_NAME, 3, 2);
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates commands that manipulates Storage Node pools, " +
               "which are used for" + eol + "resource allocations.";
    }

    @POST
    static class CreatePool extends SubCommandJsonConvert {

        CreatePool() {
            super("create", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String poolName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (poolName == null) {
                shell.requiredArg("-name", this);
                throw new AssertionError("Not reached");
            } else if (poolName.isBlank()){
                String info = "the value is empty or contain only blanks";
                shell.badArgUsage("-name", info, this);
                throw new AssertionError("Not reached");
            }

            String msg = poolName.contains(PoolCommand.RESERVED_SUBSTRING) ?
                RESERVED_CANDIDATE_NAME_WARNING : "";
            try {
                List<String> poolNames = cs.getStorageNodePoolNames();
                if (poolNames.indexOf(poolName) >= 0) {
                    msg += "Pool already exists: " + poolName;
                } else {
                    cs.addStorageNodePool(poolName);
                    msg += "Added pool " + poolName;
                  }
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            if (shell.getJson()) {
                String operation = POOL_COMMAND_NAME + " " + getCommandName();
                ObjectNode returnValue = JsonUtils.createObjectNode();
                returnValue.put("pool_name", poolName);
                CommandResult cmdResult =
                    new CommandSucceeds(returnValue.toString());
                return Shell.toJsonReport(operation, cmdResult);
            }
            return msg;
        }

        @Override
        protected String getCommandSyntax() {
            return "pool create -name <name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Creates a new Storage Node pool to be used for resource " +
                "distribution" + eolt + "when creating or modifying " +
                "a store.";
        }
    }

    @POST
    static class ClonePool extends SubCommandJsonConvert {

        ClonePool() {
            super("clone", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String poolName = null;
            String sourcePoolName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else if ("-from".equals(arg) ){
                    sourcePoolName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (poolName == null) {
                shell.requiredArg("-name", this);
            }
            if (sourcePoolName == null) {
                shell.requiredArg("-from", this);
            }
            @SuppressWarnings("null")
            String msg = poolName.contains(PoolCommand.RESERVED_SUBSTRING) ?
                RESERVED_CANDIDATE_NAME_WARNING : "";
            try {
                List<String> poolNames = cs.getStorageNodePoolNames();
                if (poolNames.indexOf(poolName) >= 0) {
                    msg += "Pool already exists: " + poolName;
                } else if (poolNames.indexOf(sourcePoolName) < 0) {
                    msg += "Source pool does not exist: " + sourcePoolName;
                    exitCode = Shell.EXIT_UNKNOWN;
                    if (shell.getJson()) {
                        String operation = POOL_COMMAND_NAME + " " +
                                           getCommandName();
                        CommandResult cmdResult =
                            new CommandFails(msg, ErrorMessage.NOSQL_5200,
                                             CommandResult.NO_CLEANUP_JOBS);
                        return Shell.toJsonReport(operation, cmdResult);
                    }
                    return msg;
                } else {
                    cs.cloneStorageNodePool(poolName, sourcePoolName);
                    msg += "Cloned pool " + poolName;
                }
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            if (shell.getJson()) {
                String operation = POOL_COMMAND_NAME + " " + getCommandName();
                ObjectNode returnValue = JsonUtils.createObjectNode();
                returnValue.put("pool_name", poolName);
                CommandResult cmdResult =
                    new CommandSucceeds(returnValue.toString());
                return Shell.toJsonReport(operation, cmdResult);
            }
            return msg;
        }

        @Override
        protected String getCommandSyntax() {
            return "pool clone -name <name> -from <source pool name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Clone an existing Storage Node pool so as to " + eolt +
                "a new Storage Node pool to be used for resource " + eolt +
                "distribution when creating or modifying a store.";
        }
    }

    @POST
    static class RemovePool extends SubCommandJsonConvert {

        RemovePool() {
            super("remove", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String poolName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (poolName == null) {
                shell.requiredArg("-name", this);
            }
            String msg = "";
            try {
                List<String> poolNames = cs.getStorageNodePoolNames();
                if (poolNames.indexOf(poolName) == -1) {
                    msg = "Pool does not exist: " + poolName;
                } else {
                    cs.removeStorageNodePool(poolName);
                    msg = "Removed pool " + poolName;
                }
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            if (shell.getJson()) {
                String operation = POOL_COMMAND_NAME + " " + getCommandName();
                ObjectNode returnValue = JsonUtils.createObjectNode();
                returnValue.put("pool_name", poolName);
                CommandResult cmdResult =
                    new CommandSucceeds(returnValue.toString());
                return Shell.toJsonReport(operation, cmdResult);
            }
            return msg;
        }

        @Override
        protected String getCommandSyntax() {
            return "pool remove -name <name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Removes a Storage Node pool.";
        }
    }

    @POST
    static class JoinPool extends SubCommandJsonConvert {

        JoinPool() {
            super("join", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String poolName = null;
            List<String> storageNodes = new ArrayList<String>();
            List<StorageNodeId> snids = new ArrayList<StorageNodeId>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else if ("-sn".equals(arg)) {
                    String sn = Shell.nextArg(args, i++, this);
                    try {
                        StorageNodeId snid = StorageNodeId.parse(sn);
                        snids.add(snid);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(sn);
                    }
                    storageNodes.add(sn);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (poolName == null) {
                shell.requiredArg("-name", this);
            }
            if (storageNodes.size() == 0) {
                shell.requiredArg("-sn", this);
            }
            try {
                final String operation = POOL_COMMAND_NAME + " " +
                    getCommandName();
                String msg = "";
                List<String> poolNames = cs.getStorageNodePoolNames();
                if (poolNames.indexOf(poolName) == -1) {
                    msg = "Pool does not exist: " + poolName;
                    exitCode = Shell.EXIT_UNKNOWN;
                    if (shell.getJson()) {
                        CommandResult cmdResult =
                            new CommandFails(msg, ErrorMessage.NOSQL_5200,
                                             CommandResult.NO_CLEANUP_JOBS);
                        return Shell.toJsonReport(operation, cmdResult);
                    }
                    return msg;
                }
                Topology t = cs.getTopology();

                /*
                 * If an unknown StorageNode is found, all earlier
                 * additions will still have worked.
                 */
                for (StorageNodeId snid : snids) {
                    if (t.get(snid) == null) {
                        msg = "Storage Node does not exist: " + snid;
                        exitCode = Shell.EXIT_UNKNOWN;
                        if (shell.getJson()) {
                            CommandResult cmdResult =
                                new CommandFails(msg, ErrorMessage.NOSQL_5200,
                                                 CommandResult.NO_CLEANUP_JOBS);
                            return Shell.toJsonReport(operation, cmdResult);
                        }
                        return msg;
                    }
                    cs.addStorageNodeToPool(poolName, snid);
                }

                if (shell.getJson()) {
                    ObjectNode returnValue = JsonUtils.createObjectNode();
                    returnValue.put("pool_name", poolName);
                    CommandSucceeds cmdResult =
                        new CommandSucceeds(returnValue.toString());
                    return Shell.toJsonReport(operation, cmdResult);
                }
                return "Added Storage Node(s) " + snids + " to pool " +
                       poolName;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "pool join -name <name> [-sn <snX>]* " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Adds Storage Nodes to an existing Storage Node pool.";
        }
    }

    @POST
    static class LeavePool extends SubCommandJsonConvert {

        LeavePool() {
            super("leave", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String poolName = null;
            List<String> storageNodes = new ArrayList<String>();
            List<StorageNodeId> snids = new ArrayList<StorageNodeId>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else if ("-sn".equals(arg)) {
                    String sn = Shell.nextArg(args, i++, this);
                    try {
                        StorageNodeId snid = StorageNodeId.parse(sn);
                        snids.add(snid);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(sn);
                    }
                    storageNodes.add(sn);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (poolName == null) {
                shell.requiredArg("-name", this);
            }
            if (storageNodes.size() == 0) {
                shell.requiredArg("-sn", this);
            }
            try {
                final String operation = POOL_COMMAND_NAME + " " +
                    getCommandName();
                String msg = "";
                List<String> poolNames = cs.getStorageNodePoolNames();
                if (poolNames.indexOf(poolName) == -1) {
                    msg = "Pool does not exist: " + poolName;
                    exitCode = Shell.EXIT_UNKNOWN;
                    if (shell.getJson()) {
                        CommandResult cmdResult =
                            new CommandFails(msg, ErrorMessage.NOSQL_5200,
                                             CommandResult.NO_CLEANUP_JOBS);
                        return Shell.toJsonReport(operation, cmdResult);
                    }
                    return msg;
                }
                Topology t = cs.getTopology();

                /*
                 * If an unknown StorageNode is found, all earlier
                 * additions will still have worked.
                 */
                for (StorageNodeId snid : snids) {
                    if (t.get(snid) == null) {
                        msg = "Storage Node does not exist: " + snid;
                        exitCode = Shell.EXIT_UNKNOWN;
                        if (shell.getJson()) {
                            CommandResult cmdResult =
                                new CommandFails(msg, ErrorMessage.NOSQL_5200,
                                                 CommandResult.NO_CLEANUP_JOBS);
                            return Shell.toJsonReport(operation, cmdResult);
                        }
                        return msg;
                    }

                    cs.removeStorageNodeFromPool(poolName, snid);
                }

                if (shell.getJson()) {
                    ObjectNode returnValue = JsonUtils.createObjectNode();
                    returnValue.put("pool_name", poolName);
                    CommandSucceeds cmdResult =
                        new CommandSucceeds(returnValue.toString());
                    return Shell.toJsonReport(operation, cmdResult);
                }
                return "Removed Storage Node(s) " + snids + " from pool " +
                       poolName;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "pool leave -name <name> [-sn <snX>]* " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Remove Storage Nodes from an existing Storage Node pool.";
        }
    }
}
