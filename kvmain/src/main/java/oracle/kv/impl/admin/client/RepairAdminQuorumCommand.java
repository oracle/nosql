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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

@POST
class RepairAdminQuorumCommand extends ShellCommand {

    private static final Logger logger = LoggerUtils.getLogger(
        RepairAdminQuorumCommand.class, "command");

    static final String COMMAND_NAME = "repair-admin-quorum";

    static final String COMMAND_SYNTAX =
        COMMAND_NAME + " " +
        CommandParser.getJsonUsage() + " " +
        "{-zn <id>|-znname <name>|-admin <id>}...";

    static final String COMMAND_DESC =
        "Repairs admin quorum by reducing membership of the admin group to" +
        " the" + eolt + "admins in the specified zones or the specific" +
        " admins listed. This" + eolt + "command should be used when" +
        " attempting to recover from a failure that" + eolt + "has resulted" +
        " in a loss of admin quorum.";

    RepairAdminQuorumCommand() {

        /* Allow abbeviation as "repair-a". */
        super(COMMAND_NAME, 8);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        return new RepairAdminQuorumExecutor<String>() {

            @Override
            public String repairedAdminQuorum(Set<AdminId> requestedAdmins) {
                return
                    "Repaired admin quorum using admins: " + requestedAdmins;
            }
        }.commonExecute(args, shell);
    }

    private abstract class
        RepairAdminQuorumExecutor<T> implements Executor<T> {
        @Override
        public T commonExecute(String[] args, Shell shell)
            throws ShellException {
            Shell.checkHelp(args, RepairAdminQuorumCommand.this);
            final CommandShell cmd = (CommandShell) shell;

            final Set<DatacenterId> zoneIds = new HashSet<DatacenterId>();
            final Set<String> zoneNames = new HashSet<String>();
            final Set<AdminId> adminIds = new HashSet<AdminId>();
            boolean specifiedAdmins = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-zn".equals(arg)) {
                    zoneIds.add(parseDatacenterId(
                        Shell.nextArg(args, i++,
                                      RepairAdminQuorumCommand.this)));
                    specifiedAdmins = true;
                } else if ("-znname".equals(arg)) {
                    zoneNames.add(Shell.nextArg(
                        args, i++, RepairAdminQuorumCommand.this));
                    specifiedAdmins = true;
                } else if ("-admin".equals(arg)) {
                    adminIds.add(parseAdminid(
                        Shell.nextArg(args, i++,
                                      RepairAdminQuorumCommand.this)));
                    specifiedAdmins = true;
                } else {
                    shell.unknownArgument(arg, RepairAdminQuorumCommand.this);
                }
            }
            if (!specifiedAdmins) {
                throw new ShellUsageException(
                    "Need to specify -zn, -znname, or -admin flags",
                    RepairAdminQuorumCommand.this);
            }
            try {
                Set<AdminId> requestedAdmins =
                    repairAdminQuorum(cmd, zoneIds, zoneNames, adminIds);
                return repairedAdminQuorum(requestedAdmins);
            } catch (Exception e) {
                throw new ShellException("Problem repairing admin quorum: " +
                                         e.getMessage(),
                                         e);
            }
        }

        public abstract T repairedAdminQuorum(Set<AdminId> requestedAdmins);
    }

    @Override
    public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
        throws ShellException {
        final ShellCommandResult scr =
            ShellCommandResult.getDefault("repair admin quorum");
        return new RepairAdminQuorumExecutor<ShellCommandResult>() {

            @Override
            public ShellCommandResult
                repairedAdminQuorum(Set<AdminId> requestedAdmins) {
                final ObjectNode on = JsonUtils.createObjectNode();
                final ArrayNode adminArray = on.putArray("requestedAdmins");
                for (AdminId admin : requestedAdmins) {
                    adminArray.add(admin.toString());
                }
                scr.setReturnValue(on);
                return scr;
            }
        }.commonExecute(args, shell);
    }

    /** Make repair call through a method to simplify testing. */
    Set<AdminId> repairAdminQuorum(CommandShell cmd,
                                   Set<DatacenterId> zoneIds,
                                   Set<String> zoneNames,
                                   Set<AdminId> adminIds)
        throws ShellException {

        try {
            CommandServiceAPI cs = cmd.getAdmin();
            for (final String zoneName : zoneNames) {
                zoneIds.add(CommandUtils.getDatacenterId(zoneName, cs, this));
            }
            Set<AdminId> result = cs.repairAdminQuorum(zoneIds, adminIds);
            if (result != null) {
                return result;
            }

            /* Retry the command after the current admin is restarted */
            try {
                ServiceUtils.waitForAdmin(cmd.getAdminHostname(),
                                          cmd.getAdminPort(),
                                          null, /* loginManager */
                                          90, /* timeoutSec */
                                          ServiceStatus.RUNNING,
                                          logger);
            } catch (Exception e) {
                /* Let the repairAdminQuorum call signal the problem */
            }
            cs = cmd.getAdmin();
            result = cs.repairAdminQuorum(zoneIds, adminIds);
            if (result != null) {
                return result;
            }

            /*
             * Only do one retry, since multiple restarts of the current admin,
             * while possible, are not expected
             */
            throw new ShellException(
                "Problem repairing admin quorum: " +
                "The command needs to be retried manually because" +
                " there were multiple restarts of the current admin");
        } catch (RemoteException e) {
            cmd.noAdmin(e);
            throw new AssertionError("Not reached");
        }
    }

    @Override
    protected String getCommandSyntax() {
        return COMMAND_SYNTAX;
    }

    @Override
    protected String getCommandDescription() {
        return COMMAND_DESC;
    }
}
