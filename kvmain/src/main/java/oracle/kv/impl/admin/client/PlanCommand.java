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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminNotReadyException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandSucceeds;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.HostPort;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.ObjectNode;

/** Subcommands of plan */
class PlanCommand extends CommandWithSubs {

    /*
     * TODO need to add command for start/stop all arbiters
     */
    private static final List<? extends SubCommand> subs = Arrays.asList(
        new AddIndexSub(),              /* add-index */
        new AddTableSub(),              /* add-table */
        new CancelSub(),                /* cancel */
        /*
         * TODO : Yet to implement support for change-admindir[size]
         * and change-rnlogdir[size].
         */
        new ChangeParamsSub(),          /* change-parameters */
        new ChangeStorageDirSub(),      /* change-storagedir */
        new ChangeUserSub(),            /* change-user */
        new CreateUserSub(),            /* create-user */
        new DeployAdminSub(),           /* deploy-admin */
        new DeployDCSub(),              /* deploy-datacenter */
        new DeploySNSub(),              /* deploy-sn */
        new DeployTopologySub(),        /* deploy-topology */
        new DeployZoneSub(),            /* deploy-zone */
        new DeregisterEsCluster(),      /* deregister-es */
        new DropUserSub(),              /* drop-user */
        new EnableRequestsSub(),        /* enable-requests */
        new EvolveTableSub(),           /* evolve-table */
        new ExecuteSub(),               /* execute */
        new FailoverSub(),              /* failover */
        new GrantSub(),                 /* grant */
        new InterruptSub(),             /* interrupt */
        new MigrateSNSub(),             /* migrate-sn */
        new NetworkRestoreSub(),        /* network-restore */
        new PlanWaitSub(),              /* wait */
        new RegisterEsCluster(),        /* register-es */
        new RemoveAdminSub(),           /* remove-admin */
        new RemoveDatacenterSub(),      /* remove-datacenter */
        new RemoveIndexSub(),           /* remove-index */
        new RemoveSNSub(),              /* remove-sn */
        new RemoveTableSub(),           /* remove-table */
        new RemoveZoneSub(),            /* remove-zone */
        new RepairTopologySub(),        /* repair-topology */
        new RevokeSub(),                /* revoke */
        new SetTableLimitsSub(),        /* set-table-limits */
        new StartServiceSub(),          /* start-service */
        new StopServiceSub(),           /* stop-service */
        new UpdateTlsCredentialsSub(),  /* update-tls-credentials */
        new VerifyDataSub()             /* verify-data */
                                                                         );

    private static final String PLAN_COMMAND_NAME = "plan";

    PlanCommand() {
        super(subs,
              PLAN_COMMAND_NAME,
              4,  /* prefix length */
              0); /* min args -- let subs control it */
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates operations, or jobs that modify store state." +
            eol + "All subcommands with the exception of " +
            "interrupt and wait change" + eol + "persistent state. Plans " +
            "are asynchronous jobs so they return immediately" + eol +
            "unless -wait is used.  Plan status can be checked using " +
            "\"show plans\"." + eol + "Optional arguments for all plans " +
            "include:" +
            eolt + "-wait -- wait for the plan to complete before returning" +
            eolt + "-plan-name -- name for a plan.  These are not unique" +
            eolt + "-noexecute -- do not execute the plan.  If specified " +
            "the plan" + eolt + "              " +
            "can be run later using \"plan execute\"" +
            eolt + "-force -- used to force plan execution and plan retry";
    }

    /*
     * Base abstract class for PlanSubCommands.  This class extracts
     * the generic flags from the command line.
     */
    abstract static class PlanSubCommand extends SubCommand {

        protected boolean execute;
        protected boolean wait;
        protected boolean force;
        protected String planName;
        static final String genericFlags =
            eolt + "[-plan-name <name>] [-wait] [-noexecute] [-force] " +
            CommandParser.getJsonUsage();
        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;
        static final String lastPlanNotFound =
            "Found no plans created by the current user.";

        protected PlanSubCommand(String name, int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            wait = false;
            execute = true;
            force = false;
            planName = null;

            return exec(args, shell);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args,
                                                    Shell shell)
            throws ShellException {
            wait = false;
            execute = true;
            force = false;
            planName = null;

            return execJsonOutput(args, shell);
        }

        protected int checkGenericArg(String arg, String[] args, int i)
            throws ShellException {

            int rval = 0;
            if ("-plan-name".equals(arg)) {
                planName = Shell.nextArg(args, i, this);
                rval = 1;
            } else if ("-wait".equals(arg)) {
                wait = true;
            } else if ("-noexecute".equals(arg)) {
                execute = false;
            } else if ("-force".equals(arg)) {
                force = true;
            } else {
                throw new ShellUsageException("Invalid argument: " + arg,
                                              this);
            }
            return rval;
        }

        public abstract String exec(String[] args, Shell shell)
            throws ShellException;

        /*
         * Execute and return general POJO for sub commands. This method can
         * be override to provide individual JSON output format in
         * return value field.
         */
        public ShellCommandResult execJsonOutput(String[] args,
                                                 Shell shell)
            throws ShellException {
            /* Bridge and filter V1 result */
            final String result = exec(args, shell);
            return filterJsonResult(result);
        }

        /**
         * Return the most recently created plan's id.  If users have no
         * SYSVIEW privilege, the most recently plan created by them will be
         * returned. If users do not have ownership of any plan, 0 will be
         * returned.
         */
        protected static int getLastPlanId(CommandServiceAPI cs)
            throws RemoteException {

            int range[] =
                cs.getPlanIdRange(0L, (new Date()).getTime(), 1);

            return range[0];
        }

        /*
         * Encapsulate plan execution and optional waiting in a single place
         */
        protected String executePlan(int planId, CommandServiceAPI cs,
                                     Shell shell)
            throws ShellException, RemoteException {

            /*
             * Implicitly approve plan.  TODO: change server side to do this
             */
            cs.approvePlan(planId);
            if (execute) {
                cs.executePlan(planId, force);
                if (wait) {
                    shell.echo("Executed plan " + planId +
                        ", waiting for completion..." + Shell.eol);
                    final Plan.State state =
                        awaitPlan(planId, 0, null, cs, shell);
                    if (state != Plan.State.SUCCEEDED) {
                        exitCode = Shell.EXIT_UNKNOWN;
                    }
                    if (!shell.getJson()) {
                        return state.getWaitMessage(planId);
                    }
                    long options = StatusReport.SHOW_FINISHED_BIT;
                    if (shell.getVerbose()) {
                        options |= StatusReport.VERBOSE_BIT;
                    }
                    return cs.getPlanStatus(
                        planId, options, shell.getJson());
                }
            }

            if (shell.getJson()) {
                ObjectNode returnValue = JsonUtils.createObjectNode();
                returnValue.put("plan_id", planId);
                CommandResult cmdResult = new CommandSucceeds(
                    returnValue.toString());
                return Shell.toJsonReport(
                    PLAN_COMMAND_NAME + " " + getCommandName(), cmdResult);
            }
            if (execute) {
                return "Started plan " + planId + ". Use show plan -id " +
                    planId + " to check status." + eolt +
                    "To wait for completion, use plan wait -id " + planId;
            }
            return "Created plan without execution: " + planId;
        }
    }

    private static Plan.State awaitPlan(int planId,
                                        int timeout,
                                        TimeUnit timeUnit,
                                        CommandServiceAPI cs,
                                        Shell shell)
        throws ShellException {
        Exception cause;
        final long endTime = (timeout == 0) ? Long.MAX_VALUE :
                        System.currentTimeMillis() + timeUnit.toMillis(timeout);
        while (true) {

            /*
             * Await on the Admin. Retry on RemoteException or
             * AdminNotReadyException, either indefinately (timeout == 0) or
             * until the timeout expires.
             */
            try {
                return cs.awaitPlan(planId, timeout, timeUnit);
            } catch (AdminFaultException afe) {
                if (!afe.getFaultClassName().equals(
                                      AdminNotReadyException.class.getName())) {
                    throw afe;
                }
                cause = afe;
            } catch (RemoteException re) {
                cause = re;
            }

            try {
                if (System.currentTimeMillis() >= endTime) {
                    throw cause;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new ShellException("Exception waiting for plan " + planId,
                                         e);
            }
            /* Get a new connection to the Admin */
            cs = ((CommandShell)shell).getAdmin(true);
        }
    }

    @POST
    static class ChangeStorageDirSub extends PlanSubCommand {

        ChangeStorageDirSub() {
            super("change-storagedir", 9);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId snId = null;
            String storageDir = null;
            String storageDirSize = null;
            boolean add = true;
            boolean addOrRemove = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-sn".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    snId = parseSnid(argString);
                } else if ("-storagedir".equals(arg)) {
                    storageDir = Shell.nextArg(args, i++, this);
                } else if ("-path".equals(arg)) {
                    /*
                     * [#21880] use -storagedir as the flag name, but support
                     * -path until next R3 for backward compatibility.
                     */
                    storageDir = Shell.nextArg(args, i++, this);
                } else if ("-storagedirsize".equals(arg)) {
                    storageDirSize = Shell.nextArg(args, i++, this);
                } else if ("-add".equals(arg)) {
                    add = true;
                    addOrRemove = true;
                } else if ("-remove".equals(arg)) {
                    add = false;
                    addOrRemove = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (snId == null || !addOrRemove) {
                shell.requiredArg(null, this);
            }
            try {
                final Parameters p = cs.getParameters();
                final StorageNodeParams snp = p.get(snId);
                if (snp == null) {
                    throw new ShellUsageException("Unknown storage node: " +
                                                  snId, this);
                }
                ParameterMap storageDirMap = null;
                try {
                    storageDirMap = StorageNodeParams.
                                        changeStorageDirMap(cs.getParameters(),
                                                            snp, add,
                                                            storageDir,
                                                            storageDirSize);
                } catch (IllegalCommandException e) {
                    throw new ShellUsageException(e.getMessage(), this);
                }
                final int planId =
                    cs.createChangeParamsPlan(planName, snId, storageDirMap);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected boolean matches(String commandName) {
            /* Allow deprecated name until R3 [#21880] */
            return super.matches(commandName) ||
                   Shell.matches(commandName, "change-mountpoint",
                                 prefixMatchLength);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan change-storagedir -sn <id> " + eolt +
                   "-storagedir <path to storage directory> " +
                   "-add|-remove " +
                   "[-storagedirsize <size of storage directory>] " +
                   genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
            "Adds or removes a storage directory on a Storage Node for use" +
            eolt +
            "by a Replication Node. When -add is specified, the optional" +
            eolt +
            "-storagedirsize flag can be specified to set the size of the" +
            eolt +
            "directory. The size format is \"number [unit]\", where unit" +
            eolt +
            "can be KB, MB, GB, or TB. The unit is case insensitive and may" +
            eolt +
            "be separated from the number by a space, \"-\", or \"_\".";
        }
    }

    @POST
    static final class ChangeParamsSub extends PlanSubCommand {

        final String incompatibleAllError =
            "Invalid argument combination: Only one of the flags " +
            "-all-rns, -all-sns, -all-admins, -all-ans, -global " +
            "and -security may be used.";

        final String serviceAllError =
            "Invalid argument combination: -service flag cannot be used " +
            "with -all-rns, -all-admins, -all-ans, -global or -security flags";

        final String serviceDcError =
            "Invalid argument combination: -service flag cannot be used " +
            "with -zn, -znname, -dc, or -dcname flag";

        final String commandSyntax =
            "plan change-parameters -security | -global | -service <id> | " +
            eolt +
            "-all-rns [-zn <id> | -znname <name>] | " +
            eolt +
            "-all-ans [-zn <id> | -znname <name>] | " +
            eolt +
            "-all-admins [-zn <id> | -znname <name>] [-dry-run]" +
            genericFlags + " -params [name=value]*";

        final String commandDesc =
            "Changes parameters for either the specified service, or for" +
            eolt +
            "all service instances of the same type that are deployed to" +
            eolt +
            "the specified zone or all zones. " +
            eolt +
            "  -security changes store-wide security parameters and should" +
            eolt +
            "  not be used with other flags. " +
            eolt +
            "  -global changes store-wide non-security parameters and should" +
            eolt +
            "  not be used with other flags. " +
            eolt +
            "  -service affects a single component and should not be used " +
            eolt +
            "  with either the -zn or -znname flag.  " +
            eolt +
            eolt +
            "One of the -all-* flags can be combined with -zn or -znname to " +
            eolt +
            "to change all instances of the service type deployed to the " +
            eolt +
            "specified zone, leaving unchanged any instances of the " +
            eolt +
            "specified type deployed to other zones. If one of the -all-* " +
            eolt +
            "flags is used without also specifying the zone, then the desired "+
            eolt +
            "parameter change will be applied to all instances of the " +
            eolt +
            "specified type within the store, regardless of zone. " +
            eolt +
            eolt +
            "The parameters to change are specified via the -params flag" +
            eolt +
            "and consist of name/value pairs separated by spaces, where" +
            eolt +
            "any parameter values with embedded spaces must be quoted" +
            eolt +
            "(for example, name=\"value with spaces\").  Finally, if the" +
            eolt +
            "-dry-run flag is specified, the new parameters are returned" +
            eolt +
            "without applying the specified change." +
            eol +
            eolt +
            "Use \"show parameters\" to see what parameters can be " +
            "modified";

        String serviceName;
        StorageNodeId snid;
        RepNodeId rnid;
        AdminId aid;
        ArbNodeId anid;
        boolean allAdmin, allRN, allSN, security, allAN, global;

        boolean dryRun;

        ChangeParamsSub() {
            super("change-parameters", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            return new ChangeParameterExecutor<String>() {

                @Override
                public String
                    planExecutionResult(String serverText,
                                        String deprecatedDcFlagPrefix)
                    throws ShellException {
                    if (shell.getJson()) {
                        return serverText;
                    }
                    return deprecatedDcFlagPrefix + serverText;
                }

                @Override
                public String dryRunResult(ParameterMap map,
                                           boolean showHidden)
                    throws ShellException {
                    return CommandUtils.formatParams(map,
                                                     showHidden, null);
                }
            }.commonExecute(args, shell);
        }

        @Override
        public ShellCommandResult execJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("plan change-parameter");
            return new ChangeParameterExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    planExecutionResult(String serverText,
                                        String deprecatedDcFlagPrefix)
                    throws ShellException {
                    return filterJsonResult(serverText);
                }

                @Override
                public ShellCommandResult
                    dryRunResult(ParameterMap map,
                                 boolean showHidden)
                    throws ShellException {
                    scr.setReturnValue(
                        CommandUtils.formatParamsJson(map, showHidden, null));
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return commandSyntax;
        }

        /* TODO: help differentiated by service type */
        @Override
        protected String getCommandDescription() {
            return commandDesc;
        }

        private abstract class
            ChangeParameterExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ChangeParamsSub.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                serviceName = null;
                snid = null;
                aid = null;
                rnid = null;
                anid = null;
                allAdmin = allRN = allSN = dryRun = security = allAN =
                    global = false;

                int i;
                boolean foundParams = false;

                DatacenterId dcid = null;
                String dcName = null;
                boolean getDcId = false;
                boolean deprecatedDcFlag = false;

                for (i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-service".equals(arg)) {
                        serviceName = Shell.nextArg(
                            args, i++, ChangeParamsSub.this);
                    } else if ("-all-rns".equals(arg)) {
                        allRN = true;
                    } else if ("-all-admins".equals(arg)) {
                        allAdmin = true;
                    } else if ("-all-sns".equals(arg)) {
                        allSN = true;
                    } else if ("-all-ans".equals(arg)) {
                        allAN = true;
                    } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                        dcid = DatacenterId.parse(
                            Shell.nextArg(args, i++, ChangeParamsSub.this));
                        if (CommandUtils.
                                isDeprecatedDatacenterId(arg, args[i])) {
                            deprecatedDcFlag = true;
                        }
                    } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                        dcName = Shell.nextArg(
                            args, i++, ChangeParamsSub.this);
                        if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                            deprecatedDcFlag = true;
                        }
                    } else if ("-global".equals(arg)) {
                        global = true;
                    } else if ("-security".equals(arg)) {
                        security = true;
                    } else if ("-dry-run".equals(arg)) {
                        dryRun = true;
                    } else if ("-params".equals(arg)) {
                        ++i;
                        foundParams = true;
                        break;
                    } else {
                        i += checkGenericArg(arg, args, i);
                    }
                }

                /* Verify argument combinations */
                if (serviceName == null) {
                    if (!(allAdmin || allRN || allSN || allAN ||
                          global || security)) {
                        shell.requiredArg(
                            null, ChangeParamsSub.this);
                    } else {
                        /*
                         * check whether multiple incompatible options are
                         * given.
                         */
                        if (((allAdmin ? 1 : 0) +
                             (allRN ? 1 : 0) +
                             (allSN ? 1 : 0) +
                             (allAN ? 1 : 0) +
                             (global ? 1 : 0) +
                             (security ? 1 : 0)) > 1) {

                            throw new ShellUsageException(
                                incompatibleAllError, ChangeParamsSub.this);
                        }
                        if (dcName != null) {
                            getDcId = true;
                        }
                    }
                } else {
                    if (allAdmin || allRN || allSN || security ||
                        allAN || global) {
                        throw new ShellUsageException(
                            serviceAllError, ChangeParamsSub.this);
                    }
                    if (dcid != null || dcName != null) {
                        throw new ShellUsageException(
                            serviceDcError, ChangeParamsSub.this);
                    }
                }

                if (!foundParams) {
                    shell.requiredArg("-params", ChangeParamsSub.this);
                }

                if (args.length <= i) {
                    throw new ShellArgumentException(
                        "No parameters were specified");
                }

                final String deprecatedDcFlagPrefix =
                    deprecatedDcFlag ? dcFlagsDeprecation : "";

                try {
                    int planId = 0;
                    final ParameterMap map = createChangeMap(cs, args, i,
                                                             cmd.getHidden());

                    if (dryRun) {
                        return dryRunResult(map, cmd.getHidden());
                    }

                    if (getDcId) {
                        dcid = CommandUtils.getDatacenterId(
                            dcName, cs, ChangeParamsSub.this);
                    }

                    if (rnid != null) {
                        shell.verboseOutput("Changing parameters for " + rnid);
                        planId =
                            cs.createChangeParamsPlan(planName, rnid, map);
                    } else if (allRN) {
                        shell.verboseOutput(
                            "Changing parameters for all RepNodes" +
                            (dcName != null ?
                             " deployed to the " + dcName + " zone" :
                            (dcid != null ?
                             " deployed to the zone with id = " + dcid :
                             "")));
                        planId =
                            cs.createChangeAllParamsPlan(planName, dcid, map);
                    } else if (snid != null) {
                        shell.verboseOutput("Changing parameters for " + snid);
                        planId =
                            cs.createChangeParamsPlan(planName, snid, map);
                    }  else if (anid != null) {
                        shell.verboseOutput("Changing parameters for " + anid);
                        planId =
                            cs.createChangeParamsPlan(planName, anid, map);
                    } else if (allAN) {
                        shell.verboseOutput(
                            "Changing parameters for all ArbNodes" +
                            (dcName != null ?
                             " deployed to the " + dcName + " zone" :
                            (dcid != null ?
                             " deployed to the zone with id = " + dcid :
                             "")));
                        planId =
                            cs.createChangeAllANParamsPlan(
                                planName, dcid, map);
                    } else if (allAdmin) {
                        shell.verboseOutput(
                            "Changing parameters for all Admins" +
                            (dcName != null ?
                             " deployed to the " + dcName + " zone" :
                             (dcid != null ?
                              " deployed to the zone with id = " + dcid :
                              "")));
                        planId =
                            cs.createChangeAllAdminsPlan(planName, dcid, map);
                    } else if (global) {
                        shell.verboseOutput(
                            "Changing global component parameters");
                        planId =
                            cs.createChangeGlobalComponentsParamsPlan(planName,
                                                                      map);
                    } else if (security) {
                        shell.verboseOutput(
                            "Changing global security parameters");
                        planId =
                            cs.createChangeGlobalSecurityParamsPlan(
                                planName, map);
                    } else if (aid != null) {
                        shell.verboseOutput("Changing parameters for " + aid);
                        planId = cs.createChangeParamsPlan(planName, aid, map);
                    } else if (allSN) {
                        throw new ShellUsageException(
                            "Can't change all SN params at this time",
                            ChangeParamsSub.this);
                    }
                    if (shell.getVerbose()) {
                        shell.verboseOutput
                            ("New parameters:" + eol +
                             CommandUtils.formatParams(map,
                                                       cmd.getHidden(),
                                                       null));
                    }
                    return planExecutionResult(
                        executePlan(planId, cs, shell),
                        deprecatedDcFlagPrefix);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                planExecutionResult(String serverText,
                                    String deprecatedDcFlagPrefix)
                throws ShellException;

            public abstract T dryRunResult(ParameterMap map,
                                           boolean showHidden)
                throws ShellException;
        }
        private ParameterMap getServiceMap(CommandServiceAPI cs)
            throws ShellException, RemoteException {

            ParameterMap map = null;
            final Parameters p = cs.getParameters();
            try {
                rnid = RepNodeId.parse(serviceName);
                final RepNodeParams rnp = p.get(rnid);
                if (rnp != null) {
                    map = rnp.getMap();
                } else {
                    throw new ShellUsageException
                        ("No such service: " + serviceName, this);
                }
            } catch (IllegalArgumentException ignored) {
                try {
                    snid = StorageNodeId.parse(serviceName);
                    final StorageNodeParams snp = p.get(snid);
                    if (snp != null) {
                        map = snp.getMap();
                    } else {
                        throw new ShellUsageException
                            ("No such service: " + serviceName, this);
                    }
                } catch (IllegalArgumentException ignored1) {
                    try {
                        aid = AdminId.parse(serviceName);
                        final AdminParams ap = p.get(aid);
                        if (ap != null) {
                            map = ap.getMap();
                        } else {
                            throw new ShellUsageException
                                ("No such service: " + serviceName, this);
                        }
                    } catch (IllegalArgumentException ignored2) {
                        try {
                            anid = ArbNodeId.parse(serviceName);
                            final ArbNodeParams ap = p.get(anid);
                            if (ap != null) {
                                map = ap.getMap();
                            } else {
                                throw new ShellUsageException
                                    ("No such service: " + serviceName, this);
                            }
                        } catch (IllegalArgumentException ignored3) {
                            throw new ShellUsageException
                                ("Invalid service name: " + serviceName, this);
                        }
                    }
                }
            }
            return map;
        }

        private ParameterMap createChangeMap(CommandServiceAPI cs,
                                             String[] args,
                                             int i,
                                             boolean showHidden)
            throws ShellException, RemoteException {

            ParameterMap map = null;
            ParameterState.Info info = null;
            ParameterState.Scope scope;
            if (serviceName != null) {
                map = getServiceMap(cs);
                scope = null; /* allow any scope */
                if (map.getType().equals(ParameterState.REPNODE_TYPE)) {
                    info = ParameterState.Info.REPNODE;
                } else if (map.getType().equals(ParameterState.SNA_TYPE)) {
                    info = ParameterState.Info.SNA;
                } else {
                    info = ParameterState.Info.ADMIN;
                }
            } else {
                scope = ParameterState.Scope.STORE;
                map = new ParameterMap();
                if (allRN) {
                    map.setType(ParameterState.REPNODE_TYPE);
                    info = ParameterState.Info.REPNODE;
                } else if (allSN) {
                    map.setType(ParameterState.SNA_TYPE);
                    info = ParameterState.Info.SNA;
                } else if (global) {
                    map.setType(ParameterState.GLOBAL_TYPE);
                    info = ParameterState.Info.GLOBAL;
                } else if (security) {
                    map.setType(ParameterState.GLOBAL_TYPE);
                    info = ParameterState.Info.GLOBAL;
                } else {
                    map.setType(ParameterState.ADMIN_TYPE);
                    info = ParameterState.Info.ADMIN;
                }
            }
            CommandUtils.parseParams(map, args, i, info, scope,
                                     showHidden, this);
            return map;
        }
    }

    @POST
    static final class ChangeUserSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan change-user -name <user name> [-disable | -enable]" +
            eolt + "[-set-password [-password <new password>] " +
            "[-retain-current-password]]" + eolt +
            "[-clear-retained-password]" + genericFlags;

        static final String COMMAND_DESC =
            "Change a user with the specified name in the store. " +
            "The" + eolt + "-retain-current-password argument option " +
            "causes the current password to" + eolt + "be remembered " +
            "during the -set-password operation as a valid alternate " +
            eolt + "password for configured retention time or until" +
            " cleared using -clear-retained-password." + eolt +
            "If a retained password has already been set for the user," +
            eolt + "setting retained password again will cause an " +
            "error to be reported.";

        static final String changeUserCommandDeprecation =
            "The command:" + eol + eolt +
            "plan change-user" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "execute \'ALTER USER\'" + eol + eol;

        static final String RETAIN_FLAG = "-retain-current-password";
        static final String CLEAR_FLAG = "-clear-retained-password";
        static final String DISABLE_FLAG = "-disable";
        static final String ENABLE_FLAG = "-enable";
        static final String SET_PASSWORD_FLAG = "-set-password";
        static final String PASSWORD_FLAG = "-password";
        static final String NAME_FLAG = "-name";

        ChangeUserSub() {
            super("change-user", 10);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {
            return new ChangeUserExecutor<String>() {

                @Override
                public String nothingChange(String text)
                    throws ShellException {
                    return changeUserCommandDeprecation + text;
                }

                @Override
                public String PlanResult(int planId,
                                         CommandServiceAPI cs,
                                         Shell commandShell)
                    throws ShellException, RemoteException {
                    return changeUserCommandDeprecation +
                           executePlan(planId, cs, commandShell);
                }
            }.commonExecute(args, shell);
        }

        private abstract class
            ChangeUserExecutor<T> implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ChangeUserSub.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                /* Flags for change options */
                String userName = null;
                String password = null;
                boolean retainPassword = false;
                boolean clearRetainedPassword = false;
                boolean changePassword = false;
                char[] newPlainPassword = null;

                /* Use boxed boolean to help identify non-changed user state */
                Boolean isEnabled = null;

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if (NAME_FLAG.equals(arg)) {
                        userName =
                            Shell.nextArg(args, i++, ChangeUserSub.this);
                    } else if (PASSWORD_FLAG.equals(arg)) {
                        password =
                            Shell.nextArg(args, i++, ChangeUserSub.this);
                    } else if (SET_PASSWORD_FLAG.equals(arg)) {
                        changePassword = true;
                    } else if (DISABLE_FLAG.equals(arg)) {
                        isEnabled = false;
                    } else if (ENABLE_FLAG.equals(arg)) {
                        isEnabled = true;
                    } else if (RETAIN_FLAG.equals(arg)) {
                        retainPassword = true;
                    } else if (CLEAR_FLAG.equals(arg)) {
                        clearRetainedPassword = true;
                    } else {
                        i += checkGenericArg(arg, args, i);
                    }
                }
                if (userName == null) {
                    shell.requiredArg(NAME_FLAG, ChangeUserSub.this);
                }
                if (password != null && !changePassword) {
                    throw new ShellArgumentException(
                        "Option -password is only valid in conjunction " +
                        "with -set-password.");
                }
                if (retainPassword && !changePassword) {
                    throw new ShellArgumentException(
                        "Option -retain-current-password is only valid in " +
                        "conjunction with -set-password.");
                }
                /* Nothing changes */
                if (isEnabled == null &&
                    !changePassword && !clearRetainedPassword) {
                    return nothingChange(
                        "Nothing changed for user " + userName);
                }

                /* Get new password from user input */
                if (changePassword) {
                    if (password != null) {
                        if (password.isEmpty()) {
                            throw new ShellArgumentException(
                                "Password may not be empty");
                        }
                        newPlainPassword = password.toCharArray();
                    } else {
                        /* No interactive mode in JSON mode */
                        if (shell.getJson() || shell.getJsonV1()) {
                            throw new ShellArgumentException(
                                "Password may not be empty");
                        }
                        final PasswordReader READER =
                            new ShellPasswordReader();
                        newPlainPassword =
                            CommandUtils.getPasswordFromInput(
                                READER, ChangeUserSub.this);
                    }
                }

                try {
                    final int planId =
                        cs.createChangeUserPlan(
                            planName, userName, isEnabled,
                            newPlainPassword, retainPassword,
                            clearRetainedPassword);
                    SecurityUtils.clearPassword(newPlainPassword);
                    return PlanResult(planId, cs, shell);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T nothingChange(String text)
                throws ShellException;

            public abstract T
                PlanResult(int planId,
                           CommandServiceAPI cs,
                           Shell commandShell)
                throws ShellException, RemoteException;
        }

        @Override
        public ShellCommandResult
            execJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("change user plan");
            return new ChangeUserExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult nothingChange(String text)
                    throws ShellException {
                    scr.setDescription(text);
                    return scr;
                }
                @Override
                public ShellCommandResult PlanResult(int planId,
                                                     CommandServiceAPI cs,
                                                     Shell commandShell)
                    throws ShellException, RemoteException {
                    return filterJsonResult(
                               executePlan(planId, cs, commandShell));
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC + eol + eolt +
                   "This command is deprecated and has been replaced by:" +
                   eol + eolt + "execute \'ALTER USER\'";
        }
    }

    @POST
    static final class CreateUserSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan create-user -name <user name> [-admin] [-disable]" +
            eolt + "[-password <new password>]" + genericFlags;

        static final String COMMAND_DESC =
            "Create a user with the specified name in the store." +
            eolt + "The -admin argument indicates that the created " +
            "user has full " + eolt + "administrative privileges.";

        static final String createUserCommandDeprecation =
            "The command:" + eol + eolt +
            "plan create-user" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "execute \'CREATE USER\'" + eol + eol;

        static final String DISABLE_FLAG = "-disable";
        static final String ADMIN_FLAG = "-admin";
        static final String NAME_FLAG = "-name";
        static final String PASSWORD_FLAG = "-password";

        CreateUserSub() {
            super("create-user", 10);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            if (shell.getJson()) {
                return executeCommand(args, shell);
            }
            return createUserCommandDeprecation + executeCommand(args, shell);
        }

        private String executeCommand(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String userName = null;
            String password = null;
            boolean isAdmin = false;
            boolean isEnabled = true;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (NAME_FLAG.equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else if (PASSWORD_FLAG.equals(arg)) {
                    password = Shell.nextArg(args, i++, this);
                } else if (ADMIN_FLAG.equals(arg)) {
                    isAdmin = true;
                } else if (DISABLE_FLAG.equals(arg)) {
                    isEnabled = false;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (userName == null ) {
                shell.requiredArg(NAME_FLAG, this);
            }

            /* Get password from user input. */
            char[] plainPasswd;
            if (password != null) {
                if (password.isEmpty()) {
                    throw new ShellArgumentException(
                        "Password may not be empty");
                }
                plainPasswd = password.toCharArray();
            } else {
                /* No interactive mode in JSON mode */
                if (shell.getJson() || shell.getJsonV1()) {
                    throw new ShellArgumentException(
                        "Password may not be empty");
                }
                final PasswordReader READER = new ShellPasswordReader();
                plainPasswd =
                    CommandUtils.getPasswordFromInput(READER, this);
            }

            try {
                final int planId =
                    cs.createCreateUserPlan(planName, userName, isEnabled,
                                            isAdmin, plainPasswd);
                SecurityUtils.clearPassword(plainPasswd);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC + eol + eolt +
                   "This command is deprecated and has been replaced by:" +
                   eol + eolt + "execute \'CREATE USER\'";
        }
    }

    @POST
    static final class DeployAdminSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan deploy-admin -sn <id>" +
            eolt + genericFlags;

        static final String COMMAND_DESC =
            "Deploys an Admin to the specified storage node.";

        DeployAdminSub() {
            super("deploy-admin", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId snid = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-port".equals(arg)) {
                    shell.echo("WARNING: the -port argument is obsolete " +
                        "and was benignly ignored." + Shell.eol);
                    /* Consume the obsolete arg that should follow -port*/
                    Shell.nextArg(args, i++, this);
                } else if ("-sn".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    snid = parseSnid(argString);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (snid == null) {
                shell.requiredArg(null, this);
            }
            try {
                final int planId =
                    cs.createDeployAdminPlan(planName, snid,
                                             null /* default to zone type */);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
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

    @POST
    static final class DropUserSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan drop-user -name <user name>" + genericFlags;

        static final String COMMAND_DESC =
            "Drop a user with the specified name in the store. A" +
            eolt + "logged-in user may not drop itself.";

        static final String dropUserCommandDeprecation =
            "The command:" + eol + eolt +
            "plan drop-user" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "execute \'DROP USER\'" + eol + eol;

        DropUserSub() {
            super("drop-user", 7);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            if (shell.getJson()) {
                return executeCommand(args, shell);
            }
            return dropUserCommandDeprecation + executeCommand(args, shell);
        }

        private String executeCommand(String[] args, Shell shell)
                throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String userName = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-name".equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (userName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                /*
                 * For failure recovery, execute the plan even though the user
                 * to drop does not exist.
                 */
                final int planId =
                    cs.createDropUserPlan(planName, userName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC + eol + eolt +
                   "This command is deprecated and has been replaced by:" +
                   eol + eolt + "execute \'DROP USER\'";
        }
    }

    @POST
    static final class RemoveAdminSub extends PlanSubCommand {

        final String adminDcError =
            "Invalid argument combination: -admin flag cannot be used " +
            "with -zn, -znname, -dc, or -dcname flag";

        final String dcIdNameError =
            "Invalid argument combination: must use only one of the -zn," +
            " -znname, -dc, or -dcname flags";

        final String commandSyntax =
            "plan remove-admin -admin <id> | -zn <id> | -znname <name> " +
            genericFlags;
            /*
             * TODO: Add hidden parameter support for -failed-sn in
             * command syntax.
             */

        final String commandDesc =
            "Removes the desired Admin instances; either the single" +
            eolt +
            "specified instance, or all instances deployed to the specified" +
            eolt +
            "zone. If the -admin flag is used and there are 3 or " +
            eolt +
            "fewer Admins running in the store, or if the -zn or -znname" +
            eolt +
            "flag is used and the removal of all Admins from the specified" +
            eolt +
            "zone would result in only one or two Admins in the store," +
            eolt +
            "then the desired Admins will be removed only if the -force" +
            eolt +
            "flag is also specified. Additionally, if the -admin flag is" +
            eolt +
            "used and there is only one Admin in the store, or if the -zn or" +
            eolt +
            "-znname flag is used and the removal of all Admins from the" +
            eolt +
            "specified zone would result in the removal of all Admins" +
            eolt +
            "from the store, then the desired Admins will not be removed.";
            /*
             * TODO: Add hidden parameter support for -failed-sn in
             * command description.
             */

        final String noAdminError =
            "There is no Admin in the store with the specified id ";

        final String only1AdminError =
            "Only one Admin in the store, so cannot remove the sole Admin ";

        final String tooFewAdminError =
            "Removing the specified Admin will result in fewer than 3" +
            eolt +
            "Admins in the store; which is strongly discouraged because" +
            eolt +
            "the loss of one of the remaining Admins will cause quorum" +
            eolt +
            "to be lost. If you still wish to remove the desired Admin," +
            eolt +
            "specify the -force flag. ";

        final String noAdminDcError =
            "There are no Admins in the specified zone ";

        final String allAdminDcError =
            "The specified zone contains all the Admins in the store," +
            eolt +
            "and so cannot be removed from the specified zone ";

        final String tooFewAdminDcError =
            "Removing all Admins from the specified zone will result" +
            eolt +
            "in fewer than 3 Admins in the store; which is strongly" +
            eolt +
            "discouraged because the loss of one of the remaining Admins " +
            eolt +
            "will cause quorum to be lost. If you still wish to remove" +
            eolt +
            "all of the Admins from the desired zone, specify the" +
            eolt +
            "-force flag. ";

        RemoveAdminSub() {
            super("remove-admin", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            AdminId aid = null;

            DatacenterId dcid = null;
            String dcName = null;
            boolean getDcId = false;
            boolean deprecatedDcFlag = false;
            boolean failedSN = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-admin".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    aid = parseAdminid(argString);
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = DatacenterId.parse(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else if ("-failed-sn".equals(arg)) {
                    failedSN = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            /* Verify argument combinations */
            if (aid == null) {
                if (dcid == null) {
                    if (dcName == null) {
                        shell.requiredArg(null, this);
                    } else {
                        getDcId = true;
                    }
                } else {
                    if (dcName != null) {
                        throw new ShellUsageException(dcIdNameError, this);
                    }
                }
            } else {
                if (dcid != null || dcName != null) {
                    throw new ShellUsageException(adminDcError, this);
                }
            }

            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";

            /*
             * If admin count is low, there are restrictions.
             */
            Parameters parameters = null;
            try {
                parameters = cs.getParameters();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
                return "";
            }

            final int nAdmins = parameters.getAdminCount();

            if (aid != null) {

                if (parameters.get(aid) == null) {
                    throw new ShellArgumentException(
                        noAdminError + "[" + aid + "]");
                }

                if (nAdmins == 1) {
                    throw new ShellArgumentException(
                        only1AdminError + "[" + aid + "]");
                }

                if (nAdmins < 4 && !force) {
                    throw new ShellArgumentException(
                        tooFewAdminError + "There are only " + nAdmins +
                        " Admins in the store.");
                }

            } else {

                final Set<AdminId> adminIdSet;
                try {

                    if (getDcId) {
                        dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                    }

                    adminIdSet =
                        parameters.getAdminIds(dcid, cs.getTopology());
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                    return "";
                }

                String dcErrStr = "";
                if (dcName != null) {
                    dcErrStr = dcName;
                } else if (dcid != null) {
                    dcErrStr = dcid.toString();
                }

                if (adminIdSet.isEmpty()) {
                    throw new ShellArgumentException(
                        noAdminDcError + "[" + dcErrStr + "]");
                }

                if (adminIdSet.size() == nAdmins) {
                    throw new ShellArgumentException(
                        allAdminDcError + "[" + dcErrStr + "]");
                }

                if (nAdmins - adminIdSet.size() < 3 && !force) {
                    throw new ShellArgumentException(
                        tooFewAdminDcError + "There are " + nAdmins +
                        " Admins in the store and " + adminIdSet.size() +
                        " Admins in the specified zone " +
                        "[" + dcErrStr + "]");
                }
            }

            try {
                final int planId =
                    cs.createRemoveAdminPlan(planName, dcid, aid, failedSN);
                if (shell.getJson()) {
                    return executePlan(planId, cs, shell);
                }
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return commandSyntax;
        }

        @Override
        protected String getCommandDescription() {
            return commandDesc;
        }
    }

    @POST
    static final class DeployDCSub extends DeployZoneSub {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "plan deploy-datacenter" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "plan deploy-zone" + eol + eol;

        DeployDCSub() {
            super("deploy-datacenter", 10);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        /** Add deprecation message. */
        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            if (shell.getJson()) {
                super.exec(args, shell);
            }
            return dcCommandDeprecation + super.exec(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:" +
                eol + eolt +
                "plan deploy-zone";
        }
    }

    @POST
    static class DeployZoneSub extends PlanSubCommand {
        static final String COMMAND_DESC =
            "Deploys the specified zone to the store." + eolt +
            "The replication factor must be non-negative, and can" + eolt +
            "only be 0 for a primary zone that allows arbiters or" + eolt +
            "for a secondary zone." + eolt +
            "If -type is not specified, creates a primary zone." + eolt +
            "Arbiters are not allowed by default, are only" + eolt +
            "allowed for primary zones, and must be allowed for" + eolt +
            "primary zones with replication factor 0." + eolt +
            "Master affinity is disabled by default, and is only" + eolt +
            "allowed for primary zones with a replication factor" + eolt +
            "greater than 0.";

        DeployZoneSub() {
            super("deploy-zone", 10);
        }

        DeployZoneSub(final String name, final int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String dcName = null;
            Integer rf = null;
            DatacenterType type = DatacenterType.PRIMARY;
            boolean allowArbiters = false;
            boolean masterAffinity = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-name".equals(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                } else if ("-rf".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    rf = parseUnsignedInt(argString);
                } else if ("-type".equals(arg)) {
                    final String typeValue = Shell.nextArg(args, i++, this);
                    type = parseDatacenterType(typeValue);
                } else if ("-arbiters".equals(arg)) {
                    allowArbiters = true;
                } else if ("-no-arbiters".equals(arg)) {
                    allowArbiters = false;
                } else if ("-master-affinity".equals(arg)) {
                    masterAffinity = true;
                } else if ("-no-master-affinity".equals(arg)) {
                    masterAffinity = false;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (rf == null) {
                shell.requiredArg("rf", this);
                throw new AssertionError("Not reached");
            }

            /*
             * TODO: This check is performed on the server as of version 23.1.
             * Remove this check when the prerequisite version has been updated
             * to require 23.1.
             */
            try {
                Rules.validateAllowArbiters(allowArbiters, type, rf);
            } catch (IllegalCommandException e) {
                throw new ShellUsageException(e.getMessage(), this);
            }

            if (dcName == null) {
                shell.requiredArg("name", this);
            } else if (dcName.isBlank()) {
                String info = "the value is empty or contain only blanks";
                shell.badArgUsage("name", info, this);
            }

            try {
                final int planId;
                try {
                    planId = cs.createDeployDatacenterPlan(
                        planName, dcName, rf, type, allowArbiters,
                        masterAffinity);
                } catch (AdminFaultException afe) {
                    convertIllegalCommandException(afe);
                    throw afe;
                }
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan " + name + " -name <zone name>" +
                eolt + "-rf <replication factor>" +
                eolt + "[-type {primary | secondary}]" +
                eolt + "[-arbiters | -no-arbiters]" +
                eolt + "[-master-affinity | -no-master-affinity]" +
                eolt + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC;
        }
    }

    @POST
    static final class DeploySNSub extends PlanSubCommand {

        DeploySNSub() {
            super("deploy-sn", 9);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            DatacenterId dcid = null;
            String host = null;
            String dcName = null;
            int port = 0;
            boolean deprecatedDcFlag = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-host".equals(arg)) {
                    host = Shell.nextArg(args, i++, this);
                } else if ("-port".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    port = parseUnsignedInt(argString);
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = DatacenterId.parse(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";
            if ((dcid == null && dcName == null) ||
                host == null || port == 0) {
                shell.requiredArg(null, this);
            }
            try {
                if (dcid == null) {
                    dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                }
                final int planId = cs.createDeploySNPlan(planName, dcid,
                                                         host, port, null);
                String status = executePlan(planId, cs, shell);
                if (shell.getJson()) {
                    return status;
                }
                return deprecatedDcFlagPrefix + status;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan deploy-sn -zn <id> | -znname <name> " +
                "-host <host> -port <port>" +
                eolt + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Deploys the storage node at the specified host and port " +
                 "into the" + eolt + "specified zone.";
        }
    }

    @POST
    static final class DeployTopologySub extends PlanSubCommand {

        DeployTopologySub() {
            super("deploy-topology", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String topoName = null;
            RepGroupId failedShard = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-failed-shard".equals(arg)) {
                    failedShard =
                        RepGroupId.parse(Shell.nextArg(args, i++,this));
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (topoName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                if (failedShard != null) {
                    /* Ensure RNs of the failed shard are not running*/
                    Topology currentTopo = cs.getTopology();
                    Map<ResourceId, ServiceChange> statusMap =
                        cs.getStatusMap();
                    CommandUtils.ensureRNNotRunning(failedShard, currentTopo,
                                                    statusMap, this);
                }
                final int planId = cs.createDeployTopologyPlan(planName,
                                                               topoName,
                                                               failedShard);
                return executePlan(planId, cs, shell);
            } catch (AdminFaultException afe) {
                convertCommandFaultException(afe);
                throw afe;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan deploy-topology -name <topology name>" +
                   genericFlags;
            /*
             * TODO: Add hidden parameter support for -failed-shard
             * in getCommandSyntax.
             */
        }

        @Override
        protected String getCommandDescription() {
            return
                "Deploys the specified topology to the store.  This " +
                "operation can" + eolt + "take a while, depending on " +
                "the size and state of the store.";
        }
    }

    @POST
     static final class RepairTopologySub extends PlanSubCommand {

            RepairTopologySub() {
                super("repair-topology", 4);
            }

            @Override
            public String exec(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    i += checkGenericArg(arg, args, i);
                }

                try {
                    final int planId = cs.createRepairPlan(planName);
                    return executePlan(planId, cs, shell);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return "";
            }

        @Override
        protected String getCommandSyntax() {
            return "plan repair-topology " + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Inspects the store's deployed, current topology for " +
                "inconsistencies" + eolt +
                "in location metadata that may have arisen from the " +
                "interruption" + eolt +
                "or cancellation of previous deploy-topology or migrate-sn " +
                "plans. Where " + eolt +
                "possible, inconsistencies are repaired. This operation can"
                + eolt + "take a while, depending on the size and state of " +
                "the store.";
        }
    }

    @POST
    static final class ExecuteSub extends PlanSubCommand {

        ExecuteSub() {
            super("execute", 3);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            int planId = 0;

            try {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-id".equals(arg)) {
                        final String argString =
                            Shell.nextArg(args, i++, this);
                        planId = parseUnsignedInt(argString);
                    } else if ("-last".equals(arg)) {
                        planId = getLastPlanId(cs);
                        if (planId == 0) {
                            throw new ShellArgumentException(
                                lastPlanNotFound);
                        }
                    } else {
                        i += checkGenericArg(arg, args, i);
                    }
                }
                if (planId == 0) {
                    shell.requiredArg("-id|-last", this);
                }
                CommandUtils.ensurePlanExists(planId, cs, this);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan execute -id <id> | -last" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Executes a created, but not yet executed plan.  The plan " +
                "must have" + eolt + "been previously created using the " +
                "-noexecute flag. Use -last to" + eolt + "reference the " +
                "most recently created plan.";
        }
    }

    @POST
    static class FailoverSub extends PlanSubCommand {

        FailoverSub() {

            /* For safety, no abbreviation. */
            super("failover", 8);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = getAdmin(cmd);
            final Set<DatacenterId> primaryZones = new HashSet<>();
            final Set<DatacenterId> offlineZones = new HashSet<>();
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                String zoneName = null;
                DatacenterId dcId = null;
                if ("-zn".equals(arg)) {
                    zoneName = Shell.nextArg(args, i++, this);
                    dcId = parseDatacenterId(zoneName);
                } else if ("-znname".equals(arg)) {
                    zoneName = Shell.nextArg(args, i++, this);
                    dcId = getDatacenterId(cmd, cs, zoneName);
                } else {
                    i += checkGenericArg(arg, args, i);
                    continue;
                }
                if (++i >= args.length) {
                    shell.requiredArg("-type", this);
                }
                final String typeFlag = args[i];
                if ("-type".equals(typeFlag)) {
                    final String type = Shell.nextArg(args, i++, this);
                    if ("primary".equals(type)) {
                        primaryZones.add(dcId);
                    } else if ("offline-secondary".equals(type)) {
                        offlineZones.add(dcId);
                    } else {
                        invalidArgument(type);
                    }
                    if (primaryZones.contains(dcId) &&
                        offlineZones.contains(dcId)) {
                        throw new ShellUsageException(
                            "Zone " + zoneName +
                            " was specified with multiple types",
                            this);
                    }
                } else {
                    shell.requiredArg("-type", this);
                }
            }
            if (offlineZones.isEmpty()) {
                throw new ShellUsageException(
                    "Must specify at least one offline-secondary zone", this);
            }
            return failover(cmd, cs, primaryZones, offlineZones);
        }

        /** Make getAdmin call through a method to simplify testing. */
        CommandServiceAPI getAdmin(CommandShell cmd)
            throws ShellException {

            return cmd.getAdmin();
        }

        /**
         * Make datacenter name conversion call through a method to simplify
         * testing.
         */
        DatacenterId getDatacenterId(CommandShell cmd,
                                     CommandServiceAPI cs,
                                     String dcName)
            throws ShellException {
            try {
                return CommandUtils.getDatacenterId(dcName, cs, this);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
                throw new AssertionError("Not reached");
            }
        }

        /** Make failover call through a method to simplify testing. */
        String failover(CommandShell cmd, CommandServiceAPI cs,
                        Set<DatacenterId> primaryZones,
                        Set<DatacenterId> offlineZones)
            throws ShellException {

            try {
                final int planId = cs.createFailoverPlan(
                    planName, primaryZones, offlineZones);
                return executePlan(planId, cs, cmd);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
                throw new AssertionError("Not reached");
            }
        }

        @Override
        protected String getCommandSyntax() {
            return "plan failover" + eolt +
                "{ {-zn <zone-id>|-znname <zone-name>}" +
                " -type {primary|offline-secondary} }..." +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Changes zone types to failover to a changed set of" +
                " primary" + eolt + "zones following a failure of primary" +
                " zones that has resulted in a loss of" + eolt + "quorum.";
        }
    }

    @POST
    static final class GrantSub extends PlanSubCommand {
        static final String USER_FLAG = "-user";
        static final String ROLE_FLAG = "-role";
        static final String COMMAND_SYNTAX =
            "plan grant -user <user name> [-role <role name>]*" + genericFlags;
        static final String COMMAND_DESC =
            "Grant roles to a user with specified name. The -role option " +
            "accepts" + eolt + "only predefined roles in KVStore currently.";

        static final String grantCommandDeprecation =
            "The command:" + eol + eolt +
            "plan grant" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "execute \'GRANT\'" + eol + eol;

        GrantSub() {
            super("grant", 3);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            if (shell.getJson()) {
                return executeCommand(args, shell);
            }
            return grantCommandDeprecation + executeCommand(args, shell);
        }

        private String executeCommand(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            final Set<String> roles = new HashSet<>();

            String userName = null;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (USER_FLAG.equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else if (ROLE_FLAG.equals(arg)) {
                    final String role = Shell.nextArg(args, i++, this);
                    roles.add(role.toLowerCase());
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (userName == null) {
                shell.requiredArg(USER_FLAG, this);
            }

            if (roles.isEmpty()) {
                shell.requiredArg(ROLE_FLAG, this);
            }

            try {
                final int planId =
                    cs.createGrantPlan(planName, userName, roles);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {

            return COMMAND_SYNTAX;
        }
        @Override
        protected String getCommandDescription() {

            return COMMAND_DESC + eol + eolt +
                   "This command is deprecated and has been replaced by:" +
                   eol + eolt + "execute \'GRANT\'";
        }
    }

    @POST
    static final class InterruptSub extends InterruptCancelSub {

        InterruptSub() {
            super("interrupt", 3, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan interrupt -id <plan id> | -last " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Interrupts a running plan. An interrupted plan can " +
                "only be re-executed" + eolt + "or canceled.  Use -last " +
                "to reference the most recently" + eolt + "created plan.";
        }
    }

    @POST
    static final class CancelSub extends InterruptCancelSub {

        CancelSub() {
            super("cancel", 3, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan cancel -id <plan id> | -last " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Cancels a plan that is not running.  A running plan " +
                "must be" + eolt + "interrupted before it can be canceled. " +
                "Use -last to reference the most" + eolt + "recently " +
                "created plan.";
        }
    }

    /*
     * Put interrupt/cancel into same code.
     */
    abstract static class InterruptCancelSub extends PlanSubCommand {
        private final boolean isInterrupt;

        protected InterruptCancelSub(String name, int prefixMatchLength,
                                     boolean isInterrupt) {
            super(name, prefixMatchLength);
            this.isInterrupt = isInterrupt;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            return new InterruptCancelExecutor<String>() {
                @Override
                public String successDescription(String text)
                    throws ShellException {
                    return text;
                }

                @Override
                public String failureDescription(String text)
                    throws ShellException {
                    return text;
                }
            }.commonExecute(args, shell);
        }

        private abstract class
            InterruptCancelExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, InterruptCancelSub.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                int planId = 0;

                try {
                    for (int i = 1; i < args.length; i++) {
                        final String arg = args[i];
                        if ("-id".equals(arg)) {
                            final String argString =
                                Shell.nextArg(
                                    args, i++, InterruptCancelSub.this);
                            planId = parseUnsignedInt(argString);
                        } else if ("-last".equals(arg)) {
                            planId = getLastPlanId(cs);
                            if (planId == 0) {
                                return failureDescription(lastPlanNotFound);
                            }
                        } else {
                            shell.unknownArgument(
                                arg, InterruptCancelSub.this);
                        }
                    }
                    if (planId == 0) {
                        shell.requiredArg(
                            "-id|-last", InterruptCancelSub.this);
                    }
                    CommandUtils.ensurePlanExists(
                        planId, cs, InterruptCancelSub.this);
                    if (isInterrupt) {
                        cs.interruptPlan(planId);
                        return successDescription(
                            "Plan " + planId + " was interrupted");
                    }
                    cs.cancelPlan(planId);
                    return successDescription(
                        "Plan " + planId + " was canceled");
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T successDescription(String text)
                throws ShellException;

            public abstract T failureDescription(String text)
                throws ShellException;
        }

        @Override
        public ShellCommandResult execJsonOutput(String[] args,
                                                 Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("plan cancel|interrupt");
            return new InterruptCancelExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult successDescription(String text)
                    throws ShellException {
                    scr.setDescription(text);
                    return scr;
                }

                @Override
                public ShellCommandResult failureDescription(String text)
                    throws ShellException {
                    scr.setReturnCode(ErrorMessage.NOSQL_5100.getValue());
                    scr.setDescription(text);
                    return scr;
                }
            }.commonExecute(args, shell);
        }
    }

    @POST
    static final class MigrateSNSub extends PlanSubCommand {

        MigrateSNSub() {
            super("migrate-sn", 7);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId fromSnid = null;
            StorageNodeId toSnid = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-from".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    fromSnid = parseSnid(argString);
                } else if ("-to".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    toSnid = parseSnid(argString);
                } else if ("-admin-port".equals(arg)) {
                    shell.echo(
                        "WARNING: the -admin-port argument is obsolete " +
                        "and was benignly ignored." + Shell.eol);
                    /* Consume the obsolete arg that should follow -admin-port*/
                    Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (fromSnid == null || toSnid == null) {
                shell.requiredArg(null, this);
            }
            try {
                /*
                 * If the old SN hosted an admin with an admin web service,
                 * the port arg is required
                 */
                CommandUtils.ensureStorageNodeExists(fromSnid, cs, this);
                CommandUtils.ensureStorageNodeExists(toSnid, cs, this);
                final int planId =
                    cs.createMigrateSNPlan(planName, fromSnid, toSnid);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan migrate-sn -from <id> -to <id> " + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Migrates the services from one storage node to another. " +
                "The old node" + eolt + "must not be running.";
        }
    }

    @POST
    static final class NetworkRestoreSub extends PlanSubCommand {

        static String COMMAND_NAME = "network-restore";
        static String SOURCE_ID_FLAG = "-from";
        static String TARGET_ID_FLAG = "-to";
        static String RETAIN_LOG_FLAG = "-retain-logs";
        static String COMMAND_DESC = "Network restore a RepNode from " +
            "another one in their replication group.";
        static String COMMAND_SYNTAX =
            "plan network-restore -from <id> -to <id> -retain-logs" +
            genericFlags;

        NetworkRestoreSub() {
            super(COMMAND_NAME, 7);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            RepNodeId sourceNode = null;
            RepNodeId targetNode = null;
            boolean retainOrigLog = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (SOURCE_ID_FLAG.equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    sourceNode = parseRnid(argString);
                } else if (TARGET_ID_FLAG.equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    targetNode = parseRnid(argString);
                } else if (RETAIN_LOG_FLAG.equals(arg)) {
                    retainOrigLog = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (sourceNode == null) {
                shell.requiredArg(SOURCE_ID_FLAG, this);
            }
            if (targetNode == null) {
                shell.requiredArg(TARGET_ID_FLAG, this);
            }
            try {
                CommandUtils.ensureRepNodeExists(sourceNode, cs, this);
                CommandUtils.ensureRepNodeExists(targetNode, cs, this);
                final int planId = cs.createNetworkRestorePlan(
                    planName, sourceNode, targetNode, retainOrigLog);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
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

    @POST
    static final class RemoveSNSub extends PlanSubCommand {

        RemoveSNSub() {
            super("remove-sn", 9);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId snid = null;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-sn".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    snid = parseSnid(argString);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (snid == null) {
                shell.requiredArg("-sn", this);
            }

            try {
                CommandUtils.ensureStorageNodeExists(snid, cs, this);
                final int planId = cs.createRemoveSNPlan(planName, snid);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan remove-sn -sn <id>" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Removes the specified storage node from the topology.";
        }
    }

    @POST
    static final class StartServiceSub extends StartStopServiceSub {

        StartServiceSub() {
            super("start-service", 4, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan start-service" +
                " {-service <id> | -all-rns [-zn <id> | -znname <name>]" +
                " | -all-ans [-zn <id> | -znname <name>]" +
                " | -zn <id> | -znname <name>}" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Starts the specified service(s).";
        }
    }

    @POST
    static final class StopServiceSub extends StartStopServiceSub {

        StopServiceSub() {
            super("stop-service", 4, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan stop-service" +
                " {-service <id> | -all-rns [-zn <id> | -znname <name>]" +
                " | -all-ans [-zn <id> | -znname <name>]" +
                " | -zn <id> | -znname <name>}" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Stops the specified service(s).";
        }
    }

    /*
     * Start/stop specified services
     */
    abstract static class StartStopServiceSub extends PlanSubCommand {
        private final Set<ResourceId> serviceIds= new HashSet<>();
        private final boolean isStart;

        protected StartStopServiceSub(String name, int prefixMatchLength,
                                      boolean isStart) {
            super(name, prefixMatchLength);
            this.isStart = isStart;
        }

        /*
         * Adds all the replicationNode IDs given the zoneId (datacenterId)
         */
        private void getRepNodesByZoneId(Topology topology, String zoneId)
            throws ShellException {

            final DatacenterId dcId = parseDatacenterId(zoneId);

            if (topology.get(dcId) == null) {
                throw new IllegalArgumentException("The specified zone id " +
                                                   "does not exist");
            }
            for (RepNodeId rnId : topology.getRepNodeIds(dcId)) {
                addService(rnId);
            }
        }

        /*
         * Adds all the replicationNode IDs given the zoneName (datacenterName)
         */
        private void getRepNodesByZoneName(Topology topology,
                                           String zoneName) {

            final Datacenter zone = topology.getDatacenter(zoneName);
            if (zone == null) {
                throw new IllegalArgumentException("The specified zone name " +
                                                   "does not exist");
            }
            for (RepNodeId rnId : topology.getRepNodeIds(zone.getResourceId())){
                addService(rnId);
            }
        }

        /*
         * Adds all the ArbNode IDs given the zoneId (datacenterId)
         */
        private void getArbNodesByZoneId(Topology topology, String zoneId)
            throws ShellException {

            final DatacenterId dcId = parseDatacenterId(zoneId);

            if (topology.get(dcId) == null) {
                throw new IllegalArgumentException("The specified zone id " +
                                                   "does not exist");
            }
            for (ArbNodeId anId : topology.getArbNodeIds(dcId)) {
                addService(anId);
            }
        }

        /*
         * Adds all the ArbNode IDs given the zoneName (datacenterName)
         */
        private void getArbNodesByZoneName(Topology topology,
                                           String zoneName) {

            final Datacenter zone = topology.getDatacenter(zoneName);
            if (zone == null) {
                throw new IllegalArgumentException("The specified zone name " +
                                                   "does not exist");
            }
            for (ArbNodeId rnId : topology.getArbNodeIds(zone.getResourceId())){
                addService(rnId);
            }
        }

        /*
         * Adds all the ArbNode IDs
         */
        private void getAllArbNodes(Topology topology) {
            for (ArbNodeId anId : topology.getArbNodeIds(null)) {
                addService(anId);
            }
        }

        /*
         * Adds all the Admin IDs given the zoneId (datacenterId)
         */
        private void getAdminsByZoneId(CommandServiceAPI cs,
                                       DatacenterId zoneId)
            throws RemoteException {

            final Topology topology = cs.getTopology();

            if (topology.get(zoneId) == null) {
                throw new IllegalArgumentException("The specified zone id " +
                                                   "does not exist");
            }
            final Parameters p = cs.getParameters();
            for (final AdminId aid : p.getAdminIds(zoneId, topology)) {
                addService(aid);
            }
        }

        /*
         * Adds all the Admin IDs given the zoneName (datacenterName)
         */
        private void getAdminsByZoneName(CommandServiceAPI cs, String zoneName)
            throws RemoteException {

            final Topology topology = cs.getTopology();
            final Datacenter zone = topology.getDatacenter(zoneName);
            if (zone == null) {
                throw new IllegalArgumentException("The specified zone name " +
                                                   "does not exist");
            }
            getAdminsByZoneId(cs, zone.getResourceId());
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            serviceIds.clear();
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String zoneId = null;
            String zoneName = null;
            boolean allRNs = false;
            boolean allANs = false;

            try {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-service".equals(arg)) {
                        final String serviceName =
                                Shell.nextArg(args, i++, this);
                        addService(serviceName, cs);
                    } else if ("-all-rns".equals(arg)) {
                        allRNs = true;
                    } else if ("-all-ans".equals(arg)) {
                        allANs = true;
                    }  else if ("-zn".equals(arg)) {
                        zoneId = Shell.nextArg(args, i++, this);

                    /* Parse -zname because it was released by accident */
                    } else if ("-zname".equals(arg) || "-znname".equals(arg)) {
                        zoneName = Shell.nextArg(args, i++, this);
                    } else {
                        i += checkGenericArg(arg, args, i);
                    }
                }

                if (!serviceIds.isEmpty()) {
                    /*
                     * Since the -service flag was used, make sure no others
                     * were
                     */
                    if (allRNs) {
                        throw new ShellUsageException(
                            "Cannot use -service and -all-rns flags together",
                            this);
                    }
                    if (allANs) {
                        throw new ShellUsageException(
                            "Cannot use -service and -all-ans flags together",
                            this);
                    }
                    if ((zoneId != null) || (zoneName != null)) {
                        throw new ShellUsageException(
                            "Cannot use -zn or -znname flags with the" +
                            " -service flag", this);
                    }
                } else if ((zoneId != null) && (zoneName != null)) {
                    throw new ShellUsageException(
                        "Cannot use both the -zn and -znname flags", this);
                } else if (allRNs) {
                    /* All RNs (and only RNs) in a zone or the store */
                    if (zoneId != null) {
                        getRepNodesByZoneId(cs.getTopology(), zoneId);
                    } else if (zoneName != null) {
                        getRepNodesByZoneName(cs.getTopology(), zoneName);
                    } else {
                        /* Special case all RNs and only RNs of the store */
                        final int planId = isStart ?
                                    cs.createStartAllRepNodesPlan(planName) :
                                    cs.createStopAllRepNodesPlan(planName);

                        return executePlan(planId, cs, shell);
                    }
                } else if (allANs) {
                    /* All ANs (and only ANs) in a zone or the store */
                    if (zoneId != null) {
                        getArbNodesByZoneId(cs.getTopology(), zoneId);
                    } else if (zoneName != null) {
                        getArbNodesByZoneName(cs.getTopology(), zoneName);
                    } else {
                        /* All ANs and only ANs of the store */
                        getAllArbNodes(cs.getTopology());
                    }
                } else if (zoneId != null) {
                    /* All services in the zone */
                    getRepNodesByZoneId(cs.getTopology(), zoneId);
                    getAdminsByZoneId(cs, DatacenterId.parse(zoneId));
                    getArbNodesByZoneId(cs.getTopology(), zoneId);
                } else if (zoneName != null) {
                    /* All services in the zone */
                    getRepNodesByZoneName(cs.getTopology(), zoneName);
                    getAdminsByZoneName(cs, zoneName);
                    getArbNodesByZoneName(cs.getTopology(), zoneName);
                } else {
                    /* Nothing was specified! */
                    shell.requiredArg("-service|-all-rns|-all-ans|-zn|-znname",
                                      this);
                }

                /*
                 * No point in issuing a plan if there are no services.
                 */
                if (serviceIds.isEmpty()) {
                    throw new ShellArgumentException(
                        "No services were found in the specified zone");
                }

                final int planId = isStart ?
                   cs.createStartServicesPlan(planName, serviceIds) :
                   cs.createStopServicesPlan(planName, serviceIds);

                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
                throw new AssertionError("Not reached");
            }
        }

        /*
         * Parses the service name and adds the ID to the services. Throws
         * ShellException if the name is not a valid service ID or the
         * service type is not supported.
         */
        private void addService(String serviceName, CommandServiceAPI cs)
            throws ShellException, RemoteException {

            try {
                final RepNodeId rnId = parseRnid(serviceName);
                CommandUtils.ensureRepNodeExists(rnId, cs, this);
                addService(rnId);
                return;
            } catch (ShellException ignore) { }

            try {
                final ArbNodeId anId = parseAnid(serviceName);
                CommandUtils.ensureArbNodeExists(anId, cs, this);
                addService(anId);
                return;
            } catch (ShellException ignore) { }

            try {
                final AdminId adminId = parseAdminid(serviceName);
                final Parameters p = cs.getParameters();
                if (p.get(adminId) == null) {
                    throw new ShellUsageException("Admin does not exist: " +
                                                  adminId, this);
                }
                addService(adminId);
                return;
            } catch (ShellException ignore) { }
            throw new ShellException("Unknown or unsupported service: " +
                                     serviceName);
        }

        private void addService(ResourceId resId) {
            serviceIds.add(resId);
        }
    }

    @POST
    static final class PlanWaitSub extends PlanSubCommand {

        PlanWaitSub() {
            super("wait", 3);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            return new PlanWaitExecutor<String>() {

                @Override
                public String stateResult(State state, int planId)
                    throws ShellException {
                    return state.getWaitMessage(planId);
                }
            }.commonExecute(args, shell);
        }

        private abstract class
            PlanWaitExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, PlanWaitSub.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                int planId = 0;
                int timeoutSecs = 0;
                try {
                    for (int i = 1; i < args.length; i++) {
                        final String arg = args[i];
                        if ("-id".equals(arg)) {
                            final String argString =
                                Shell.nextArg(args, i++, PlanWaitSub.this);
                            planId = parseUnsignedInt(argString);
                        } else if ("-seconds".equals(arg)) {
                            final String argString =
                                Shell.nextArg(args, i++, PlanWaitSub.this);
                            timeoutSecs = parseUnsignedInt(argString);
                        } else if ("-last".equals(arg)) {
                            planId = getLastPlanId(cs);
                            if (planId == 0) {
                                throw new ShellArgumentException(
                                    lastPlanNotFound);
                            }
                        } else {
                            shell.unknownArgument(arg, PlanWaitSub.this);
                        }
                    }
                    if (planId == 0) {
                        shell.requiredArg("-id", PlanWaitSub.this);
                    }
                    CommandUtils.ensurePlanExists(
                        planId, cs, PlanWaitSub.this);
                    final Plan.State state =
                        awaitPlan(planId, timeoutSecs,
                                  TimeUnit.SECONDS, cs, shell);
                    return stateResult(state, planId);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }
            public abstract T stateResult(Plan.State state, int planId)
                throws ShellException;
        }

        @Override
        public ShellCommandResult execJsonOutput(String[] args, Shell shell)
            throws ShellException {

            final ShellCommandResult scr =
                ShellCommandResult.getDefault("plan wait");
            return new PlanWaitExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    stateResult(State state, int planId)
                    throws ShellException {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    on.put("planId", planId);
                    on.put("state", state.toString());
                    scr.setReturnValue(on);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return
                "plan wait -id <id> | -last [-seconds <timeout in seconds>] " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Waits for the specified plan to complete.  If the " +
                "optional timeout" + eolt + "is specified, wait that long, " +
                "otherwise wait indefinitely.  Use -last" + eolt +
                "to reference the most recently created plan.";

        }
    }

    @POST
    static final class RemoveDatacenterSub extends RemoveZoneSub {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "plan remove-datacenter" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "plan remove-zone" + eol + eol;

        RemoveDatacenterSub() {
            super("remove-datacenter", 9);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        /** Add deprecation message. */
        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            if (shell.getJson()) {
                return super.exec(args, shell);
            }
            return dcCommandDeprecation + super.exec(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:" +
                eol + eolt +
                "plan remove-zone";
        }
    }

    @POST
    static class RemoveZoneSub extends PlanSubCommand {
        static final String ID_FLAG = "-zn";
        static final String NAME_FLAG = "-znname";

        RemoveZoneSub() {
            super("remove-zone", 9);
        }

        RemoveZoneSub(final String name, final int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            DatacenterId id = null;
            String nameFlag = null;
            boolean deprecatedDcFlag = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (CommandUtils.isDatacenterIdFlag(arg)) {
                    final String argVal = Shell.nextArg(args, i++, this);
                    id = parseDatacenterId(argVal);
                    if (CommandUtils.isDeprecatedDatacenterId(arg, argVal)) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    nameFlag = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (id == null && nameFlag == null) {
                shell.requiredArg(ID_FLAG + " | " + NAME_FLAG, this);
            }
            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";
            try {
                if (id != null) {
                    CommandUtils.ensureDatacenterExists(id, cs, this);
                } else {
                    id = CommandUtils.getDatacenterId(nameFlag, cs, this);
                }

                final int planId = cs.createRemoveDatacenterPlan(planName, id);
                if (shell.getJson()) {
                    return executePlan(planId, cs, shell);
                }
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        public String getCommandSyntax() {
            return "plan " + name + " " + ID_FLAG + " <id> | " +
                   NAME_FLAG + " <name>" + genericFlags;
        }

        @Override
        public String getCommandDescription() {
            return "Removes the specified zone from the store.";
        }
    }

    static final class AddTableSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";
        static final String NAMESPACE_FLAG = "-namespace";

        AddTableSub() {
            super("add-table", 5);
        }

        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        @SuppressWarnings("null")
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String tableName = null;
            String namespace = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (NAMESPACE_FLAG.equals(arg)) {
                    namespace = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }
            Object obj = shell.getVariable(tableName);
            if (obj == null || !(obj instanceof TableBuilder)) {
                invalidArgument("table " + tableName +
                    " is not yet built, please run the \'table create\' " +
                    "command to build it first.");
            }

            try {
                TableBuilder tb = (TableBuilder)obj;
                if (namespace == null) {
                    namespace = (tb.getNamespace() != null) ?
                                tb.getNamespace() : cmd.getNamespace();
                }
                final int planId =
                    cs.createAddTablePlan(planName,
                                          namespace,
                                          tb.getName(),
                                          ((tb.getParent()!=null)?
                                           tb.getParent().getFullName():null),
                                          tb.getFieldMap(),
                                          tb.getPrimaryKey(),
                                          tb.getPrimaryKeySizes(),
                                          tb.getShardKey(),
                                          tb.getDefaultTTL(),
                                          null, /* limits */
                                          tb.isR2compatible(),
                                          0,
                                          tb.getJsonCollection(),
                                          tb.getMRCounters(),
                                          tb.getDescription());
                shell.removeVariable(tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan add-table " + TABLE_NAME_FLAG + " <name> " +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Add a new table to the store.  " + "The table name is an " +
                "optionally namespace" + eolt + "qualified dot-separated " +
                "name with the format" + eolt +
                "[ns:]tableName[.childTableName]*.  Use the " +
                "table create command to" + eolt + "create the named table." +
                "  Use \"table list -create\" to see the list of " +
                eolt + "tables that can be added.";
        }
    }

    @POST
    static final class EnableRequestsSub extends PlanSubCommand {

        static final String COMMAND_NAME = "enable-requests";
        static final String REQUEST_TYPE = "-request-type";
        static final String TARGET_SHARDS_FLAG = "-shards";
        static final String SHARDS_DELIMITER = ",";
        static final String STORE_FLAG = "-store";
        static final String COMMAND_SYNTAX =
            "plan " + COMMAND_NAME + " " + REQUEST_TYPE +
            " {all|readonly|none} {"+ TARGET_SHARDS_FLAG +
            " <shardId[,shardId]*> | " + STORE_FLAG + "}";
        static final String COMMAND_DESC =
            "Change the type of user requests supported by a set of shards" +
            " or the entire store";
        static final String CONFLICT_FLAGS_ERROR =
            "Cannot use " + TARGET_SHARDS_FLAG + " and " +
            STORE_FLAG + " flags together";

        protected EnableRequestsSub() {
            super(COMMAND_NAME, 3);
        }

        private void addShards(CommandServiceAPI cs,
                               String shardsString,
                               Set<RepGroupId> shards)
            throws RemoteException, ShellException {

            if (shardsString == null) {
                return;
            }
            final String[] shardsArray = shardsString.split(SHARDS_DELIMITER);
            for (String shard : shardsArray) {
                final RepGroupId shardId = parseShardId(shard.trim());
                CommandUtils.ensureShardExists(shardId, cs, this);
                shards.add(shardId);
            }
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String requestType = null;
            String shardString = null;
            boolean entireStore = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (REQUEST_TYPE.equals(arg)) {
                    requestType = Shell.nextArg(args, i++, this);
                } else if (TARGET_SHARDS_FLAG.equals(arg)) {
                    shardString = Shell.nextArg(args, i++, this);
                } else if (STORE_FLAG.equals(arg)) {
                    entireStore = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (shardString != null && entireStore) {
                throw new ShellUsageException(CONFLICT_FLAGS_ERROR, this);
            }
            if (!entireStore && shardString == null) {
                shell.requiredArg(null, this);
            }
            if (requestType == null) {
                shell.requiredArg(REQUEST_TYPE, this);
            }
            try {
                final Set<RepGroupId> shards = new HashSet<RepGroupId>();
                if (TARGET_SHARDS_FLAG != null) {
                    addShards(cs, shardString, shards);
                }
                final int planId = cs.createEnableRequestsPlan(
                    planName, requestType, shards, entireStore);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC;
        }
    }

    static final class EvolveTableSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";

        EvolveTableSub() {
            super("evolve-table", 8);
        }

        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        @SuppressWarnings("null")
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String tableName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }

            Object obj = shell.getVariable(tableName);
            if (obj == null ||
                !(obj instanceof TableEvolver)) {
                invalidArgument("table " + tableName +
                    " is not yet built, please run command \'table evolve\'" +
                    " to build it first.");
            }

            TableEvolver te = (TableEvolver)obj;
            try {
                final int planId =
                    cs.createEvolveTablePlan(planName,
                        te.getTable().getInternalNamespace(),
                        te.getTable().getFullName(),
                        te.getTableVersion(),
                        te.getFieldMap(),
                        te.getNewTableTTL(),
                        te.getNewBeforeImageTTL(),
                        te.getRegions());
                shell.removeVariable(tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan evolve-table " + TABLE_NAME_FLAG + " <name> " +
                   genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Evolve a table in the store.  The table name is an " +
                "optionally namespace" + eolt + "qualified " +
                "dot-separated name with the format " + eolt +
                "[ns:]tableName[.childTableName]*.  The named table must " +
                "have been" + eolt + "evolved using the \"table evolve\" " +
                "command.  Use \"table list -evolve\" " +
                "to " + eolt + "see the list of tables that can be evolved.";
        }
    }

    static final class RemoveTableSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";
        static final String TABLE_NS_FLAG = "-namespace";
        /* No longer supported */
        private static final String KEEP_DATA_FLAG = "-keep-data";

        RemoveTableSub() {
            super("remove-table", 8);
        }

        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String tableName = null;
            String namespace = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (TABLE_NS_FLAG.equals(arg)) {
                    namespace = Shell.nextArg(args, i++, this);
                } else if (KEEP_DATA_FLAG.equals(arg)) {
                    invalidArgument(KEEP_DATA_FLAG + " is no longer supported");
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }
            if (namespace == null) {
                namespace = cmd.getNamespace();
            }

            try {
                final int planId =
                    cs.createRemoveTablePlan(planName, namespace, tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan remove-table " + TABLE_NAME_FLAG + " <name> " +
                   genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Remove a table from the store.  The table name is an " +
                "optionally" + eolt + "namespace qualified dot-separated " +
                "name with the format" + eolt +
                "[ns:]tableName[.childTableName]*.  The named table must exist " +
                "and must" + eolt + "not have " +
                "any child tables.  Indexes on the table are automatically " +
                eolt + "removed.  Data stored in this table is also " +
                "removed.  Depending on " + eolt + "the indexes and amount of " +
                "data stored in the table this may be a " + eolt +
                "long-running plan.";
        }
    }

    @POST
    static final class RevokeSub extends PlanSubCommand {
        static final String USER_FLAG = "-user";
        static final String ROLE_FLAG = "-role";
        static final String COMMAND_SYNTAX =
            "plan revoke -user <user name> [-role <role name>]*" + genericFlags;
        static final String COMMAND_DESC =
            "Revoke roles from a user with specified name. The -role option " +
            "accepts" + eolt + "only predefined roles in KVStore currently.";

        static final String revokeCommandDeprecation =
            "The command:" + eol + eolt +
            "plan revoke" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "execute \'REVOKE\'" + eol + eol;

        RevokeSub() {
            super("revoke", 3);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            if (shell.getJson()) {
                return executeCommand(args, shell);
            }
            return revokeCommandDeprecation + executeCommand(args, shell);
        }

        private String executeCommand(String[] args, Shell shell)
                throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            final Set<String> roles = new HashSet<String>();

            String userName = null;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (USER_FLAG.equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else if (ROLE_FLAG.equals(arg)) {
                    final String role = Shell.nextArg(args, i++, this);
                    roles.add(role.toLowerCase());
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (userName == null) {
                shell.requiredArg(USER_FLAG, this);
            }

            if (roles.isEmpty()) {
                shell.requiredArg(ROLE_FLAG, this);
            }

            try {
                final int planId =
                    cs.createRevokePlan(planName, userName, roles);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {

            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {

            return COMMAND_DESC + eol + eolt +
                   "This command is deprecated and has been replaced by:" +
                   eol + eolt + "execute \'REVOKE\'";
        }
    }

    static final class AddIndexSub extends PlanSubCommand {
        static final String INDEX_NAME_FLAG = "-name";
        static final String TABLE_FLAG = "-table";
        static final String TABLE_NS_FLAG = "-namespace";
        static final String FIELD_FLAG = "-field";
        static final String DESC_FLAG = "-desc";

        AddIndexSub() {
            super("add-index", 5);
        }

        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String indexName = null;
            String tableName = null;
            String namespace = null;
            String desc = null;
            List<String> fields = new ArrayList<>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (INDEX_NAME_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (TABLE_NS_FLAG.equals(arg)) {
                    namespace = Shell.nextArg(args, i++, this);
                } else if (FIELD_FLAG.equals(arg)) {
                    fields.add(Shell.nextArg(args, i++, this));
                } else if (DESC_FLAG.equals(arg)) {
                    desc = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (indexName == null) {
                shell.requiredArg(INDEX_NAME_FLAG, this);
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }
            if (fields.isEmpty()) {
                shell.requiredArg(FIELD_FLAG, this);
            }
            if (namespace == null) {
                namespace = cmd.getNamespace();
            }

            try {
                final int planId =
                    cs.createAddIndexPlan(planName,
                                          namespace,
                                          indexName,
                                          tableName,
                                          fields.toArray(new String[0]),
                                          null,
                                          true, /* indexNulls */
                                          desc);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan add-index " + INDEX_NAME_FLAG + " <name> " +
                TABLE_FLAG + " <name> " +
                "[" + FIELD_FLAG + " <name>]* " + eolt +
                "[" + DESC_FLAG + " <description>]" +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Add an index to a table in the store.  " +
                "The table name is an optionally" + eolt + "namespace " +
                "qualified dot-separated name with the format" + eolt +
                "[ns:]tableName[.childTableName]*.";
        }
    }

    static final class RemoveIndexSub extends PlanSubCommand {
        static final String INDEX_NAME_FLAG = "-name";
        static final String TABLE_FLAG = "-table";
        static final String TABLE_NS_FLAG = "-namespace";

        RemoveIndexSub() {
            super("remove-index", 8);
        }

        @Override
        protected boolean isDeprecated() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String indexName = null;
            String tableName = null;
            String namespace = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (INDEX_NAME_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (TABLE_NS_FLAG.equals(arg)) {
                    namespace = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (indexName == null) {
                shell.requiredArg(INDEX_NAME_FLAG, this);
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }
            if (namespace == null) {
                namespace = cmd.getNamespace();
            }

            try {
                final int planId =
                    cs.createRemoveIndexPlan(planName, namespace,
                                             indexName, tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan remove-index " + INDEX_NAME_FLAG + " <name> " +
                    TABLE_FLAG + " <name> " +
                    genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Remove an index from a table.  The table name is an " +
                "optionally namespace" + eolt + "qualified " +
                "dot-separated name with the format " + eolt +
                "[ns:]tableName[.childTableName]*.";
        }
    }

    @POST
    static final class RegisterEsCluster extends PlanSubCommand {
        static final String COMMAND_SYNTAX=
            "plan register-es -clustername <es-cluster-name> " + eol +
            "-host <es-node-host> -port <es-node-http-port>" +
            " -secure <true|false>" + eol +
            CommandParser.getJsonUsage();

        static final String COMMAND_DESC =
            "Registers an Elasticsearch cluster with the store." + eol +
            "It is only necessary to register one node of the cluster, " + eol +
            "as the other nodes in the cluster will be found automatically.";

        RegisterEsCluster() {
            super("register-es", 5);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String clusterName = null;
            String hostName = null;
            int port = 0;
            String secureVal = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-clustername".equals(arg)) {
                    clusterName = Shell.nextArg(args, i++, this);
                } else if ("-host".equals(arg)) {
                    hostName = Shell.nextArg(args, i++, this);
                } else if ("-port".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    port = parseUnsignedInt(argString);
                } else if ("-secure".equals(arg)) {
                        secureVal = Shell.nextArg(args, i++, this);
                }else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (clusterName == null) {
                shell.requiredArg("-clustername", this);
            }
            if (hostName == null) {
                shell.requiredArg("-hostName", this);
            }
            if (port == 0) {
                shell.requiredArg("-port", this);
            }
            /*
             * FTS Default is secure FTS.
             * Secure FTS is only available in EE version.
             * For CE and BE version, secure flag need to be explicitly
             *  set to false.
             */
            if (secureVal == null) {
                secureVal = "true";
            }

            final HostPort hp = new HostPort(hostName, port);
            try {
                final int planId =
                    cs.createRegisterESClusterPlan(planName,
                                                   clusterName,
                                                   hp.toString(),
                                                   Boolean.parseBoolean(secureVal),
                                                   force);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }

            return "";
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

    @POST
    static final class DeregisterEsCluster extends PlanSubCommand {
        static final String COMMAND_SYNTAX=
            "plan deregister-es " +
            CommandParser.getJsonUsage();

        static final String COMMAND_DESC =
            "Deregisters an Elasticsearch cluster from the store." + eol +
            "This can be done only if all full text indexes are first removed.";

        DeregisterEsCluster() {
            super("deregister-es", 5);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            for (int i = 1; i < args.length; i++) {
                i += checkGenericArg(args[i], args, i);
            }

            try {
                final int planId =
                    cs.createDeregisterESClusterPlan(planName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }

            return "";
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

    @POST
    static final class VerifyDataSub extends PlanSubCommand {
        final String COMMAND_SYNTAX = "plan verify-data " +
            "[-verify-log <enable|disable> [-log-read-delay <milliseconds>]] " +
            eolt +
            "[-verify-btree <enable|disable> " +
            "[-btree-batch-delay <milliseconds>] " +
            eolt +
            "[-index <enable|disable>] [-datarecord <enable|disable>]] " +
            eolt +
            "[-valid-time <time>] [-show-corrupt-files <enable|disable>]" +
            eolt +
            "-service <id> | -all-services [-zn <id> | -znname <name>] " +
            eolt +
            "| -all-rns [-zn <id> | -znname <name>] | " +
            eolt +
            "-all-admins [-zn <id> | -znname <name>] " + genericFlags;

        final String COMMAND_DESC =
            "-verify-log verifies the checksum of each data record " +
            eolt +
            "in the log file of JE. It is enabled by default. " +
            eolt +
            "\t -log-read-delay configures the delay time" +
            eolt +
            "\t between file reads for -verify-log and the default value " +
            eolt +
            "\t is 100 millisecond." +
            eolt +
            eolt +
            "-verify-btree verifies that the b-tree of " +
            eolt +
            "database in memory contains the valid reference " +
            eolt +
            "to each data record in disk. It is enabled by default. " +
            eolt +
            "-verify-btree can be combined with -datarecord and -index." +
            eolt +
            "\t -btree-batch-delay configures the delay time, in " +
            eolt +
            "\t milliseconds, between batches (1000 records) for " +
            eolt +
            "\t -verify-btree and the default value is 10 milliseconds." +
            eolt +
            eolt +
            "\t -datarecord is disabled by default. If it is enabled, " +
            eolt +
            "\t the plan will read and verify data records on disk which " +
            eolt +
            "\t are not in the cache. This will take longer and " +
            eolt +
            "\t cause more read IO." +
            eolt +
            eolt +
            "\t -index is enabled by default. It runs verification " +
            eolt +
            "\t on indexes. -datarecord needs to be enabled to do " +
            eolt +
            "\t a full verification. If -datarecord is disabled, the " +
            eolt +
            "\t command will only verify the reference from index to " +
            eolt +
            "\t primary table, but not the reference from primary table to " +
            eolt +
            "\t index." +
            eolt +
            eolt +
            "-valid-time specifies the amount of time for which an existing " +
            eolt +
            "verification will be considered valid and not be rerun. The " +
            eolt +
            "format is 'number unit' where the unit can be minutes or " +
            eolt +
            "seconds. The unit is case insensitive and can be separated " +
            eolt +
            "from the number by a space, \"-\" or \"_\". The default is " +
            eolt +
            "'10 minutes'." +
            eolt +
            eolt +
            "-show-corrupt-files specifies whether to show corrupt files," +
            eolt +
            " including missing files and reserved files that are " +
            eolt +
            "referenced. It's disabled by default." +
            eolt +
            eolt +
            "Users can run the verification on either the specified " +
            eolt +
            "service using -service, or all service instances of " +
            eolt +
            "the specified type or all types that are deployed to " +
            eolt +
            "the specified zone or all zones using one of the -all-* flags. " +
            eolt +
            "A -all-* flag can be combined with -zn or -znname to verify " +
            eolt +
            "data on all instances of the service type deployed to the " +
            eolt +
            "specified zone. If one of the -all-* flags is used without " +
            eolt +
            "also specifying the zone, then the verification will be run on " +
            eolt +
            "all instances of the specified type or all types within the " +
            eolt +
            "store, regardless of zone.";

        final String logOrBtreeError =
            "Invalid argument: -verify-log, -verify-btree or both must " +
            "be enabled.";

        final String btreeComboError =
            "Invalid argument combination: -btree-batch-delay, -index and " +
            "-datarecord flags cannot be used if -verify-btree is disabled.";

        final String logComboError =
            "invalid argument combination: -log-read-delay flag cannot be " +
            "used if -verify-log is disabled.";

        final String serviceAllError =
            "Invalid argument combination: -service flag cannot be used " +
            "with -all-admins, -all-rns, or -all-services flag.";

        final String serviceDcError =
            "Invalid argument combination: -service flag cannot be used " +
            "with -zn or -znname flag.";

        final String negativeDelayError =
            "-btree-batch-delay and -log-read-delay cannot be less than zero.";

        final String incompatibleAllError =
            "Invalid argument combination: Only one of the flags " +
            "-all-rns, -all-admins, and -all-services may be used.";

        private static final String[] allowedRegex = {"\\s+", "-", "_"};

        boolean verifyLog;
        boolean verifyBtree;
        long btreeDelay;
        long logDelay;
        String serviceName;
        boolean allAdmin, allRN, allService;
        boolean index;
        boolean dataRecord;
        long validTime;
        boolean showMissingFiles;
        RepNodeId rnid;
        AdminId aid;

        VerifyDataSub() {
            super("verify-data", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            verifyLog = VerificationOptions.DEFAULT_VERIFY_LOG;
            verifyBtree = VerificationOptions.DEFAULT_VERIFY_BTREE;
            allAdmin = allRN = allService = false;
            dataRecord = VerificationOptions.DEFAULT_VERIFY_DATARECORD;
            index = VerificationOptions.DEFAULT_VERIFY_INDEX;
            btreeDelay = VerificationOptions.DEFAULT_BTREE_DELAY;
            logDelay = VerificationOptions.DEFAULT_LOG_DELAY;

            serviceName = null;
            rnid = null;
            aid = null;
            validTime = VerificationOptions.DEFAULT_EXPIRED_TIME;
            showMissingFiles = VerificationOptions.DEFAULT_SHOW_MISSING_FILES;


            DatacenterId dcid = null;
            String dcName = null;
            boolean deprecatedDcFlag = false;
            boolean getDcId = false;

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-verify-log".equals(arg)) {
                    verifyLog = enabled(Shell.nextArg(args, i++, this),
                                        "-verify-log");
                } else if ("-verify-btree".equals(arg)) {
                    verifyBtree = enabled(Shell.nextArg(args, i++, this),
                                          "-verify-btree");
                    if (!verifyBtree) {
                        index = false;
                    }
                } else if ("-btree-batch-delay".equals(arg)) {
                    btreeDelay =
                        Integer.parseInt(Shell.nextArg(args, i++, this));
                    if (btreeDelay < 0) {
                        throw new ShellUsageException(negativeDelayError, this);
                    }
                } else if ("-log-read-delay".equals(arg)) {
                    logDelay = Integer.parseInt(Shell.nextArg(args, i++, this));
                    if (logDelay < 0) {
                        throw new ShellUsageException(negativeDelayError, this);
                    }
                } else if ("-service".equals(arg)) {
                    serviceName = Shell.nextArg(args, i++, this);
                } else if ("-all-admins".equals(arg)) {
                    allAdmin = true;
                } else if ("-all-rns".equals(arg)) {
                    allRN = true;
                } else if ("-all-services".equals(arg)) {
                    allService = true;
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = DatacenterId.parse(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else if ("-index".equals(arg)) {
                    index = enabled(Shell.nextArg(args, i++, this), "-index");
                } else if ("-datarecord".equals(arg)) {
                    dataRecord = enabled(Shell.nextArg(args, i++, this),
                                         "-datarecord");
                } else if ("-valid-time".equals(arg)) {
                    validTime = parseValidTime(Shell.nextArg(args, i++, this));

                } else if ("-show-corrupt-files".equals(arg)) {
                    showMissingFiles = enabled(Shell.nextArg(args, i++, this),
                                               "-show-corrupt-files");
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (!(verifyLog || verifyBtree)) {
                throw new ShellUsageException(logOrBtreeError, this);
            }

            if ((!verifyBtree) && (index || dataRecord || (btreeDelay > -1))) {
                throw new ShellUsageException(btreeComboError, this);
            }

            if ((!verifyLog) && (logDelay > -1)) {
                throw new ShellUsageException(logComboError, this);
            }

            if (serviceName == null) {
                if (!(allAdmin || allRN || allService)) {
                    shell.requiredArg(null, this);
                } else {
                    if ((allAdmin ? 1 : 0) + (allRN ? 1 : 0) +
                        (allService ? 1 : 0) > 1) {
                        throw new ShellUsageException(incompatibleAllError,
                                                      this);
                    }
                    if (dcName != null) {
                        getDcId = true;
                    }
                }
            } else {
                if (allAdmin || allRN || allService) {
                    throw new ShellUsageException(serviceAllError, this);
                }

                if (dcid != null || dcName != null) {
                    throw new ShellUsageException(serviceDcError, this);
                }

            }

            String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";

            try {
                int planId = 0;

                if (getDcId) {
                    dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                }
                VerificationOptions options =
                    new VerificationOptions(verifyBtree, verifyLog, index,
                                            dataRecord, btreeDelay, logDelay,
                                            validTime, showMissingFiles);
                if (serviceName != null) {
                    getServiceId(cs);
                    if (aid != null) {
                        shell.verboseOutput("Verifying the database for " +
                                            aid);
                        planId = cs.createVerifyServicePlan(planName, aid,
                                                            options);
                    } else {
                        shell.verboseOutput("Verifying the database for " +
                                            rnid);
                        planId = cs.createVerifyServicePlan(planName, rnid,
                                                            options);
                    }
                } else {
                    String suffixMessage = "";

                    if (dcName != null) {
                        suffixMessage = " deployed to the " + dcName +
                                        " zone";
                    } else if (dcid != null) {
                        suffixMessage = " deployed to the zone with id" +
                                        " = " + dcid;
                    }

                    if (allAdmin) {
                        shell.verboseOutput(
                            "Verifying the databases for all Admins" +
                            suffixMessage);

                        planId = cs.createVerifyAllAdminsPlan(planName, dcid,
                                                              options);

                    } else if (allRN) {
                        shell.verboseOutput(
                            "Verifying the databases for all RepNodes" +
                            suffixMessage);

                        planId = cs.createVerifyAllRepNodesPlan(planName, dcid,
                                                                options);

                    } else {
                        shell.verboseOutput(
                            "Verifying the databases for all services" +
                            suffixMessage);

                        planId = cs.createVerifyAllServicesPlan(planName, dcid,
                                                                options);
                    }

                }
                if (shell.getJson()) {
                    return executePlan(planId, cs, shell);
                }
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);

            } catch (RemoteException e) {
                cmd.noAdmin(e);
                return "";
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

        private void getServiceId(CommandServiceAPI cs)
            throws ShellException, RemoteException {
            Parameters param = cs.getParameters();
            try {
                rnid = RepNodeId.parse(serviceName);
                if (param.get(rnid) == null) {
                    throw new ShellUsageException("No such service" +
                                                  serviceName, this);
                }
            } catch (IllegalArgumentException e) {
                try {
                    aid = AdminId.parse(serviceName);
                    if (param.get(aid) == null) {
                        throw new ShellUsageException("No such service" +
                                                      serviceName, this);
                    }
                } catch (IllegalArgumentException e2) {
                    throw new ShellUsageException("Invalid service name: " +
                                                  serviceName, this);
                }
            }

        }

        private boolean enabled(String input, String option)
            throws ShellException {
            if (input.equals("enable")) {
                return true;
            } else if (input.equals("disable")) {
                return false;
            } else {
                throw new ShellUsageException(option +
                    " must be followed by enable or disable",
                    this);
            }
        }

        private long parseValidTime(String value)
            throws ShellUsageException {
            String msg = "-valid-time must be followed by a valid time " +
                "with a valid unit.";
            if (value == null) {
                throw new ShellUsageException(msg, this);
            }
            for (String regex : allowedRegex) {
                String[] tokens = value.split(regex);
                if (tokens.length == 2) {
                    try {
                        long time = Long.parseLong(tokens[0]);
                        if (tokens[1].equals("minutes")) {
                            time = time * 60000;
                            if (time >= 0) {
                                return time;
                            }
                        } else if (tokens[1].equals("seconds")) {
                            time = time * 1000;
                            if (time >= 0) {
                                return time;
                            }
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
            throw new ShellUsageException(msg, this);
        }

    }

    @POST
    static final class SetTableLimitsSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";
        static final String TABLE_NS_FLAG = "-namespace";
        static final String READ_LIMIT_FLAG = "-read-limit";
        static final String WRITE_LIMIT_FLAG = "-write-limit";
        static final String SIZE_LIMIT_FLAG = "-size-limit";
        static final String INDEX_LIMIT_FLAG = "-index-limit";
        static final String CHILD_TABLE_LIMIT_FLAG = "-child-table-limit";
        static final String INDEX_KEY_SIZE_LIMIT_FLAG = "-index-key-size-limit";

        static final String COMMAND_DESC =
            "Sets table limits. At least one limit must be specified." + eol +
            "Limits which are omitted are not changed";

        SetTableLimitsSub() {
            super("set-table-limits", 6);
        }

        @Override
        protected boolean isHidden() {
            return true;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String tableName = null;
            String namespace = null;

            /* NO_CHANGE indicates that the limit was not specified */
            int readLimit = TableLimits.NO_CHANGE;
            int writeLimit = TableLimits.NO_CHANGE;
            int sizeLimit = TableLimits.NO_CHANGE;
            int indexLimit = TableLimits.NO_CHANGE;
            int childTableLimit = TableLimits.NO_CHANGE;
            int indexKeySizeLimit = TableLimits.NO_CHANGE;

            boolean limitSpecified = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                case TABLE_NAME_FLAG:
                    tableName = Shell.nextArg(args, i++, this);
                    break;
                case TABLE_NS_FLAG:
                    namespace = Shell.nextArg(args, i++, this);
                    break;
                case READ_LIMIT_FLAG:
                    readLimit = parseLimit(Shell.nextArg(args, i++, this));
                    limitSpecified = true;
                    break;
                case WRITE_LIMIT_FLAG:
                    writeLimit = parseLimit(Shell.nextArg(args, i++, this));
                    limitSpecified = true;
                    break;
                case SIZE_LIMIT_FLAG:
                    sizeLimit = parseLimit(Shell.nextArg(args, i++, this));
                    limitSpecified = true;
                    break;
                case INDEX_LIMIT_FLAG:
                    indexLimit = parseLimit(Shell.nextArg(args, i++, this));
                    limitSpecified = true;
                    break;
                case CHILD_TABLE_LIMIT_FLAG:
                    childTableLimit =
                                    parseLimit(Shell.nextArg(args, i++, this));
                    limitSpecified = true;
                    break;
                case INDEX_KEY_SIZE_LIMIT_FLAG:
                    indexKeySizeLimit =
                                    parseLimit(Shell.nextArg(args, i++, this));
                    limitSpecified = true;
                    break;
                default:
                    i += checkGenericArg(arg, args, i);
                    break;
                }
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }
            if (!limitSpecified) {
                shell.requiredArg("at least one limit must be specified", this);
            }
            try {
                final int planId =
                    cs.createTableLimitPlan(planName, namespace, tableName,
                                            new TableLimits(readLimit,
                                                            writeLimit,
                                                            sizeLimit,
                                                            indexLimit,
                                                            childTableLimit,
                                                            indexKeySizeLimit));
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        private int parseLimit(String arg) throws ShellException {
            return arg.equalsIgnoreCase("none") ? TableLimits.NO_LIMIT :
                                                  parseUnsignedInt(arg);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan set-table-limit " + TABLE_NAME_FLAG + " <name> " +
                   "[" + TABLE_NS_FLAG + " <name>] " + eolt +
                   "[" + READ_LIMIT_FLAG + " <KB/sec>|none] " + eolt +
                   "[" + WRITE_LIMIT_FLAG + " <KB/sec>|none] " + eolt +
                   "[" + SIZE_LIMIT_FLAG + " <GB>|none]" + eolt +
                   "[" + INDEX_LIMIT_FLAG + " <# indexes>|none]" + eolt +
                   "[" + CHILD_TABLE_LIMIT_FLAG + "<# tables>|none]" + eolt +
                   "[" + INDEX_KEY_SIZE_LIMIT_FLAG + " <bytes>|none]" +
                   genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC;
        }
    }

    @POST
    static private final class UpdateTlsCredentialsSub extends PlanSubCommand {

        UpdateTlsCredentialsSub() {
            super("update-tls-credentials", 6);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            boolean retrieve = true;
            boolean install = true;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                case "-retrieve-only":
                    install = false;
                    break;
                case "-install-only":
                    retrieve = false;
                    break;
                default:
                    i += checkGenericArg(arg, args, i);
                    break;
                }
            }
            if (!retrieve && !install) {
                throw new ShellUsageException(
                    "The -retrieve-only and -install-only flags cannot both" +
                    " be specified",
                    UpdateTlsCredentialsSub.this);
            }
            try {
                return executePlan(
                    cs.createUpdateTlsCredentialsPlan(
                        planName, retrieve, install),
                    cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan update-tls-credentials" +
                " [-retrieve-only|install-only]" +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Retrieves and installs credential updates to the set of" +
                " shared TLS" + eolt +
                "credentials used by SNAs in the store, doing both by default" +
                " or just one" + eolt +
                "if requested.";
        }
    }
}
