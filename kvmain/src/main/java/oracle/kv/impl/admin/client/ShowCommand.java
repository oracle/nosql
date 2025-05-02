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

import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_AGENT_ID;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_STATISTICS;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_TABLE_ID;
import static oracle.kv.impl.systables.MRTableAgentStatDesc.COL_NAME_TIMESTAMP;
import static oracle.nosql.common.json.JsonUtils.getAsText;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVVersion;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandResult.CommandSucceeds;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RegionMapper;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.systables.MRTableAgentStatDesc;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.TopologyPrinter.Filter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Index;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;
import oracle.kv.util.shell.ShowCommandBase;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/*
 * show and its subcommands
 */
public class ShowCommand extends ShowCommandBase {
    private static final Logger logger =
        LoggerUtils.getLogger(ShowCommand.class, "command");

    private static final List<? extends SubCommand> subs =
        Arrays.asList(new ShowAdmins(),
                      new ShowDatacenters(),
                      new ShowEvents(),
                      new ShowFaults(),
                      new ShowIndexes(),
                      new ShowMRTableAgentStat(),
                      new ShowParameters(),
                      new ShowPerf(),
                      new ShowPlans(),
                      new ShowPools(),
                      new ShowSnapshots(),
                      new ShowTables(),
                      new ShowTlsCredentials(),
                      new ShowTopology(),
                      new ShowUpgradeOrder(),
                      new ShowUsers(),
                      new ShowVersions(),
                      new ShowZones());

    private static final String SHOW_COMMAND_NAME = "show";

    ShowCommand() {
        super(subs);
    }

    /*
     * ShowParameters
     */
    @POST
    private static final class ShowParameters extends SubCommand {

        private ShowParameters() {
            super("parameters", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowParameterExecutor<String>() {

                @Override
                public String parameterMapResult(ParameterMap map,
                                                 boolean showHidden,
                                                 Info info)
                    throws ShellException {
                    return CommandUtils.formatParams(
                               map, showHidden, info);
                }

                @Override
                public String snParamResult(boolean showHidden,
                                            Info info,
                                            StorageNodeParams snp)
                   throws ShellException {
                   String result =
                       CommandUtils.formatParams(snp.getMap(), showHidden,
                                                 info);
                   final ParameterMap storageDirMap =
                       snp.getStorageDirMap();
                   if (storageDirMap != null && !storageDirMap.isEmpty()) {
                       result += "Storage directories:" + eol;
                       for (Parameter param : storageDirMap) {
                           result +=
                               Shell.makeWhiteSpace(4) +
                               "path=" + param.getName() +
                               Shell.makeWhiteSpace(1) +
                               "size=" + param.asString() +
                               eol;
                       }
                   }
                   final ParameterMap rnLogDirMap =
                       snp.getRNLogDirMap();
                   if (rnLogDirMap != null && !rnLogDirMap.isEmpty()) {
                       result += "RN Log directories:" + eol;
                       for (Parameter param : rnLogDirMap) {
                           result +=
                               Shell.makeWhiteSpace(4) +
                               "path=" + param.getName() +
                               eol;
                       }
                   }
                   final ParameterMap adminDirMap =
                       snp.getAdminDirMap();
                   if (adminDirMap != null && !adminDirMap.isEmpty()) {
                        result += "Admin directory:" + eol;
                        for (Parameter param : adminDirMap) {
                            result +=
                                Shell.makeWhiteSpace(4) +
                                "path=" + param.getName() +
                                Shell.makeWhiteSpace(1) +
                                "size=" + param.asString() +
                                eol;
                        }
                    }
                   return result;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowParameterExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                /*
                 * parameters -policy | -global | -security | -service <name>
                 */
                if (args.length < 2) {
                    shell.badArgCount(ShowParameters.this);
                }
                Shell.checkHelp(args, ShowParameters.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                String serviceName = null;
                boolean isPolicy = false;
                boolean isSecurity = false;
                boolean isGlobal = false;

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-policy".equals(arg)) {
                        isPolicy = true;
                    } else if ("-service".equals(arg)) {
                        serviceName =
                            Shell.nextArg(args, i++, ShowParameters.this);
                    } else if ("-security".equals(arg)) {
                        isSecurity = true;
                    } else if ("-global".equals(arg)) {
                        isGlobal = true;
                    } else {
                        shell.unknownArgument(arg, ShowParameters.this);
                    }
                }

                if (((isPolicy ? 1 : 0) +
                     (isSecurity ? 1 : 0) +
                     (isGlobal ? 1 : 0)) > 1) {
                    throw new ShellUsageException(
                        "-policy, -global and -security cannot be used " +
                        "together",
                        ShowParameters.this);
                }
                if (isPolicy) {
                    if (serviceName != null) {
                        throw new ShellUsageException(
                            "-policy cannot be combined with a service",
                            ShowParameters.this);
                    }
                    try {
                        final ParameterMap map = cs.getPolicyParameters();
                        return parameterMapResult(
                            map, cmd.getHidden(), ParameterState.Info.POLICY);
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                }
                if (isSecurity) {
                    if (serviceName != null) {
                        throw new ShellUsageException
                            ("-security cannot be combined with a service",
                             ShowParameters.this);
                    }
                    try {
                        final ParameterMap map =
                            cs.getParameters().getGlobalParams().
                                getGlobalSecurityPolicies();
                        return parameterMapResult(map, cmd.getHidden(), null);
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                }
                if (isGlobal) {
                    if (serviceName != null) {
                        throw new ShellUsageException(
                            "-global cannot be combined with a service",
                            ShowParameters.this);
                    }
                    try {
                        final ParameterMap map =
                            cs.getParameters().getGlobalParams().
                                getGlobalComponentsPolicies();
                        return parameterMapResult(map, cmd.getHidden(), null);
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                }

                if (serviceName == null) {
                    shell.requiredArg("-service|-policy|-security|-global",
                                      ShowParameters.this);
                }

                RepNodeId rnid = null;
                AdminId aid = null;
                ArbNodeId anid = null;
                StorageNodeId snid = null;
                try {
                    rnid = RepNodeId.parse(serviceName);
                } catch (IllegalArgumentException ignored) {
                    try {
                        snid = StorageNodeId.parse(serviceName);
                    } catch (IllegalArgumentException ignored1) {
                        try {
                            aid = AdminId.parse(serviceName);
                        } catch (IllegalArgumentException ignored2) {
                            try {
                                anid = ArbNodeId.parse(serviceName);
                            } catch (IllegalArgumentException ignored3) {
                                invalidArgument(serviceName);
                            }
                        }
                    }
                }

                Parameters p;
                try {
                    p = cs.getParameters();
                    if (rnid != null) {
                        final RepNodeParams rnp = p.get(rnid);
                        if (rnp == null) {
                            noSuchService(rnid);
                        } else {
                            /*
                             * TODO : Need to check if we need to show
                             * rn log dir as part of show parameter
                             * -service rgx-rny
                             */
                            return parameterMapResult(
                                rnp.getMap(), cmd.getHidden(),
                                ParameterState.Info.REPNODE);
                        }
                    } else if (snid != null) {
                        final StorageNodeParams snp = p.get(snid);
                        if (snp == null) {
                            noSuchService(snid);
                        } else {
                            return snParamResult(cmd.getHidden(),
                                                 ParameterState.Info.SNA,
                                                 snp);
                        }
                    } else if (aid != null) {
                        final AdminParams ap = p.get(aid);
                        if (ap == null) {
                            noSuchService(aid);
                        } else {
                            return parameterMapResult(
                                ap.getMap(), cmd.getHidden(),
                                ParameterState.Info.ADMIN);
                        }
                    } else if (anid != null) {
                        final ArbNodeParams anp = p.get(anid);
                        if (anp == null) {
                            noSuchService(anid);
                        } else {
                            return parameterMapResult(
                                anp.getMap(), cmd.getHidden(),
                                ParameterState.Info.ARBNODE);
                        }
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                parameterMapResult(ParameterMap map,
                                   boolean showHidden,
                                   ParameterState.Info info)
                throws ShellException;

            public abstract T snParamResult(boolean showHidden,
                                            ParameterState.Info info,
                                            StorageNodeParams snp)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args,
                                                    Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show parameters");
            return new ShowParameterExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    parameterMapResult(ParameterMap map,
                                       boolean showHidden,
                                       Info info)
                    throws ShellException {
                    scr.setReturnValue(
                        CommandUtils.formatParamsJson(map, showHidden, info));
                    return scr;
                }

                @Override
                public ShellCommandResult snParamResult(boolean showHidden,
                                                        Info info,
                                                        StorageNodeParams snp)
                    throws ShellException {
                    final ObjectNode snNode =
                        CommandUtils.formatParamsJson(snp.getMap(),
                                                      showHidden, info);
                    final ParameterMap storageDirMap = snp.getStorageDirMap();
                    if (storageDirMap != null && !storageDirMap.isEmpty()) {
                        final ArrayNode storageDirArray =
                            snNode.putArray("storageDirs");
                        for (Parameter param : storageDirMap) {
                            final ObjectNode dirNode =
                                JsonUtils.createObjectNode();
                            dirNode.put("path", param.getName());
                            dirNode.put("size", param.asString());
                            storageDirArray.add(dirNode);
                        }
                    }
                    final ParameterMap rnLogDirMap = snp.getRNLogDirMap();
                    if (rnLogDirMap != null && !rnLogDirMap.isEmpty()) {
                        final ArrayNode rnLogDirArray =
                            snNode.putArray("rnlogDirs");
                        for (Parameter param : rnLogDirMap) {
                            final ObjectNode dirNode =
                                JsonUtils.createObjectNode();
                            dirNode.put("path", param.getName());
                            rnLogDirArray.add(dirNode);
                        }
                    }
                    final ParameterMap adminDirMap = snp.getAdminDirMap();
                    if (adminDirMap != null && !adminDirMap.isEmpty()) {
                        final ArrayNode adminDirArray =
                            snNode.putArray("adminDirs");
                        for (Parameter param : adminDirMap) {
                            final ObjectNode dirNode =
                                JsonUtils.createObjectNode();
                            dirNode.put("path", param.getName());
                            dirNode.put("size", param.asString());
                            adminDirArray.add(dirNode);
                        }
                    }
                    scr.setReturnValue(snNode);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show parameters -policy | -global | -security |" +
                   " -service <name> " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays service parameters and state for the specified " +
                "service." + eolt + "The service may be a RepNode, " +
                "StorageNode, or Admin service," + eolt + "as identified " +
                "by any valid string, for example" + eolt + "rg1-rn1, sn1, " +
                "admin2, etc.  Use the -policy flag to show global policy" +
                eolt + "default parameters. Use the -security flag to show global " +
                "security parameters. Use the -global flag to show global " +
                "component parameters.";
        }

        private void noSuchService(ResourceId rid)
            throws ShellException {

            throw new ShellArgumentException
                ("No such service: " + rid);
        }
    }

    /*
     * ShowAdmins
     */
    @POST
    private static final class ShowAdmins extends SubCommand {

        private ShowAdmins() {
            super("admins", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowAdminExecutor<String>() {

                @Override
                public String adminsResult(List<ParameterMap> admins,
                                           Topology t,
                                           String currentAdminHost,
                                           int currentAdminPort,
                                           RegistryUtils registryUtils)
                    throws ShellException {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("");
                    for (ParameterMap map : admins) {
                        final AdminParams params = new AdminParams(map);
                        final StorageNodeId snId =
                            params.getStorageNodeId();
                        sb.append(params.getAdminId());
                        sb.append(": Storage Node ").append(snId);
                        sb.append(" storageDir=" )
                            .append(params.getAdminStorageDir());
                        sb.append(" type=").append(params.getType());

                        sb.append(" (");
                        final StorageNode sn = t.get(snId);
                        if (currentAdminHost.equals(sn.getHostname()) &&
                            currentAdminPort == sn.getRegistryPort()) {
                            sb.append("connected ");
                        }
                        AdminStatus adminStatus;
                        try {
                            adminStatus =
                                registryUtils.getAdmin(snId).
                                getAdminStatus();
                        } catch (Exception e) {
                            adminStatus = null;
                        }
                        if (adminStatus == null) {
                            sb.append("UNREACHABLE");
                        } else {
                            sb.append(adminStatus.getServiceStatus());
                            final State state =
                                adminStatus.getReplicationState();
                            if (state != null) {
                                sb.append(",").append(state);
                                if (state.isMaster() &&
                                    !adminStatus.
                                        getIsAuthoritativeMaster()) {
                                    sb.append(" (non-authoritative)");
                                }
                            }
                        }
                        sb.append(")");
                        sb.append(eol);
                    }
                    return sb.toString();
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowAdminExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowAdmins.this);
                if (args.length > 2) {
                    shell.badArgCount(ShowAdmins.this);
                }
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                final String currentAdminHost = cmd.getAdminHostname();
                final int currentAdminPort = cmd.getAdminPort();

                try {
                    final List<ParameterMap> admins = cs.getAdmins();
                    final Topology t = cs.getTopology();
                    final RegistryUtils registryUtils =
                        new RegistryUtils(t, cmd.getLoginManager(), logger);
                    return adminsResult(admins, t, currentAdminHost,
                        currentAdminPort, registryUtils);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T adminsResult(List<ParameterMap> admins,
                                           Topology t,
                                           String currentAdminHost,
                                           int currentAdminPort,
                                           RegistryUtils registryUtils)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show admins");
            return new ShowAdminExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    adminsResult(List<ParameterMap> admins, Topology t,
                                 String currentAdminHost, int currentAdminPort,
                                 RegistryUtils registryUtils)
                    throws ShellException {

                    final ObjectNode top = JsonUtils.createObjectNode();
                    final ArrayNode adminArray = top.putArray("admins");
                    for (ParameterMap map : admins) {
                        final ObjectNode adminNode =
                            JsonUtils.createObjectNode();
                        final AdminParams params = new AdminParams(map);
                        final StorageNodeId snId = params.getStorageNodeId();
                        adminNode.put(
                            "adminId", params.getAdminId().toString());
                        adminNode.put("snId", snId.toString());
                        adminNode.put("type", params.getType().toString());

                        adminNode.put("connected", false);
                        final StorageNode sn = t.get(snId);
                        if (currentAdminHost != null &&
                            currentAdminHost.equals(sn.getHostname()) &&
                            currentAdminPort == sn.getRegistryPort()) {
                            adminNode.put("connected", true);
                        }

                        AdminStatus adminStatus;
                        try {
                            adminStatus =
                                registryUtils.getAdmin(snId).getAdminStatus();
                        } catch (Exception e) {
                            adminStatus = null;
                        }
                        if (adminStatus == null) {
                            adminNode.put("adminStatus", "UNREACHABLE");
                        } else {
                            adminNode.put("adminStatus",
                                          adminStatus.getServiceStatus().
                                              toString());
                            final State state =
                                adminStatus.getReplicationState();
                            if (state != null) {
                                adminNode.put(
                                    "replicationState", state.toString());
                                adminNode.put("authoritative", true);
                                if (state.isMaster() &&
                                    !adminStatus.getIsAuthoritativeMaster()) {
                                    adminNode.put("authoritative", false);
                                }
                            }
                        }
                        adminArray.add(adminNode);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show admins " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Displays basic information about deployed Admin services.";
        }
    }

    /*
     * ShowTopology
     */
    @POST
    static final class ShowTopology extends SubCommand {
        static final String rnFlag = "-rn";
        static final String snFlag = "-sn";
        static final String stFlag = "-store";
        static final String shFlag = "-shard";
        static final String statusFlag = "-status";
        static final String perfFlag = "-perf";
        static final String anFlag = "-an";
        static final String dcFlagsDeprecation =
            "The -dc flag is deprecated and has been replaced by -zn." +
            eol + eol;

        private ShowTopology() {
            super("topology", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowTopologyExecutor<String>() {
                @Override
                public String
                    createTopologyResult(Topology t, PrintStream out,
                                         Parameters p,
                                         EnumSet<Filter> filter,
                                         Map<ResourceId, ServiceChange>
                                             statusMap,
                                         Map<ResourceId, PerfEvent> perfMap,
                                         boolean verbose,
                                         ByteArrayOutputStream outStream,
                                         String deprecatedDcFlagPrefix)
                    throws ShellException{
                    TopologyPrinter.printTopology(t, out, p, filter,
                                                  statusMap,
                                                  perfMap,
                                                  verbose);
                    return deprecatedDcFlagPrefix + outStream;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowTopologyExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                EnumSet<TopologyPrinter.Filter> filter =
                    EnumSet.noneOf(Filter.class);
                Shell.checkHelp(args, ShowTopology.this);
                boolean hasComponents = false;
                boolean deprecatedDcFlag = false;
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (CommandUtils.isDatacenterIdFlag(args[i])) {
                            filter.add(Filter.DC);
                            hasComponents = true;
                            if ("-dc".equals(args[i])) {
                                deprecatedDcFlag = true;
                            }
                        } else if (args[i].equals(rnFlag)) {
                            filter.add(Filter.RN);
                            hasComponents = true;
                        } else if (args[i].equals(snFlag)) {
                            filter.add(Filter.SN);
                            hasComponents = true;
                        } else if (args[i].equals(stFlag)) {
                            filter.add(Filter.STORE);
                            hasComponents = true;
                        } else if (args[i].equals(shFlag)) {
                            filter.add(Filter.SHARD);
                            hasComponents = true;
                        } else if (args[i].equals(statusFlag)) {
                            filter.add(Filter.STATUS);
                        } else if (args[i].equals(perfFlag)) {
                            filter.add(Filter.PERF);
                        } else if (args[i].equals(anFlag)) {
                            filter.add(Filter.AN);
                            hasComponents = true;
                        } else {
                            shell.unknownArgument(args[i], ShowTopology.this);
                        }
                    }
                } else {
                    filter = TopologyPrinter.all;
                    hasComponents = true;
                }

                if (!hasComponents) {
                    filter.addAll(TopologyPrinter.components);
                }

                final String deprecatedDcFlagPrefix =
                    !deprecatedDcFlag ? "" : dcFlagsDeprecation;

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);

                    final Topology t = cs.getTopology();
                    final Parameters p = cs.getParameters();
                    Map<ResourceId, ServiceChange> statusMap = null;
                    if (filter.contains(Filter.STATUS)) {
                        statusMap = cs.getStatusMap();
                    }
                    Map<ResourceId, PerfEvent> perfMap = null;
                    if (filter.contains(Filter.PERF)) {
                        perfMap = cs.getPerfMap();
                    }
                    return createTopologyResult(t, out, p, filter, statusMap,
                                                perfMap, shell.getVerbose(),
                                                outStream,
                                                deprecatedDcFlagPrefix);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                createTopologyResult(Topology t, PrintStream out,
                                     Parameters params,
                                     EnumSet<Filter> filter,
                                     Map<ResourceId, ServiceChange> statusMap,
                                     Map<ResourceId, PerfEvent> perfMap,
                                     boolean verbose,
                                     ByteArrayOutputStream outStream,
                                     String deprecatedDcFlagPrefix)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show topology");
            return new ShowTopologyExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult
                    createTopologyResult(Topology t, PrintStream out,
                                         Parameters p,
                                         EnumSet<Filter> filter,
                                         Map<ResourceId, ServiceChange>
                                             statusMap,
                                         Map<ResourceId, PerfEvent> perfMap,
                                         boolean verbose,
                                         ByteArrayOutputStream outStream,
                                         String deprecatedDcFlagPrefix)
                    throws ShellException{
                    scr.setReturnValue(
                        TopologyPrinter.printTopologyJson(
                            t, p, filter, verbose));
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return
                "show topology [-zn] [-rn] [-an] [-sn] [-store] [-status]" +
                " [-perf] " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays the current, deployed topology. " +
                "By default show the entire " + eolt +
                "topology. The optional flags restrict the " +
                "display to one or more of" + eolt + "Zones, " +
                "RepNodes, StorageNodes and Storename," + eolt + "or " +
                "specify service status or performance.";
        }
    }

    /*
     * ShowEvents
     */
    @POST
    private static final class ShowEvents extends SubCommand {

        private ShowEvents() {
            super("events", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowEventExecutor<String>() {

                @Override
                public String singleEvent(String[] shellArgs,
                                          CommandServiceAPI cs,
                                          CommandShell cmd,
                                          Shell commandShell)
                    throws ShellException {
                    return showSingleEvent(shellArgs, cs, cmd, commandShell);
                }

                @Override
                public String multiEvents(List<CriticalEvent> events)
                    throws ShellException {
                    String msg = "";
                    for (CriticalEvent ev : events) {
                        msg += ev.toString() + eol;
                    }
                    return msg;
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowEventExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowEvents.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                if (Shell.checkArg(args, "-id")) {
                    return singleEvent(args, cs, cmd, shell);
                }
                try {
                    Date fromTime = null;
                    Date toTime = null;
                    CriticalEvent.EventType et = CriticalEvent.EventType.ALL;

                    for (int i = 1; i < args.length; i++) {
                        if ("-from".equals(args[i])) {
                            if (++i >= args.length) {
                                shell.badArgCount(ShowEvents.this);
                            }
                            fromTime =
                                parseTimestamp(args[i], ShowEvents.this);
                            if (fromTime == null) {
                                throw new ShellArgumentException(
                                    "Can't parse " + args[i] +
                                    " as a timestamp.");
                            }
                        } else if ("-to".equals(args[i])) {
                            if (++i >= args.length) {
                                shell.badArgCount(ShowEvents.this);
                            }
                            toTime = parseTimestamp(args[i], ShowEvents.this);
                        } else if ("-type".equals(args[i])) {
                            if (++i >= args.length) {
                                shell.badArgCount(ShowEvents.this);
                            }
                            try {
                                final String etype = args[i].toUpperCase();
                                et = Enum.valueOf(
                                         CriticalEvent.EventType.class,
                                         etype);
                            } catch (IllegalArgumentException iae) {
                                throw new ShellUsageException
                                    ("Can't parse " + args[i] +
                                     " as an EventType.", ShowEvents.this);
                            }
                        } else {
                            shell.unknownArgument(args[i], ShowEvents.this);
                        }
                    }

                    final long from =
                        (fromTime == null ? 0L : fromTime.getTime());
                    final long to = (toTime == null ? 0L : toTime.getTime());

                    final List<CriticalEvent> events =
                        cs.getEvents(from, to, et);
                    return multiEvents(events);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                singleEvent(String[] args, CommandServiceAPI cs,
                            CommandShell cmd, Shell shell)
                throws ShellException;

            public abstract T multiEvents(List<CriticalEvent> events)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args,
                                                    Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show events");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode eventArray = top.putArray("events");
            return new ShowEventExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult singleEvent(String[] shellArgs,
                                                      CommandServiceAPI cs,
                                                      CommandShell cmd,
                                                      Shell commandShell)
                    throws ShellException {

                    if (args.length != 3) {
                        shell.badArgCount(ShowEvents.this);
                    }

                    if (!"-id".equals(args[1])) {
                        shell.unknownArgument(args[1], ShowEvents.this);
                    }
                    final String eventId = args[2];
                    try {
                        final CriticalEvent event =
                            cs.getOneEvent(eventId);
                        if (event == null) {
                            throw new ShellArgumentException(
                                "No event matches the id " + eventId);
                        }
                        eventArray.add(event.getDetailString());
                        scr.setReturnValue(top);
                        return scr;
                    } catch (RemoteException re) {
                        cmd.noAdmin(re);
                    }
                    return null;
                }

                @Override
                public ShellCommandResult
                    multiEvents(List<CriticalEvent> events)
                    throws ShellException {
                    for (CriticalEvent ev : events) {
                        eventArray.add(ev.getDetailString());
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show events [-id <id>] | [-from <date>] " +
                "[-to <date>]" + eolt + "[-type <stat|log|perf>] " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays event details or list of store events.  Status " +
                "events indicate" + eolt + "changes in service status.  " +
                "Log events correspond to records written " + eolt + "to " +
                "the store's log, except that only records logged at " +
                "\"SEVERE\" are " + eolt + "displayed; which should be " +
                "investigated immediately.  To view records " + eolt +
                "logged at \"WARNING\" or lower consult the store's log " +
                "file." + eolt + "Performance events are not usually " +
                "critical but may merit investigation." + eolt + eolt +
                getDateFormatsUsage();
        }

        private String showSingleEvent(String[] args,
                                       CommandServiceAPI cs,
                                       CommandShell cmd,
                                       Shell shell)
            throws ShellException {

            if (args.length != 3) {
                shell.badArgCount(this);
            }

            if (!"-id".equals(args[1])) {
                shell.unknownArgument(args[1], this);
            }
            final String eventId = args[2];
            try {
                final CriticalEvent event = cs.getOneEvent(eventId);
                if (event == null) {
                    return "No event matches the id " + eventId;
                }
                return event.getDetailString();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowPlans
     *
     * show plans with no arguments: list the ten most recent plans.
     *
     * -last: show details of the most recently created plan.
     *
     * -id <id>: show details of the plan with the given sequence number, or
     *   If -num <n> is also given, list <n> plans, starting with plan #<id>.
     *
     * -num <n>: set the number of plans to list. Defaults to 10.
     *   If unaccompanied: list the n most recently created plans.
     *
     * -from <date>: list plans starting with those created after <date>.
     *
     * -to <date>: list plans ending with those created before <date>.
     *   Combining -from with -to describes the range between the two <dates>.
     *   Otherwise -num <n> applies; its absence implies the default of 10.
     */
    @POST
    private static final class ShowPlans extends SubCommandJsonConvert {

        private static final String lastPlanNotFound =
            "Found no plans created by the current user.";

        private String operation;

        private ShowPlans() {
            super("plans", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            operation = SHOW_COMMAND_NAME + " " + getCommandName();
            if ((args.length == 3 && Shell.checkArg(args, "-id")) ||
                 (args.length == 2 && Shell.checkArg(args, "-last"))) {
                return showSinglePlan(shell, args, cs, cmd);
            }

            int planId = 0;
            int howMany = 0;
            Date fromTime = null, toTime = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-id".equals(arg)) {
                    planId = parseUnsignedInt(Shell.nextArg(args, i++, this));
                } else if ("-num".equals(arg)) {
                    howMany = parseUnsignedInt(Shell.nextArg(args, i++, this));
                } else if ("-from".equals(arg)) {
                    fromTime =
                        parseTimestamp(Shell.nextArg(args, i++, this), this);
                } else if ("-to".equals(arg)) {
                    toTime =
                        parseTimestamp(Shell.nextArg(args, i++, this), this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (planId != 0 && !(fromTime == null && toTime == null)) {
                throw new ShellUsageException
                    ("-id cannot be used in combination with -from or -to",
                     this);
            }

            /* If no other range selector is given, default to most recent. */
            if (planId == 0 && fromTime == null && toTime == null) {
                toTime = new Date();
            }

            /* If no other range limit is given, default to 10. */
            if ((fromTime == null || toTime == null) && howMany == 0) {
                howMany = 10;
            }

            /*
             * If a time-based range is requested, we need to get plan ID range
             * information first.
             */
            if (! (fromTime == null && toTime == null)) {
                try {

                    int range[] =
                        cs.getPlanIdRange
                        (fromTime == null ? 0L : fromTime.getTime(),
                         toTime == null ? 0L : toTime.getTime(),
                         howMany);

                    planId = range[0];
                    howMany = range[1];
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }

            String msg = "";
            ObjectNode returnNode = JsonUtils.createObjectNode();
            ArrayNode planNodes = returnNode.putArray("plans");
            while (howMany > 0) {
                try {
                    SortedMap<Integer, Plan> sortedPlans =
                        new TreeMap<Integer, Plan>
                        (cs.getPlanRange(planId, howMany));

                    /* If we got zero plans back, we're out of plans. */
                    if (sortedPlans.size() == 0) {
                        break;
                    }

                    for (Integer k : sortedPlans.keySet()) {
                        final Plan p = sortedPlans.get(k);
                        if (shell.getJson()) {
                            ObjectNode node = JsonUtils.createObjectNode();
                            node.put("id", p.getId());
                            node.put("name", p.getName());
                            node.put("state", p.getState().name());
                            planNodes.add(node);
                        } else {
                            msg += String.format("%6d %-24s %s" + eol,
                                p.getId(),
                                p.getName(),
                                p.getState().toString());
                        }

                        howMany--;
                        planId = k.intValue() + 1;
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }
            if (shell.getJson()) {
                CommandResult result = new CommandSucceeds(
                    returnNode.toString());
                return Shell.toJsonReport(operation, result);
            }
            return msg;
        }

        @Override
        protected String getCommandSyntax() {
            return "show plans [-last] [-id <id>] [-from <date>] " +
                   "[-to <date>] [-num <howMany>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Shows details of the specified plan or lists all plans " +
                "that have been" + eolt + "created along with their " +
                "corresponding plan IDs and status." + eolt +
                eolt +
                "With no argument: lists the ten most recent plans." + eolt +
                "-last: shows details of the most recent plan" + eolt +
                "-id <id>: shows details of the plan with the given id;" + eolt+
                "    if -num <n> is also given, list <n> plans," + eolt +
                "    starting with plan #<id>." + eolt +
                "-num <n>: sets the number of plans to list." + eolt +
                "    Defaults to 10." + eolt +
                "-from <date>: lists plans after <date>." + eolt +
                "-to <date>: lists plans before <date>." + eolt +
                "    Combining -from with -to describes the range" + eolt +
                "    between the two <dates>.  Otherwise -num applies." + eolt +
                "-json: return result in json format." + eolt +
                eolt +
                getDateFormatsUsage();
        }

        /*
         * Show details of a single plan.  TODO: add flags for varying details:
         * -tasks -finished, etc.
         */
        private String showSinglePlan(Shell shell, String[] args,
                                      CommandServiceAPI cs, CommandShell cmd)
            throws ShellException {

            int planId = 0;
            final boolean verbose = shell.getVerbose();

            try {
                if ("-last".equals(args[1])) {
                    planId = PlanCommand.PlanSubCommand.getLastPlanId(cs);
                    if (planId == 0) {
                        if (shell.getJson()) {
                            CommandResult result =
                                new CommandFails(lastPlanNotFound,
                                                 ErrorMessage.NOSQL_5200,
                                                 CommandResult.NO_CLEANUP_JOBS);
                            return Shell.toJsonReport(operation, result);
                        }
                        return lastPlanNotFound;
                    }
                } else if ("-id".equals(args[1])) {
                    if (args.length != 3) {
                        shell.badArgCount(this);
                    }
                    planId = parseUnsignedInt(args[2]);
                } else {
                    shell.unknownArgument(args[1], this);
                }

                long options = StatusReport.SHOW_FINISHED_BIT;
                if (verbose) {
                    options |= StatusReport.VERBOSE_BIT;
                }
                final String planStatus =
                    cs.getPlanStatus(
                        planId, options, shell.getJson());
                if (shell.getJson()) {
                    CommandResult result = new CommandSucceeds(planStatus);
                    return Shell.toJsonReport(operation, result);
                }
                return planStatus;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowPools
     */
    @POST
    static final class ShowPools extends SubCommand {

        ShowPools() {
            super("pools", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
             throws ShellException {

            return new ShowPoolExecutor<String>() {

                @Override
                public String allPoolsResult(List<String> poolNames,
                                             CommandServiceAPI cs,
                                             Topology topo)
                    throws ShellException, RemoteException {
                    String res = "";
                    for (String pn : poolNames) {
                       res += pn + ": ";
                       for (StorageNodeId snid :
                           cs.getStorageNodePoolIds(pn)) {
                           DatacenterId dcid = topo.get(snid)
                               .getDatacenterId();
                           String dcName = topo.getDatacenterMap()
                               .get(dcid).getName();
                           res += snid.toString() + " zn:[id="
                               + dcid + " name="
                               + dcName + "], ";
                        }
                        res = res.substring(0, res.length() - 2);
                        res += eol;
                    }
                    return res;
                }

                @Override
                public String singlePoolResult(String poolName,
                                               CommandServiceAPI cs,
                                               Topology topo)
                    throws ShellException, RemoteException {
                    String res = poolName + ": ";
                    for (StorageNodeId snid :
                        cs.getStorageNodePoolIds(poolName)) {
                        DatacenterId dcid = topo.get(snid)
                            .getDatacenterId();
                        String dcName = topo.getDatacenterMap()
                            .get(dcid).getName();
                        res += snid.toString() + " zn:[id="
                            + dcid + " name="
                            + dcName + "], ";
                    }
                    res = res.substring(0, res.length() - 2);
                    res += eol;
                    return res;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowPoolExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowPools.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                String poolName = null;
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-name".equals(arg)) {
                        poolName = Shell.nextArg(args, i++, ShowPools.this);
                    } else {
                        throw new ShellUsageException(
                            "Invalid argument: " + arg, ShowPools.this);
                    }
                }

                try{
                    Topology topo = cs.getTopology();
                    /* show pools */
                    if (args.length == 1) {
                        final List<String> poolNames =
                            cs.getStorageNodePoolNames();
                        return allPoolsResult(poolNames, cs, topo);
                    }
                    if (cs.getStorageNodePoolNames().contains(poolName)) {
                        return singlePoolResult(poolName, cs, topo);
                    }
                    throw new ShellArgumentException(
                        "Not a valid pool name: " + poolName);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                allPoolsResult(List<String> poolNames,
                               CommandServiceAPI cs,
                               Topology topo)
                throws ShellException, RemoteException;

            public abstract T
                singlePoolResult(String poolName,
                                 CommandServiceAPI cs,
                                 Topology topo)
                throws ShellException, RemoteException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show pool");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode poolArray = top.putArray("pools");
            return new ShowPoolExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    allPoolsResult(List<String> poolNames,
                                   CommandServiceAPI cs,
                                   Topology topo)
                    throws ShellException, RemoteException {

                    for (String pn : poolNames) {
                        final ObjectNode poolNode =
                            JsonUtils.createObjectNode();
                        poolNode.put("poolName", pn);
                        final ArrayNode snArray = poolNode.putArray("sns");
                        for (StorageNodeId snid :
                            cs.getStorageNodePoolIds(pn)) {
                            final ObjectNode snNode =
                                JsonUtils.createObjectNode();
                            final DatacenterId dcid =
                                topo.get(snid).getDatacenterId();
                            final String dcName =
                                topo.getDatacenterMap().get(dcid).getName();
                            snNode.put("resourceId", snid.toString());
                            snNode.put("znId", dcid.toString());
                            snNode.put("zoneName", dcName);
                            snArray.add(snNode);
                        }
                        poolArray.add(poolNode);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    singlePoolResult(String poolName,
                                     CommandServiceAPI cs,
                                     Topology topo)
                    throws ShellException, RemoteException {
                    final ObjectNode poolNode =
                        JsonUtils.createObjectNode();
                    poolNode.put("poolName", poolName);
                    final ArrayNode snArray = poolNode.putArray("sns");
                    for (StorageNodeId snid :
                        cs.getStorageNodePoolIds(poolName)) {
                        final ObjectNode snNode =
                            JsonUtils.createObjectNode();
                        final DatacenterId dcid =
                            topo.get(snid).getDatacenterId();
                        final String dcName =
                            topo.getDatacenterMap().get(dcid).getName();
                        snNode.put("snId", snid.toString());
                        snNode.put("zoneId", dcid.toString());
                        snNode.put("zoneName", dcName);
                        snArray.add(snNode);
                    }
                    poolArray.add(poolNode);
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show pools [-name <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Lists the storage node pools";
        }
    }

    /*
     * TODO: Add filter flags
     */
    @POST
    private static final class ShowPerf extends SubCommand {

        private ShowPerf() {
            super("perf", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowPerfExecutor<String>() {
                @Override
                public String
                    multiPerfResult(Map<ResourceId, PerfEvent> map) {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);
                    out.println(PerfEvent.HEADER);
                    for (PerfEvent pe : map.values()) {
                        out.println(pe.getColumnFormatted());
                    }
                    return outStream.toString();
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowPerfExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final Map<ResourceId, PerfEvent> perfMap = cs.getPerfMap();
                    return multiPerfResult(perfMap);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }
            public abstract T multiPerfResult(
                Map<ResourceId, PerfEvent> map);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show perf");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode perfArray = top.putArray("perfs");
            return new ShowPerfExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult
                multiPerfResult(Map<ResourceId, PerfEvent> map) {
                    for (PerfEvent pe : map.values()) {
                        perfArray.add(pe.getColumnFormatted());
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show perf " +
                    CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays recent performance information for each " +
                "Replication Node.";
        }
    }

    @POST
    private static final class ShowSnapshots extends SubCommand {

        private ShowSnapshots() {
            super("snapshots", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowSnapshotExecutor<String>() {

                @Override
                public String multiLineResult(String[] lines) {
                    String ret = "";
                    for (String ss : lines) {
                        ret += ss + eol;
                    }
                    return ret;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowSnapshotExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                Shell.checkHelp(args, ShowSnapshots.this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                StorageNodeId snid = null;
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-sn".equals(arg)) {
                        final String sn =
                            Shell.nextArg(args, i++, ShowSnapshots.this);
                        try {
                            snid = StorageNodeId.parse(sn);
                        } catch (IllegalArgumentException iae) {
                            invalidArgument(sn);
                        }
                    } else {
                        shell.unknownArgument(arg, ShowSnapshots.this);
                    }
                }
                try {
                    final Snapshot snapshot =
                        new Snapshot(cs, shell.getVerbose(),
                                     shell.getOutput());
                    String [] list = null;
                    if (snid != null) {
                        list = snapshot.listSnapshots(snid);
                    } else {
                        list = snapshot.listSnapshots();
                    }
                    return multiLineResult(list);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                return null;
            }

            public abstract T multiLineResult(String[] lines);

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show snapshot");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode snapshotArray = top.putArray("snapshots");
            return new ShowSnapshotExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    multiLineResult(String[] lines) {
                    for (String ss : lines) {
                        snapshotArray.add(ss);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show snapshots [-sn <id>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists snapshots on the specified Storage Node. If no " +
                "Storage Node" + eolt + "is specified one is chosen from " +
                "the store.";
        }
    }

    @POST
    private static final class ShowUpgradeOrder extends SubCommand {

        private ShowUpgradeOrder() {
            super("upgrade-order", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "show upgrade-order " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the Storage Nodes which need to be upgraded in an " +
                "order that" + eolt + "prevents disruption to the store's " +
                "operation.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowUpgradeOrderExecutor<String>() {

                @Override
                public String retrieveUpgradeOrder(CommandServiceAPI cs,
                                                   KVVersion current,
                                                   KVVersion prerequisite)
                    throws RemoteException {
                    return cs.getUpgradeOrder(current, prerequisite);
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowUpgradeOrderExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                try {
                    /*
                     * Thus command gets the order for upgrading to the
                     * version of the CLI.
                     */
                    return
                        retrieveUpgradeOrder(cs,
                                             KVVersion.CURRENT_VERSION,
                                             KVVersion.PREREQUISITE_VERSION);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T retrieveUpgradeOrder(CommandServiceAPI cs,
                                                   KVVersion current,
                                                   KVVersion prerequisite)
                throws RemoteException;
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show upgrade-order");
            return new ShowUpgradeOrderExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    retrieveUpgradeOrder(CommandServiceAPI cs,
                                         KVVersion current,
                                         KVVersion prerequisite)
                    throws RemoteException {
                    final ObjectNode top = JsonUtils.createObjectNode();
                    List<Set<StorageNodeId>> result =
                        cs.getUpgradeOrderList(current, prerequisite);
                    final ArrayNode orderArray =
                        top.putArray("upgradeOrders");
                    for (Set<StorageNodeId> set : result) {
                        final ObjectNode on = JsonUtils.createObjectNode();
                        final ArrayNode upgradeNodes =
                            on.putArray("upgradeNodes");
                        for (StorageNodeId id : set) {
                            upgradeNodes.add(id.toString());
                        }
                        orderArray.add(on);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }

            }.commonExecute(args, shell);
        }

    }

    /*
     * ShowDatacenters
     */
    @POST
    static final class ShowDatacenters extends ShowZones {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "show datacenters" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "show zones" + eol + eol;

        ShowDatacenters() {
            super("datacenters", 4);
        }

        /** This is a deprecated command. Return true. */
        @Override
        protected boolean isDeprecated() {
            return true;
        }

        /** Add deprecation message. */
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return dcCommandDeprecation + super.execute(args, shell);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            return super.executeJsonOutput(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:"
                + eol + eolt +
                "show zones";
        }
    }

    @POST
    static class ShowZones extends SubCommand {
        static final String ID_FLAG = "-zn";
        static final String NAME_FLAG = "-znname";
        static final String DESC_STR = "zone";
        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        private ShowZones() {
            super("zones", 4);
        }

        ShowZones(final String name, final int prefixLength) {
            super(name, prefixLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowZoneExecutor<String>() {

                @Override
                public String zoneResult(DatacenterId id,
                                         String nameFlag,
                                         Topology topo,
                                         Parameters params,
                                         String deprecatedDcFlagPrefix) {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);
                    TopologyPrinter.printZoneInfo(
                        id, nameFlag, topo, out, params);
                    return deprecatedDcFlagPrefix + outStream;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowZoneExecutor<T> implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                DatacenterId id = null;
                String nameFlag = null;
                boolean deprecatedDcFlag = false;

                Shell.checkHelp(args, ShowZones.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (CommandUtils.isDatacenterIdFlag(args[i])) {
                            try {
                                id = DatacenterId.parse(
                                    Shell.nextArg(args, i++, ShowZones.this));
                            } catch (IllegalArgumentException e) {
                                throw new ShellUsageException(
                                    "Invalid zone ID: " + args[i],
                                    ShowZones.this);
                            }
                            if (CommandUtils.isDeprecatedDatacenterId(
                                    args[i-1], args[i])) {
                                deprecatedDcFlag = true;
                            }
                        } else if (CommandUtils.
                                   isDatacenterNameFlag(args[i])) {
                            nameFlag =
                                Shell.nextArg(args, i++, ShowZones.this);
                            if (CommandUtils.isDeprecatedDatacenterName(
                                    args[i-1])) {
                                deprecatedDcFlag = true;
                            }
                        } else {
                            shell.unknownArgument(args[i], ShowZones.this);
                        }
                    }
                }

                final String deprecatedDcFlagPrefix =
                    !deprecatedDcFlag ? "" : dcFlagsDeprecation;

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final Topology topo = cs.getTopology();
                    final Parameters params = cs.getParameters();
                    return zoneResult(id, nameFlag, topo, params,
                                      deprecatedDcFlagPrefix);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T
                zoneResult(DatacenterId id,
                           String nameFlag,
                           Topology topo,
                           Parameters params,
                           String deprecatedDcFlagPrefix)
                throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show zones");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode zoneArray = top.putArray("zns");
            return new ShowZoneExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    zoneResult(DatacenterId id, String nameFlag,
                               Topology topo, Parameters params,
                               String deprecatedDcFlagPrefix)
                    throws ShellException{
                    final boolean showAll =
                        ((id == null) && (nameFlag == null) ? true : false);
                    Datacenter showZone = null;

                    /*
                     * Display zones, sorted by ID
                     */
                    final List<Datacenter> dcList =
                        topo.getSortedDatacenters();
                    for (final Datacenter zone : dcList) {
                        if (showAll) {
                            zoneArray.add(zone.toJson());
                        } else {
                            if ((id != null) &&
                                id.equals(zone.getResourceId())) {
                                showZone = zone;
                                break;
                            } else if ((nameFlag != null) &&
                                nameFlag.equals(zone.getName())) {
                                showZone = zone;
                                break;
                            }
                        }
                    }
                    if (showAll) {
                        scr.setReturnValue(top);
                        return scr;
                    }

                    /*
                     * If showZone is null, then the id or name input is
                     * unknown
                     */
                    if (showZone == null) {
                        throw new ShellArgumentException(
                            DatacenterId.DATACENTER_PREFIX +
                            ": unknown id or name");
                    }

                    final ArrayNode snArray = JsonUtils.createArrayNode();
                    final DatacenterId showZoneId = showZone.getResourceId();
                    final List<StorageNode> snList =
                        topo.getSortedStorageNodes();
                    StorageNodeParams snp = null;

                    for (StorageNode sn: snList) {
                        if (showZoneId.equals(sn.getDatacenterId())) {
                            final ObjectNode snNode =
                                JsonUtils.createObjectNode();
                            snNode.put("resourceId",
                                sn.getResourceId().toString());
                            snNode.put("hostname", sn.getHostname());
                            snNode.put("registryPort", sn.getRegistryPort());
                            if (params != null) {
                                snp = params.get(sn.getResourceId());
                                if (snp != null) {
                                    snNode.put("capacity", snp.getCapacity());
                                }
                            }
                            snArray.add(snNode);
                        }
                    }

                    ObjectNode zone = showZone.toJson();
                    zone.put("sns", snArray);
                    zoneArray.add(zone);

                    scr.setReturnValue(top);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + name +
                   " [" + ID_FLAG + " <id> | " + NAME_FLAG + " <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the names of all " + DESC_STR + "s, or display " +
                "information about a" + eolt +
                "specific " + DESC_STR + ". If no " + DESC_STR + " is " +
                "specified, list the names" + eolt +
                "of all " + DESC_STR + "s. If a specific " + DESC_STR +
                " is specified using" + eolt +
                "either the " + DESC_STR + "'s id (via the '" + ID_FLAG +
                "' flag), or the " + DESC_STR + "'s" + eolt +
                "name (via the '" + NAME_FLAG + "' flag), then list " +
                "information such as the" + eolt + "names of the storage " +
                "nodes deployed to that " + DESC_STR + ".";
        }
    }

    /*
     * ShowTables
     */
    @POST
    private static final class ShowTables extends SubCommand {
        final static String CMD_TEXT = "tables";
        final static String TABLE_FLAG = "-name";
        final static String PARENT_FLAG = "-parent";
        final static String LEVEL_FLAG = "-level";
        final static String NAMESPACE_FLAG = "-namespace";

        private ShowTables() {
            super(CMD_TEXT, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowTableExecutor<String>() {

                @Override
                public String emptyTableResult() {
                    return "No table found.";
                }

                @Override
                public String singleTableResult(TableImpl table,
                                                boolean showId)
                    throws ShellException {
                    return table.toJsonString(true, regionMapper);
                }

                @Override
                public String allTablesResult(Map<String, Table> tableMap,
                                              Integer maxLevel,
                                              boolean verbose)
                    throws ShellException {
                    return getAllTablesInfo(tableMap, maxLevel, verbose,
                                            regionMapper, namespace);
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowTableExecutor<T>
            implements Executor<T> {

            protected RegionMapper regionMapper;
            protected String namespace;

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String tableName = null;
                String parentName = null;
                Integer maxLevel = null;
                Shell.checkHelp(args, ShowTables.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (TABLE_FLAG.equals(args[i])) {
                            tableName =
                                Shell.nextArg(args, i++, ShowTables.this);
                        } else if (PARENT_FLAG.equals(args[i])) {
                            parentName =
                                Shell.nextArg(args, i++, ShowTables.this);
                        } else if (NAMESPACE_FLAG.equals(args[i])) {
                            namespace =
                                Shell.nextArg(args, i++, ShowTables.this);
                        } else if (LEVEL_FLAG.equals(args[i])) {
                            String sLevel =
                                Shell.nextArg(args, i++, ShowTables.this);
                            maxLevel = parseUnsignedInt(sLevel);
                        } else {
                            shell.unknownArgument(args[i], ShowTables.this);
                        }
                    }
                }

                final CommandShell cmd = (CommandShell) shell;
                if (namespace == null) {
                    namespace = cmd.getNamespace();
                }
                namespace = NameUtils.switchToInternalUse(namespace);

                final CommandServiceAPI cs = cmd.getAdmin();
                TableMetadata meta = null;
                try {
                    meta = cs.getMetadata(TableMetadata.class,
                                          MetadataType.TABLE);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                if (meta == null) {
                    return emptyTableResult();
                }
                regionMapper = meta.getRegionMapper();

                /* Show the specified table's meta data. */
                if (tableName != null) {
                    TableImpl table = meta.getTable(namespace, tableName);
                    if (table == null) {
                        throw new ShellArgumentException("Table " +
                            NameUtils.makeQualifiedName(
                                namespace, tableName) +
                            " does not exist.");
                    }
                    return singleTableResult(table, cmd.getHidden());
                }

                /* Show multiple tables's meta data. */
                Map<String, Table> tableMap = null;
                boolean verbose = shell.getVerbose();
                if (parentName != null) {
                    TableImpl tbParent = meta.getTable(namespace, parentName);
                    if (tbParent == null) {
                        throw new ShellArgumentException("Table " +
                            NameUtils.makeQualifiedName(
                                namespace, parentName) +
                            " does not exist.");
                    }
                    tableMap = tbParent.getChildTables();
                } else {
                    if (namespace == null) {
                        /* Sorted the tables by name */
                        tableMap = new TreeMap<String, Table>(meta.getTables());
                    } else {
                        tableMap = meta.getTables(namespace);
                    }
                }
                if (tableMap == null || tableMap.size() == 0) {
                    return emptyTableResult();
                }
                return allTablesResult(tableMap, maxLevel, verbose);
            }

            public abstract T emptyTableResult();

            public abstract T singleTableResult(TableImpl table,
                                                boolean showId)
                throws ShellException;

            public abstract T
                allTablesResult(Map<String, Table> tableMap,
                                Integer maxLevel, boolean verbose)
                throws ShellException;

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show tables");

            return new ShowTableExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult emptyTableResult() {
                    scr.setDescription("No table found.");
                    return scr;
                }

                @Override
                public ShellCommandResult
                    singleTableResult(TableImpl table,
                                      boolean showId)
                    throws ShellException {
                    /*
                     * To avoid passing ObjectNode to TableJsonUtils, which
                     * no longer uses Jackson databind, get the information
                     * as a string and re-parse it here. The real solution
                     * is to eliminate databind in this package and the CLI
                     * entirely, perhaps by using FieldValue instead, which
                     * can mimic the databind functionality in this space.
                     * A re-parse may still be used, but it won't use
                     * databind.
                     */
                    String jsonString = table.toJsonString(false, false,
                                                           regionMapper);
                    ObjectNode on = JsonUtils.parseJsonObject(jsonString);

                    /* null check to prevent warnings; it wont' be null */
                    if (showId && on != null) {
                        on.put("tableId", table.getId());
                    }

                    scr.setReturnValue(on);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    allTablesResult(Map<String, Table> tableMap,
                                          Integer maxLevel,
                                          boolean verbose)
                    throws ShellException {
                    scr.setReturnValue(
                        getAllTablesInfoJson(tableMap, maxLevel, verbose,
                                             regionMapper));
                    return scr;
                }

            }.commonExecute(args, shell);
        }

        private String getAllTablesInfo(Map<String, Table> tableMap,
                                        Integer maxLevel, boolean verbose,
                                        RegionMapper regionMapper,
                                        String namespace) {
            boolean isSysDefNamespace = (namespace == null);
            if (!verbose) {
                return "Tables in " +
                       (isSysDefNamespace ? "all namespaces: " :
                                            "namespace: " + namespace) +
                    eolt + getTableAndChildrenName(tableMap, 0, maxLevel,
                                                   isSysDefNamespace);
            }
            return getTableAndChildrenMetaInfo(tableMap, 0, maxLevel,
                                               regionMapper,
                                               isSysDefNamespace);
        }

        private ObjectNode getAllTablesInfoJson(Map<String, Table> tableMap,
                                                Integer maxLevel,
                                                boolean verbose,
                                                RegionMapper regionMapper) {
            if (!verbose) {
                return getTableAndChildrenJson(tableMap, 0, maxLevel);
            }
            return getTableAndChildrenMetaInfoJson(tableMap, 0, maxLevel,
                                                   regionMapper);
        }

        private String getTableAndChildrenName(Map<String, Table> tableMap,
                                               int curLevel,
                                               Integer maxLevel,
                                               boolean isSysDefNamespace) {
            final String INDENT = "  ";
            String indent = "";
            StringBuilder sb = new StringBuilder();
            if (curLevel > 0) {
                for (int i = 0; i < curLevel; i++) {
                    indent += INDENT;
                }
            }
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl)entry.getValue();
                sb.append(indent);
                sb.append((isSysDefNamespace ? table.getFullNamespaceName() :
                                               table.getFullName()));
                String desc = table.getDescription();
                if (desc != null && desc.length() > 0) {
                    sb.append(" -- ");
                    sb.append(desc);
                }
                sb.append(Shell.eolt);
                if (maxLevel != null && curLevel == maxLevel) {
                    continue;
                }
                Map<String, Table> childTabs = table.getChildTables();
                if (childTabs != null) {
                    sb.append(getTableAndChildrenName(childTabs,
                                                      curLevel + 1,
                                                      maxLevel,
                                                      isSysDefNamespace));
                }
            }
            return sb.toString();
        }

        private ObjectNode getTableAndChildrenJson(Map<String, Table> tableMap,
                                                   int curLevel,
                                                   Integer maxLevel) {
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode tableArray = top.putArray("tables");
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                final ObjectNode tableNode = JsonUtils.createObjectNode();
                final TableImpl table = (TableImpl) entry.getValue();
                tableNode.put("name", table.getFullNamespaceName());
                tableNode.put("description", table.getDescription());
                if (maxLevel != null && curLevel == maxLevel) {
                    tableArray.add(tableNode);
                    continue;
                }
                Map<String, Table> childTabs = table.getChildTables();
                if (childTabs != null) {
                    tableNode.set("childTables",
                                  getTableAndChildrenJson(childTabs,
                                      curLevel + 1, maxLevel));
                }
                tableArray.add(tableNode);
            }
            return top;
        }

        private String getTableAndChildrenMetaInfo(Map<String, Table> tableMap,
                                                   int curLevel,
                                                   Integer maxLevel,
                                                   RegionMapper regionMapper,
                                                   boolean isSysDefNamespace) {
            StringBuffer sb = new StringBuffer();
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl)entry.getValue();
                sb.append((isSysDefNamespace ? table.getFullNamespaceName() :
                                               table.getFullName()));
                sb.append(":");
                sb.append(Shell.eol);
                sb.append(table.toJsonString(true, regionMapper));
                sb.append(Shell.eol);
                if (maxLevel != null && curLevel == maxLevel) {
                    continue;
                }
                if (table.getChildTables() != null) {
                    sb.append(getTableAndChildrenMetaInfo(table.getChildTables(),
                                                          curLevel++,
                                                          maxLevel,
                                                          regionMapper,
                                                          isSysDefNamespace));
                }
            }
            return sb.toString();
        }

        private ObjectNode getTableAndChildrenMetaInfoJson(
                Map<String, Table> tableMap,
                int curLevel,
                Integer maxLevel,
                RegionMapper regionMapper) {
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode tableArray = top.putArray("tables");
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl) entry.getValue();
                final ObjectNode tableNode = JsonUtils.createObjectNode();
                tableNode.put("name", table.getFullName());
                /*
                 * TODO: figure out another way to do this
                TableJsonUtils.toJsonString(table, tableNode, false,
                                            regionMapper);
                */
                if (maxLevel != null && curLevel == maxLevel) {
                    tableArray.add(tableNode);
                    continue;
                }
                if (table.getChildTables() != null) {
                    tableNode.set("childTables",
                                  getTableAndChildrenMetaInfoJson(
                                      table.getChildTables(),
                                      curLevel++, maxLevel, regionMapper));
                }
                tableArray.add(tableNode);
            }
            return top;
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + CMD_TEXT + " [" + NAMESPACE_FLAG + " <name>] [" +
                    TABLE_FLAG + " <name>] [" + PARENT_FLAG + " <name>] " +
                    eolt + "[" + LEVEL_FLAG + " <level>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Display table metadata.  By default the names of all " +
                "top-tables" + eolt +
                "and their tables in all namespaces are listed.  Top-level " +
                "tables" + eolt +
                "are those without parents.  The level of child tables " +
                "can be" + eolt +
                "limited by specifying the " + LEVEL_FLAG + " flag.  If a " +
                "specific table is" + eolt +
                "named its detailed metadata is displayed.  The table name " +
                "is an"+ eolt +
                "optionally namespace qualified dot-separated name with the " +
                "format"+ eolt +
                "[ns:]tableName[.childTableName]*.  Flag " + PARENT_FLAG +
                " is used to show"+ eolt +
                "all child tables for the given parent table.  Flag " +
                NAMESPACE_FLAG + " is"+ eolt +
                "used to show all tables within the given namespace.";
        }
    }

    /*
     * ShowIndexes
     */
    @POST
    private static final class ShowIndexes extends SubCommand {
        final static String CMD_TEXT = "indexes";
        final static String INDEX_FLAG = "-name";
        final static String TABLE_FLAG = "-table";
        final static String NAMESPACE_FLAG = "-namespace";

        private ShowIndexes() {
            super(CMD_TEXT, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowIndexExecutor<String>() {

                @Override
                public String singleIndexResult(Index index,
                                                String indexName,
                                                String tableName) {
                    return getIndexInfo(index);
                }

                @Override
                public String
                    allTableAllIndexResult(Map<String, Table> tableMap) {
                    return getAllTablesIndexesInfo(tableMap);
                }

                @Override
                public String tableIndexResult(TableImpl table)
                    throws ShellException{
                    String ret = getTableIndexesInfo(table);
                    if (ret == null) {
                        return "No Index found.";
                    }
                    return ret;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowIndexExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String namespace = null;
                String tableName = null;
                String indexName = null;
                Shell.checkHelp(args, ShowIndexes.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if (TABLE_FLAG.equals(args[i])) {
                            tableName =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else if (NAMESPACE_FLAG.equals(args[i])) {
                            namespace =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else if (INDEX_FLAG.equals(args[i])) {
                            indexName =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else if (TABLE_FLAG.equals(args[i])) {
                            tableName =
                                Shell.nextArg(args, i++, ShowIndexes.this);
                        } else {
                            shell.unknownArgument(args[i], ShowIndexes.this);
                        }
                    }
                }

                if (indexName != null && tableName == null) {
                    shell.requiredArg(TABLE_FLAG, ShowIndexes.this);
                }

                final CommandShell cmd = (CommandShell) shell;
                if (namespace == null) {
                    namespace = cmd.getNamespace();
                }
                namespace = NameUtils.switchToInternalUse(namespace);

                final CommandServiceAPI cs = cmd.getAdmin();
                TableMetadata meta = null;
                try {
                    meta = cs.getMetadata(TableMetadata.class,
                                          MetadataType.TABLE);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                if (meta == null) {
                    throw new ShellArgumentException("No table found.");
                }

                if (tableName != null) {
                    TableImpl table = meta.getTable(namespace, tableName);
                    if (table == null) {
                        throw new ShellArgumentException("Table " +
                            NameUtils.makeQualifiedName(namespace, tableName) +
                            " does not exist.");
                    }
                    if (indexName != null) {
                        Index index = table.getIndex(indexName);
                        if (index == null) {
                            throw new ShellArgumentException(
                                "Index " + indexName + " on table " +
                                tableName + " does not exist.");
                        }
                        return singleIndexResult(index,
                                                 indexName, tableName);
                    }
                    return tableIndexResult(table);
                }

                Map<String, Table> tableMap = null;
                if (namespace == null) {
                    /* Sorted tables by name */
                    tableMap = new TreeMap<String, Table>(meta.getTables());
                } else {
                    tableMap = meta.getTables(namespace);
                }
                if (tableMap == null || tableMap.isEmpty()) {
                    throw new ShellArgumentException("No table found.");
                }
                return allTableAllIndexResult(tableMap);
            }

            public abstract T singleIndexResult(Index index,
                                                String indexName,
                                                String tableName);

            public abstract T
                allTableAllIndexResult(Map<String, Table> tableMap);

            public abstract T
                tableIndexResult(TableImpl table) throws ShellException;
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show index");
            return new ShowIndexExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    singleIndexResult(Index index,
                                      String indexName,
                                      String tableName) {
                    scr.setReturnValue(getIndexInfoJson(index));
                    return scr;
                }

                @Override
                public ShellCommandResult
                    allTableAllIndexResult(Map<String, Table> tableMap) {
                    scr.setReturnValue(getAllTablesIndexesInfoJson(tableMap));
                    return scr;
                }

                @Override
                public ShellCommandResult
                    tableIndexResult(TableImpl table)  throws ShellException {
                    final ObjectNode on =
                        getTableIndexesInfoJson(table);
                    if (on == null) {
                        scr.setDescription("No Index found.");
                        return scr;
                    }
                    scr.setReturnValue(on);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        private String getAllTablesIndexesInfo(Map<String, Table> tableMap) {
            StringBuilder sb = new StringBuilder();
            appendTablesIndexes(sb, tableMap);
            if (sb.length() > 0) {
                return sb.toString();
            }
            return "No index";
        }

        private void appendTablesIndexes(StringBuilder sb,
                                         Map<String, Table> tableMap) {
            for (Entry<String, Table> entry: tableMap.entrySet()) {
                Table table = entry.getValue();
                Map<String, Index> indexes = table.getIndexes();
                if (!indexes.isEmpty()) {
                    if (sb.length() > 0) {
                        sb.append(eol);
                    }
                    appendIndexes(sb, table.getFullNamespaceName(), indexes);
                }
                if (!table.getChildTables().isEmpty()) {
                    appendTablesIndexes(sb, table.getChildTables());
                }
            }
        }

        private ObjectNode
            getAllTablesIndexesInfoJson(Map<String, Table> tableMap) {
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode tableArray = top.putArray("tables");

            for (Entry<String, Table> entry: tableMap.entrySet()) {
                Table table = entry.getValue();
                ObjectNode tableNode = null;

                /* Append table's indexes */
                Map<String, Index> map = table.getIndexes();
                if (!map.isEmpty()) {
                    ObjectNode ret = getTableIndexesInfoJson(table);
                    tableNode = JsonUtils.createObjectNode();
                    tableNode.set("table", ret);
                }

                /* Append child table's indexes */
                Map<String, Table> childTables = table.getChildTables();
                if (!childTables.isEmpty()) {
                    ObjectNode ret = getAllTablesIndexesInfoJson(childTables);
                    if (!ret.isEmpty()) {
                        if (tableNode == null) {
                            tableNode = JsonUtils.createObjectNode();
                        }
                        ArrayNode childArray = tableNode.putArray("childTable");
                        childArray.add(ret);
                    }
                }

                if (tableNode != null) {
                    tableArray.add(tableNode);
                }
            }
            return top;
        }

        private String getTableIndexesInfo(Table table) {
            Map<String, Index> map = table.getIndexes();
            if (map == null || map.isEmpty()) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            appendIndexes(sb,
                          table.getFullNamespaceName(),
                          table.getIndexes());
            return sb.toString();
        }

        private void appendIndexes(StringBuilder sb,
                                   String tableName,
                                   Map<String, Index> indexes) {
            sb.append("Indexes on table ");
            sb.append(tableName);
            sb.append(eol);
            boolean first = true;
            for (Entry<String, Index> entry: indexes.entrySet()) {
                if (first) {
                    first = false;
                    sb.append(Shell.tab);
                } else {
                    sb.append(eolt);
                }
                appendIndex(sb, entry.getValue());
            }
        }

        private ObjectNode getTableIndexesInfoJson(Table table) {
            Map<String, Index> map = table.getIndexes();
            if (map == null || map.isEmpty()) {
                return null;
            }
            final ObjectNode on = JsonUtils.createObjectNode();
            if (NameUtils.switchToInternalUse(table.getNamespace()) != null) {
                on.put("namespace", table.getNamespace());
            }
            on.put("tableName", table.getFullName());
            final ArrayNode indexArray = on.putArray("indexes");
            for (Entry<String, Index> entry: map.entrySet()) {
                indexArray.add(getIndexInfoJson(entry.getValue()));
            }
            return on;
        }

        private String getIndexInfo(Index index) {
            StringBuilder sb = new StringBuilder();
            appendIndex(sb, index);
            return sb.toString();
        }

        private void appendIndex(StringBuilder sb, Index index) {
            sb.append(index.getName());
            sb.append(" (");
            boolean first = true;
            for (String s : index.getFields()) {
                if (!first) {
                    sb.append(", ");
                } else {
                    first = false;
                }
                sb.append(s);
            }
            sb.append(")");

            if (index.getType().equals(Index.IndexType.TEXT)) {
                sb.append(", type: " + Index.IndexType.TEXT);
            } else if (index.getType().equals(Index.IndexType.SECONDARY)) {
                sb.append(", type: " + Index.IndexType.SECONDARY);
            }

            if (index.getDescription() != null) {
                sb.append(" -- ");
                sb.append(index.getDescription());
            }
        }

        private ObjectNode getIndexInfoJson(Index index) {
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("name", index.getName());
            final ArrayNode fieldArray = on.putArray("fields");
            for (String s : index.getFields()) {
                fieldArray.add(s);
            }

            on.put("type", index.getType().name());
            on.put("description", index.getDescription());
            return on;
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + CMD_TEXT + " [" + NAMESPACE_FLAG + " <name>] [" +
                    TABLE_FLAG + " <name>] [" + INDEX_FLAG + " <name>] " +
                    eolt + CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Display index metadata. By default the indexes metadata " +
                "of all tables" + eolt +
                "in all namespaces are listed.  If a namespace is specified, " +
                "the indexes" + eolt +
                "of the tables in the namespace are displayed, if a specific " +
                "table is" + eolt +
                "named its indexes metadata are displayed, if a specific " +
                "index of the" + eolt +
                "table is named its metadata is displayed.";
        }
    }

    /*
     * ShowUsers
     *
     * Print the user information stored in the security metadata copy. If
     * no user name is specified, a brief information of all users will be
     * printed. Otherwise, only the specified user will be printed.
     *
     * While showing the information of all users, the format will be:
     * <br>"user: id=xxx name=xxx"<br>
     * For showing the details of a specified user, it will be:
     * <br>"user: id=xxx name=xxx state=xxx type=xxx retained-passwd=xxxx"
     */
    @POST
    private static final class ShowUsers extends SubCommand {

        static final String NAME_FLAG = "-name";
        static final String DESC_STR = "user";

        private ShowUsers() {
            super("users", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowUserExecutor<String>() {

                @Override
                public String noUserResult(String message) {
                    return message;
                }

                @Override
                public String
                    multiUserResult(
                        Collection<UserDescription> usersDesc) {
                    final ByteArrayOutputStream outStream =
                        new ByteArrayOutputStream();
                    final PrintStream out = new PrintStream(outStream);
                    for (final UserDescription desc : usersDesc) {
                        out.println("user: " + desc.brief());
                    }
                    return outStream.toString();
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowUserExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String nameFlag = null;

                Shell.checkHelp(args, ShowUsers.this);
                if (args.length > 1) {
                    for (int i = 1; i < args.length; i++) {
                        if ("-name".equals(args[i])) {
                            nameFlag =
                                Shell.nextArg(args, i++, ShowUsers.this);
                            if (nameFlag == null || nameFlag.isEmpty()) {
                                throw new ShellUsageException(
                                    "User name could not be empty.",
                                    ShowUsers.this);
                            }
                        } else {
                            shell.unknownArgument(args[i], ShowUsers.this);
                        }
                    }
                }

                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();
                try {
                    final Map<String, UserDescription> userDescMap =
                            cs.getUsersDescription();

                    if (userDescMap == null || userDescMap.isEmpty()) {
                        return noUserResult("No users.");
                    }
                    if (nameFlag != null) { /* Print details for a user */
                        final UserDescription desc = userDescMap.get(nameFlag);
                        final String message = desc == null ?
                            "User with name of " + nameFlag + " not found." :
                            "user: " + desc.details();
                        throw new ShellArgumentException(message);
                    }

                    /* Print summary for all users */
                    final Collection<UserDescription> usersDesc =
                           userDescMap.values();
                    return multiUserResult(usersDesc);

                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return null;
            }

            public abstract T noUserResult(String text);

            public abstract T
                multiUserResult(Collection<UserDescription> usersDesc);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show users");
            final ObjectNode top = JsonUtils.createObjectNode();
            final ArrayNode userArray = top.putArray("users");
            return new ShowUserExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    noUserResult(String text) {
                    scr.setDescription(text);
                    return scr;
                }

                @Override
                public ShellCommandResult
                    multiUserResult(
                        Collection<UserDescription> usersDesc) {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    for (final UserDescription desc : usersDesc) {
                        on.put("user", desc.brief());
                        userArray.add(on);
                    }
                    scr.setReturnValue(top);
                    return scr;
                }

            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + "users" + " [-name <name>] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the names of all " + DESC_STR + "s, or displays " +
                "information about a" + eolt +
                "specific " + DESC_STR + ". If no " + DESC_STR + " is " +
                "specified, lists the names" + eolt +
                "of all " + DESC_STR + "s. If a " + DESC_STR +
                " is specified using the " + NAME_FLAG + " flag," + eolt +
                "then lists detailed information about the " + DESC_STR + ".";
        }
    }

    /*
     * ShowVersions
     *
     * Print client and server version information.
     */
    @POST
    private static final class ShowVersions extends SubCommand {

        private ShowVersions() {
            super("versions", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            return new ShowVersionExecutor<String>() {

                @Override
                public String versionResult(StorageNodeStatus status,
                                            String exceptionMessage,
                                            String adminHostName) {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("Client version: ");
                    sb.append(
                        KVVersion.CURRENT_VERSION.getNumericVersionString());
                    sb.append(eol);
                    if (status == null) {
                        sb.append("Cannot reach server at host ");
                        sb.append(adminHostName);
                        sb.append(": " + eol);
                        sb.append(exceptionMessage);
                    } else {
                        sb.append("Server version: ");
                        sb.append(status.getKVVersion());
                    }
                    return sb.toString();
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowVersionExecutor<T> implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                CommandShell cmd = (CommandShell) shell;
                StorageNodeStatus status = null;
                String exceptionMessage = "";
                try{
                    CommandServiceAPI cs = cmd.getAdmin();
                    LoadParameters adminConfig = cs.getParams();
                    int snId = adminConfig.getMap(ParameterState.SNA_TYPE).
                        getOrZeroInt(ParameterState.COMMON_SN_ID);
                    String storeName =
                        adminConfig.getMap(ParameterState.GLOBAL_TYPE).
                        get(ParameterState.COMMON_STORENAME).asString();
                    /* ping service doesn't need to login,
                     * so set login manager as null here */
                    StorageNodeAgentAPI sna =
                        RegistryUtils.getStorageNodeAgent(
                            storeName, cmd.getAdminHostname(),
                            cmd.getAdminPort(), new StorageNodeId(snId),
                            null /* loginManager */, logger);
                    status = sna.ping();
                } catch (RemoteException re) {
                    exceptionMessage = re.toString();
                } catch (NotBoundException e) {
                    exceptionMessage = e.toString();
                }

                return versionResult(
                    status, exceptionMessage, cmd.getAdminHostname());
            }

            public abstract T versionResult(StorageNodeStatus status,
                                            String exceptionMessage,
                                            String adminHostName);

        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {

            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show versions");

            return new ShowVersionExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    versionResult(StorageNodeStatus status,
                                  String exceptionMessage,
                                  String adminHostName) {
                    final ObjectNode on = JsonUtils.createObjectNode();
                    on.put(
                        "clientVersion",
                        KVVersion.CURRENT_VERSION.getNumericVersionString());
                    if (status != null) {
                        final KVVersion version = status.getKVVersion();
                        on.put("serverVersion",
                               version.getNumericVersionString());
                        if (version.getReleaseEdition() != null) {
                            on.put("serverEdition",
                                   version.getReleaseEdition());
                        }
                    }
                    scr.setReturnValue(on);
                    return scr;
                }

            }.commonExecute(args, shell);
        }

        @Override
        protected String getCommandSyntax() {
            return "show versions " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return
                "Display client and connected server version information. "
                + eolt + "If you want to get all servers version, please use "
                + "ping instead.";
        }
    }

    /*
     * Show MRTable-agent-stat
     *
     * Print statistics of multi-region table agents.
     */
    private static final class ShowMRTableAgentStat extends SubCommand {
        static final String SUBCOMMAND_NAME = "mrtable-agent-statistics";
        static final String AGENT_FLAG = "-agent";
        static final String TABLE_FLAG = "-table";
        static final String MERGE_FLAG = "-merge-agents";
        /* The value of the table id column for agent stat. */
        private static final Integer AGENT_COL_TABLE_ID_VALUE = 0;
        /* Per-agent stat fields in the the merged result. */
        private static final Set<String> PER_AGENT_STAT_FIELDS =
            Stream.of("regionStat", "beginMs", "endMs", "intervalMs",
                      "initialization").
            collect(Collectors.toSet());
        /* Fields that should not be merged. */
        private static final Set<String> NO_MERGE_FIELDS =
            Stream.of("agentId", "timestamp", "localRegion", "tableName").
            collect(Collectors.toSet());

        protected ShowMRTableAgentStat() {
            super(SUBCOMMAND_NAME, 3);

        }

        @Override
        public String execute(String[] args,
                              Shell shell)
            throws ShellException {
            return new ShowMRTableAgentStatExecutor<String>() {
                @Override
                public String convertResult(ObjectNode result) {
                    if (result != null) {
                        return result.toPrettyString();
                    }
                    return "No multi-region agent statistics are available.";
                }
            }.commonExecute(args, shell);
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show " + SUBCOMMAND_NAME);
            return new ShowMRTableAgentStatExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult convertResult(ObjectNode result) {
                    if (result != null) {
                        scr.setReturnValue(result);
                    } else {
                        scr.setDescription(
                            "No multi-region agent statistics are available.");
                    }
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowMRTableAgentStatExecutor<T>
            implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                String agentId = null;
                Long tableId = null;
                String tableName = null;
                boolean merge = false;
                Shell.checkHelp(args, ShowMRTableAgentStat.this);
                for (int i = 1; i < args.length; i++) {
                    if (AGENT_FLAG.equals(args[i])) {
                        agentId = Shell.nextArg(args, i++,
                                                ShowMRTableAgentStat.this);
                    } else if (TABLE_FLAG.equals(args[i])) {
                        tableName = Shell.nextArg(args, i++,
                                                  ShowMRTableAgentStat.this);
                    } else if (MERGE_FLAG.equals(args[i])) {
                        merge = true;
                    } else {
                        shell.unknownArgument(args[i],
                                              ShowMRTableAgentStat.this);
                    }
                }

                final CommandShell cmd = (CommandShell) shell;
                TableAPI tableAPI = cmd.getStore().getTableAPI();
                if (tableName != null) {
                    Table table = tableAPI.getTable(tableName);
                    if (table == null) {
                        throw new ShellException("Table " + tableName +
                                                 " does not exist.");
                    }
                    if (!((TableImpl)table).isMultiRegion()) {
                        throw new ShellException("Table " + tableName +
                            " is not a multi-region table.");
                    }
                    tableId = ((TableImpl)table).getId();
                }
                String sysTableName = MRTableAgentStatDesc.TABLE_NAME;
                Table sysTable = tableAPI.getTable(sysTableName);
                try {
                    ObjectNode result;
                    if (agentId != null && tableId != null) {
                        /* Show stat for a table on a single agent. */
                        result = singleResult(tableAPI, sysTable, tableId,
                                              agentId, tableName);
                    } else if (agentId != null) {
                        /* Show stat for a single agent. */
                        result = singleResult(tableAPI, sysTable,
                                              AGENT_COL_TABLE_ID_VALUE,
                                              agentId, tableName);
                    } else if (tableId != null) {
                        /* Show stat for a table on all agents. */
                        result = multipleResult(tableAPI, sysTable, tableId,
                                                tableName, merge);
                    } else {
                        /* Show stat for all agents. */
                        result = multipleResult(tableAPI, sysTable,
                                                AGENT_COL_TABLE_ID_VALUE,
                                                tableName, merge);
                    }
                    return convertResult(result);
                } catch (Exception e) {
                    throw new ShellException(e.getMessage(), e);
                }

            }

            public abstract T convertResult(ObjectNode result);

            /* Put stat into the top json node. */
            private void putStatNode(Row row,
                                     ObjectNode topNode,
                                     String agentId,
                                     long tableId,
                                     String tableName) {
                ObjectNode node =  topNode.putObject(agentId);
                if (tableId != 0) {
                    node.put(COL_NAME_TABLE_ID, tableId);
                    node.put("tableName", tableName);
                }
                node.put(COL_NAME_TIMESTAMP,
                           row.get(COL_NAME_TIMESTAMP).asLong().get());

                String returnValue = row.get(COL_NAME_STATISTICS).
                    toJsonString(false);
                JsonNode returnValueNode = JsonUtils.parseJsonNode(returnValue);
                node.set(COL_NAME_STATISTICS, returnValueNode);
            }

            /* Get stat of multiple agents. */
            private ObjectNode multipleResult(TableAPI tableAPI,
                                              Table sysTable,
                                              long tableId,
                                              String tableName,
                                              boolean merge) {
                PrimaryKey pk = sysTable.createPrimaryKey();
                pk.put(COL_NAME_TABLE_ID, tableId);
                TableIterator<Row> iterator =
                    tableAPI.tableIterator(pk, null,
                    new TableIteratorOptions(Direction.REVERSE,
                                             Consistency.ABSOLUTE,
                                             0L, null));
                String agentId = null;
                ObjectNode result = null;
                ObjectNode mergedStat = JsonUtils.createObjectNode();
                while (iterator.hasNext()) {
                    Row curRow = iterator.next();
                    String curAgentId = curRow.
                        get(COL_NAME_AGENT_ID).asString().get();
                    /* Find the latest stat for each agent. */
                    if (agentId == null || !agentId.equals(curAgentId)) {
                        if (result == null) {
                            result = JsonUtils.createObjectNode();
                        }
                        if (merge) {
                            aggregateStat(mergedStat, curRow, curAgentId);
                        } else {
                            putStatNode(curRow, result, curAgentId,
                                        tableId, tableName);
                        }
                        agentId = curAgentId;
                    }
                }
                if (merge && (result != null)) {
                    if (tableId != 0) {
                        result.put(COL_NAME_TABLE_ID, tableId);
                        result.put("tableName", tableName);
                    }
                    result.merge(mergedStat);
                }
                return result;
            }

            /* Aggregate the stat of multiple agents. */
            private void aggregateStat(ObjectNode mergedStat,
                                       Row row,
                                       String agentId) {
                String returnValue = row.get(COL_NAME_STATISTICS).
                    toJsonString(false);
                JsonNode returnValueNode = JsonUtils.parseJsonNode(returnValue);
                for (String fname : returnValueNode.fieldNames()) {
                    JsonNode value = returnValueNode.get(fname);
                    if (NO_MERGE_FIELDS.contains(fname)) {
                        /* This field cannot be merged, so skip it. */
                        continue;
                    }

                    if (!mergedStat.has(fname)) {
                        if (PER_AGENT_STAT_FIELDS.contains(fname)) {
                            /*
                             * Make per-agent stat in the merged result
                             * */
                            ObjectNode node = JsonUtils.createObjectNode();
                            node.set(agentId, value);
                            mergedStat.set(fname, node);
                        } else {
                            /*
                             * Fields where numbers from different agents
                             * should be added up in the merged result.
                             * */
                            mergedStat.set(fname, value);
                        }
                    } else {
                        JsonNode curNode = mergedStat.get(fname);
                        if (PER_AGENT_STAT_FIELDS.contains(fname)) {
                            ((ObjectNode)curNode).set(agentId, value);
                        } else {
                            /* Add up numbers from multiple agents. */
                            long sum = Long.parseLong(curNode.toString()) +
                                Long.parseLong(value.toString());
                            mergedStat.set(fname, JsonUtils.createJsonNode(sum));
                        }
                    }
                }
            }

            /* Get stat of a single agent. */
            private ObjectNode singleResult(TableAPI tableAPI,
                                            Table sysTable,
                                            long tableId,
                                            String agentId,
                                            String tableName) {
                PrimaryKey pk = sysTable.createPrimaryKey();
                pk.put(COL_NAME_TABLE_ID, tableId);
                pk.put(COL_NAME_AGENT_ID, agentId);
                TableIterator<Row> iterator =
                    tableAPI.tableIterator(pk, null,
                    new TableIteratorOptions(Direction.REVERSE,
                                             Consistency.ABSOLUTE,
                                             0L, null));

                if (iterator.hasNext()) {
                    ObjectNode result = JsonUtils.createObjectNode();
                    /* Get the latest stat. */
                    Row row = iterator.next();
                    putStatNode(row, result, agentId, tableId, tableName);
                    return result;
                }
                return null;

            }

        }

        @Override
        protected String getCommandSyntax() {
            return "show " + SUBCOMMAND_NAME + " [-agent <agentID>] " +
                "[-table <tableName>] [-merge-agents] " +
                CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Shows the latest statistics for multi-region " +
                "table agents." +
                eol + eolt +
                "With no argument: shows combined statistics over all " +
                "tables for each" +
                eolt +
                "agent." +
                eol + eolt +
                "The -agent flag limits the statistics shown to the agent " +
                "with the" +
                eolt +
                "specified agent ID." +
                eol + eolt +
                "The -table flag limits the statistics shown to the " +
                "multi-region table" +
                eolt +
                "with the specified name on all agents, or a single agent " +
                "if -agent is" +
                eolt +
                "specified." +
                eol + eolt +
                "The -merge-agents flag combines statistics over all agents.";
        }
    }

    /**
     * Implement the 'show tls-credentials' command.
     * <p>
     * Print information about TLS credentials that have been installed and
     * files that are available for updates.
     */
    @POST
    static final class ShowTlsCredentials extends SubCommand {

        private ShowTlsCredentials() {
            super("tls-credentials", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException
        {
            return new ShowTlsCredentialsExecutor<String>() {
                @Override
                public String versionResult(String result) {
                    return convertToNonJson(
                        JsonUtils.parseJsonObject(result));
                }
            }.commonExecute(args, shell);
        }

        static String convertToNonJson(ObjectNode result) {
            final StringBuilder sb = new StringBuilder();
            try (final Formatter fmt = new Formatter(sb)) {

                /* Summary info */
                final String installedCredentialsStatus =
                    getAsText(result, "installedCredentialsStatus");
                final String pendingUpdatesStatus =
                    getAsText(result, "pendingUpdatesStatus");
                fmt.format("Installed credentials status: %s\n" +
                           "Pending updates status: %s\n",
                           installedCredentialsStatus,
                           pendingUpdatesStatus);

                final ObjectNode sns = result.getObject("sns");

                /* Sort by SNs by name */
                final Map<String, JsonNode> snsMap = new TreeMap<>();
                sns.entrySet().forEach(
                    e -> snsMap.put(e.getKey(), e.getValue()));

                for (final Entry<String, JsonNode> entry : snsMap.entrySet()) {
                    final String sn = entry.getKey();
                    final ObjectNode snNode = (ObjectNode) entry.getValue();
                    fmt.format("SN %s:\n", sn);
                    final JsonNode exception = snNode.get("exception");
                    if (exception != null) {
                        fmt.format("  exception: %s\n", exception.asText());
                        continue;
                    }
                    for (final String credType :
                             new String[] { "installed", "updates" }) {
                        final ObjectNode credNode = snNode.getObject(
                            credType.equals("installed") ?
                            "installedCredentials" :
                            "pendingUpdates");
                        boolean printedHeader = false;
                        for (final String fileType :
                                 new String[] { "keystore", "truststore" }) {
                            final ObjectNode fileNode =
                                credNode.getObject(fileType);
                            if (fileNode == null) {
                                continue;
                            }
                            if (!printedHeader) {
                                fmt.format("  %s:\n", credType);
                                printedHeader = true;
                            }
                            final String file = getAsText(fileNode, "file");
                            fmt.format("    %s: file=%s", fileType, file);
                            final String hash = getAsText(fileNode, "hash");
                            final String modTime = getAsText(
                                fileNode, "modTime", "not found");
                            fmt.format(" modTime=%s hash=%s\n",
                                       modTime, hash);
                        }
                    }
                }
            }
            return sb.toString();
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException
        {
            return new ShowTlsCredentialsExecutor<ShellCommandResult>() {
                @Override
                public ShellCommandResult versionResult(String result) {
                    final ShellCommandResult scr =
                        ShellCommandResult.getDefault("show tls-credentials");
                    scr.setReturnValue(JsonUtils.parseJsonObject(result));
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        private abstract class ShowTlsCredentialsExecutor<T>
                implements Executor<T> {
            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException
            {
                if (args.length > 1) {
                    shell.badArgCount(ShowTlsCredentials.this);
                }
                final CommandShell cmd = (CommandShell) shell;
                T ret = null;
                try {
                    final CommandServiceAPI cs = cmd.getAdmin();
                    ret = versionResult(cs.getTlsCredentialsInfo());
                } catch (RemoteException e) {
                    cmd.noAdmin(e);
                } catch (AdminFaultException e) {
                    convertIllegalCommandException(e);
                    throw e;
                }
                return ret;
            }
            abstract T versionResult(String result);
        }

        @Override
        protected String getCommandSyntax() {
            return "show tls-credentials " + CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Shows information about the TLS credentials installed," +
                " and updates\n" +
                "waiting to be installed, on all SNAs.";
        }
    }

    /* Other classes and methods */

    /**
     * When specifying event timestamps, these formats are accepted.
     */
    private static String[] dateFormats = {
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd",
        "MM-dd-yyyy HH:mm:ss.SSS",
        "MM-dd-yyyy HH:mm:ss",
        "MM-dd-yyyy HH:mm",
        "MM-dd-yyyy",
        "HH:mm:ss.SSS",
        "HH:mm:ss",
        "HH:mm"
    };

    private static String getDateFormatsUsage() {
        String usage =
            "<date> can be given in the following formats," + eolt +
            "which are interpreted in the UTC time zone." + eolt;

        for (String fs : dateFormats) {
            usage += eolt + "    " + fs;
        }

        return usage;
    }

    /**
     * Apply the above formats in sequence until one of them matches.
     */
    private static Date parseTimestamp(String s, ShellCommand command)
        throws ShellUsageException {

        TimeZone tz = TimeZone.getTimeZone("UTC");

        Date r = null;
        for (String fs : dateFormats) {
            final DateFormat f = new SimpleDateFormat(fs);
            f.setTimeZone(tz);
            f.setLenient(false);
            try {
                r = f.parse(s);
                break;
            } catch (ParseException pe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        if (r == null) {
            throw new ShellUsageException
                ("Invalid date format: " + s, command);
        }

        /*
         * If the date parsed is in the distant past (i.e., in January 1970)
         * then the string lacked a year/month/day.  We'll be friendly and
         * interpret the time as being in the recent past, that is, today.
         */

        final Calendar rcal = Calendar.getInstance(tz);
        rcal.setTime(r);

        if (rcal.get(Calendar.YEAR) == 1970) {
            final Calendar nowCal = Calendar.getInstance();
            nowCal.setTime(new Date());

            rcal.set(nowCal.get(Calendar.YEAR),
                     nowCal.get(Calendar.MONTH),
                     nowCal.get(Calendar.DAY_OF_MONTH));

            /* If the resulting time is in the future, subtract one day. */

            if (rcal.after(nowCal)) {
                rcal.add(Calendar.DAY_OF_MONTH, -1);
            }
            r = rcal.getTime();
        }
        return r;
    }
}
