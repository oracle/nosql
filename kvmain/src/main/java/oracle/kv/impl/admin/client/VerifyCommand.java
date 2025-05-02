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

import static oracle.nosql.common.json.JsonUtils.createObjectNode;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.SecurityViolation;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;

import oracle.nosql.common.json.ObjectNode;

public class VerifyCommand extends CommandWithSubs {

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new VerifyConfig(),
                                     new VerifyUpgrade(),
                                     new VerifyPrerequisite()
                                     );

    VerifyCommand() {
        super(subs,
              "verify",
              4,  /* prefix length */
              0); /* min args -- let subs control it */
    }

    @Override
    protected String getCommandOverview() {
        return "The verify command encapsulates commands that check various " +
               "parameters of the store.";
    }

    /*
     * Override execute() to implement the legacy "verify [-silent]" command
     * (no subcommands). Remove this support in the next major release.
     */
    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if ((args.length == 1) ||
            ((args.length == 2) && args[1].equals("-silent"))) {

            final String[] newArgs =
                    (args.length == 1) ?
                            new String[] {"verify", "configuration"} :
                            new String[] {"verify", "configuration", "-silent"};

            /* Do not show additional message in JSON mode */
            if (shell.getJson()) {
                return super.execute(newArgs, shell);
            }

            /*
             * Prefix a deprecation message to the output of the new command.
             */
            return "The command:" + eol + eolt +
                   "verify [-silent]" + eol + eol +
                   "is deprecated and has been replaced by: " +  eol + eolt +
                   "verify configuration [-silent]" + eol + eol +
                   super.execute(newArgs, shell);

        }
        return super.execute(args, shell);
    }

    @Override
    public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
        throws ShellException {

        if ((args.length == 1) ||
            ((args.length == 2) && args[1].equals("-silent"))) {

            final String[] newArgs =
                    (args.length == 1) ?
                            new String[] {"verify", "configuration"} :
                            new String[] {"verify", "configuration",
                                          "-silent"};
            return super.executeJsonOutput(newArgs, shell);
        }
        return super.executeJsonOutput(args, shell);
    }

    @POST
    static class VerifyConfig extends SubCommandJsonConvert {

        VerifyConfig() {
            super("configuration", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "verify configuration [-silent] " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Verifies the store configuration by iterating over the " +
                   "components and checking" + eolt + "their state " +
                   "against that expected in the Admin database.  This call " +
                   "may" + eolt + "take a while on a large store.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length > 2) {
                shell.badArgCount(this);
            }
            CommandShell cmd = (CommandShell) shell;
            CommandServiceAPI cs = cmd.getAdmin();
            try {
                boolean showProgress = true;
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-silent".equals(arg)) {
                        showProgress = false;
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }
                VerifyResults results =
                    cs.verifyConfiguration(showProgress, true,
                                           shell.getJson());
                List<Problem> violations = results.getViolations();
                if (violations != null && !violations.isEmpty()) {
                    exitCode = Shell.EXIT_UNKNOWN;
                }
                return results.display();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }

    @POST
    private static class VerifyUpgrade extends SubCommandJsonConvert {

        private VerifyUpgrade() {
            super("upgrade", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "verify upgrade [-silent] [-sn snX]* " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Verifies the storage nodes (and their managed components) "+
                   "are at or above the current version." + eolt +
                   "This call may take a while on a large store.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            try {
                boolean showProgress = true;
                List<StorageNodeId> snIds = null;

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];

                    if ("-silent".equals(arg)) {
                        showProgress = false;
                    } else if ("-sn".equals(arg)) {
                        if (snIds == null) {
                            snIds = new ArrayList<StorageNodeId>(1);
                        }
                        String sn = Shell.nextArg(args, i++, this);
                        try {
                            snIds.add(StorageNodeId.parse(sn));
                        } catch (IllegalArgumentException iae) {
                            invalidArgument(sn);
                        }

                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                final VerifyResults results =
                        cs.verifyUpgrade(KVVersion.CURRENT_VERSION,
                                         snIds, showProgress, true,
                                         shell.getJson());
                List<Problem> violations = results.getViolations();
                if (violations != null && !violations.isEmpty()) {
                    exitCode = Shell.EXIT_UNKNOWN;
                }
                return results.display();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }

    @POST
    static class VerifyPrerequisite extends SubCommandJsonConvert {

        VerifyPrerequisite() {
            super("prerequisite", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "verify prerequisite [-silent] [-sn snX]* " +
                   CommandParser.getJsonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Verifies the storage nodes are at or above the " +
                   "prerequisite software version needed to upgrade to " +
                   "the current version." + eolt +
                   "This call may take a while on a large store.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            try {
                boolean showProgress = true;
                List<StorageNodeId> snIds = null;

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];

                    if ("-silent".equals(arg)) {
                        showProgress = false;
                    } else if ("-sn".equals(arg)) {
                        if (snIds == null) {
                            snIds = new ArrayList<StorageNodeId>(1);
                        }
                        String sn = Shell.nextArg(args, i++, this);
                        try {
                            snIds.add(StorageNodeId.parse(sn));
                        } catch (IllegalArgumentException iae) {
                            invalidArgument(sn);
                        }

                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                /*
                 * Security violation checks, if failed, does not proceed with
                 * remote server verification.
                 * TODO: re-modularize to share code in VerifyConfiguration
                 */
                final List<Problem> secViolations =
                    checkSecurityViolation(cs, snIds, cmd);
                if (secViolations != null && !secViolations.isEmpty()) {
                    exitCode = Shell.EXIT_UNKNOWN;
                    return displayResults(secViolations, shell.getJson());
                }

                final VerifyResults results =
                    cs.verifyPrerequisite(KVVersion.CURRENT_VERSION,
                                          KVVersion.PREREQUISITE_VERSION,
                                          snIds,
                                          showProgress, true,
                                          shell.getJson());
                List<Problem> violations = results.getViolations();
                if (violations != null && !violations.isEmpty()) {
                    exitCode = Shell.EXIT_UNKNOWN;
                }
                return results.display();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        private List<Problem> checkSecurityViolation(CommandServiceAPI cs,
                                                     List<StorageNodeId> snIds,
                                                     CommandShell cmd)
            throws RemoteException {

            if (!cmd.isSecuredAdmin()) {
                return null;
            }
            if (snIds == null) {
                snIds = cs.getTopology().getSortedStorageNodeIds();
            }
            final Parameters params = cs.getParameters();
            final List<Problem> violations = new ArrayList<Problem>();
            for (StorageNodeId snId : snIds) {

                /* Check if ES is enabled in existing store */
                final String esCluster =
                    params.get(snId).getSearchClusterName();
                if (esCluster != null && !esCluster.isEmpty()) {
                    violations.add(
                        new SecurityViolation(
                            snId,
                            "A registered ES cluster is not allowed for a " +
                            "secure store."));
                }
            }

            return violations;
        }

        private String displayResults(List<Problem> violations, boolean json) {

            if (json) {
                final ObjectNode jsonTop = createObjectNode();
                VerifyConfiguration.problemsToJson(
                    jsonTop, violations, "violations");
                final CommandResult result = new CommandFails(
                    "There are violations.",
                    ErrorMessage.NOSQL_5200,
                    CommandResult.NO_CLEANUP_JOBS);
                CommandJsonUtils.updateNodeWithResult(
				    jsonTop, "verify prerequisite -json", result);
				return CommandJsonUtils.toJsonString(jsonTop);
            }
            final StringBuilder sb = new StringBuilder();
            final int numViolations = violations.size();
            sb.append("Verification complete, ").append(numViolations);
            sb.append((numViolations == 1) ? " violation, " : " violations, ");
            sb.append(" found.").append(eol);

            for (Problem p : violations) {
                sb.append("Verification violation: ")
                  .append("[").append(p.getResourceId()).append("]\t")
                  .append(p)
                  .append(eol);
            }
            return sb.toString();
        }
    }
}
