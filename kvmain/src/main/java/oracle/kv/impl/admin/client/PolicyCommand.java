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

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;

@POST
class PolicyCommand extends ShellCommand {

    PolicyCommand() {
        super("change-policy", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        return new PolicyCommandExecutor<String>() {

            @Override
            public String dryRunResult(ParameterMap map, boolean showHidden) {
                return CommandUtils.formatParams(map, showHidden, null);
            }

            @Override
            public String successResult(String message) {
                return message;
            }
        }.commonExecute(args, shell);
    }

    private abstract class PolicyCommandExecutor<T> implements Executor<T> {
        @Override
        public T commonExecute(String[] args, Shell shell)
            throws ShellException {
            if (args.length < 3) {
                shell.badArgCount(PolicyCommand.this);
            }
            CommandShell cmd = (CommandShell) shell;
            CommandServiceAPI cs = cmd.getAdmin();

            boolean dryRun = false;
            int i;
            boolean foundParams = false;
            for (i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-dry-run".equals(arg)) {
                    dryRun = true;
                } else if ("-params".equals(arg)) {
                    ++i;
                    foundParams = true;
                    break;
                } else {
                    shell.unknownArgument(arg, PolicyCommand.this);
                }
            }

            if (!foundParams) {
                shell.requiredArg(null, PolicyCommand.this);
            }
            if (args.length <= i) {
                throw new ShellArgumentException(
                    "No parameters were specified");
            }
            final boolean showHidden = cmd.getHidden();
            try {
                ParameterMap map = cs.getPolicyParameters();
                CommandUtils.
                    parseParams(map, args, i, ParameterState.Info.POLICY,
                                null, showHidden, PolicyCommand.this);
                if (dryRun) {
                    return dryRunResult(map, showHidden);
                }
                if (shell.getVerbose()) {
                    shell.verboseOutput
                        ("New policy parameters:" + eol +
                         CommandUtils.formatParams(map, showHidden, null));
                }
                cs.setPolicies(map);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return successResult("");
        }

        public abstract T
            dryRunResult(ParameterMap map, boolean showHidden);

        public abstract T successResult(String message);
    }

    @Override
    public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
        throws ShellException {
        final ShellCommandResult scr =
            ShellCommandResult.getDefault("change policy");
        return new PolicyCommandExecutor<ShellCommandResult>() {

            @Override
            public ShellCommandResult
                dryRunResult(ParameterMap map, boolean showHidden) {
                scr.setReturnValue(
                    CommandUtils.formatParamsJson(map, showHidden, null));
                return scr;
            }

            @Override
            public ShellCommandResult
                successResult(String message) {
                if (message.equals("")) {
                    return scr;
                }
                scr.setDescription(message);
                return scr;
            }
        }.commonExecute(args, shell);
    }

    @Override
    protected String getCommandSyntax() {
        return name + " " +
               CommandParser.getJsonUsage() + " " +
               "[-dry-run] -params [name=value]*";
    }

    @Override
    protected String getCommandDescription() {
        return
            "Modifies store-wide policy parameters that apply to not yet " +
            "deployed" + eolt + "services. The parameters to change " +
            "follow the -params flag and are" + eolt + "separated by " +
            "spaces. Parameter values with embedded spaces must be" +
            eolt + "quoted.  For example name=\"value with spaces\". " +
            "If -dry-run is" + eolt + "specified the new parameters " +
            "are returned without changing them.";
    }
}
