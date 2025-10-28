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
package oracle.kv.shell;

import java.rmi.RemoteException;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreConfig;
import oracle.kv.StatementResult;
import oracle.kv.StatementResult.Kind;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.client.admin.DdlStatementExecutor;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.OutputMode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/**
 * Implements the 'execute <ddl statement>' command.
 */
public class ExecuteCommand extends ShellCommand {

    /**
     * Prefix matching of 4 characters means 'execute' and 'exec' are supported.
     */
    public ExecuteCommand() {
        super("execute", 4);
    }

    /**
     * Usage: execute <ddl statement>
     */
    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length != 2) {
            shell.badArgCount(this);
        }

        final String statement = args[1];

        /* Statement is empty */
        if (statement.length() == 0) {
            throw new ShellUsageException("Empty statement",  this);
        }

        CommandShell cmd = (CommandShell) shell;
        try {
            if (cmd.isAnonymousLogin()) {
                try {
                    Topology topo = cmd.getAdmin().getTopology();
                    final DdlStatementExecutor exec = new DdlStatementExecutor(
                        topo, cmd.getLoginManager(),
                        KVStoreConfig.DEFAULT_MAX_CHECK_RETRIES,
                        KVStoreConfig.DEFAULT_MAX_CHECK_INTERVAL_MILLIS,
                        ClientLoggerUtils.getLogger(getClass(), name),
                        null /* clientId */,
                        Protocols.getDefault());

                    /*
                     * Haven't deployed any storage node, fall back to
                     * use given admin host and port to find bootstrap admin
                     */
                    if (topo.getStorageNodeIds().isEmpty()) {
                        exec.findClientAdminService(cmd.getAdminHostname(),
                                                    cmd.getAdminPort());
                    }
                    final StatementResult sr =
                        DdlStatementExecutor.waitExecutionResult(
                            exec.executeDdl(statement.toCharArray(),
                                            cmd.getNamespace(),
                                            null, /* ExecuteOptions */
                                            null /* TableLimits */));
                    return displayResults(cmd, sr);
                } catch (RemoteException e) {
                    throw new FaultException(e.getMessage(), e, false);
                }
            }
            return displayResults(cmd, cmd.getStore().
                                  executeSync(statement,
                                              cmd.getExecuteOptions()));
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        } catch (KVSecurityException kvse) {
            String msg = "Query failed: " + kvse.getMessage();
            throw new ShellException(msg, kvse);
        } catch (FaultException fe) {
            if (fe.getCause() != null &&
                fe.getCause().getClass().equals(RemoteException.class)) {
                RemoteException re = (RemoteException) fe.getCause();
                cmd.noAdmin(re);
                return "failed";
            }
            throw new ShellException(fe.getMessage(), fe);
        }
    }

    private String displayResults(final Shell shell,
                                  final StatementResult result)
        throws ShellException {

        final CommandShell cmdShell = (CommandShell)shell;
        final Kind kind = result.getKind();
        switch(kind) {
        case DDL:
            return cmdShell.displayDDLResults(result);
        case QUERY:
            return cmdShell.displayDMLResults(OutputMode.JSON, result);
        default:
            break;
        }
        return null;
    }

    @Override
    protected String getCommandSyntax() {
        return name + " <statement>";
    }

    @Override
    protected String getCommandDescription() {
        return "Executes the specified statement synchronously. The statement"+
            eolt + "must be enclosed in single or double quotes.";
    }
}
