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
package oracle.kv.impl.query.shell;

import java.io.IOException;
import java.io.PrintStream;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.StatementResult;
import oracle.kv.StatementResult.Kind;
import oracle.kv.StoreIteratorException;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/* A command to execute SQL statement */
public class ExecuteCommand extends ShellCommand {

    private final static String NAME = "";

    public ExecuteCommand() {
        super(NAME, 0);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

    	final OnqlShell sqlShell = (OnqlShell)shell;
        final String statement = args[0];
        final KVStore store = sqlShell.getStore();
        final StatementResult result;
        try {
            ExecuteOptions options = sqlShell.getExecuteOptions();
            final PreparedStatement ps = store.prepare(statement, options);
            result = store.executeSync(ps, options);
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        } catch (KVSecurityException kvse) {
            throw new ShellException(kvse.getMessage(), kvse);
        } catch (StoreIteratorException sie) {
            if (sie.getCause() != null) {
                throw new ShellException(sie.getCause().getMessage(),
                                         sie.getCause());
            }
            throw new ShellException(sie.getMessage(), sie);
        } catch (FaultException fe) {
            throw new ShellException(fe.getMessage(), fe);
        }
        return displayResults(shell, statement, result);
    }

    @Override
    protected boolean isMultilineInput() {
        return true;
    }

    private String displayResults(final Shell shell,
                                  final String statement,
                                  final StatementResult result)
        throws ShellException {

        final OnqlShell sqlShell = (OnqlShell)shell;
        final Kind kind = result.getKind();
        switch(kind) {
        case DDL:
            return sqlShell.displayDDLResults(result);
        case QUERY: {
            final PrintStream queryOutput = sqlShell.getQueryOutput();
            final boolean isPagingEnabled;
            final boolean isOutputFile = (queryOutput != sqlShell.getOutput());
            if (isOutputFile) {
                isPagingEnabled = false;
                queryOutput.println(createStatementComment(statement));
            } else {
                isPagingEnabled = sqlShell.isPagingEnabled();
            }

            final String ret =
                sqlShell.displayDMLResults(sqlShell.getQueryOutputMode(),
                                           result, isPagingEnabled,
                                           queryOutput);
            if (isOutputFile) {
                try {
                    ((OnqlShell)shell).flushOutput();
                } catch (IOException ignored) {
                }
            }
            return ret;
        }
        default:
            break;
        }
        return null;
    }

    private String createStatementComment(final String statement) {
        final String fmt = "%s%s";
        return String.format(fmt, Shell.COMMENT_MARK, statement.toUpperCase());
    }

    @Override
    public String getCommandSyntax() {
        return null;
    }

    @Override
    public String getCommandDescription() {
        return null;
    }
}
