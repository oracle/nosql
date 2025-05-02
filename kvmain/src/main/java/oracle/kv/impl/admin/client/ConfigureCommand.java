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

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand.ShellCommandJsonConvert;
import oracle.kv.util.shell.ShellException;

import oracle.nosql.common.json.ObjectNode;

@POST
class ConfigureCommand extends ShellCommandJsonConvert {
    private static final int maxStoreNameLen = 255;

    ConfigureCommand() {
        super("configure", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length < 3) {
            shell.badArgCount(this);
        }
        if (!"-name".equals(args[1])) {
            shell.requiredArg("-name", this);
        }

        String storeName = args[2];
        CommandShell cmd = (CommandShell) shell;
        CommandServiceAPI cs = cmd.getAdmin();
        try {
            validateStoreName(storeName);
            cs.configure(storeName);
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        }
        if (shell.getJson()) {
            ObjectNode returnValue = JsonUtils.createObjectNode();
            returnValue.put("store_name", storeName);
            CommandResult cmdResult =
                new CommandResult.CommandSucceeds(returnValue.toString());
            return Shell.toJsonReport(getCommandName(), cmdResult);
        }
        return "Store configured: " + storeName;
    }

    @Override
    protected String getCommandSyntax() {
        return "configure -name <storename> " +
                CommandParser.getJsonUsage();
    }

    @Override
    protected String getCommandDescription() {
        return
            "Configures a new store.  This call must be made before " +
            "any other" + eolt + "administration can be performed.";
    }

    private void validateStoreName(String store)
        throws ShellException {

        if (store.length() > maxStoreNameLen) {
            throw new ShellArgumentException
                ("Invalid store name.  It exceeds the maximum length of " +
                 maxStoreNameLen);
        }
        for (char c : store.toCharArray()) {
            if (!isValid(c)) {
                throw new ShellArgumentException
                        ("Invalid store name: " + store +
                         ".  It must consist of " +
                         "letters, digits, hyphen, underscore, period.");
            }
        }
    }

    private boolean isValid(char c) {
        if (Character.isLetterOrDigit(c) ||
            c == '-' ||
            c == '_' ||
            c == '.') {
            return true;
        }
        return false;
    }
}
