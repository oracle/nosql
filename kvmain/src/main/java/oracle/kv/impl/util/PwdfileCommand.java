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

package oracle.kv.impl.util;

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.PasswordStore.LoginId;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

/**
 * Security shell implementation of the pwdfile main command.
 */
class PwdfileCommand extends CommandWithSubs {

    private enum Action {
        SET, DELETE, LIST;
    }

    PwdfileCommand() {
        super(Arrays.asList(new PwdfileCreate(),
                            new PwdfileSecret(),
                            new PwdfileLogin()), /* currently hidden */
              "pwdfile", 4, 1);
    }

    @Override
    public String getCommandOverview() {
        return "The pwdfile command allows creation and modification of " +
            "an Oracle NoSQL pwdfile.";
    }

    /**
     * PwdfileCreate - implements the "pwdfile create" subcommand.
     */
    private static final class PwdfileCreate extends SubCommand {
        private static final String CREATE_COMMAND_NAME = "create";
        private static final String CREATE_COMMAND_DESC =
            "Creates a new password file.";
        private static final String CREATE_COMMAND_ARGS =
            "-file <pwdfile>";

        private PwdfileCreate() {
            super(CREATE_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String file = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-file".equals(arg)) {
                    file = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (file == null) {
                shell.requiredArg("-file", this);
            }

            return doCreate(file, shell);
        }

        /**
         * Performs the actual work for the pwdfile create command.
         * @param file the name of the file that will hold the password store
         * @param shell the Shell instance in which we are running
         */
        private String doCreate(String file, Shell shell)
            throws ShellException {

            try {
                final PasswordManager pwdMgr =
                    PasswordManager.load(FILE_STORE_MANAGER_CLASS);
                final File pwdfileLoc = new File(file);
                final PasswordStore pwdfile = pwdMgr.getStoreHandle(pwdfileLoc);
                if (pwdfile.exists()) {
                    return "A pwdfile already exists at that location";
                }

                /* We currently do not support password-protected files */
                final char[] pwd = null;

                pwdfile.create(pwd);
                return "Created";
            } catch (PasswordStoreException pwse) {
                throw new ShellException(
                    "PasswordStore error: " + pwse.getMessage(), pwse);
            } catch (Exception e) {
                throw new ShellException(
                    "Unexpected error: " + e.getMessage(), e);
            }
        }

        @Override
        public String getCommandSyntax() {
            return "pwdfile create " + CREATE_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return CREATE_COMMAND_DESC;
        }
    }

    /**
     * Implements the "pwdfile secret" subcommand.
     */
    private static final class PwdfileSecret extends SubCommand {

        private static final String SECRET_COMMAND_NAME = "secret";
        private static final String SECRET_COMMAND_DESC =
            "Manipulate secrets in a password file.";
        private static final String SECRET_COMMAND_ARGS =
            "-file <pwdfile> " +
            "{-set -alias <aliasname> [ -secret <secret> ]} | " +
            "{-delete -alias <aliasname>} | " +
            "{-list}";

        private PwdfileSecret() {
            super(SECRET_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            String file = null;
            String alias = null;
            String secret = null;
            final EnumSet<Action> actions = EnumSet.noneOf(Action.class);

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-file".equals(arg)) {
                    file = Shell.nextArg(args, i++, this);
                } else if ("-alias".equals(arg)) {
                    alias = Shell.nextArg(args, i++, this);
                } else if ("-set".equals(arg)) {
                    actions.add(Action.SET);
                } else if ("-list".equals(arg)) {
                    actions.add(Action.LIST);
                } else if ("-delete".equals(arg)) {
                    actions.add(Action.DELETE);
                } else if ("-secret".equals(arg)) {
                    secret = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (file == null || actions.size() != 1) {
                shell.badArgCount(this);
            }

            final Action action = actions.iterator().next();

            if ((action == Action.SET || action == Action.DELETE) &&
                alias == null) {
                shell.badArgCount(this);
            }

            if (action == Action.LIST && alias != null) {
                shell.badArgCount(this);
            }

            return doSecret(file, action, alias, secret, shell);
        }

        @Override
        public String getCommandSyntax() {
            return "pwdfile secret " + SECRET_COMMAND_ARGS;
        }
        @Override
        public String getCommandDescription() {
            return SECRET_COMMAND_DESC;
        }

        /**
         * Performs the work for the command.
         */
        private String doSecret(String file,
                                Action action,
                                String alias,
                                String secretArg,
                                Shell shell)
            throws ShellException {

            try {
                final PasswordStore pwdfile = openStore(new File(file), shell);
                final PasswordReader pwdReader =
                    ((SecurityShell) shell).getPasswordReader();
                final String retString;

                if (action == Action.SET) {
                    char[] secret;
                    char[] verifySecret;
                    if (secretArg != null)  {
                        secret = secretArg.toCharArray();
                        verifySecret = secret;
                    } else {
                        secret = pwdReader.readPassword(
                            "Enter the secret value to store: ");
                        verifySecret = pwdReader.readPassword(
                            "Re-enter the secret value for verification: ");
                    }

                    if (SecurityUtils.passwordsMatch(secret, verifySecret)) {
                        try {
                            if (pwdfile.setSecret(alias, secret)) {
                                retString = "Secret updated";
                            } else {
                                retString = "Secret created";
                            }
                        } catch (IllegalArgumentException e) {
                            throw new ShellException(e.getMessage(), e);
                        }
                        pwdfile.save();
                    } else {
                        retString = "The passwords do not match";
                    }
                } else if (action == Action.DELETE) {
                    if (pwdfile.deleteSecret(alias)) {
                        pwdfile.save();
                        retString = "Secret deleted";
                    } else {
                        retString = "Secret did not exist";
                    }
                } else if (action == Action.LIST) {
                    final Collection<String> secretAliases =
                        pwdfile.getSecretAliases();
                    if (secretAliases.size() == 0) {
                        retString = "The pwdfile contains no secrets";
                    } else {
                        final StringBuilder sb = new StringBuilder();
                        sb.append("The pwdfile contains the following secrets:");
                        for (String s : secretAliases) {
                            sb.append(eol);
                            sb.append("   ");
                            sb.append(s);
                        }
                        retString = sb.toString();
                    }
                }
                else {
                    throw new AssertionError("Unhandled action " + action);
                }
                return retString;

            } catch (ShellException se) {
                throw se;
            } catch (PasswordStoreException pwse) {
                throw new ShellException(
                    "PasswordStore error: " + pwse.getMessage(), pwse);
            } catch (Exception e) {
                throw new ShellException(
                    "Unknown error: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Implements the "pwdfile login" subcommand.
     */
    private static final class PwdfileLogin extends SubCommand {

        private static final String LOGIN_COMMAND_NAME = "login";
        private static final String LOGIN_COMMAND_DESC =
            "Manipulate logins in an Oracle Pwdfile.";
        private static final String LOGIN_COMMAND_ARGS =
            "-file <pwdfile> -set [-secret <secret>] |-delete " +
            "-database <db> -user <username>";

        private PwdfileLogin() {
            super(LOGIN_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            String file = null;
            String user = null;
            String db = null;
            String secret = null;
            final EnumSet<Action> actions = EnumSet.noneOf(Action.class);

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-file".equals(arg)) {
                    file = Shell.nextArg(args, i++, this);
                } else if ("-database".equals(arg) || "-db".equals(arg)) {
                    db = Shell.nextArg(args, i++, this);
                } else if ("-user".equals(arg)) {
                    user = Shell.nextArg(args, i++, this);
                } else if ("-set".equals(arg)) {
                    actions.add(Action.SET);
                } else if ("-list".equals(arg)) {
                    actions.add(Action.LIST);
                } else if ("-delete".equals(arg)) {
                    actions.add(Action.DELETE);
                } else if ("-secret".equals(arg)) {
                    secret = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (file == null || actions.size() != 1) {
                shell.badArgCount(this);
            }

            final Action action = actions.iterator().next();

            if ((action == Action.SET || action == Action.DELETE) &&
                (db == null)) {
                shell.badArgCount(this);
            }

            if ((action == Action.SET) &&
                (user == null)) {
                shell.badArgCount(this);
            }

            if (action == Action.LIST && (db != null || user != null)) {
                shell.badArgCount(this);
            }
            return doLogin(file, action, user, db, secret, shell);
        }

        @Override
        public String getCommandSyntax() {
            return "pwdfile login " + LOGIN_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return LOGIN_COMMAND_DESC;
        }

        /* Not visible currently */
        @Override
        protected boolean isHidden() {
            return true;
        }

        private String doLogin(String file,
                               Action action,
                               String user,
                               String db,
                               String secretArg,
                               Shell shell)
            throws ShellException {

            try {
                final PasswordStore pwdfile = openStore(new File(file), shell);
                final PasswordReader pwdReader =
                    ((SecurityShell) shell).getPasswordReader();
                final LoginId loginId = new LoginId(db, user);
                final String retString;

                if (action == Action.SET) {
                    char[] secret;
                    char[] verifySecret;
                    if (secretArg != null) {
                        secret = secretArg.toCharArray();
                        verifySecret = secret;
                    } else {
                        secret = pwdReader.readPassword(
                            "Enter the secret value to store: ");
                        verifySecret = pwdReader.readPassword(
                            "Re-enter the secret value for verification: ");
                    }

                    if (SecurityUtils.passwordsMatch(secret, verifySecret)) {
                        try {
                            if (pwdfile.setLogin(loginId, secret)) {
                                retString = "Login updated";
                            } else {
                                retString = "Login created";
                            }
                        } catch (IllegalArgumentException e) {
                            throw new ShellException(e.getMessage(), e);
                        }
                        pwdfile.save();
                    } else {
                        retString = "The passwords do not match";
                    }
                } else if (action == Action.DELETE) {

                    /*
                     * If the user specified a user (not required), check
                     * that that the user in the entry matches the specified
                     * user before deleting.
                     */
                    final LoginId pwdfileLoginId =
                        pwdfile.getLoginId(loginId.getDatabase());

                    if (pwdfileLoginId != null) {
                        if (loginId.getUser() == null ||
                            loginId.getUser().equals(
                                pwdfileLoginId.getUser())) {
                            pwdfile.deleteLogin(loginId.getDatabase());
                            pwdfile.save();
                            retString = "Login deleted";
                        } else {
                            retString = "The specified user does not match " +
                                    "the pwdfile entry";
                        }
                    } else {
                        retString = "Login did not exist";
                    }
                } else if (action == Action.LIST) {
                    final Collection<LoginId> logins = pwdfile.getLogins();
                    if (logins.size() == 0) {
                        retString = "The pwdfile contains no logins";
                    } else {
                        final StringBuilder sb = new StringBuilder();
                        sb.append("The pwdfile contains the following logins:");
                        for (LoginId lid : logins) {
                            sb.append(eol);
                            sb.append("   ");
                            sb.append(lid.getDatabase());
                            sb.append(" as ");
                            sb.append(lid.getUser());
                        }
                        retString = sb.toString();
                    }
                }
                else {
                    throw new AssertionError("Unhandled action " + action);
                }
                return retString;
            } catch (ShellException se) {
                throw se;
            } catch (PasswordStoreException pwse) {
                throw new ShellException(
                    "PasswordStore error: " + pwse.getMessage(), pwse);
            } catch (Exception e) {
                throw new ShellException(
                    "Unknown error: " + e.getMessage(), e);
            }
        }
    }

    private static PasswordStore openStore(
        File pwdfileLoc,
        @SuppressWarnings("unused") Shell shell)
        throws Exception {

        final PasswordManager pwdMgr =
            PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        final PasswordStore pwdfile = pwdMgr.getStoreHandle(pwdfileLoc);

        if (!pwdfile.exists()) {
            throw new ShellException("The store does not yet exist");
        }
        pwdfile.open(null);
        return pwdfile;
    }
}
