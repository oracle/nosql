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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.Direction;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.StoreIteratorException;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

public class DeleteCommand extends CommandWithSubs {

    final static String START_FLAG = "-start";
    final static String END_FLAG = "-end";

    final static String COMMAND_OVERVIEW =
        "The delete command encapsulates commands that delete " + eol +
        "key/value pairs from a store and rows from a table";

    private static final
        List<? extends SubCommand> subs =
                   Arrays.asList(new DeleteKVCommand(),
                                 new DeleteTableCommand());
    public DeleteCommand() {
        super(subs, "delete", 3, 2);
        overrideJsonFlag = true;
    }

    @Override
    protected String getCommandOverview() {
        return COMMAND_OVERVIEW;
    }

    static class DeleteKVCommand extends SubCommand {
        final static String COMMAND_NAME = "kv";

        final static String KEY_FLAG = "-key";
        final static String KEY_FLAG_DESC = KEY_FLAG + " <key>";
        final static String MULTI_FLAG = "-all";
        final static String MULTI_FLAG_DESC = MULTI_FLAG;
        final static String START_FLAG_DESC = START_FLAG + " <prefixString>";
        final static String END_FLAG_DESC = END_FLAG + " <prefixString>";

        final static String COMMAND_SYNTAX =
            "delete " + COMMAND_NAME + " [" + KEY_FLAG_DESC + "]" +
            " [" + START_FLAG_DESC + "] [" + END_FLAG_DESC + "] [" +
            MULTI_FLAG_DESC +"]";

        final static String COMMAND_DESCRIPTION =
            "Deletes one or more keys. If " + MULTI_FLAG + " is specified, " +
            "deletes all" + eolt + "keys starting at the specified key. " +
            "If no key is specified" + eolt + "delete all keys in the store." +
            eolt + START_FLAG + " and " + END_FLAG + " flags can be used for " +
            "restricting the range used " + eolt + "for deletion.";

        DeleteKVCommand() {
            super(COMMAND_NAME, 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            boolean all = false;
            Key key = null;
            String keyString = null;
            String rangeStart = null;
            String rangeEnd = null;

            KVStore store = ((CommandShell) shell).getStore();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (KEY_FLAG.equals(arg)) {
                    keyString = Shell.nextArg(args, i++, this);
                    try {
                        key = CommandUtils.createKeyFromURI(keyString);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(iae.getMessage());
                    }
                } else if (START_FLAG.equals(arg)) {
                    rangeStart = Shell.nextArg(args, i++, this);
                } else if (END_FLAG.equals(arg)) {
                    rangeEnd = Shell.nextArg(args, i++, this);
                } else if (MULTI_FLAG.equals(arg)) {
                    all = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (key == null && !all) {
                shell.requiredArg("-key", this);
            }

            String returnValue = null;
            if (key != null && !all) {
                try {
                    if (store.delete(key)) {
                        returnValue = "Key deleted: " + keyString;
                    } else {
                        returnValue = "Key deletion failed: " + keyString;
                    }
                } catch (Exception e) {
                    throw new ShellException(
                        "Exception from NoSQL DB in delete:" + eolt +
                        e.getMessage(), e);
                }
            } else {
                /* Iterate the store */
                KeyRange kr = null;
                if (rangeStart != null || rangeEnd != null) {
                    try {
                        kr = new KeyRange(rangeStart, true, rangeEnd, true);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(iae.getLocalizedMessage());
                    }
                }

                long numdeleted = 0;
                try {
                    if (key != null && key.getMinorPath() != null
                        && key.getMinorPath().size() > 0) {
                        /* There's a minor key path, use it to advantage */
                        numdeleted = store.multiDelete(key, kr, null);
                    } else {
                        /* A generic store iteration */
                        Iterator<Key> it = store.storeKeysIterator(
                                Direction.UNORDERED, 100, key, kr, null);
                        if (!it.hasNext() && key != null) {
                            /**
                             * A complete major path won't work with store
                             * iterator and we can't distinguish between a
                             * complete major path or not, so if store
                             * iterator fails entire, try the key as a
                             * complete major path.
                             */
                            numdeleted = store.multiDelete(key, kr, null);
                        } else {
                            /**
                             * Brute force iterate and delete a key at a time.
                             */
                            while (it.hasNext()) {
                                if (store.delete(it.next())) {
                                    numdeleted++;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new ShellException(
                        "Exception from NoSQL DB in delete:" +
                        eolt + e.getMessage(), e);
                }
                returnValue = numdeleted +
                              ((numdeleted > 1)?" Keys " : " Key ") +
                              "deleted starting at " +
                              (keyString == null ? "root" : keyString);
            }
            return returnValue;
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESCRIPTION;
        }
    }

    /*
     * table delete command
     * */
    static class DeleteTableCommand extends SubCommand {
        final static String COMMAND_NAME = "table";
        final static String TABLE_FLAG = "-name";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG = "-value";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String NULL_VALUE_FLAG = "-null-value";
        final static String NULL_VALUE_FLAG_DESC = "-null-value";
        final static String ANCESTOR_FLAG = "-ancestor";
        final static String ANCESTOR_FLAG_DESC = ANCESTOR_FLAG + " <name>";
        final static String CHILD_FLAG = "-child";
        final static String CHILD_FLAG_DESC = CHILD_FLAG + " <name>";
        final static String INDEX_FLAG = "-index";
        final static String INDEX_FLAG_DESC = INDEX_FLAG + " <name>";
        final static String JSON_FLAG = "-json";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";
        final static String DELETE_ALL_FLAG = "-delete-all";
        final static String DELETE_ALL_FLAG_DESC = DELETE_ALL_FLAG;
        final static String START_FLAG_DESC = START_FLAG + " <value>";
        final static String END_FLAG_DESC = END_FLAG + " <value>";

        final static String COMMAND_SYNTAX =
            "delete " + COMMAND_NAME + " " + TABLE_FLAG_DESC + eolt +
            "[" + FIELD_FLAG_DESC + " " + VALUE_FLAG_DESC + "]+" + eolt +
            "[" + FIELD_FLAG_DESC + " [" + START_FLAG_DESC + "] [" +
            END_FLAG_DESC + "]]" + eolt +
            "[" + ANCESTOR_FLAG_DESC + "]+ [" + CHILD_FLAG_DESC + "]+" + eolt +
            "[" + JSON_FLAG_DESC + "] [" + DELETE_ALL_FLAG_DESC + "]";

        final static String COMMAND_DESCRIPTION =
            "Deletes one or more rows from the named table.  The " +
            "table name" + eolt + "is an optionally namespace qualified " +
            "dot-separated name with the format " + eolt +
            "[ns:]tableName[.childTableName]+. " +
            FIELD_FLAG + " and " + VALUE_FLAG + " pairs are" + eolt +
            "used to specify a primary key to use for the deletion.  " +
            FIELD_FLAG + ", " + START_FLAG + eolt + " and " + END_FLAG + " " +
            "flags can be used to specify a range of keys to be deleted" +
            "." + eolt +
            ANCESTOR_FLAG + " and " + CHILD_FLAG + " flags can be used " +
            "to delete rows from specified" + eolt +
            "ancestor and/or descendant tables as well as the target" +
            " table." + eolt +
            JSON_FLAG + " indicates that the key field values are " +
            "in JSON format." + eolt +
            DELETE_ALL_FLAG + " is used to delete all rows in a table.";

        DeleteTableCommand() {
            super(COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String namespace = null;
            String tableName = null;
            String frFieldName = null;
            String rgStart = null;
            String rgEnd = null;
            String jsonString = null;
            boolean deleteAll = false;

            List<String> lstAncestor = new ArrayList<String>();
            List<String> lstChild = new ArrayList<String>();
            HashMap<String, String> mapVals = new HashMap<String, String>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (FIELD_FLAG.equals(arg)) {
                    String fname = Shell.nextArg(args, i++, this);
                    if (++i < args.length) {
                        arg = args[i];
                        if (VALUE_FLAG.equals(arg)) {
                            String fVal = Shell.nextArg(args, i++, this);
                            mapVals.put(fname, fVal);
                        } else {
                            while (i < args.length) {
                                arg = args[i];
                                if (START_FLAG.equals(arg)) {
                                    rgStart = Shell.nextArg(args, i++, this);
                                } else if (END_FLAG.equals(arg)) {
                                    rgEnd = Shell.nextArg(args, i++, this);
                                } else {
                                    break;
                                }
                                i++;
                            }
                            if (rgStart == null && rgEnd == null) {
                                invalidArgument(arg + ", " + VALUE_FLAG +
                                    " or " + START_FLAG + " | " + END_FLAG +
                                    " is required");
                            }
                            frFieldName = fname;
                            i--;
                        }
                    } else {
                        shell.requiredArg(VALUE_FLAG + " or " + START_FLAG +
                            " | " + END_FLAG, this);
                    }
                } else if (ANCESTOR_FLAG.equals(arg)) {
                    lstAncestor.add(Shell.nextArg(args, i++, this));
                } else if (CHILD_FLAG.equals(arg)) {
                    lstChild.add(Shell.nextArg(args, i++, this));
                } else if (JSON_FLAG.equals(arg)) {
                    jsonString = Shell.nextArg(args, i++, this);
                } else if (DELETE_ALL_FLAG.equals(arg)) {
                    deleteAll = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }

            if (mapVals.isEmpty() && frFieldName == null &&
                jsonString == null && !deleteAll) {
                shell.requiredArg(FIELD_FLAG + " | " + JSON_FLAG + " | " +
                    DELETE_ALL_FLAG, this);
            }

            final CommandShell cmdShell = (CommandShell) shell;
            final TableAPI tableImpl = cmdShell.getStore().getTableAPI();
            namespace = NameUtils.getNamespaceFromQualifiedName(tableName);
            final String fullName =
                NameUtils.getFullNameFromQualifiedName(tableName);
            if (namespace == null) {
                namespace = cmdShell.getNamespace();
            }

            final Table table =
                CommandUtils.findTable(tableImpl, namespace, fullName);

            /* Create key and set value */
            PrimaryKey key = null;
            if (jsonString == null) {
                key = table.createPrimaryKey();
                for (Map.Entry<String, String> entry: mapVals.entrySet()) {
                    String fname = entry.getKey();
                    CommandUtils.putIndexKeyValues(key, fname,
                                                   entry.getValue());
                }
            } else {
                key = CommandUtils.createKeyFromJson(table, null, jsonString)
                        .asPrimaryKey();
            }

            /* Initialize MultiRowOptions. */
            MultiRowOptions mro = null;
            if (rgStart != null || rgEnd != null ||
                !lstAncestor.isEmpty() || !lstChild.isEmpty()) {
                mro = CommandUtils.createMultiRowOptions(tableImpl,
                        table, key, lstAncestor, lstChild,
                        frFieldName, rgStart, rgEnd);
            }

            /* Execute delete operation. */
            return doDeleteOperation(tableImpl, key, mro);
        }

        private String doDeleteOperation(final TableAPI tableImpl,
                                         final PrimaryKey key,
                                         final MultiRowOptions mro)
            throws ShellException {

            final StringBuilder sb = new StringBuilder();
            new RunTableAPIOperation() {
                @Override
                void doOperation() throws ShellException {
                    long nDel = 0;
                    if (mro == null && CommandUtils.matchFullPrimaryKey(key)) {
                        if (tableImpl.delete(key, null, null)) {
                            nDel = 1;
                        }
                    } else {
                        if (CommandUtils.matchFullMajorKey(key)) {
                            nDel = tableImpl.multiDelete(key, mro, null);
                        } else {
                            nDel = deleteKeys(tableImpl, key, mro);
                        }
                    }
                    sb.append(nDel);
                    sb.append(((nDel > 1)?" rows " : " row "));
                    sb.append("deleted.");
                }
            }.run();
            return sb.toString();
        }

        /**
         * Deletes records one at a time based on the primary key and
         * range, if provided.
         */
        private long deleteKeys(final TableAPI tableImpl,
                                final PrimaryKey key,
                                final MultiRowOptions mro)
            throws ShellException {

            TableIterator<PrimaryKey> itr =
                tableImpl.tableKeysIterator(key, mro, null);
            long nDel = 0;
            try {
                while (itr.hasNext()) {
                    if (tableImpl.delete(itr.next(), null, null)) {
                        nDel++;
                    }
                }
            } catch (StoreIteratorException sie) {
                Throwable t = sie.getCause();
                if (t instanceof FaultException) {
                    throw (FaultException)t;
                }
                if (t instanceof KVSecurityException) {
                    throw (KVSecurityException)t;
                }
                throw new ShellException(
                    (t != null ? t.getMessage() : sie.getMessage()));
            } finally {
                itr.close();
            }
            return nDel;
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESCRIPTION;
        }
    }
}
