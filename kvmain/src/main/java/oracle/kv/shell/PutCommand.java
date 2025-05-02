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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.LoadTableUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

public class PutCommand extends CommandWithSubs {
    final static String VALUE_FLAG = "-value";
    final static String JSON_FLAG = "-json";
    final static String FILE_FLAG = "-file";
    final static String IFABSENT_FLAG = "-if-absent";
    final static String IFABSENT_FLAG_DESC = IFABSENT_FLAG;
    final static String IFPRESENT_FLAG = "-if-present";
    final static String IFPRESENT_FLAG_DESC = IFPRESENT_FLAG;

    final static String COMMAND_OVERVIEW =
        "The put command encapsulates commands that put key/value pairs " +
        "into a store" + eol + "and rows into a table.";

    private static final
        List<? extends SubCommand> subs =
                   Arrays.asList(new PutKVCommand(),
                                 new PutTableCommand());
    public PutCommand() {
        super(subs, "put", 3, 2);
        overrideJsonFlag = true;
    }

    @Override
    protected String getCommandOverview() {
        return COMMAND_OVERVIEW;
    }

    static class PutKVCommand extends SubCommand {
        final static String KV_COMMAND = "kv";
        final static String KEY_FLAG = "-key";
        final static String KEY_FLAG_DESC = KEY_FLAG + " <key>";
        final static String HEX_FLAG = "-hex";
        final static String HEX_FLAG_DESC = HEX_FLAG;
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <valueString>";
        final static String FILE_FLAG_DESC = FILE_FLAG;

        final static String PUT_KV_SYNTAX =
            "put " + KV_COMMAND + " " + KEY_FLAG_DESC + " " +
            VALUE_FLAG_DESC + " [" + FILE_FLAG_DESC + "] " + "[" +
            HEX_FLAG_DESC + "]" + eolt +
            "[" + IFABSENT_FLAG_DESC + " | " + IFPRESENT_FLAG_DESC + "]";

        final static String PUT_KV_DESCRIPTION =
            "Puts the specified key, value pair into the store" + eolt +
            FILE_FLAG + " indicates that the value parameter is a file that " +
            "contains the" + eolt + "actual value" + eolt +
            HEX_FLAG + " indates that the value is a BinHex encoded byte " +
            "value with Base64" + eolt +
            IFABSENT_FLAG + " indicates to put a key/value pair only if " +
            "no value for " + eolt + "the given key is present. " + eolt +
            IFPRESENT_FLAG + " indicates to put a key/value pair only if " +
            "a value for " + eolt + "the given key is present. ";

        PutKVCommand() {
            super(KV_COMMAND, 2);
        }

        @SuppressWarnings("null")
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            Key key = null;
            Value value = null;
            byte[] valueBytes = null;
            String stringValue = null;
            boolean isPutIfAbsent = false;
            boolean isPutIfPresent = false;
            boolean isFile = false;
            boolean isBinHex = false;
            KVStore store = ((CommandShell)shell).getStore();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (KEY_FLAG.equals(arg)) {
                    String keyString = Shell.nextArg(args, i++, this);
                    try {
                        key = CommandUtils.createKeyFromURI(keyString);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(iae.getMessage());
                    }
                } else if (VALUE_FLAG.equals(arg)) {
                    stringValue = Shell.nextArg(args, i++, this);
                } else if (FILE_FLAG.equals(arg)) {
                    isFile = true;
                } else if (HEX_FLAG.equals(arg)) {
                    isBinHex = true;
                } else if (IFABSENT_FLAG.equals(arg)) {
                    isPutIfAbsent = true;
                } else if (IFPRESENT_FLAG.equals(arg)) {
                    isPutIfPresent = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (key == null) {
                shell.requiredArg(KEY_FLAG, this);
            }
            if (stringValue == null) {
                shell.requiredArg(VALUE_FLAG, this);
            }

            /* By default, set isPutIfAbsent/ifPutIfPresent to true. */
            if (!isPutIfAbsent && !isPutIfPresent) {
                isPutIfAbsent = true;
                isPutIfPresent = true;
            }

            if (isFile) {
                valueBytes = CommandUtils.readFromFile(stringValue);
            } else {
                valueBytes = stringValue.getBytes();
            }

            if (isBinHex) {
                byte[] decoded =
                    CommandUtils.decodeBase64(new String(valueBytes));
                value = Value.createValue(decoded);
            } else {
                /* create the actual value */
                value = Value.createValue(valueBytes);
            }

            String retString = null;
            boolean updated = false;
            try {
                if (isPutIfAbsent && isPutIfPresent) {
                    if (store.putIfAbsent(key, value) == null) {
                        store.putIfPresent(key, value);
                        updated = true;
                    }
                } else if (isPutIfAbsent) {
                    if (store.putIfAbsent(key, value) == null) {
                        retString = "A value was already present with the " +
                            "given key " + CommandUtils.createURI(key) + ".";
                    }
                } else {
                    updated = true;
                    if (store.putIfPresent(key, value) == null) {
                        retString = "No existing value was present with the " +
                            "given key " + CommandUtils.createURI(key) + ".";
                    }
                }
            } catch (Exception e) {
                throw new ShellException("Exception from NoSQL DB in put. " +
                                         e.getMessage(), e);
            }

            if (retString == null) {
                retString = "Operation successful, record " +
                            (updated?"updated.":"inserted.");
            } else {
                retString = "Operation failed, " + retString;
            }
            return retString;
        }

        @Override
        protected String getCommandSyntax() {
            return PUT_KV_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return PUT_KV_DESCRIPTION;
        }
    }

    /*
     * table put command
     * */
    static class PutTableCommand extends SubCommand {
        final static String TABLE_COMMAND = "table";

        final static String TABLE_FLAG = "-name";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String NULL_VALUE_FLAG = "-null-value";
        final static String NULL_VALUE_FLAG_DESC = "-null-value";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";
        final static String FILE_FLAG_DESC = FILE_FLAG + " <file>";
        final static String UPDATE_FLAG = "-update";
        final static String UPDATE_FLAG_DESC = UPDATE_FLAG;
        final static String EXACT_FLAG = "-exact";
        final static String EXACT_FLAG_DESC = EXACT_FLAG;

        final static String PUT_TABLE_SYNTAX =
            "put " + TABLE_COMMAND + " " + TABLE_FLAG_DESC +
            " [" + IFABSENT_FLAG_DESC + " | " + IFPRESENT_FLAG_DESC + "]" +
            eolt + "[" + JSON_FLAG_DESC + "] [" + FILE_FLAG_DESC + "]" +
            "[" + EXACT_FLAG_DESC + "] [" + UPDATE_FLAG_DESC + "]";

        final static String PUT_TABLE_DESCRIPTION =
            "Put a row into the named table.  The table name is an " +
            "optionally" + eolt + "namespace qualified " +
            "dot-separated name with the format " + eolt +
            "[ns:]tableName[.childTableName]+." + eolt +
            IFABSENT_FLAG + " indicates to put a row only if the row does " +
            "not exist." + eolt +
            IFPRESENT_FLAG + " indicates to put a row only if the row " +
            "already exists." + eolt +
            JSON_FLAG + " indicates that the value is a JSON string." + eolt +
            FILE_FLAG + " can be used to load JSON strings from a file." +
            eolt + EXACT_FLAG + " indicates that the input json string or file"
            + " must contain values" + eolt + "for all columns in the table, " +
            "and cannot contain extraneous fields." + eolt +
            UPDATE_FLAG + " can be used to partially update the " +
            "existing record.";

        private final static String currentPutParams = "currentPutParams";
        private final TableCmdWithSubs cmdSubs = new TablePutSubs();

        PutTableCommand() {
            super(TABLE_COMMAND, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            PutArgs putParams =
                (PutArgs)getVariable(currentPutParams);
            if (putParams == null) {
                Shell.checkHelp(args, this);
                String tableName = null;
                String namespace = null;
                String jsonString = null;
                String fileName = null;
                Boolean ifAbsent = null;
                boolean isUpdate = false;
                boolean jsonExact = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (TABLE_FLAG.equals(arg)) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else if (IFABSENT_FLAG.equals(arg)) {
                        ifAbsent = true;
                    } else if (IFPRESENT_FLAG.equals(arg)) {
                        ifAbsent = false;
                    } else if (JSON_FLAG.equals(arg)) {
                        jsonString = Shell.nextArg(args, i++, this);
                    } else if (FILE_FLAG.equals(arg)) {
                        fileName = Shell.nextArg(args, i++, this);
                    } else if (UPDATE_FLAG.equals(arg)) {
                        isUpdate = true;
                        ifAbsent = false;
                    } else if (EXACT_FLAG.equals(arg)) {
                        jsonExact = true;
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                if (tableName == null) {
                    shell.requiredArg(TABLE_FLAG, this);
                }

                final CommandShell cmdShell = ((CommandShell)shell);
                final TableAPI tableImpl = cmdShell.getStore().getTableAPI();
                namespace = NameUtils.getNamespaceFromQualifiedName(tableName);
                final String fullName =
                    NameUtils.getFullNameFromQualifiedName(tableName);
                if (namespace == null) {
                    namespace = cmdShell.getNamespace();
                }

                Table table = CommandUtils.findTable(tableImpl,
                                                     namespace,
                                                     fullName);

                final WriteOptions wro =
                    new WriteOptions(cmdShell.getStoreDurability(),
                                     cmdShell.getRequestTimeout(),
                                     TimeUnit.MILLISECONDS);

                if (fileName != null) {
                    return putJsonFromFile(tableImpl, table, fileName, wro,
                                           ifAbsent, isUpdate, jsonExact);
                }
                Row row = null;
                if (jsonString != null) {
                    try {
                        /*
                         * Put a single row parsed from the inputed JSON string.
                         */
                        row = createRowFromJson(tableImpl, table, jsonString,
                                                isUpdate, jsonExact);
                    } catch (IllegalArgumentException iae) {
                        throw new ShellException(iae.getMessage(), iae);
                    }
                    return doPutRow(tableImpl, row, wro, ifAbsent);
                }
                row = table.createRow();
                ShellCommand cmd = this.clone();
                cmd.addVariable(currentPutParams,
                                new PutArgs(row, ifAbsent, isUpdate));
                cmd.setPrompt(tableName);
                shell.pushCurrentCommand(cmd);
                return null;
            }
            return cmdSubs.execute(putParams, args, shell);
        }

        private static Row createRowFromJson(TableAPI tableImpl,
                                             Table table,
                                             String json,
                                             boolean isUpdate,
                                             boolean isExact) {
            Row row;
            if (isUpdate == true) {
                PrimaryKey key = table.createPrimaryKeyFromJson(json, false);
                row = tableImpl.get(key, null);
                if (row == null) {
                    throw new IllegalArgumentException(
                        "No existing record was present with the given " +
                        "primary key: " + key.toJsonString(false));
                }
                row.copyFrom(table.createRowFromJson(json, isExact));
            } else {
                row = table.createRowFromJson(json, isExact);
            }
            return row;
        }

        private String putJsonFromFile(final TableAPI tableImpl,
                                       final Table table,
                                       final String fileName,
                                       final WriteOptions wro,
                                       final Boolean ifAbsent,
                                       final boolean isUpdate,
                                       final boolean jsonExact)
            throws ShellException {

            final StringBuilder message = new StringBuilder();
            long numRows = 0;
            try {
                numRows = new LoadTableUtils.Loader(tableImpl) {
                    @Override
                    public Row createRowFromJson(Table target, String json) {
                        return PutTableCommand.createRowFromJson(tableImpl,
                                                                 table,
                                                                 json,
                                                                 isUpdate,
                                                                 jsonExact);
                    }

                    @Override
                    public boolean doPut(TableAPI tableAPI,
                                         Row row,
                                         WriteOptions options) {

                        PutResult ret = executePutOp(tableAPI, row,
                                                     options, ifAbsent);
                        if (ret == PutResult.NONE) {
                            message.append(getReturnMessage(ret,
                                                            ifAbsent,
                                                            row));
                            return false;
                        }
                        return true;
                    }
                }.loadJsonToTable(table, fileName, wro, true);
            } catch (IOException ioe) {
                throw new ShellException(ioe.getMessage(), ioe);
            } catch (RuntimeException rte) {
                throw new ShellException(rte.getMessage(), rte);
            }

            if (message.length() > 0) {
                return message.toString();
            }
            final String op = (ifAbsent == null) ? "Loaded " :
                                (ifAbsent ? "Inserted " : "Updated ");
            return message.append(op)
                    .append(numRows)
                    .append((numRows > 1 ? " rows to ":" row to "))
                    .append(table.getFullName())
                    .toString();
        }

        private static class TablePutSubs extends TableCmdWithSubs {
            private static final
                List<? extends TableCmdSubCommand> tablePutSubs =
                               Arrays.asList(new TableAddArrayValueSub(),
                                             new TableAddMapValueSub(),
                                             new TableAddRecordValueSub(),
                                             new TableAddValueSub(),
                                             new TableCancelSub(),
                                             new TableExitSub(),
                                             new TableShowSub());
            TablePutSubs() {
                super(tablePutSubs, "", 0, 2);
            }

            @Override
            protected String getCommandOverview() {
                return "Set field value.";
            }
        }

        @Override
        protected String getCommandSyntax() {
            return PUT_TABLE_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return PUT_TABLE_DESCRIPTION;
        }

        private static class PutArgs {
            private final String fieldName;
            private FieldValue fieldValue;
            private final Boolean ifAbsent;
            private final boolean isUpdate;

            PutArgs(String name, FieldValue value) {
                this(name, value, null, false);
            }

            PutArgs(FieldValue value, Boolean ifAbsent, boolean isUpdate) {
                this(null, value, ifAbsent, isUpdate);
            }

            PutArgs(String name, FieldValue value,
                    Boolean ifAbsent, boolean isUpdate) {
                this.fieldName = name;
                this.fieldValue = value;
                this.ifAbsent = ifAbsent;
                this.isUpdate = isUpdate;
            }

            FieldValue getFieldValue() {
                return this.fieldValue;
            }

            void setFieldValue(FieldValue value) {
                this.fieldValue = value;
            }

            Boolean isPutIfAbsent() {
                return this.ifAbsent;
            }

            String getFieldName() {
                return this.fieldName;
            }

            boolean isUpdate() {
                return this.isUpdate;
            }
        }

        static abstract class TableCmdWithSubs extends CommandWithSubs {
            private PutArgs putArgs = null;
            TableCmdWithSubs(List<? extends SubCommand> subCommands,
                             String name,
                             int prefixLength,
                             int minArgCount) {
                super(subCommands, name, prefixLength, minArgCount);
                initSubs(subCommands);
            }

            private void initSubs(List<? extends SubCommand> tblSubs) {
                for (SubCommand command: tblSubs) {
                    ((TableCmdSubCommand)command).setParentCommand(this);
                }
            }

            protected String execute(PutArgs opArgs,
                                     String[] args,
                                     Shell shell)
                throws ShellException {

                if (isHelpCommand(args[1], shell)) {
                    String[] argsEx = new String[args.length - 1];
                    argsEx[0] = args[0];
                    System.arraycopy(args, 2, argsEx, 1, args.length - 2);
                    return getHelp(argsEx, shell);
                }
                this.putArgs = opArgs;
                return execute(args, shell);
            }

            protected FieldValue getFieldValue() {
                return putArgs.getFieldValue();
            }

            protected void setFieldValue(FieldValue value) {
                putArgs.setFieldValue(value);
            }

            protected Boolean isPutIfAbsent() {
                return putArgs.isPutIfAbsent();
            }

            protected String getFieldName() {
                return putArgs.getFieldName();
            }

            protected boolean isUpdate() {
                return putArgs.isUpdate();
            }

            private boolean isHelpCommand(String commandName, Shell shell) {
                ShellCommand cmd = shell.findCommand(commandName);
                return (cmd instanceof Shell.HelpCommand);
            }
        }

        static abstract class TableCmdSubCommand extends SubCommand {
            CommandWithSubs parentCommand = null;

            protected TableCmdSubCommand(String name, int prefixMatchLength) {
                super(name, prefixMatchLength);
            }

            protected void setParentCommand(CommandWithSubs command) {
                parentCommand = command;
            }

            protected FieldValue getCurrentFieldValue() {
                return ((TableCmdWithSubs)parentCommand).getFieldValue();
            }

            protected void setCurrentFieldValue(FieldValue value) {
                ((TableCmdWithSubs)parentCommand).setFieldValue(value);
            }

            protected String getCurrentFieldName() {
                return ((TableCmdWithSubs)parentCommand).getFieldName();
            }

            protected Boolean isPutIfAbsent() {
                return ((TableCmdWithSubs)parentCommand).isPutIfAbsent();
            }

            protected boolean isPutUpdate() {
                return ((TableCmdWithSubs)parentCommand).isUpdate();
            }
        }

        static class TableAddValueSub extends TableCmdSubCommand {
            final static String COMMAND = "add-value";
            final static String FILE_BINARY_DESC = FILE_FLAG +
                " <file-with-binary-content>";

            final static String COMMAND_DESCRIPTION =
                "Set field value." + eolt +
                "-file flag can be used to input binary value from " +
                "a file" + eolt + "for BINARY or FIXED_BINARY field.";

            private final boolean valueIsNullable;
            private final boolean fieldIsRequired;

            protected TableAddValueSub() {
                this(true, true);
            }

            protected TableAddValueSub(boolean fieldIsRequired,
                                       boolean valueIsNullable) {
                super(COMMAND, 5);
                this.fieldIsRequired = fieldIsRequired;
                this.valueIsNullable = valueIsNullable;
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                String fieldName = null;
                String sValue = null;
                boolean nullValue = false;
                boolean isFile = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (FIELD_FLAG.equals(arg)) {
                        fieldName = Shell.nextArg(args, i++, this);
                    } else if (VALUE_FLAG.equals(arg)) {
                        sValue = Shell.nextArg(args, i++, this);
                    } else if (valueIsNullable && NULL_VALUE_FLAG.equals(arg)) {
                        nullValue = true;
                    } else if (FILE_FLAG.equals(arg)) {
                        sValue = Shell.nextArg(args, i++, this);
                        isFile = true;
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                FieldValue currentVal = getCurrentFieldValue();
                if (fieldIsRequired && fieldName == null) {
                    shell.requiredArg(FIELD_FLAG, this);
                }
                if (nullValue) {
                    putNull(currentVal, fieldName);
                    return null;
                }

                FieldValue fdVal = null;
                if (sValue == null) {
                    /* Get fieldValue object from current command variable. */
                    ShellCommand cmd = shell.getCurrentCommand();
                    fdVal = (FieldValue)cmd.getVariable(fieldName);
                    if (fdVal == null) {
                        shell.requiredArg(VALUE_FLAG, this);
                    } else {
                        cmd.removeVariable(fieldName);
                    }
                } else {
                    FieldDef def =
                        CommandUtils.getFieldDef(currentVal, fieldName);
                    /* -file can only be used for binary or fixed field. */
                    if (isFile && (!def.isBinary() && !def.isFixedBinary())) {
                        invalidArgument(FILE_FLAG +
                            " can not be used for " + def.getType() + " field");
                    }
                    /**
                     * add-value command could not be used for complex type
                     * field: array, map and record.
                     */
                    if (def.isArray() || def.isMap() || def.isRecord()) {
                        String sType = def.getType().toString().toLowerCase();
                        throw new ShellException("Can't use " + COMMAND +
                            " for " + sType + " field, please run add-" +
                            sType + "-value to add value.");
                    }
                    fdVal = CommandUtils.createFieldValue(def, sValue, isFile);
                }
                putValue(currentVal, fieldName, fdVal);

                if (currentVal.isRow() && isPutUpdate()) {
                    TableAPI tableImpl =
                        ((CommandShell)shell).getStore().getTableAPI();
                    Row newRow = updateIfExists(tableImpl, currentVal.asRow());
                    if (newRow != null) {
                        setCurrentFieldValue(newRow);
                    }
                }
                return null;
            }

            private void putNull(FieldValue currentVal, String fieldName)
                throws ShellException {

                if (currentVal instanceof RecordValue) {
                    try {
                        currentVal.asRecord().putNull(fieldName);
                    } catch (IllegalArgumentException iae) {
                        throw new ShellException(iae.getMessage());
                    }
                } else {
                    throw new ShellException(
                        "Can not set null to the field: " + fieldName);
                }
            }

            private void putValue(FieldValue currentVal,
                                  String field, FieldValue value)
                throws ShellException {

                try {
                    if (currentVal.isRecord()) {
                        currentVal.asRecord().put(field, value);
                    } else if (currentVal.isArray()) {
                        currentVal.asArray().add(value);
                    } else if (currentVal.isMap()) {
                        currentVal.asMap().put(field, value);
                    }
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
            }

            private Row updateIfExists(TableAPI tableImpl, Row row) {
                /* No need to update if all fields are filled in. */
                if (row.size() == row.getTable().getFields().size()) {
                    return null;
                }

                /* Check if all primary key fields' value are provided. */
                List<String> pkFields = row.getTable().getPrimaryKey();
                for(String fname: pkFields) {
                    if (row.get(fname) == null) {
                        return null;
                    }
                }

                /* Check if row associated with the primary key exists. */
                PrimaryKey key = row.createPrimaryKey();
                Row retRow = tableImpl.get(key, null);
                if (retRow == null) {
                    return null;
                }

                /* Copy values from the current row. */
                retRow.copyFrom(row);
                return retRow;
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }

            @Override
            protected String getCommandSyntax() {
                String fieldDesc =
                    fieldIsRequired ? " " + FIELD_FLAG_DESC : "";
                String nullValueDesc =
                    valueIsNullable ? NULL_VALUE_FLAG + " | " + eolt : "";
                return COMMAND + fieldDesc + " [" + VALUE_FLAG_DESC +
                    " | " + nullValueDesc + FILE_BINARY_DESC + "]";
            }
        }

        abstract static class TableAddComplexValueSub
            extends TableCmdSubCommand {

            private static final String VAR_NAME = "currentVariable";
            private final boolean fieldIsRequired;

            TableAddComplexValueSub(String name, int prefixMatchLength) {
                this(name, prefixMatchLength, true);
            }

            TableAddComplexValueSub(String name, int prefixMatchLength,
                                    boolean fieldIsRequired) {
                super(name, prefixMatchLength);
                this.fieldIsRequired = fieldIsRequired;
            }

            abstract FieldValue createValue(FieldDef fieldDef)
                throws ShellException;

            abstract TableCmdWithSubs getCmdSubs();

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                String varName = VAR_NAME;
                PutArgs putArgs = ((PutArgs)getVariable(varName));
                if (putArgs == null) {
                    Shell.checkHelp(args, this);
                    FieldValue currentVal = getCurrentFieldValue();
                    String fieldName = null;
                    for (int i = 1; i < args.length; i++) {
                        String arg = args[i];
                        if (FIELD_FLAG.equals(arg)) {
                            fieldName = Shell.nextArg(args, i++, this);
                        } else {
                            shell.unknownArgument(arg, this);
                        }
                    }

                    if (fieldIsRequired && fieldName == null) {
                        shell.requiredArg(FIELD_FLAG, this);
                    }
                    FieldDef fieldDef =
                        CommandUtils.getFieldDef(currentVal, fieldName);
                    FieldValue retVal = createValue(fieldDef);
                    ShellCommand cmd = this.clone();
                    if (fieldName == null) {
                        fieldName = "element";
                    }
                    cmd.addVariable(varName, new PutArgs(fieldName, retVal));
                    cmd.setPrompt(fieldName);
                    shell.pushCurrentCommand(cmd);
                    return null;
                }
                return getCmdSubs().execute(putArgs, args, shell);
            }

            @Override
            protected String getCommandSyntax() {
                return getCommandName() + " " +
                        (fieldIsRequired ?
                            FIELD_FLAG_DESC :
                            CommandParser.optional(FIELD_FLAG_DESC));
            }
        }

        /* sub command: add-map-value */
        static class TableAddMapValueSub extends TableAddComplexValueSub {
            final static String COMMAND = "add-map-value";
            final static String COMMAND_DESCRIPTION = "Set map field value.";
            private static TableCmdWithSubs cmdSubs = new MapValueSubs();

            private static class MapValueSubs extends TableCmdWithSubs {
                private static
                    List<? extends TableCmdSubCommand> complexValueSubs =
                               Arrays.asList(new TableAddArrayValueSub(),
                                             new TableAddMapValueSub(),
                                             new TableAddRecordValueSub(),
                                             new TableAddValueSub(true, false),
                                             new TableCancelSub(),
                                             new TableExitSub(),
                                             new TableShowSub());
                MapValueSubs() {
                    super(complexValueSubs, "", 0, 2);
                }

                @Override
                public String getCommandOverview() {
                    return COMMAND_DESCRIPTION;
                }
            }

            TableAddMapValueSub() {
                super(COMMAND, 5);
            }

            TableAddMapValueSub(boolean fieldIsRequired) {
                super(COMMAND, 5, fieldIsRequired);
            }

            @Override
            TableCmdWithSubs getCmdSubs() {
                return cmdSubs;
            }

            @Override
            FieldValue createValue(FieldDef fieldDef)
                throws ShellException {

                try {
                    return fieldDef.createMap();
                } catch (ClassCastException cce) {
                    throw new ShellException(cce.getMessage());
                }
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }
        }

        /* sub command: add-array-value */
        static class TableAddArrayValueSub extends TableAddComplexValueSub {
            final static String COMMAND = "add-array-value";
            final static String COMMAND_DESCRIPTION = "Set array field value.";
            private static TableCmdWithSubs cmdSubs = new ArrayValueSubs();

            private static class ArrayValueSubs extends TableCmdWithSubs {
                private static
                    List<? extends TableCmdSubCommand> complexValueSubs =
                               Arrays.asList(new TableAddArrayValueSub(false),
                                             new TableAddMapValueSub(false),
                                             new TableAddRecordValueSub(false),
                                             new TableAddValueSub(false, false),
                                             new TableCancelSub(),
                                             new TableExitSub(),
                                             new TableShowSub());
                ArrayValueSubs() {
                    super(complexValueSubs, "", 0, 2);
                }

                @Override
                public String getCommandOverview() {
                    return COMMAND_DESCRIPTION;
                }
            }

            TableAddArrayValueSub() {
                super(COMMAND, 5);
            }

            TableAddArrayValueSub(boolean fieldIsRequired) {
                super(COMMAND, 5, fieldIsRequired);
            }

            @Override
            TableCmdWithSubs getCmdSubs() {
                return cmdSubs;
            }

            @Override
            FieldValue createValue(FieldDef fieldDef)
                throws ShellException {

                try {
                    return fieldDef.createArray();
                } catch (ClassCastException cce) {
                    throw new ShellException(cce.getMessage());
                }
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }
        }

        /* sub command: add-record-value */
        static class TableAddRecordValueSub extends TableAddComplexValueSub {
            final static String COMMAND = "add-record-value";
            final static String COMMAND_DESCRIPTION = "Set record field value.";
            private static TableCmdWithSubs cmdSubs = new RecordValueSubs();

            private static class RecordValueSubs extends TableCmdWithSubs {
                private static
                    List<? extends TableCmdSubCommand> complexValueSubs =
                               Arrays.asList(new TableAddArrayValueSub(),
                                             new TableAddMapValueSub(),
                                             new TableAddRecordValueSub(),
                                             new TableAddValueSub(),
                                             new TableCancelSub(),
                                             new TableExitSub(),
                                             new TableShowSub());
                RecordValueSubs() {
                    super(complexValueSubs, "", 0, 2);
                }

                @Override
                public String getCommandOverview() {
                    return COMMAND_DESCRIPTION;
                }
            }

            TableAddRecordValueSub() {
                super(COMMAND, 5);
            }

            TableAddRecordValueSub(boolean fieldIsRequired) {
                super(COMMAND, 5, fieldIsRequired);
            }

            @Override
            TableCmdWithSubs getCmdSubs() {
                return cmdSubs;
            }

            @Override
            FieldValue createValue(FieldDef fieldDef)
                throws ShellException {

                try {
                    return fieldDef.createRecord();
                } catch (ClassCastException cce) {
                    throw new ShellException(cce.getMessage());
                }
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }
        }

        /* sub command: show */
        static class TableShowSub extends TableCmdSubCommand {
            final static String COMMAND = "show";
            final static String COMMAND_DESCRIPTION = "Show field value.";

            protected TableShowSub() {
                super(COMMAND, 2);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 1) {
                    shell.badArgCount(this);
                }
                FieldValue value = getCurrentFieldValue();
                return value.toJsonString(true);
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }
        }

        /* sub command: cancel */
        static class TableCancelSub extends TableCmdSubCommand {
            final static String COMMAND = "cancel";
            final static String COMMAND_DESCRIPTION =
                "Cancel the current operation.";

            protected TableCancelSub() {
                super(COMMAND, 4);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 1) {
                    shell.badArgCount(this);
                }
                shell.popCurrentCommand();
                return null;
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }

        }

        /* sub command: exit */
        static class TableExitSub extends TableCmdSubCommand {
            final static String COMMAND = "exit";
            final static String COMMAND_DESCRIPTION =
                "Exit the current operation.";

            protected TableExitSub() {
                super(COMMAND, 4);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 1) {
                    shell.badArgCount(this);
                }

                String retString = null;
                final FieldValue fieldValue = getCurrentFieldValue();
                if (fieldValue.isRow()) {
                    final CommandShell cmdShell = (CommandShell)shell;
                    final TableAPI tableImpl =
                        cmdShell.getStore().getTableAPI();
                    final WriteOptions wro =
                        new WriteOptions(cmdShell.getStoreDurability(),
                                         cmdShell.getRequestTimeout(),
                                         TimeUnit.MILLISECONDS);

                    retString = doPutRow(tableImpl, fieldValue.asRow(),
                                         wro, isPutIfAbsent());
                    shell.popCurrentCommand();
                } else {
                    String fieldName = getCurrentFieldName();
                    shell.popCurrentCommand();
                    shell.getCurrentCommand()
                        .addVariable(fieldName, fieldValue);
                    shell.runLine("add-value -field \"" + fieldName + "\"");
                }
                return retString;
            }

            @Override
            protected String getCommandDescription() {
                return COMMAND_DESCRIPTION;
            }
        }

        private static String doPutRow(final TableAPI tableImpl,
                                       final Row row,
                                       final WriteOptions options,
                                       final Boolean ifAbsent)
            throws ShellException {

            final StringBuilder sb = new StringBuilder();
            new RunTableAPIOperation() {
                @Override
                void doOperation(){
                    PutResult putResult = executePutOp(tableImpl, row,
                                                       options, ifAbsent);
                    String ret = getReturnMessage(putResult, ifAbsent, row);
                    sb.append(ret);
                }
            }.run();
            if (sb.length() == 0) {
                return null;
            }
            return sb.toString();
        }

        /**
         * An enumeration of put result.
         */
        private enum PutResult {
            INSERTED,   /* Put record successfully using putIfAbsent() */
            UPDATED,    /* Put record successfully using putIfPresent() */
            NONE        /* No row is inserted or updated if -if-absent is specified
                           but a record was already present with the given ken or
                           if -if-present is specified but no existing record was
                           present with the given primary key. */
        }

        /**
         * Executes put operation, returns PutResult object.
         *
         *  @param tableImpl the TableAPI object
         *  @param row the row to be put to store
         *  @param options the WriteOptions used for put operation.
         *  @param ifAbsent a flag that indicates to use putIfAbsent() if true or
         *          use putIfPresent() if false or attempt to use putIfAbsent()
         *          firstly then putIfPresent() if it is null.
         *
         *  @return PutResult object.
         */
        private static PutResult executePutOp(TableAPI tableImpl,
                                              Row row,
                                              WriteOptions options,
                                              Boolean ifAbsent) {

            if (ifAbsent != null) {
                if (ifAbsent) {
                    if (tableImpl.putIfAbsent(row, null, options) != null) {
                        return PutResult.INSERTED;
                    }
                } else {
                    if (tableImpl.putIfPresent(row, null, options) != null) {
                        return PutResult.UPDATED;
                    }
                }
                return PutResult.NONE;
            }
            if (tableImpl.putIfAbsent(row, null, options) != null) {
                return PutResult.INSERTED;
            }
            tableImpl.putIfPresent(row, null, options);
            return PutResult.UPDATED;
        }

        /**
         * Returns the message of put result.
         */
        private static String getReturnMessage(PutResult putResult,
                                               Boolean ifAbsent,
                                               Row row) {

            final String keyString = row.createPrimaryKey().toJsonString(false);
            if (putResult == PutResult.NONE) {
                if (ifAbsent) {
                    return "Operation failed, A record was already present " +
                        "with the given primary key: " + keyString;
                }
                return "Operation failed, No existing record was present with " +
                    "the given primary key: " + keyString;
            }
            return "Operation successful, row " +
                ((putResult == PutResult.UPDATED)? "updated.":"inserted.");
        }
    }
}
