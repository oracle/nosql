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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.ArrayBuilder;
import oracle.kv.impl.api.table.MapBuilder;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RecordBuilder;
import oracle.kv.impl.api.table.RegionMapper;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableBuilderBase;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.Table;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.CommonShell;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

class TableCommand extends CommandWithSubs {
    final static String TABLE_NAME_FLAG = "-name";
    final static String TABLE_NS_FLAG = "-namespace";
    final static String DESC_FLAG = "-desc";
    final static String CREATE_FLAG = "-create";
    final static String EVOLVE_FLAG = "-evolve";
    final static String ALL_FLAG = "-all";
    final static String ORIGINAL_FLAG = "-original";
    final static String FIELD_NAME_FLAG = "-name";
    final static String FIELD_FLAG = "-field";
    final static String TYPE_FLAG = "-type";
    final static String DEFAULT_FLAG = "-default";
    final static String NOT_NULLABLE_FLAG = "-not-nullable";
    final static String SIZE_FLAG = "-size";
    final static String ENUM_VALUE_FLAG = "-enum-values";
    final static String PRECISION_FLAG = "-precision";

    final static String TABLE_NAME_FLAG_DESC = TABLE_NAME_FLAG + " <name>";
    final static String DESC_FLAG_DESC = DESC_FLAG + " <description>";
    final static String CREATE_FLAG_DESC = CREATE_FLAG;
    final static String EVOLVE_FLAG_DESC = EVOLVE_FLAG;
    final static String ALL_FLAG_DESC = ALL_FLAG;
    final static String ORIGINAL_FLAG_DESC = ORIGINAL_FLAG;
    final static String FIELD_NAME_FLAG_DESC = FIELD_NAME_FLAG + " <name>";
    final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
    final static String TYPE_FLAG_DESC = TYPE_FLAG + " <type>";
    final static String DEFAULT_FLAG_DESC = DEFAULT_FLAG + " <value>";
    final static String NOT_NULLABLE_FLAG_DESC = NOT_NULLABLE_FLAG;
    final static String SIZE_FLAG_DESC = SIZE_FLAG + " <size>";
    final static String PRECISION_FLAG_DESC = PRECISION_FLAG + " <num>";
    final static String ENUM_VALUE_FLAG_DESC =
        ENUM_VALUE_FLAG + " <value[,value[,...]]>";

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new TableClearSub(),
                                     new TableCreateSub(),
                                     new TableEvolveSub(),
                                     new TableListSub());

    TableCommand() {
        super(subs, "table", 3, 2);
        overrideJsonFlag = true;
    }

    @Override
    protected String getCommandOverview() {
        return
            "This command has been deprecated in favor of the" + eol +
            "execute <statement> command for most operations." + eol +
            "The table command encapsulates commands for building tables " +
            "for addition or evolution. " + eol +
            "Tables are created and modified in two steps.  " +
            "The table command creates new tables or" + eol +
            "evolves existing tables and saves them in temporary, " +
            "non-persistent storage in the admin" + eol +
            "client.  These tables are kept by name when exiting the create " +
            "and evolve sub-commands. " + eol +
            "The second step is to use the plan command to deploy " +
            "the new and changed tables" + eol +
            "to the store itself.  The temporary list of tables " +
            "can be examined and cleared using" + eol +
            "the list and clear sub-commands.";
    }

    @Override
    protected boolean isDeprecated() {
        return true;
    }

    private static class TableBuilderVariable {
        private final String name;
        private final TableBuilderBase builder;

        TableBuilderVariable(String varName, TableBuilderBase tb) {
            this.name = varName;
            this.builder = tb;
        }

        public String getName() {
            return this.name;
        }

        public TableBuilderBase getTableBuilder() {
            return this.builder;
        }
    }

    static abstract class TableBuildCmdWithSubs extends CommandWithSubs {

        private TableBuilderVariable varBuilder = null;
        TableBuildCmdWithSubs(List<? extends SubCommand> subCommands,
                              String name,
                              int prefixLength,
                              int minArgCount) {
            super(subCommands, name, prefixLength, minArgCount);
            initSubs(subCommands);
        }

        private void initSubs(List<? extends SubCommand> tblSubs) {
            for (SubCommand command: tblSubs) {
                ((TableBuildSubCommand)command).setParentCommand(this);
            }
        }

        public String execute(TableBuilderVariable tbv,
                              String[] args,
                              Shell shell)
            throws ShellException {
            /* Handle the help command. */
            if (isHelpCommand(args[1], shell)) {
                String[] argsEx = new String[args.length - 1];
                argsEx[0] = args[0];
                System.arraycopy(args, 2, argsEx, 1, args.length - 2);
                return getHelp(argsEx, shell);
            }
            varBuilder = tbv;
            return execute(args, shell);
        }

        protected TableBuilderBase getTableBuilder() {
            return varBuilder.getTableBuilder();
        }

        protected String getTableBuilderName() {
            return varBuilder.getName();
        }

        private boolean isHelpCommand(String commandName, Shell shell) {
            ShellCommand cmd = shell.findCommand(commandName);
            return (cmd instanceof Shell.HelpCommand);
        }
    }

    static abstract class TableBuildSubCommand extends SubCommand {
        private CommandWithSubs parentCommand = null;

        protected TableBuildSubCommand(String name, int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        protected void setParentCommand(CommandWithSubs command) {
            parentCommand = command;
        }

        protected TableBuilderBase getCurrentTableBuilder() {
            return ((TableBuildCmdWithSubs)parentCommand).getTableBuilder();
        }

        protected String getCurrentTableBuilderName() {
            return ((TableBuildCmdWithSubs)parentCommand).getTableBuilderName();
        }
    }

    /* The TableCreate subCommand and its TableBuildSubCommands. */
    static class TableCreateSub extends SubCommand {
        private static final String VAR_TABLEBUILDER = "CurrentTabBuilder";
        private static final TableCreateSubs createSubs = new TableCreateSubs();

        TableCreateSub() {
            super("create", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            TableBuilderVariable tbv =
                (TableBuilderVariable)getVariable(VAR_TABLEBUILDER);
            if (tbv == null) {
                Shell.checkHelp(args, this);
                String tableName = null;
                String namespace = null;
                String desc = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (TABLE_NAME_FLAG.equals(arg)) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else if (TABLE_NS_FLAG.equals(arg)) {
                        namespace = Shell.nextArg(args, i++, this);
                    } else if (DESC_FLAG.equals(arg)) {
                        desc = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                if (tableName == null) {
                    shell.requiredArg(TABLE_NAME_FLAG, this);
                }
                if (namespace == null) {
                    namespace = ((CommonShell)shell).getNamespace();
                }

                /* Parse tableName, split parent name and table id. */
                String parentName = null;
                String[] names = parseFullName(tableName);
                String tableId = names[0];
                if (names.length == 2) {
                    parentName = names[1];
                }

                /* Check the existence of parentName table. */
                Table tbParent = null;
                if (parentName != null) {
                    tbParent = findTable(namespace, parentName, true, shell);
                }

                try {
                    TableBuilderBase tb = TableBuilder.createTableBuilder(
                            namespace, tableId, desc, tbParent,
                            getRegionMapper(shell));
                    tbv = new TableBuilderVariable(tableName, tb);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                ShellCommand cmd = this.clone();
                cmd.addVariable(VAR_TABLEBUILDER, tbv);
                cmd.setPrompt(tableName);
                shell.pushCurrentCommand(cmd);
                return null;
            }
            String returnVal = createSubs.execute(tbv, args, shell);

            try {
                tbv.getTableBuilder().validatePrimaryKeyFields();
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage(), iae);
            } catch (IllegalCommandException ice) {
                throw new ShellException(ice.getMessage(), ice);
            }

            return returnVal;
        }

        /**
         * Parses full name into two parts: shortName and parentName. Where
         * parentNAme can be a dotted name too.
         */
        private static String[] parseFullName(String tableFullName) {
            String[] retNames;
            int pos = tableFullName.lastIndexOf(NameUtils.CHILD_SEPARATOR);
            if (pos > 0) {
                retNames = new String[2];
                retNames[0] = tableFullName.substring(pos + 1);
                retNames[1] = tableFullName.substring(0, pos);
            } else {
                retNames = new String[]{tableFullName};
            }
            return retNames;
        }

        private static class TableCreateSubs extends TableBuildCmdWithSubs {
            private static final
                List<? extends TableBuildSubCommand> tblCreateSubs =
                               Arrays.asList(new TableBuildAddArraySub(),
                                             new TableBuildAddFieldSub(),
                                             new TableBuildRemoveFieldSub(),
                                             new TableBuildAddMapSub(),
                                             new TableBuildAddRecordSub(),
                                             new TableBuildCancelSub(),
                                             new TableBuildExitSub(),
                                             new TableBuildShardKeySub(),
                                             new TableBuildPrimaryKeySub(),
                                             new TableBuildSetDescSub(),
                                             new TableBuildShowSub());
            TableCreateSubs() {
                super(tblCreateSubs, "", 0, 2);
            }

            @Override
            public String getCommandOverview() {
                return "Build a new table to be added to the store.";
            }
        }

        @Override
        protected String getCommandSyntax() {
            return "table create " + TABLE_NAME_FLAG_DESC +
                " [" + DESC_FLAG_DESC + "] ";
        }

        @Override
        protected String getCommandDescription() {
            return "Build a new table to be added to the store.  " +
                "New tables are added using" + eolt +
                "the plan add-table command.  " +
                "The table name is an optionally namespace" + eolt +
                "qualified dot-separated name with " +
                "the format " + eolt + "[ns:]tableName[.childTableName]+.  ";
        }
    }

    /* The TableEvolve subCommand and its TableBuildSubCommands. */
    static class TableEvolveSub extends SubCommand {
        private static final String VAR_TABLEEVOLVER = "CurrentTabEvolver";
        private static final TableEvolveSubs evolveSubs = new TableEvolveSubs();

        protected TableEvolveSub() {
            super("evolve", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            TableBuilderVariable tbv =
                (TableBuilderVariable)getVariable(VAR_TABLEEVOLVER);
            if (tbv == null) {
                Shell.checkHelp(args, this);
                String tableName = null;
                String namespace = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (TABLE_NAME_FLAG.equals(arg)) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else if (TABLE_NS_FLAG.equals(arg)) {
                        namespace = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }
                if (tableName == null) {
                    shell.requiredArg(TABLE_NAME_FLAG, this);
                }
                if (namespace == null) {
                    namespace = ((CommonShell)shell).getNamespace();
                }

                Table table = findTable(namespace, tableName, true, shell);
                tbv = new TableBuilderVariable(
                    tableName, TableEvolver.createTableEvolver(table,
                                                         getRegionMapper(shell)));
                ShellCommand cmd = this.clone();
                cmd.addVariable(VAR_TABLEEVOLVER, tbv);
                cmd.setPrompt(table.getName());
                shell.pushCurrentCommand(cmd);
                return null;
            }
            return evolveSubs.execute(tbv, args, shell);
        }

        private static class TableEvolveSubs extends TableBuildCmdWithSubs {
            private static final
                List<? extends TableBuildSubCommand> tblEvolveSubs =
                                   Arrays.asList(new TableBuildAddArraySub(),
                                                 new TableBuildAddFieldSub(),
                                                 new TableBuildAddMapSub(),
                                                 new TableBuildAddRecordSub(),
                                                 new TableBuildCancelSub(),
                                                 new TableBuildExitSub(),
                                                 new TableBuildRemoveFieldSub(),
                                                 new TableBuildShowSub());
            TableEvolveSubs() {
                super(tblEvolveSubs, "", 0, 2);
            }

            @Override
            public String getCommandOverview() {
                return "Build table for evolution.";
            }
        }

        @Override
        protected String getCommandSyntax() {
            return "table evolve " + TABLE_NAME_FLAG_DESC;
        }

        @Override
        protected String getCommandDescription() {
            return "Build a table for evolution.  Tables are evolved using " +
                "the plan" + eolt + "evolve-table command.  The table name " +
                "is an optionally namespace " + eolt +
                "qualified dot-separated name with the format" + eolt +
                "[ns:]tableName[.childTableName]+.";
        }
    }

    /* TableListSub subCommand to show the table(s) building information. */
    static class TableListSub extends SubCommand {

        TableListSub() {
            super("list", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            boolean createOnly = false;
            boolean evolveOnly = false;
            String tableName = null;

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (CREATE_FLAG.equals(arg)) {
                    createOnly = true;
                } else if (EVOLVE_FLAG.equals(arg)) {
                    evolveOnly = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (!createOnly && !evolveOnly) {
                createOnly = evolveOnly = true;
            }

            if (tableName != null) {
                Object obj = shell.getVariable(tableName);
                if (obj instanceof TableBuilder ||
                    obj instanceof TableEvolver) {
                    return getTableBuilderJsonString((TableBuilderBase)obj);
                }
                return "Table " + tableName + " does not exist.";
            }

            StringBuilder sbCreateInfo = new StringBuilder();
            StringBuilder sbEvolveInfo = new StringBuilder();
            boolean verbose = shell.getVerbose();
            Set<Entry<String, Object>> variables = shell.getAllVariables();
            for (Entry<String, Object> entry: variables) {
                Object obj = entry.getValue();
                if (createOnly && (obj instanceof TableBuilder)) {
                    if (verbose) {
                        sbCreateInfo.append(
                            getTableBuilderJsonString((TableBuilder)obj));
                    } else {
                        sbCreateInfo.append(eolt);
                        sbCreateInfo.append(
                            getTableBuilderAbstractInfo((TableBuilder)obj));
                    }
                }
                if (evolveOnly && (obj instanceof TableEvolver)){
                    if (verbose) {
                        sbEvolveInfo.append(
                            getTableBuilderJsonString((TableEvolver)obj));
                    } else {
                        sbEvolveInfo.append(eolt);
                        sbEvolveInfo.append(
                            getTableBuilderAbstractInfo((TableEvolver)obj));
                    }
                }
            }
            if (sbCreateInfo.length() == 0 && sbEvolveInfo.length() == 0) {
                return "No tables available.";
            }

            String createInfo = sbCreateInfo.toString();
            String evolveInfo = sbEvolveInfo.toString();
            if (!verbose) {
                if (createInfo.length() > 0) {
                    createInfo = "Tables to be added: " + createInfo;
                }
                if (evolveInfo.length() > 0) {
                    evolveInfo = "Tables to be evolved: " + evolveInfo;
                    if (createInfo.length() > 0) {
                        evolveInfo = eol + evolveInfo;
                    }
                }
            }
            return (createInfo + evolveInfo);
        }

        private String getTableBuilderJsonString(TableBuilderBase obj) {
            final String CREATE_LABEL = "Add table ";
            final String EVOLVE_LABEL = "Evolve table ";
            String retString = null;

            if (obj instanceof TableBuilder) {
                TableBuilder tb = ((TableBuilder)obj);
                retString = CREATE_LABEL + makeTableFullName(tb) + ": " +
                            eol + tb.toJsonString(true) + eol;
            } else if (obj instanceof TableEvolver) {
                TableEvolver te = (TableEvolver)obj;
                retString = EVOLVE_LABEL + makeTableFullName(te) + ": "  +
                            eol +
                            te.toJsonString(true) +
                            eol;
            }
            return retString;
        }

        private String getTableBuilderAbstractInfo(TableBuilderBase obj) {
            String retString = null;
            if (obj instanceof TableBuilder) {
                TableBuilder tb = ((TableBuilder)obj);
                retString = makeTableFullName(tb);
                String desc = tb.getDescription();
                if (desc != null && desc.length() > 0) {
                    retString += " -- " + desc;
                }
            } else if (obj instanceof TableEvolver) {
                TableEvolver te = (TableEvolver)obj;
                retString = makeTableFullName(te);
                String desc = te.getTable().getDescription();
                if (desc != null && desc.length() > 0) {
                    retString += " -- " + desc;
                }
            }
            return retString;
        }

        @Override
        protected String getCommandSyntax() {
            return "table list [" + TABLE_NAME_FLAG_DESC + "] " +
                "["+ CREATE_FLAG_DESC +" | " + EVOLVE_FLAG_DESC + "]";
        }

        @Override
        protected String getCommandDescription() {
            return "Lists all the tables built but not yet created or " +
                "evolved.  Flag " + TableCommand.TABLE_NAME_FLAG + eolt +
                "is used to show the details of the named table.  The table " +
                "name is" + eolt + "an optionally namespace qualified" +
                " dot-separated name with the format " + eolt +
                "[ns:]tableName[.childTableName]+.  Use flag " +
                CREATE_FLAG + " or " + EVOLVE_FLAG + " to show" + eolt +
                "the tables for addition or evolution.";
        }
    }

    /* TableClearSub subCommand to clear building information. */
    static class TableClearSub extends SubCommand {
        TableClearSub() {
            super("clear", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String tableName = null;
            boolean removeAll = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (ALL_FLAG.equals(arg)) {
                    removeAll = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (tableName == null && !removeAll) {
                shell.requiredArg(TABLE_NAME_FLAG + "|" + ALL_FLAG, this);
            }

            if (tableName != null) {
                Object obj = shell.getVariable(tableName);
                if (obj != null) {
                    if (obj instanceof TableBuilderBase ) {
                        shell.removeVariable(tableName);
                        return "Table " +
                               makeTableFullName((TableBuilderBase)obj) +
                               " cleared.";
                    }
                }
                return "Table " + tableName + " does not exist.";
            }

            int count = 0;
            Set<Entry<String, Object>> vars = shell.getAllVariables();
            String tinfo = "";
            for (Iterator<Entry<String, Object>> iterator = vars.iterator();
                 iterator.hasNext(); ) {

                Entry<String, Object> entry = iterator.next();
                Object obj = entry.getValue();
                if (obj instanceof TableBuilderBase) {
                    if (tinfo.length() > 0) {
                        tinfo += ", ";
                    }
                    tinfo += makeTableFullName((TableBuilderBase)obj);
                    iterator.remove();
                    count++;
                }
            }
            if (count == 0) {
                return "No available table.";
            }
            return count + ((count > 1)?" tables":" table") + " cleared: " +
                   tinfo + ".";
        }

        @Override
        protected String getCommandSyntax() {
            return "table clear [" + TABLE_NAME_FLAG_DESC + " | " +
                    ALL_FLAG_DESC + "]";
        }

        @Override
        protected String getCommandDescription() {
            return "Clear the tables built but not yet created or evolved. " +
                "Use flag " + TABLE_NAME_FLAG + eolt + "to specify the table " +
                "to be clear.  The table name is an optionally" +
                eolt + "namespace qualified dot-separated name with the format " +
                "" + eolt + "[ns:]tableName[.childTableName]+.  Flag " +
                ALL_FLAG + " is used to clear all the " + eolt +
                "tables.";
        }
    }

    /* TableBuildSubCommand: add-field */
    static class TableBuildAddFieldSub extends TableBuildSubCommand {

        private final boolean inCollectionBuilder;

        protected TableBuildAddFieldSub() {
            this(false);
        }

        protected TableBuildAddFieldSub(boolean inCollectionBuilder) {
            super("add-field", 5);
            this.inCollectionBuilder = inCollectionBuilder;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            Shell.checkHelp(args, this);

            String fname = null;
            FieldDef.Type type = null;
            String defVal = null;
            Boolean nullable = null;
            String size = null;
            String desc = null;
            String[] values = null;
            Integer precision = null;
            TableBuilderBase tb = getCurrentTableBuilder();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (FIELD_NAME_FLAG.equals(arg)) {
                    fname = Shell.nextArg(args, i++, this);
                } else if (TYPE_FLAG.equals(arg)) {
                    String ftype = Shell.nextArg(args, i++, this);
                    type = getType(ftype);
                    if (type == null) {
                        invalidArgument(ftype);
                    }
                } else if (DESC_FLAG.equals(arg)) {
                    desc = Shell.nextArg(args, i++, this);
                } else if (DEFAULT_FLAG.equals(arg)) {
                    defVal = Shell.nextArg(args, i++, this);
                } else if (NOT_NULLABLE_FLAG.equals(arg)) {
                    nullable = false;
                } else if (SIZE_FLAG.equals(arg)) {
                    size = Shell.nextArg(args, i++, this);
                } else if (ENUM_VALUE_FLAG.equals(arg)) {
                    String str = Shell.nextArg(args, i++, this);
                    values = str.trim().split("\\,");
                } else if (PRECISION_FLAG.equals(arg)) {
                    precision = parseInt(Shell.nextArg(args, i++, this));
                }else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (type == null) {
                shell.requiredArg(TYPE_FLAG, this);
            }

            /**
             * Only the element field of Map and Array with primitive types
             * does not require a name.
             */
            if (typeRequiresName(type)) {
                if (fname == null) {
                    shell.requiredArg(FIELD_NAME_FLAG, this);
                }
            }

            /* ENUM type requires ENUM values.*/
            if (type == Type.ENUM && values == null) {
                shell.requiredArg(ENUM_VALUE_FLAG, this);
            }
            /* FIXED_BINARY type requires a size.*/
            if (type == Type.FIXED_BINARY && size == null) {
                shell.requiredArg(SIZE_FLAG, this);
            }

            if (nullable != null) {
                validateProperty(tb, type, NOT_NULLABLE_FLAG);
            }
            if (defVal != null) {
                validateProperty(tb, type, DEFAULT_FLAG);
            }
            if (size != null) {
                validateProperty(tb, type, SIZE_FLAG);
            }
            if (values != null) {
                validateProperty(tb, type, ENUM_VALUE_FLAG);
            }
            if (precision != null) {
                validateProperty(tb, type, PRECISION_FLAG);
            }

            addField(shell, tb, fname, type, defVal,
                     nullable, size, desc, values, precision);
            return null;
        }

        private void validateProperty(TableBuilderBase tabBuilder, Type type,
                                      String propFlag)
            throws ShellException{

            if (propFlag.equals(DEFAULT_FLAG) ||
                propFlag.equals(NOT_NULLABLE_FLAG)) {

                if (tabBuilder.isCollectionBuilder()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(propFlag);
                    sb.append(" is not allowed for ");
                    if (tabBuilder.isCollectionBuilder()) {
                        sb.append("the type field of ");
                        sb.append(tabBuilder.getBuilderType());
                    } else {
                        sb.append("type ");
                        sb.append(type);
                    }
                    invalidArgument(sb.toString());
                }
            } else if (propFlag.equals(SIZE_FLAG)) {
                if (type != Type.FIXED_BINARY) {
                    invalidArgument(SIZE_FLAG +
                        " can only be used with " + Type.FIXED_BINARY);
                }
            } else if (propFlag.equals(ENUM_VALUE_FLAG)) {
                if (type != Type.ENUM) {
                    invalidArgument(ENUM_VALUE_FLAG +
                        " can only be used with " + Type.ENUM);
                }
            } else if (propFlag.equals(PRECISION_FLAG)) {
                if (type != Type.TIMESTAMP) {
                    invalidArgument(PRECISION_FLAG +
                        " can only be used with " + Type.TIMESTAMP);
                }
            }
        }

        private FieldDef.Type getType(String type) {
            try {
                return FieldDef.Type.valueOf(type.toUpperCase());
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }

        private void addField(Shell shell, TableBuilderBase tabBuilder,
                              String fname, FieldDef.Type type, String defVal,
                              Boolean nullable, String size, String desc,
                              String[] values, Integer precision)
            throws ShellException {

            switch (type) {
            case INTEGER: {
                Integer idef = null;

                if (defVal != null) {
                    idef = getValidIntVal(defVal);
                }
                try {
                    tabBuilder.addInteger(fname, desc, nullable, idef);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            }
            case LONG: {
                Long ldef = null;
                if (defVal != null) {
                    ldef = getValidLongVal(defVal);
                }
                try {
                    tabBuilder.addLong(fname, desc, nullable, ldef);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            }
            case DOUBLE:{
                Double ddef = null;
                if (defVal != null) {
                    ddef = getValidDoubleVal(defVal);
                }
                try {
                    tabBuilder.addDouble(fname, desc, nullable, ddef);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            }
            case FLOAT:{
                Float fdef = null;
                if (defVal != null) {
                    fdef = getValidFloatVal(defVal);
                }
                try {
                    tabBuilder.addFloat(fname, desc, nullable, fdef);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            }
            case STRING:
                try {
                    tabBuilder.addString(fname, desc, nullable, defVal);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            case BOOLEAN: {
                Boolean bdef = null;
                if (defVal != null) {
                    bdef = Boolean.valueOf(defVal);
                }
                try {
                    tabBuilder.addBoolean(fname, desc, nullable, bdef);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            }
            case BINARY:
                try {
                    tabBuilder.addBinary(fname, desc, nullable, defVal, true);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            case FIXED_BINARY:
                int isize = 0;
                if (size != null) {
                    isize = getValidIntVal(size);
                } else {
                    throw new ShellException("FIXED_BINARY requires a size");
                }

                try {
                    tabBuilder.addFixedBinary(fname, isize, desc, nullable,
                                              defVal, true);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            case ENUM:
                try {
                    tabBuilder.addEnum(fname, values, desc, nullable, defVal);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            case TIMESTAMP:
                try {
                    Timestamp defValue = (defVal != null) ?
                               TimestampUtils.parseString(defVal) : null;
                    tabBuilder.addTimestamp
                        (fname, ((precision != null) ? precision : 0),
                         desc, nullable, defValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException ice) {
                    throw new ShellException(ice.getMessage(), ice);
                }
                break;
            case MAP:
            case ARRAY:
            case RECORD:
                Object obj = getCurrentCmdVariable(shell, fname);
                try {
                    if (tabBuilder.isCollectionBuilder()) {
                        /* Ignore element name of Map and Array. */
                        tabBuilder.addField((FieldDef)obj);
                    } else {
                        tabBuilder.addField(fname, (FieldDefImpl)obj);
                    }
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (IllegalCommandException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                }
                removeCurrentCmdVariable(shell, fname);
                break;
            default:
                throw new ShellException("Unsupported datatype: " + type);
            }
        }

        private Integer getValidIntVal(String val)
            throws ShellException {

            try {
                return Integer.valueOf(val);
            } catch (NumberFormatException nfe) {
                invalidArgument(val + ", not a valid integer number.");
            }
            return null;
        }

        private Long getValidLongVal(String val)
            throws ShellException {

            try {
                return Long.valueOf(val);
            } catch (NumberFormatException nfe) {
                invalidArgument(val + ", not a valid long number.");
            }
            return null;
        }

        private Double getValidDoubleVal(String val)
            throws ShellException {

            try {
                return Double.valueOf(val);
            } catch (NumberFormatException nfe) {
                invalidArgument(val + ", not a valid double.");
            }
            return null;
        }

        private Float getValidFloatVal(String val)
            throws ShellException {

            try {
                return Float.valueOf(val);
            } catch (NumberFormatException nfe) {
                invalidArgument(val + ", not a valid float.");
            }
            return null;
        }

        private boolean typeRequiresName(Type type) {
            if (isNameOptional()) {
                return (type == Type.ENUM ||
                        type == Type.FIXED_BINARY ||
                        type == Type.RECORD);
            }
            return true;
        }

        private boolean isNameOptional() {
            return inCollectionBuilder;
        }

        private static void removeCurrentCmdVariable(Shell shell,
                                                     String varName) {
            shell.getCurrentCommand().removeVariable(varName);
        }

        @Override
        protected String getCommandSyntax() {
            String nameDesc = isNameOptional() ?
                    CommandParser.optional(FIELD_NAME_FLAG_DESC):
                    FIELD_NAME_FLAG_DESC;
            return "add-field " + TYPE_FLAG_DESC + " " + nameDesc +
                " ["+ NOT_NULLABLE_FLAG_DESC + "] " +
                eolt +
                "[" + DEFAULT_FLAG_DESC + "] " +
                "[" + DESC_FLAG_DESC + "] " +
                eolt +
                "[" + SIZE_FLAG_DESC + "] " +
                "[" + ENUM_VALUE_FLAG_DESC + "] " +
                "[" + PRECISION_FLAG_DESC + "]" +
                eolt +
                "<type>: INTEGER, LONG, DOUBLE, FLOAT, STRING, BOOLEAN," +
                " BINARY," + eolt + "FIXED_BINARY, ENUM, TIMESTAMP";
        }

        @Override
        protected String getCommandDescription() {
            return "Add a field. Ranges are inclusive with the exception of " +
                eolt + "String, which will be set to exclusive.";
        }
    }

    static abstract class ComplexFieldBuilderSub
        extends TableBuildSubCommand {

        private static final String VAR_BUILDER = "CurrentFieldBuilder";
        boolean inCollectionBuilder;

        protected ComplexFieldBuilderSub(String name,
                                         int prefixMatchLength) {
            this(name, prefixMatchLength, false);
        }

        protected ComplexFieldBuilderSub(String name,
                                         int prefixMatchLength,
                                         boolean inCollectionBuilder) {
            super(name, prefixMatchLength);
            this.inCollectionBuilder = inCollectionBuilder;
        }

        abstract TableBuildCmdWithSubs getFieldBuilderSubs();
        abstract TableBuilderBase createFieldBuilder(String fname, String desc);

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            TableBuilderVariable tbv =
                    (TableBuilderVariable)getVariable(VAR_BUILDER);
            if (tbv == null) {
                Shell.checkHelp(args, this);
                String fname = null;
                String desc = null;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (FIELD_NAME_FLAG.equals(arg)) {
                        fname = Shell.nextArg(args, i++, this);
                    } else if (DESC_FLAG.equals(arg)) {
                        desc = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }
                if (isNameRequired() && fname == null) {
                    shell.requiredArg(FIELD_NAME_FLAG, this);
                }
                String varName = (fname == null) ? "element" : fname;
                tbv = new TableBuilderVariable(varName,
                                               createFieldBuilder(fname, desc));
                ShellCommand cmd = this.clone();
                cmd.addVariable(VAR_BUILDER, tbv);
                cmd.setPrompt(varName);
                shell.pushCurrentCommand(cmd);
                return null;
            }
            return getFieldBuilderSubs().execute(tbv, args, shell);
        }

        boolean isNameRequired() {
            return true;
        }

        String getCommonUsage() {
            return (isNameRequired() ?
                        FIELD_NAME_FLAG_DESC:
                        CommandParser.optional(FIELD_NAME_FLAG_DESC)) + " " +
                    CommandParser.optional(DESC_FLAG_DESC);
        }
    }

    /* TableBuildSubCommand: add-map-field */
    static class TableBuildAddMapSub extends ComplexFieldBuilderSub {

        private static TableBuildCmdWithSubs builderSubs = new MapBuilderSubs();

        TableBuildAddMapSub() {
            this(false);
        }

        TableBuildAddMapSub(boolean nestedCommand) {
            super("add-map-field", 5, nestedCommand);
        }

        @Override
        TableBuildCmdWithSubs getFieldBuilderSubs() {
            return builderSubs;
        }

        @Override
        TableBuilderBase createFieldBuilder(String fname, String desc) {
            return TableBuilder.createMapBuilder(desc);
        }

        @Override
        boolean isNameRequired() {
            return !inCollectionBuilder;
        }

        private static class MapBuilderSubs extends TableBuildCmdWithSubs {
            private static final
                List<? extends SubCommand> mapBuilderSubs =
                               Arrays.asList(new TableBuildAddArraySub(true),
                                             new TableBuildAddFieldSub(true),
                                             new TableBuildAddMapSub(true),
                                             new TableBuildAddRecordSub(),
                                             new TableBuildCancelSub(),
                                             new TableBuildExitSub(),
                                             new TableBuildSetDescSub(),
                                             new TableBuildShowSub());
            MapBuilderSubs() {
                super(mapBuilderSubs, "", 0, 2);
            }

            @Override
            public String getCommandOverview() {
                return "Build a map field.";
            }
        }

        @Override
        protected String getCommandSyntax() {
            return "add-map-field " + getCommonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Build a map field.";
        }
    }

    /* TableBuildSubCommand: add-array-field */
    static class TableBuildAddArraySub extends ComplexFieldBuilderSub {

        private static TableBuildCmdWithSubs builderSubs =
            new ArrayBuilderSubs();

        TableBuildAddArraySub() {
            this(false);
        }

        TableBuildAddArraySub(boolean inCollectionBuilder) {
            super("add-array-field", 5, inCollectionBuilder);
        }

        @Override
        TableBuildCmdWithSubs getFieldBuilderSubs() {
            return builderSubs;
        }

        @Override
        TableBuilderBase createFieldBuilder(String fname, String desc) {
            return TableBuilder.createArrayBuilder(desc);
        }

        private static class ArrayBuilderSubs extends TableBuildCmdWithSubs {
            private static final
                List<? extends SubCommand> arraybuilderSubs =
                               Arrays.asList(new TableBuildAddArraySub(true),
                                             new TableBuildAddFieldSub(true),
                                             new TableBuildAddMapSub(true),
                                             new TableBuildAddRecordSub(),
                                             new TableBuildCancelSub(),
                                             new TableBuildExitSub(),
                                             new TableBuildSetDescSub(),
                                             new TableBuildShowSub());
            ArrayBuilderSubs() {
                super(arraybuilderSubs, "", 0, 2);
            }

            @Override
            public String getCommandOverview() {
                return "Build a array field.";
            }
        }

        @Override
        boolean isNameRequired() {
            return !inCollectionBuilder;
        }

        @Override
        protected String getCommandSyntax() {
            return "add-array-field " + getCommonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Build a array field.";
        }
    }

    /* TableBuildSubCommand: add-record-field */
    static class TableBuildAddRecordSub extends ComplexFieldBuilderSub {

        private static TableBuildCmdWithSubs builderSubs =
            new RecordBuilderSubs();

        TableBuildAddRecordSub() {
            super("add-record-field", 5);
        }

        @Override
        TableBuildCmdWithSubs getFieldBuilderSubs() {
            return builderSubs;
        }

        @Override
        TableBuilderBase createFieldBuilder(String fname, String desc) {
            return TableBuilder.createRecordBuilder(fname, desc);
        }

        private static class RecordBuilderSubs extends TableBuildCmdWithSubs {
            private static final
                List<? extends SubCommand> recordBuilderSubs =
                               Arrays.asList(new TableBuildAddArraySub(),
                                             new TableBuildAddFieldSub(),
                                             new TableBuildAddMapSub(),
                                             new TableBuildAddRecordSub(),
                                             new TableBuildCancelSub(),
                                             new TableBuildExitSub(),
                                             new TableBuildSetDescSub(),
                                             new TableBuildShowSub());
            RecordBuilderSubs() {
                super(recordBuilderSubs, "", 0, 2);
            }

            @Override
            public String getCommandOverview() {
                return "Build a record field.";
            }
        }

        @Override
        protected String getCommandSyntax() {
            return "add-record-field " + getCommonUsage();
        }

        @Override
        protected String getCommandDescription() {
            return "Build a record field.";
        }
    }

    /* TableBuildSubCommand: remove-field */
    static class TableBuildRemoveFieldSub
        extends TableBuildSubCommand {

        protected TableBuildRemoveFieldSub() {
            super("remove-field", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String field = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (FIELD_NAME_FLAG.equals(arg)) {
                    field = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (field == null) {
                shell.requiredArg(FIELD_NAME_FLAG, this);
            }
            try {
                getCurrentTableBuilder().removeField(field);
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage());
            }
            return null;
        }

        @Override
        protected String getCommandSyntax() {
            return "remove-field " + FIELD_NAME_FLAG_DESC;
        }

        @Override
        protected String getCommandDescription() {
            return "Remove a field.";
        }
    }

    /* TableBuildSubCommand: primary-key */
    static class TableBuildPrimaryKeySub extends TableBuildSubCommand {

        protected TableBuildPrimaryKeySub() {
            super("primary-key", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            List<String> fields = new ArrayList<String>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (FIELD_FLAG.equals(arg)) {
                    fields.add(Shell.nextArg(args, i++, this));
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (fields.size() == 0) {
                shell.requiredArg(FIELD_FLAG, this);
            }
            TableBuilderBase tb = getCurrentTableBuilder();
            try {
                tb.primaryKey(fields.toArray(new String[fields.size()]));
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage(), iae);
            }
            return null;
        }

        @Override
        protected String getCommandSyntax() {
            return "primary-key " + FIELD_FLAG_DESC +
                   " [" + FIELD_FLAG_DESC + "]+";
        }

        @Override
        protected String getCommandDescription() {
            return "Set primary key.";
        }
    }

    /* TableBuildSubCommand: shard-key */
    static class TableBuildShardKeySub extends TableBuildSubCommand {

        protected TableBuildShardKeySub() {
            super("shard-key", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            List<String> fields = new ArrayList<String>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (FIELD_FLAG.equals(arg)) {
                    fields.add(Shell.nextArg(args, i++, this));
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (fields.size() == 0) {
                shell.requiredArg(FIELD_FLAG, this);
            }
            TableBuilderBase tb = getCurrentTableBuilder();
            try {
                tb.shardKey(fields.toArray(new String[fields.size()]));
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage(), iae);
            }
            return null;
        }

        @Override
        protected String getCommandSyntax() {
            return "shard-key " +  FIELD_FLAG_DESC +
                   " [" + FIELD_FLAG_DESC + "]+";
        }

        @Override
        protected String getCommandDescription() {
            return "Set shard key.";
        }
    }

    /* TableBuildSubCommand: set-description */
    static class TableBuildSetDescSub extends TableBuildSubCommand {

        protected TableBuildSetDescSub() {
            super("set-description", 5);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String description = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (DESC_FLAG.equals(arg)) {
                    description = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (description == null) {
                shell.requiredArg(DESC_FLAG, this);
            }
            getCurrentTableBuilder().setDescription(description);
            return null;
        }

        @Override
        protected String getCommandSyntax() {
            return "set-description " + DESC_FLAG_DESC;
        }

        @Override
        protected String getCommandDescription() {
            return "Set description for the table.";
        }
    }

    /* TableBuildSubCommand: show */
    static class TableBuildShowSub extends TableBuildSubCommand {

        protected TableBuildShowSub() {
            super("show", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            boolean showOriginal = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (ORIGINAL_FLAG.equals(arg)) {
                    showOriginal = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            TableBuilderBase tb = getCurrentTableBuilder();
            try {
                if (tb instanceof TableBuilder) {
                    return ((TableBuilder)tb).toJsonString(true);
                } else if (tb instanceof TableEvolver) {
                    if (showOriginal) {
                        return ((TableEvolver)tb).getTable().toJsonString(true,
                                                         getRegionMapper(shell));
                    }
                    return ((TableEvolver)tb).toJsonString(true);
                } else if (tb instanceof MapBuilder) {
                    return ((MapBuilder)tb).toJsonString(true);
                } else if (tb instanceof ArrayBuilder) {
                    return ((ArrayBuilder)tb).toJsonString(true);
                } else if (tb instanceof RecordBuilder) {
                    return ((RecordBuilder)tb).toJsonString(true);
                }
            } catch (IllegalCommandException ice) {
                throw new ShellException("Could not create JSON " +
                        "representation:" + eolt + ice.getMessage(), ice);
            } catch (IllegalArgumentException iae) {
                throw new ShellException("Could not create JSON " +
                        "representation:" + eolt + iae.getMessage(), iae);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "show [" + ORIGINAL_FLAG_DESC + "]";
        }

        @Override
        protected String getCommandDescription() {
            return "Display the table information, if building a table for " +
                "evolution, " + eolt + "use " + ORIGINAL_FLAG_DESC +
                " flag to show the original table information, the flag " +
                eolt + "will be ignored for building table for addition.";
        }
    }

    /* TableBuildSubCommand: exit */
    static class TableBuildExitSub extends TableBuildSubCommand {

        protected TableBuildExitSub() {
            super("exit", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            if (args.length != 1) {
                shell.badArgCount(this);
            }

            TableBuilderBase tb = getCurrentTableBuilder();
            String retString = null;
            if (tb instanceof TableBuilder) {
                retString = procAddTable((TableBuilder) tb, shell);
            } else if (tb instanceof TableEvolver) {
                retString = procEvolveTable((TableEvolver) tb, shell);
            } else {
                retString = procFieldBuilder(tb, getCurrentTableBuilderName(),
                                             shell);
            }
            return retString;
        }

        private String procAddTable(TableBuilder tbr, Shell shell)
            throws ShellException {

            try {
                tbr.validate();
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage(), iae);
            } catch (IllegalCommandException ice) {
                throw new ShellException(ice.getMessage(), ice);
            }

            shell.addVariable(makeTableFullName(tbr), tbr);
            shell.popCurrentCommand();
            return "Table " + makeTableFullName(tbr) + " built.";
        }

        private String procEvolveTable(TableEvolver te, Shell shell)
            throws ShellException {

            try {
                te.evolveTable();
            } catch (IllegalCommandException ice) {
                throw new ShellException(ice.getMessage(), ice);
            }

            shell.addVariable(makeTableFullName(te), te);
            shell.popCurrentCommand();
            return "Table " + makeTableFullName(te) + " built.";
        }

        private String procFieldBuilder(TableBuilderBase tb,
                                        String varName, Shell shell)
            throws ShellException {

            FieldDef fieldDef = null;
            try {
                fieldDef = tb.build();
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage(), iae);
            }
            shell.popCurrentCommand();

            /* Add an variable FieldDef to current command. */
            ShellCommand cmd = shell.getCurrentCommand();
            cmd.addVariable(varName, fieldDef);

            String ftype = getComplexFieldBuilderType(tb);
            if (ftype == null) {
                throw new ShellException("Unsupported field type.");
            }
            /*Run add-field command to add the field to tableBuilder. */
            shell.runLine("add-field -name " + varName + " -type " + ftype);
            return null;
        }

        private String getComplexFieldBuilderType(TableBuilderBase tb) {
            if (tb instanceof MapBuilder) {
                return "map";
            } else if (tb instanceof ArrayBuilder) {
                return "array";
            } else if (tb instanceof RecordBuilder) {
                return "record";
            }
            return null;
        }
        @Override
        protected String getCommandDescription() {
            return "Exit the building operation, saving the table(or field) " +
                    eolt + "for addition(or evolution) to the store.";
        }
    }

    /* TableBuildSubCommand: cancel */
    static class TableBuildCancelSub extends TableBuildSubCommand {

        protected TableBuildCancelSub() {
            super("cancel", 4);
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
            return "Exit the building operation, canceling the table(or field)"+
                    eolt + "under construction.";
        }
    }

    private static RegionMapper getRegionMapper(Shell shell)
            throws ShellException {
        final CommandShell cmd = (CommandShell) shell;
        final CommandServiceAPI cs = cmd.getAdmin();
        try {
            final TableMetadata meta = cs.getMetadata(TableMetadata.class,
                                                      MetadataType.TABLE);
            if (meta == null) {
                throw new Exception("Metadata not found");
            }
            return meta.getRegionMapper();
        } catch (Exception e) {
            throw new ShellException("Exception getting metadata: " +
                                     e.getMessage());
        }
    }

    private static Table findTable(String namespace,
                                   String tableName,
                                   boolean mustExist,
                                   Shell shell)
        throws ShellException {

        final CommandShell cmd = (CommandShell) shell;
        final CommandServiceAPI cs = cmd.getAdmin();
        TableMetadata meta = null;
        try {
            meta = cs.getMetadata(TableMetadata.class,
                                  MetadataType.TABLE);
            if (meta != null) {
                return meta.getTable(namespace, tableName, mustExist);
            }
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        } catch (IllegalStateException ise) {
            throw new ShellException(ise.getMessage(), ise);
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
        if (mustExist) {
            throw new ShellException("Table does not exist: " +
                                     makeTableFullName(namespace,
                                                       null, tableName));
        }
        return null;
    }

    private static Object getCurrentCmdVariable(Shell shell, String varName) {
        return shell.getCurrentCommand().getVariable(varName);
    }

    private static String makeTableFullName(TableBuilderBase tb) {
        String tableName = null;
        String parentName = null;

        if (tb instanceof TableBuilder) {
            tableName = ((TableBuilder)tb).getName();
            if (((TableBuilder)tb).getParent() != null) {
                parentName = ((TableBuilder)tb).getParent().getFullName();
            }
            String namespace = ((TableBuilder)tb).getNamespace();
            return makeTableFullName(namespace, tableName, parentName);
        } else if (tb instanceof TableEvolver) {
            return ((TableEvolver)tb).getTable().getFullNamespaceName();
        }
        return null;
    }

    private static String makeTableFullName(String namespace,
                                            String name,
                                            String tableName) {
        return NameUtils.makeQualifiedName(namespace, name, tableName);
    }
}
