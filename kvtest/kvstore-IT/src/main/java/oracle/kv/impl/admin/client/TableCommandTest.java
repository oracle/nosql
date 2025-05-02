/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.admin.client;

import static oracle.kv.util.shell.Shell.eolt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.util.CreateStore;
import oracle.kv.util.TableTestUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies the functionality and error paths of the TableCommand class.
 * Note that although the CLI test framework verifies many of the same aspects
 * of TableCommand as this unit test, the tests from the CLI test framework
 * do not contribute to the unit test coverage measure that is automatically
 * computed nightly. Thus, the intent of this test class is to provide
 * additional unit test coverage for the TableCommand class that will be
 * automatically measured nightly.
 */
public final class TableCommandTest extends TestBase {
    /*private final boolean verifyExceptionMsg = false;*/

    private final String command = "table";
    private final String createSub = "create";
    private final String evolveSub = "evolve";
    private final String listSub = "list";
    private final String clearSub = "clear";

    private final String buildAddFieldSub = "add-field";
    private final String buildAddArraySub = "add-array-field";
    private final String buildAddMapSub = "add-map-field";
    private final String buildAddRecordSub = "add-record-field";
    private final String buildPrimaryKeySub = "primary-key";
    private final String buildShardKeySub = "shard-key";
    private final String buildSetDescSub = "set-description";
    private final String buildShowSub = "show";
    private final String buildRemoveFieldSub = "remove-field";
    private final String buildExitSub = "exit";
    private final String buildCancelSub = "cancel";

    /*private final String[] exceptionMsgs = {
        "Table is missing state required for construction",
        "Value outside of allowed range",
        "Table does not exist",
        "Map has no field and cannot be built",
        "Array has no field and cannot be built",
        "Record has no fields and cannot be built",
        "Field type cannot be part of a primary key",
        "Primary key field is not a valid field",
        "Invalid argument",
        "Duplicated enumeration value",
        "Table, index and field names may contain only " +
        "alphanumeric values plus the character \"_\"",
        "Field for collection is already defined",
        "Schema does not exist or is disabled",
        "Shard key must be a subset of the primary key",
        "Invalid enumeration value",
        "Cannot add field, it already exists",
        "Field does not exist",
        "Cannot remove a key field",
        "Invalid argument: -default can not be used with",
        "Table, index and field names " +
            "must start with an alphabetic character",
        "Table names must be less than or equal to 32 characters",
        "Field and index names must be less than or equal to 64 characters"
    }; */
    private static final int ERR_TABLE_MISS_STATE = 0;
    private static final int ERR_TABLE_NOT_EXISTS = 3;
    private static final int ERR_MAP_MISS_FIELD = 4;
    private static final int ERR_ARRAY_MISS_FIELD = 5;
    private static final int ERR_RECORD_MISS_FIELD = 6;
    private static final int ERR_PRIMARYKEY_INVALID_TYPE = 7;
    private static final int ERR_PRIMARYKEY_FIELD_NOTEXISTS = 8;
    private static final int ERR_FIELD_INVALID_TYPE = 9;
    private static final int ERR_FIELD_NAME_ALPHANUMERIC_ONLY = 11;
    private static final int ERR_TABLE_NAME_ALPHANUMERIC_ONLY = 11;
    private static final int ERR_COLLECTION_FIELD_ALREADY_DEFINED = 12;
    private static final int ERR_SHARDKEY_SUBSET_PRIMARYKEY = 14;
    private static final int ERR_EVOLVETABLE_FIELD_ALREADY_EXISTS = 16;
    private static final int ERR_EVOLVETABLE_FIELD_NOT_EXISTS = 17;
    private static final int ERR_EVOLVETABLE_CANNOT_REMOVE_KEYFIELD = 18;
    private static final int ERR_FIELD_DEFAULT_CANNOT_USE = 20;
    private static final int ERR_TABLE_NAME_INVALID_STARTWITH = 22;
    private static final int ERR_FEILD_NAME_INVALID_STARTWITH = 22;
    private static final int ERR_TABLE_NAME_INVALID_LENGTH = 23;
    private static final int ERR_FEILD_NAME_INVALID_LENGTH = 24;
    private static final int ERR_FIELD_SIZE_CANNOT_USE = 25;
    private static final int ERR_FIELD_ENUMVALUES_CANNOT_USE = 26;
    private static final int ERR_FIELD_NOTNULLABLE_CANNOT_USE = 27;

    private static CreateStore createStore;
    private static KVStore store;
    private static CommandServiceAPI cs;
    private static final int startPort = 8000;

    @BeforeClass
    public static void staticSetup()
        throws Exception {

        TestUtils.clearTestDirectory();
        createStore = new CreateStore(
            "kvtest-" + TableCommandTest.class.getName(),
            startPort, 1, 1, 10, 1);
        createStore.start();
        final KVStoreConfig config = createStore.createKVConfig();

        store = KVStoreFactory.getStore(config);
        cs = createStore.getAdmin();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (store != null) {
            store.close();
        }
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @Override
    public void setUp()
        throws Exception {

        removeAllTables();
    }

    @Override
    public void tearDown()
        throws Exception {

        removeAllTables();
    }

    @Test
    public void testTableCommandGetCommandOverview()
        throws Exception {

        final TableCommand tableCommandObj = new TableCommand();
        final String expectedResult =
            "This command has been deprecated in favor of the" + Shell.eol +
            "execute <statement> command for most operations." + Shell.eol +
            "The table command encapsulates commands for building tables " +
            "for addition or evolution. " + Shell.eol +
            "Tables are created and modified in two steps.  " +
            "The table command creates new tables or" + Shell.eol +
            "evolves existing tables and saves them in temporary, " +
            "non-persistent storage in the admin" + Shell.eol +
            "client.  These tables are kept by name when exiting the create " +
            "and evolve sub-commands. " + Shell.eol +
            "The second step is to use the plan command to deploy " +
            "the new and changed tables" + Shell.eol +
            "to the store itself.  The temporary list of tables " +
            "can be examined and cleared using" + Shell.eol +
            "the list and clear sub-commands.";
        assertEquals(expectedResult, tableCommandObj.getCommandOverview());
    }

    /* table create sub */
    @Test
    public void testTableCreateSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableCreateSub subObj =
            new TableCommand.TableCreateSub();
        final String expectedResult =  command + " " + createSub + " " +
            TableCommand.TABLE_NAME_FLAG_DESC +
            " [" + TableCommand.DESC_FLAG_DESC + "] ";
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableCreateSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableCreateSub subObj =
            new TableCommand.TableCreateSub();
        final String expectedResult =
            "Build a new table to be added to the store.  " +
            "New tables are added using" + eolt +
            "the plan add-table command.  " +
            "The table name is an optionally namespace" + eolt +
            "qualified dot-separated name with " +
            "the format " + eolt + "[ns:]tableName[.childTableName]+.  ";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableCreateSubExecuteArgs()
        throws Exception {

        /* unknown arg */
        doExecuteUnknownArg(new TableCommand.TableCreateSub(), createSub);

        /* flag required argument */
        final String[] flagRequiredArgs = {
            TableCommand.TABLE_NAME_FLAG,
            TableCommand.DESC_FLAG
        };
        doExecuteFlagRequiredArg(new TableCommand.TableCreateSub(),
                                 createSub,
                                 flagRequiredArgs);

        /* required argument */
        final String[] requiredArgs = {TableCommand.TABLE_NAME_FLAG};
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.TABLE_NAME_FLAG, "user.email");
        argsMap.put(TableCommand.DESC_FLAG, "email information");

        doExecuteRequiredArgs(new TableCommand.TableCreateSub(),
                              createSub,
                              argsMap,
                              requiredArgs,
                              requiredArgs[0]);
    }


    /* table evolve sub */
    @Test
    public void testTableEvolveSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableEvolveSub subObj =
            new TableCommand.TableEvolveSub();
        final String expectedResult =  command + " " + evolveSub + " " +
            TableCommand.TABLE_NAME_FLAG_DESC;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableEvolveSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableEvolveSub subObj =
            new TableCommand.TableEvolveSub();
        final String expectedResult =
            "Build a table for evolution.  Tables are evolved using " +
            "the plan" + eolt + "evolve-table command.  The table name " +
            "is an optionally namespace " + eolt +
            "qualified dot-separated name with the format" + eolt +
            "[ns:]tableName[.childTableName]+.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableEvolveSubExecuteUnknownArg()
        throws Exception {

        /* unknown arguments */
        doExecuteUnknownArg(new TableCommand.TableEvolveSub(), evolveSub);

        /* flag requiring arguments */
        final String[] flagRequiredArgs = {
            TableCommand.TABLE_NAME_FLAG
        };
        doExecuteFlagRequiredArg(new TableCommand.TableEvolveSub(),
                                 evolveSub,
                                 flagRequiredArgs);

        /* requireed arguments */
        final String[] requiredArgs = {TableCommand.TABLE_NAME_FLAG};
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.TABLE_NAME_FLAG, "user.email");

        doExecuteRequiredArgs(new TableCommand.TableEvolveSub(),
                              evolveSub,
                              argsMap,
                              requiredArgs,
                              requiredArgs[0]);
    }

    @Test
    public void testTableEvolveSubExecuteRequiredArgs()
        throws Exception {

        final String[] flagRequiredArgs = {TableCommand.TABLE_NAME_FLAG};
        doExecuteFlagRequiredArg(new TableCommand.TableEvolveSub(),
                                 evolveSub,
                                 flagRequiredArgs);

        final String[] requiredArgs = {TableCommand.TABLE_NAME_FLAG};
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.TABLE_NAME_FLAG, "user.email");

        doExecuteRequiredArgs(new TableCommand.TableEvolveSub(),
                              evolveSub,
                              argsMap,
                              requiredArgs,
                              requiredArgs[0]);
    }

    @Test
    public void testTableEvolveSubExceuteTableNotExists()
        throws Exception {

        final CommandShell shell = connectToStore();
        final String[] args = new String[] {
            evolveSub,
            TableCommand.TABLE_NAME_FLAG, "TABLE_NOT_EXISTS"
        };
        execShellCommand(shell, new TableCommand.TableEvolveSub(), args,
                         ERR_TABLE_NOT_EXISTS);
    }

    /* table list sub */
    @Test
    public void testTableListSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableListSub subObj =
            new TableCommand.TableListSub();
        final String expectedResult = command + " " + listSub +
                " [" + TableCommand.TABLE_NAME_FLAG_DESC + "] " +
                "["+ TableCommand.CREATE_FLAG_DESC +" | " +
                TableCommand.EVOLVE_FLAG_DESC + "]";
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableListSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableListSub subObj =
            new TableCommand.TableListSub();
        final String expectedResult =
            "Lists all the tables built but not yet created or " +
            "evolved.  Flag " + TableCommand.TABLE_NAME_FLAG + eolt +
            "is used to show the details of the named table.  The table " +
            "name is" + eolt + "an optionally namespace qualified" +
            " dot-separated name with the format " + eolt +
            "[ns:]tableName[.childTableName]+.  Use flag " +
            TableCommand.CREATE_FLAG + " or " + TableCommand.EVOLVE_FLAG +
            " to show" + eolt + "the tables for addition or evolution.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableListSubExecuteUnknownArg()
        throws Exception {

        /* unknown argument */
        doExecuteUnknownArg(new TableCommand.TableListSub(), listSub);

        /* flag requiring arguments */
        final String[] flagRequiredArgs = {TableCommand.TABLE_NAME_FLAG};
        doExecuteFlagRequiredArg(new TableCommand.TableListSub(),
                                 listSub,
                                 flagRequiredArgs);
    }

    /* table clear sub */
    @Test
    public void testTableClearSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableClearSub subObj =
            new TableCommand.TableClearSub();
        final String expectedResult = command + " " + clearSub +
            " [" + TableCommand.TABLE_NAME_FLAG_DESC + " | " +
            TableCommand.ALL_FLAG_DESC + "]";
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableClearSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableClearSub subObj =
            new TableCommand.TableClearSub();
        final String expectedResult =
            "Clear the tables built but not yet created or evolved. " +
                "Use flag " + TableCommand.TABLE_NAME_FLAG + eolt + "to " +
                "specify the table to be clear.  The table name is an " +
                "optionally" + eolt + "namespace qualified dot-separated name" +
                " with the format " + eolt +
                "[ns:]tableName[.childTableName]+.  Flag " +
                TableCommand.ALL_FLAG + " is used to clear all the " + eolt +
                "tables.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableClearSubExecuteUnknownArg()
        throws Exception {

        /* unknown argument */
        doExecuteUnknownArg(new TableCommand.TableClearSub(), clearSub);

        /* flag requiring argument */
        final String[] flagRequiredArgs = new String[] {
            TableCommand.TABLE_NAME_FLAG
        };
        doExecuteFlagRequiredArg(new TableCommand.TableClearSub(),
                                 clearSub,
                                 flagRequiredArgs);

        /* required argument */
        final String[] requiredArgs = new String[] {
            TableCommand.TABLE_NAME_FLAG,
            TableCommand.ALL_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.TABLE_NAME_FLAG, "user");
        argsMap.put(TableCommand.ALL_FLAG, TableCommand.ALL_FLAG);
        doExecuteRequiredArgs(new TableCommand.TableClearSub(),
                              clearSub, argsMap, requiredArgs,
                              TableCommand.TABLE_NAME_FLAG + "|" +
                              TableCommand.ALL_FLAG);
    }

    /* add-field sub */
    @Test
    public void testTableBuildAddFieldSubGetCommandSyntax()
        throws Exception {

        TableCommand.TableBuildAddFieldSub subObj =
            new TableCommand.TableBuildAddFieldSub();
        String nameDesc = " " + TableCommand.FIELD_NAME_FLAG_DESC;
        String commonDesc = " ["+ TableCommand.NOT_NULLABLE_FLAG_DESC + "] " +
            eolt +
            "[" + TableCommand.DEFAULT_FLAG_DESC + "] " +
            "[" + TableCommand.DESC_FLAG_DESC + "] " +
            eolt +
            "[" + TableCommand.SIZE_FLAG_DESC + "] " +
            "[" + TableCommand.ENUM_VALUE_FLAG_DESC +  "] " +
            "[" + TableCommand.PRECISION_FLAG_DESC + "]" +
            eolt +
            "<type>: INTEGER, LONG, DOUBLE, FLOAT, STRING, BOOLEAN, " +
            "BINARY," + eolt + "FIXED_BINARY, ENUM, TIMESTAMP";
        String expectedResult =  buildAddFieldSub + " " +
                TableCommand.TYPE_FLAG_DESC + nameDesc + commonDesc;
        assertEquals(expectedResult, subObj.getCommandSyntax());

        subObj = new TableCommand.TableBuildAddFieldSub(true);
        nameDesc = " [" + TableCommand.FIELD_NAME_FLAG_DESC + "]";
        expectedResult =  buildAddFieldSub + " " +
                TableCommand.TYPE_FLAG_DESC + nameDesc + commonDesc;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildAddFieldSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildAddFieldSub subObj =
            new TableCommand.TableBuildAddFieldSub();
        final String expectedResult =
            "Add a field. Ranges are inclusive with the exception of " +
            eolt + "String, which will be set to exclusive.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableBuildAddFieldSubExecuteArgs()
        throws Exception {

        final String[] cmds = new String[] {createSub, buildAddFieldSub};
        final CommandShell shell = connectToStore();

        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG,
            "user"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);
        ShellCommand currentCmd = shell.getCurrentCommand();

        /* test for unknown argument */
        doExecuteUnknownArg(currentCmd, cmds, shell);

        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {
            TableCommand.TYPE_FLAG,
            TableCommand.FIELD_NAME_FLAG,
            TableCommand.DEFAULT_FLAG,
            TableCommand.DESC_FLAG,
            TableCommand.ENUM_VALUE_FLAG
        };
        doExecuteFlagRequiredArg(currentCmd, cmds, flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.TYPE_FLAG,
            TableCommand.FIELD_NAME_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_NAME_FLAG, "id");
        argsMap.put(TableCommand.TYPE_FLAG, "integer");
        argsMap.put(TableCommand.DEFAULT_FLAG, "1");
        argsMap.put(TableCommand.DESC_FLAG, "user id");
        argsMap.put(TableCommand.NOT_NULLABLE_FLAG,
                    TableCommand.NOT_NULLABLE_FLAG);
        doExecuteRequiredArgs(currentCmd, cmds, argsMap, requiredArgs,
                              requiredArgs[0], shell);
        for (String arg: requiredArgs) {
            doExecuteRequiredArgs(currentCmd, cmds, argsMap,
                                  new String[]{arg}, arg, shell);
        }

        argsMap.clear();
        argsMap.put(TableCommand.FIELD_NAME_FLAG, "color");
        argsMap.put(TableCommand.TYPE_FLAG, "enum");
        argsMap.put(TableCommand.ENUM_VALUE_FLAG, "WHITE,BLUE,RED,YELLOW");
        argsMap.put(TableCommand.DEFAULT_FLAG, "WHITE");
        argsMap.put(TableCommand.DESC_FLAG, "colors");
        doExecuteRequiredArgs(currentCmd, cmds, argsMap,
                              new String[]{TableCommand.ENUM_VALUE_FLAG},
                              TableCommand.ENUM_VALUE_FLAG, shell);
    }

    /* add-field: invalid data type. */
    @Test
    public void testTableBuildAddFieldInvalidDatatype()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);

        /* add-field -name field1 -type UNKNOWN_TYPE */
        final String[] args = new String[] {
            createSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "field1",
            TableCommand.TYPE_FLAG, "UNKNOWN_TYPE"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args,
                         ERR_FIELD_INVALID_TYPE);
    }

    /* add-field: invalid props for data type. */
    @Test
    public void testTableBuildAddFieldInvalidProp()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);

        /**
         * -not-nullable and -default are not allowed for the field
         * of Map or Array.
         */
        final String[] addCollectionFields = {
            buildAddMapSub, buildAddArraySub
        };
        final String[] AllTypes = {
            "Integer", "Long", "Float", "Double", "String", "Boolean",
            "Binary", "Fixed_binary", "Enum",
        };

        String[] args;
        for (String subCommand: addCollectionFields) {
            args = new String[] {
                createSub, subCommand,
                TableCommand.FIELD_NAME_FLAG, "collectionF"
            };
            execShellCommand(shell, shell.getCurrentCommand(), args);

            for (String type: AllTypes) {
                /* add-field -name field1 -type X -not-nullable */
                args = new String[] {
                    subCommand, buildAddFieldSub,
                    TableCommand.FIELD_NAME_FLAG, "field1",
                    TableCommand.TYPE_FLAG, type,
                    TableCommand.NOT_NULLABLE_FLAG
                };
                execShellCommand(shell, shell.getCurrentCommand(),
                                 addFieldAppendProps(type, args),
                                 ERR_FIELD_NOTNULLABLE_CANNOT_USE);
            }

            for (String type: AllTypes) {
                /* add-field -name field1 -type X -not-nullable */
                args = new String[] {
                    subCommand, buildAddFieldSub,
                    TableCommand.FIELD_NAME_FLAG, "field1",
                    TableCommand.TYPE_FLAG, type,
                    TableCommand.DEFAULT_FLAG, "123"
                };
                execShellCommand(shell, shell.getCurrentCommand(),
                                 addFieldAppendProps(type, args),
                                 ERR_FIELD_DEFAULT_CANNOT_USE);
            }
            execBuildCancelSub(shell, subCommand);
        }

        /* Test -size flag. */
        final String[] invalidTypesWithSize = {
            "Integer", "Long", "Float",
            "Double", "String", "Boolean",
            "Binary", "Enum"
        };
        for (String type: invalidTypesWithSize) {
            /* add-field -name field1 -type X -size Y */
            args = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, "field1",
                TableCommand.TYPE_FLAG, type,
                TableCommand.SIZE_FLAG, "10"
            };
            execShellCommand(shell, shell.getCurrentCommand(),
                addFieldAppendProps(type, args),
                ERR_FIELD_SIZE_CANNOT_USE);
        }

        /* Test -enum-values flag. */
        final String[] invalidTypesEnumValues = {
            "Integer", "Long", "Float",
            "Double", "String", "Boolean",
            "Binary", "Fixed_binary"
        };
        for (String type: invalidTypesEnumValues) {
            /* add-field -name field1 -type X -enum-values ... */
            args = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, "field1",
                TableCommand.TYPE_FLAG, type,
                TableCommand.ENUM_VALUE_FLAG, "type1,type2,type3"
            };
            execShellCommand(shell, shell.getCurrentCommand(),
                addFieldAppendProps(type, args),
                ERR_FIELD_ENUMVALUES_CANNOT_USE);
        }
    }

    private String[] addFieldAppendProps(String type, String[] args) {
        String[] newArgs = args;
        if (type.equalsIgnoreCase("fixed_binary")) {
            newArgs = new String[args.length + 2];
            System.arraycopy(args, 0, newArgs, 0, args.length);
            newArgs[newArgs.length - 2] = TableCommand.SIZE_FLAG;
            newArgs[newArgs.length - 1] = "10";
        } else if (type.equalsIgnoreCase("enum")) {
            newArgs = new String[args.length + 2];
            System.arraycopy(args, 0, newArgs, 0, args.length);
            newArgs[newArgs.length - 2] = TableCommand.ENUM_VALUE_FLAG;
            newArgs[newArgs.length - 1] = "TYPE1,TYPE2,TYPE3";
        }
        return newArgs;
    }

    /* add-field: invalid field name. */
    @Test
    public void testTableBuildAddFieldInvalidFieldName()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* kv-> table create -id user */
        final String[] createTableArgs = new String[] {
            createSub, TableCommand.TABLE_NAME_FLAG, "mytable"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                         createTableArgs);

        final String[] invalidNames1 = {
            "User.Name", "User$Name", "User|Name", "User~Name", "User-Name",
        };
        final String[] invalidNames2 = {
            "_Name", "1Name"
        };
        final String[] invalidNames3 = {
            "NameabcdefNameabcdefNameabcdefNameabcdefNameabcdefNameabcdef12345"
        };
        final String[] validNames = {
            "User_name", "User123name",
            "NameabcdefNameabcdefNameabcdefNameabcdefNameabcdefNameabcdef1234"
        };

        /* Invalid name: contains none-alphanumeric character */
        for (String name: invalidNames1) {
            final String[] args = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, name,
                TableCommand.TYPE_FLAG, "Integer"
            };
            execShellCommand(shell, shell.getCurrentCommand(), args,
                ERR_FIELD_NAME_ALPHANUMERIC_ONLY);
        }

        /* Invalid name: start with none-alphabetic character */
        for (String name: invalidNames2) {
            final String[] args = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, name,
                TableCommand.TYPE_FLAG, "Integer"
            };
            execShellCommand(shell, shell.getCurrentCommand(), args,
                ERR_FEILD_NAME_INVALID_STARTWITH);
        }

        /* Invalid name: length > 64 */
        for (String name: invalidNames3) {
            final String[] args = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, name,
                TableCommand.TYPE_FLAG, "Integer"
            };
            execShellCommand(shell, shell.getCurrentCommand(), args,
                ERR_FEILD_NAME_INVALID_LENGTH);
        }

        /* Valid names */
        for (String name: validNames) {
            final String[] args = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, name,
                TableCommand.TYPE_FLAG, "Integer"
            };
            execShellCommand(shell, shell.getCurrentCommand(), args);
        }
    }

    /* add-array-field sub */
    @Test
    public void testTableBuildAddArrayFieldSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildAddArraySub subObj =
            new TableCommand.TableBuildAddArraySub();
        final String expectedResult = buildAddArraySub + " " +
            new TableCommand.TableBuildAddArraySub().getCommonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    /* add-array-field sub */
    @Test
    public void testTableBuildAddArraySubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildAddArraySub subObj =
            new TableCommand.TableBuildAddArraySub();
        final String expectedResult = "Build a array field.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* add-array-field: unknown, flag requiring arg and required args. */
    @Test
    public void testTableBuildAddArraySubExecuteArgs()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);

        /* test for unknown argument */
        final String[] cmds = new String[] {
            createSub, buildAddArraySub
        };
        doExecuteUnknownArg(shell.getCurrentCommand(), cmds, shell);

        /* test for flag requiring argument */
        final String[] flagRequiredArgs = new String[] {
            TableCommand.FIELD_NAME_FLAG,
            TableCommand.DESC_FLAG,
        };
        doExecuteFlagRequiredArg(shell.getCurrentCommand(), cmds,
                                 flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.FIELD_NAME_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_NAME_FLAG, "arrayField");
        doExecuteRequiredArgs(shell.getCurrentCommand(), cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);

    }

    /* add-array-field: add more than one inner field */
    @Test
    public void testTableBuildAddArraySubExecuteMoreThanOneField()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);

        /* add-array-field -name arrayField */
        String[] args = new String[] {
            createSub, buildAddArraySub,
            TableCommand.FIELD_NAME_FLAG, "arrayField"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);

        /* add-field -name field1 -type Integer */
        args = new String[] {
            buildAddArraySub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "field1",
            TableCommand.TYPE_FLAG, "Integer"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);

        /* add-field -name field1 -type Integer */
        execShellCommand(shell, shell.getCurrentCommand(), args,
                         ERR_COLLECTION_FIELD_ALREADY_DEFINED);

    }

    /* add-map-field sub */
    @Test
    public void testTableBuildAddMapFieldSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildAddMapSub subObj =
            new TableCommand.TableBuildAddMapSub();
        final String expectedResult = buildAddMapSub + " " +
            new TableCommand.TableBuildAddMapSub().getCommonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildAddMapSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildAddMapSub subObj =
            new TableCommand.TableBuildAddMapSub();
        final String expectedResult = "Build a map field.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* add-map-field: unknown, flag requiring arg and required args. */
    @Test
    public void testTableBuildAddMapSubExecuteArgs()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);

        /* test for unknown argument */
        String[] cmds = new String[] {
            createSub, buildAddMapSub
        };
        doExecuteUnknownArg(shell.getCurrentCommand(), cmds, shell);


        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {
            TableCommand.FIELD_NAME_FLAG,
            TableCommand.DESC_FLAG,
        };
        doExecuteFlagRequiredArg(shell.getCurrentCommand(), cmds,
                                 flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.FIELD_NAME_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_NAME_FLAG, "mapField");
        doExecuteRequiredArgs(shell.getCurrentCommand(), cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);
    }

    /* add-map-field: add more than one field */
    @Test
    public void testTableBuildAddMapSubExecuteMoreThanOneField()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);

        /* add-map-field -name mapField */
        String[] args = new String[] {
            createSub, buildAddMapSub,
            TableCommand.FIELD_NAME_FLAG, "mapField"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);

        /* add-field -name field1 -type Integer */
        args = new String[] {
            buildAddMapSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "field1",
            TableCommand.TYPE_FLAG, "Integer"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);

        /* add-field -name field1 -type Integer */
        execShellCommand(shell, shell.getCurrentCommand(), args,
                         ERR_COLLECTION_FIELD_ALREADY_DEFINED);
    }

    /* add-record-field sub. */
    @Test
    public void testTableBuildAddRecordFieldSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildAddRecordSub subObj =
            new TableCommand.TableBuildAddRecordSub();
        final String expectedResult = buildAddRecordSub + " " +
            new TableCommand.TableBuildAddRecordSub().getCommonUsage();
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildAddRecordSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildAddRecordSub subObj =
            new TableCommand.TableBuildAddRecordSub();
        final String expectedResult = "Build a record field.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* add-record-field: unknown, flag requiring arg and required args. */
    @Test
    public void testTableBuildAddRecordSubExecuteArgs()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);

        /* test for unknown argument */
        String[] cmds = new String[] {
            createSub, buildAddRecordSub
        };
        doExecuteUnknownArg(shell.getCurrentCommand(), cmds, shell);


        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {
            TableCommand.FIELD_NAME_FLAG,
            TableCommand.DESC_FLAG,
        };
        doExecuteFlagRequiredArg(shell.getCurrentCommand(), cmds,
                                 flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.FIELD_NAME_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_NAME_FLAG, "recordField");
        doExecuteRequiredArgs(shell.getCurrentCommand(), cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);
    }

    /* primary-key sub */
    @Test
    public void testTableBuildPrimaryKeySubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildPrimaryKeySub subObj =
            new TableCommand.TableBuildPrimaryKeySub();
        final String expectedResult = buildPrimaryKeySub + " " +
            TableCommand.FIELD_FLAG_DESC +
            " [" + TableCommand.FIELD_FLAG_DESC + "]+";
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildPrimaryKeySubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildPrimaryKeySub subObj =
            new TableCommand.TableBuildPrimaryKeySub();
        final String expectedResult = "Set primary key.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* primary-key sub: unknown arg, flag requiring arg and required args. */
    @Test
    public void testTableBuildPrimaryKeySubExecuteArgs()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        final ShellCommand currentCmd = shell.getCurrentCommand();

        final String[] cmds = new String[] {createSub, buildPrimaryKeySub};
        /* test for unknown argument */
        doExecuteUnknownArg(currentCmd, cmds, shell);

        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {
            TableCommand.FIELD_FLAG
        };
        doExecuteFlagRequiredArg(currentCmd, cmds, flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.FIELD_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_FLAG, "field1");
        argsMap.put(TableCommand.FIELD_FLAG, "field2");
        doExecuteRequiredArgs(currentCmd, cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);
    }

    /* shard-key sub */
    @Test
    public void testTableBuildShardKeySubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildShardKeySub subObj =
            new TableCommand.TableBuildShardKeySub();
        final String expectedResult = buildShardKeySub + " " +
            TableCommand.FIELD_FLAG_DESC +
            " [" + TableCommand.FIELD_FLAG_DESC + "]+";
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildShardKeySubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildShardKeySub subObj =
            new TableCommand.TableBuildShardKeySub();
        final String expectedResult = "Set shard key.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* shard-key sub: test for unknown, flag requiring arg and required args. */
    @Test
    public void testTableBuildShardKeySubExecuteArgs()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        final ShellCommand currentCmd = shell.getCurrentCommand();

        final String[] cmds = new String[] {createSub, buildShardKeySub};
        /* test for unknown argument */
        doExecuteUnknownArg(currentCmd, cmds, shell);

        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {TableCommand.FIELD_FLAG};
        doExecuteFlagRequiredArg(currentCmd, cmds, flagRequiredArgs, shell);

        /* test for required arguments */
        String[] requiredArgs = new String[] {
            TableCommand.FIELD_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_FLAG, "field1");
        argsMap.put(TableCommand.FIELD_FLAG, "field2");
        doExecuteRequiredArgs(currentCmd, cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);
    }

    /* set-description sub */
    @Test
    public void testTableBuildSetDescSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildSetDescSub subObj =
            new TableCommand.TableBuildSetDescSub();
        final String expectedResult = buildSetDescSub + " " +
            TableCommand.DESC_FLAG_DESC;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildSetDescSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildSetDescSub subObj =
            new TableCommand.TableBuildSetDescSub();
        final String expectedResult = "Set description for the table.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* set-description sub: unknown arg, flag requiring arg and required args.*/
    @Test
    public void testTableBuildSetDescSubExecuteArgs()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        final ShellCommand currentCmd = shell.getCurrentCommand();

        final String[] cmds = new String[] {createSub, buildSetDescSub};
        /* test for unknown argument */
        doExecuteUnknownArg(currentCmd, cmds, shell);

        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {TableCommand.DESC_FLAG};
        doExecuteFlagRequiredArg(currentCmd, cmds, flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.DESC_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.DESC_FLAG, "this is simple table.");
        doExecuteRequiredArgs(currentCmd, cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);
    }

    /* show sub */
    @Test
    public void testTableBuildShowSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildShowSub subObj =
            new TableCommand.TableBuildShowSub();
        final String expectedResult = buildShowSub + " " +
            "[" + TableCommand.ORIGINAL_FLAG_DESC + "]";
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildShowSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildShowSub subObj =
            new TableCommand.TableBuildShowSub();
        final String expectedResult =
            "Display the table information, if building a table for " +
            "evolution, " + eolt + "use " +
            TableCommand.ORIGINAL_FLAG_DESC +
            " flag to show the original table information, the flag " +
            eolt + "will be ignored for building table for addition.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* show sub: Test for unknown arg */
    @Test
    public void testTableBuildShowSubUnknowArg()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "user"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);

        final String[] cmds = new String[] {createSub, buildShowSub};
        /* test for unknown argument */
        doExecuteUnknownArg(shell.getCurrentCommand(), cmds, shell);
    }

    /* show sub: Test TableBuildShowSub for execute command */
    @Test
    public void testTableBuildShowSubExecuteArgs()
            throws Exception {

        final String tableName = "user";
        final CommandShell shell = connectToStore();

        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
                createSub,
                TableCommand.TABLE_NAME_FLAG, tableName
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(), prepArgs);

        /* test for TableBuilder case with original flag */
        final String[] cmdsOriginal = new String[] { createSub, buildShowSub,
                TableCommand.ORIGINAL_FLAG_DESC };
        execShellCommand(shell, shell.getCurrentCommand(), cmdsOriginal);

        /* test for TableBuilder case */
        String[] cmds = new String[] { createSub, buildShowSub };
        execShellCommand(shell, shell.getCurrentCommand(), cmds);

        /* test block for RecordBuilder case */
        cmds = new String[] {
                createSub, buildAddRecordSub,
                TableCommand.FIELD_NAME_FLAG, "recordField"
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmds);

        /* ShellException (IllegalArgumentException wrapped) expected */
        cmds = new String[] { createSub, buildShowSub };
        try {
            execShellCommand(shell, shell.getCurrentCommand(), cmds);
            fail("ShellException expected");
        } catch (ShellException e) {
            assertTrue(true);
        }
        /* tableName-> add-record-field-> add-field -name name -type String */
        cmds = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, "name",
                TableCommand.TYPE_FLAG, "String"
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmds);
        /* test RecordBuilder show */
        final String[] showCmds = new String[] { createSub, buildShowSub };
        execShellCommand(shell, shell.getCurrentCommand(), showCmds);
        /* RecordBuilder-> exit */
        final String[] cmdsExit = new String[] { createSub, buildExitSub };
        execShellCommand(shell, shell.getCurrentCommand(), cmdsExit);

        /* test block for MapBuilder case */
        /* add-map-field -name mapField */
        String[] args = new String[] {
                createSub, buildAddMapSub,
                TableCommand.FIELD_NAME_FLAG, "mapField"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);
        /* add-field -name field1 -type Integer */
        args = new String[] {
                buildAddMapSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, "field1",
                TableCommand.TYPE_FLAG, "Integer"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);
        /* test for MapBuilder case */
        execShellCommand(shell, shell.getCurrentCommand(), showCmds);
        /* MapBuilder-> exit */
        execShellCommand(shell, shell.getCurrentCommand(), cmdsExit);

        /* test block for ArrayBuilder case */
        /* mytable-> add-array-field -name mapField */
        cmds = new String[] {
                createSub, buildAddArraySub,
                TableCommand.FIELD_NAME_FLAG, "arrayFeild",
                TableCommand.DESC_FLAG, "an array field"
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmds);
        /* add-field -name field1 -type Integer */
        args = new String[] {
                buildAddArraySub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, "field1",
                TableCommand.TYPE_FLAG, "Integer"
        };
        execShellCommand(shell, shell.getCurrentCommand(), args);
        /* test for ArrayBuilder case */
        execShellCommand(shell, shell.getCurrentCommand(), showCmds);
        /* ArrayBuilder-> exit */
        execShellCommand(shell, shell.getCurrentCommand(), cmdsExit);
    }

    /* show sub: test for TableEvolver */
    @Test
    public void testTableBuildShowSubTableEvolver()
            throws Exception {

        final String tableName = "user";
        final CommandShell shell = connectToStore();

        /* create user table */
        createTestTable(shell, tableName, null);

        /* start to build a table for evolve */
        final String[] evolvePrepArgs = new String[] {
                evolveSub,
                TableCommand.TABLE_NAME_FLAG, tableName
        };
        execShellCommand(shell, new TableCommand.TableEvolveSub(), evolvePrepArgs);

        /* TableEvolver original case */
        final String[] originalCmds = new String[] { evolveSub, buildShowSub,
                TableCommand.ORIGINAL_FLAG_DESC };
        execShellCommand(shell, shell.getCurrentCommand(), originalCmds);
        /* TableEvolver case */
        final String[] cmds = new String[] { evolveSub, buildShowSub };
        execShellCommand(shell, shell.getCurrentCommand(), cmds);
    }

    /* remove-field sub */
    @Test
    public void testTableBuildRemoveFieldSubGetCommandSyntax()
        throws Exception {

        final TableCommand.TableBuildRemoveFieldSub subObj =
            new TableCommand.TableBuildRemoveFieldSub();
        final String expectedResult = buildRemoveFieldSub + " " +
            TableCommand.FIELD_NAME_FLAG_DESC;
        assertEquals(expectedResult, subObj.getCommandSyntax());
    }

    @Test
    public void testTableBuildRemoveFieldSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildRemoveFieldSub subObj =
            new TableCommand.TableBuildRemoveFieldSub();
        final String expectedResult = "Remove a field.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    /* remove-field sub: unknown arg, flag requiring arg and required args. */
    @Test
    public void testTableBuildRemoveFieldSubExecuteArgs()
        throws Exception {

        final String tableName = "myTable";
        final CommandShell shell = connectToStore();
        createTestTable(shell, tableName, null);

        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            evolveSub,
            TableCommand.TABLE_NAME_FLAG, tableName
        };
        new TableCommand.TableEvolveSub().execute(prepArgs, shell);
        final ShellCommand currentCmd = shell.getCurrentCommand();

        final String[] cmds = new String[] {evolveSub, buildRemoveFieldSub};
        /* test for unknown argument */
        doExecuteUnknownArg(currentCmd, cmds, shell);

        /* test for flag requiring argument */
        final String[] flagRequiredArgs = {TableCommand.FIELD_NAME_FLAG};
        doExecuteFlagRequiredArg(currentCmd, cmds, flagRequiredArgs, shell);

        /* test for required arguments */
        final String[] requiredArgs = new String[] {
            TableCommand.FIELD_NAME_FLAG
        };
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put(TableCommand.FIELD_NAME_FLAG, "test");
        doExecuteRequiredArgs(currentCmd, cmds, argsMap,
                              requiredArgs, requiredArgs[0],
                              shell);
    }

    /* cancel sub */
    @Test
    public void testTableBuildCancelSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildCancelSub subObj =
            new TableCommand.TableBuildCancelSub();
        final String expectedResult =
            "Exit the building operation, canceling the table(or field)"+
            eolt + "under construction.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableBuildCancelSubExecuteBadArgsCount()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* kv-> table create -id user */
        final String[] createTableArgs = new String[] {
            createSub, TableCommand.TABLE_NAME_FLAG, "mytable"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                       createTableArgs);

        doExecuteBadArgCount(shell.getCurrentCommand(), buildCancelSub,
                             new String[]{createSub, buildCancelSub}, shell);
    }

    /* exit sub */
    @Test
    public void testTableBuildExitSubGetCommandDescription()
        throws Exception {

        final TableCommand.TableBuildExitSub subObj =
            new TableCommand.TableBuildExitSub();
        final String expectedResult =
            "Exit the building operation, saving the table(or field) " +
            eolt + "for addition(or evolution) to the store.";
        assertEquals(expectedResult, subObj.getCommandDescription());
    }

    @Test
    public void testTableBuildExitSubExecuteBadArgsCount()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* kv-> table create -id user */
        final String[] createTableArgs = new String[] {
            createSub, TableCommand.TABLE_NAME_FLAG, "mytable"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                       createTableArgs);

        doExecuteBadArgCount(shell.getCurrentCommand(), buildExitSub,
                             new String[]{createSub, buildExitSub}, shell);
    }

    /*
     * Build a table for addition:
     *  Field, primary-key, shard-key are necessary to be provided,
     *  missing any of them will get exception.
     * */
    @Test
    public void testExitBuildTableMissState()
        throws Exception {

        final CommandShell shell = connectToStore();

        /* kv-> table create -id user */
        final String[] createTableArgs = new String[] {
            createSub, TableCommand.TABLE_NAME_FLAG, "mytable"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                       createTableArgs);
        ShellCommand currentCmd = shell.getCurrentCommand();

        /* mytable-> exit */
        execShellCommand(shell, currentCmd,
                       new String[] {createSub, buildExitSub},
                       ERR_TABLE_MISS_STATE);

        /* mytable-> add-field -name id -type integer */
        final String[] addFieldArgs = new String[] {
            createSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "id",
            TableCommand.TYPE_FLAG, "String",
        };
        execShellCommand(shell, currentCmd, addFieldArgs);

        /* mytable-> exit */
        execShellCommand(shell, currentCmd,
                       new String[] {createSub, buildExitSub},
                       ERR_TABLE_MISS_STATE);

        /* mytable-> primary-key -field id */
        final String[] primaryKeyArgs = new String[] {
            createSub, buildPrimaryKeySub,
            TableCommand.FIELD_FLAG, "id"
        };
        execShellCommand(shell, currentCmd, primaryKeyArgs);

        /* mytable-> exit */
        execShellCommand(shell, currentCmd,
                       new String[] {createSub, buildExitSub});
    }

    /* exit sub: build a complex type and exits without add any field.*/
    @Test
    public void testExitBuildComplexTypeMissField()
        throws Exception {

        final CommandShell shell = connectToStore();

        /* kv-> table create -id user */
        final String[] createTableArgs = new String[] {
            createSub, TableCommand.TABLE_NAME_FLAG, "mytable"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                       createTableArgs);
        final ShellCommand currentCmd = shell.getCurrentCommand();

        /* mytable-> add-map-field -name mapField */
        String[] cmdArgs = new String[] {
            createSub, buildAddMapSub,
            TableCommand.FIELD_NAME_FLAG, "mapField",
            TableCommand.DESC_FLAG, "a map field",
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* mytable-> exit */
        execShellCommand(shell, shell.getCurrentCommand(),
                         new String[] {buildAddMapSub, buildExitSub},
                         ERR_MAP_MISS_FIELD);

        /* mytable-> add-field -type String */
        cmdArgs = new String[] {
            buildAddMapSub, buildAddFieldSub,
            TableCommand.TYPE_FLAG, "String",
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmdArgs);

        /* mytable-> exit */
        execShellCommand(shell, shell.getCurrentCommand(),
                         new String[] {buildAddMapSub, buildExitSub});

        /* mytable-> add-array-field -name mapField */
        cmdArgs = new String[] {
            createSub, buildAddArraySub,
            TableCommand.FIELD_NAME_FLAG, "arrayFeild",
            TableCommand.DESC_FLAG, "an array field",
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* mytable-> exit */
        execShellCommand(shell, shell.getCurrentCommand(),
                         new String[] {buildAddArraySub, buildExitSub},
                         ERR_ARRAY_MISS_FIELD);

        /* mytable-> add-field -name intField -type Integer */
        cmdArgs = new String[] {
            buildAddArraySub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "intField",
            TableCommand.TYPE_FLAG, "Integer",
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmdArgs);

        /* mytable-> exit */
        execShellCommand(shell, shell.getCurrentCommand(),
                         new String[] {buildAddArraySub, buildExitSub});

        /* mytable-> add-record-field -name recordField */
        cmdArgs = new String[] {
            createSub, buildAddRecordSub,
            TableCommand.FIELD_NAME_FLAG, "recordFeild",
            TableCommand.DESC_FLAG, "a record field",
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* mytable-> exit */
        execShellCommand(shell, shell.getCurrentCommand(),
                         new String[] {buildAddRecordSub, buildExitSub},
                         ERR_RECORD_MISS_FIELD);
    }

    /*
     * Primary key field: primary key field should be a valid field
     * with supported type: Integer, Long, Double, String, Enum.
     * */
    @Test
    public void testPrimaryKeyFieldInValidDatatype()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG, "mytable"
        };

        /* Binary */
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        execAddFieldSub(shell, "binaryField", "binary");
        /* Invalid type for primary key: Binary */
        execPrimaryKeySub(shell, "binaryField");
        execShardKeySub(shell, "binaryField");
        /* exit */
        execBuildExitSub(shell, createSub, ERR_PRIMARYKEY_INVALID_TYPE);
        execBuildCancelSub(shell, createSub);

        /* Map */
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        execAddMapFieldSub(shell, "mapField", "String");
        /* Invalid type for primary key: Map */
        execPrimaryKeySub(shell, "mapField");
        execShardKeySub(shell, "mapField");
        /* exit */
        execBuildExitSub(shell, createSub, ERR_PRIMARYKEY_INVALID_TYPE);
        execBuildCancelSub(shell, createSub);

        /* Array */
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        execAddArrayFieldSub(shell, "arrayField", "String");
        /* Invalid type for primary key: Array */
        execPrimaryKeySub(shell, "arrayField");
        execShardKeySub(shell, "arrayField");
        /* exit */
        execBuildExitSub(shell, createSub, ERR_PRIMARYKEY_INVALID_TYPE);
        execBuildCancelSub(shell, createSub);

        /* Record */
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        execAddRecordFieldSub(shell, "recordField", "String");
        /* Invalid type for primary key: Record */
        execPrimaryKeySub(shell, "recordField");
        execShardKeySub(shell, "recordField");
        /* exit */
        execBuildExitSub(shell, createSub, ERR_PRIMARYKEY_INVALID_TYPE);
        execBuildCancelSub(shell, createSub);

        /* Valid data types: Integer, Long, String, Double, Enum. */
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        /* Integer */
        execAddFieldSub(shell, "intField", "Integer");
        /* Long */
        execAddFieldSub(shell, "longField", "Long");
        /* String */
        execAddFieldSub(shell, "stringField", "String");
        /* Double */
        execAddFieldSub(shell, "doubleField", "Double");
        /* Boolean */
        execAddFieldSub(shell, "booleanField", "Boolean");
        /* Enum */
        execAddEnumFieldSub(shell, "enumField", "TYPE1,TYPE2,TYPE3");
        execPrimaryKeySub(shell, "intField", "longField", "stringField",
                          "doubleField", "booleanField", "enumField");
        execShardKeySub(shell, "intField", "longField", "stringField",
                        "doubleField", "booleanField", "enumField");
        /* exit */
        execBuildExitSub(shell, createSub);
    }

    /*
     * Primary key field: primary key field should be a defined field.
     * */
    @Test
    public void testPrimaryKeyFieldNotExists()
        throws Exception {

        final CommandShell shell = connectToStore();
        /* start to build a table for addition */
        final String[] prepArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG,
            "mytable"
        };
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);

        /* Integer */
        execAddFieldSub(shell, "intField", "Integer");
        /* FIELD_NOT_EIXSTS */
        execPrimaryKeySub(shell, ERR_PRIMARYKEY_FIELD_NOTEXISTS,
                          "FIELD_NOT_EXISTS");
        execShardKeySub(shell, ERR_PRIMARYKEY_FIELD_NOTEXISTS,
                        "FIELD_NOT_EXISTS");

        /* Set a valid field to primary-key and shard key. */
        execShellCommand(shell,  new TableCommand.TableCreateSub(), prepArgs);
        /* Integer */
        execAddFieldSub(shell, "intField", "Integer");
        execPrimaryKeySub(shell, "intField");
        execShardKeySub(shell, "intField");
        /* exit */
        execBuildExitSub(shell, createSub);
    }

    /*
     * Shard key field: shard key field should be a prefix subset
     * of primary key fields.
     * */
    public void testShardKeyPrefixSubsetOfPrimaryKey()
        throws Exception {

        final CommandShell shell = connectToStore();
        final String tableName = "mytest";

        /* kv-> table create -id tableName */
        String[] createTableArgs = new String[] {
            createSub, TableCommand.TABLE_NAME_FLAG, tableName
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                         createTableArgs);
        ShellCommand currentCmd = shell.getCurrentCommand();

        /* tableName-> add-field -name id -type integer */
        String[] cmdArgs = new String[] {
            createSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "id",
            TableCommand.TYPE_FLAG, "Integer"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> add-field -name name -type String */
        cmdArgs = new String[] {
            createSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "name",
            TableCommand.TYPE_FLAG, "String"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> primary-key -field id -field name*/
        cmdArgs = new String[] {
            createSub, buildPrimaryKeySub,
            TableCommand.FIELD_FLAG, "id",
            TableCommand.FIELD_FLAG, "name"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> shard-key -field FIELD_NOT_EXIST */
        cmdArgs = new String[] {
            createSub, buildShardKeySub,
            TableCommand.FIELD_FLAG, "FIELD_NOT_EXIST"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> shard-key -field name */
        cmdArgs = new String[] {
            createSub, buildShardKeySub,
            TableCommand.FIELD_FLAG, "name"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> exit */
        cmdArgs = new String[] {
            createSub, buildExitSub
        };
        execShellCommand(shell, currentCmd, cmdArgs,
                         ERR_SHARDKEY_SUBSET_PRIMARYKEY);
    }

    /* Test naming of table */
    @Test
    public void testNamingOfTable()
        throws Exception {

        final CommandShell shell = connectToStore();
        final String[] invalidName1 = {
            ".Email", "Us~Email", "Us&Email", "Us|Email", "Us-Email"
        };
        final String[] invalidName2 = {
            "_Email", "1Email"
        };
        String longName = "";
        for (int i = 0; i < 25; i++) {
        	longName = longName + "Email67890";
        }
        final String[] invalidName3 = {
        	longName + "Email67", longName + "Email67890"
        };

        final String[] validNames = {
            "Us_Email", "UsEmail_", "Us1Email", "UsEmail1"
        };
        /* Invalid table name:
         *  Contains non alphanumeric character not "-" or "_" */
        for (String id: invalidName1) {
            /* table id */
            String[] args = new String[] {
                createSub,
                TableCommand.TABLE_NAME_FLAG, id
            };
            execShellCommand(shell, new TableCommand.TableCreateSub(), args,
                ERR_TABLE_NAME_ALPHANUMERIC_ONLY);
        }

        /* Invalid table name: Start with non alphabetic character. */
        for (String id: invalidName2) {
            /* table id */
            String[] args = new String[] {
                createSub,
                TableCommand.TABLE_NAME_FLAG, id
            };
            execShellCommand(shell, new TableCommand.TableCreateSub(), args,
                ERR_TABLE_NAME_INVALID_STARTWITH);
        }

        /* Invalid table name: length > 256. */
        for (String id: invalidName3) {
            /* table id */
            String[] args = new String[] {
                createSub,
                TableCommand.TABLE_NAME_FLAG, id
            };
            execShellCommand(shell, new TableCommand.TableCreateSub(), args,
                ERR_TABLE_NAME_INVALID_LENGTH);
        }

        /* Valid table names. */
        for (String id: validNames) {
            /* table id */
            String[] args = new String[] {
                createSub,
                TableCommand.TABLE_NAME_FLAG, id
            };
            execShellCommand(shell, new TableCommand.TableCreateSub(), args);
            execBuildCancelSub(shell, createSub);
        }
    }

    /*
     * Build a nested table for addition: parent table should be existed.
     */
    public void testParentTableNotExisted()
        throws Exception {

        final CommandShell shell = connectToStore();

        /* kv-> table create -id mytable */
        final String parentTable = "mytable";
        final String[] createTableArgs = new String[] {
            createSub,
            TableCommand.TABLE_NAME_FLAG,
            parentTable + ".table1"
        };
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                         createTableArgs, ERR_TABLE_NOT_EXISTS);

        /* create table "parentTable1" */
        createTestTable(shell, parentTable, null);
        execShellCommand(shell, new TableCommand.TableCreateSub(),
                         createTableArgs);
    }

    @Test
    public void testEvolveTableAddRemoveField()
        throws Exception {

        final String tableName = "mytable";
        final CommandShell shell = connectToStore();

        /* create table: mytable */
        createTestTable(shell, tableName, null);

        String[] cmdArgs = new String[] {
            evolveSub,
            TableCommand.TABLE_NAME_FLAG, tableName
        };
        /* table evolve -table mytable */
        execShellCommand(shell, new TableCommand.TableEvolveSub(), cmdArgs);

        /* add-field -name id */
        execAddFieldSub(shell, "id", "Integer",
                        ERR_EVOLVETABLE_FIELD_ALREADY_EXISTS);

        /* remove-field -field FIELD_NOT_EXISTS */
        cmdArgs = new String[] {
            evolveSub, buildRemoveFieldSub,
            TableCommand.FIELD_NAME_FLAG, "FIELD_NOT_EXISTS"
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmdArgs,
                         ERR_EVOLVETABLE_FIELD_NOT_EXISTS);

        /* remove-field -field id (id - primary key field)*/
        cmdArgs = new String[] {
            evolveSub, buildRemoveFieldSub,
            TableCommand.FIELD_NAME_FLAG, "id"
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmdArgs,
                         ERR_EVOLVETABLE_CANNOT_REMOVE_KEYFIELD);

    }

    private CommandShell connectToStore()
        throws Exception {

        final CommandShell shell = new CommandShell(System.in, System.out);

        shell.connectAdmin(
            "localhost", createStore.getRegistryPort(),
            createStore.getDefaultUserName(),
            createStore.getDefaultUserLoginPath());
        return shell;
    }

    private void removeAllTables()
        throws Exception {

        removeTables(store.getTableAPI().getTables());
        removeAllData();
    }

    private void removeAllData() {
        Iterator<Key> iter = store.storeKeysIterator
            (Direction.UNORDERED, 10);
        try {
            while (iter.hasNext()) {
                store.delete(iter.next());
            }
        } catch (Exception e) {
            System.err.println("Exception cleaning store: " + e);
        }
    }

    private void removeTables(Map<String, Table> tables)
        throws Exception {

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getValue().getChildTables().size() > 0) {
                removeTables(entry.getValue().getChildTables());
            }
            TableImpl table = (TableImpl)entry.getValue();
            if (table.isSystemTable()) {
                continue;
            }

            for (Map.Entry<String, Index> index : table.getIndexes().entrySet()) {
                removeIndex(table, index.getKey(), true);
            }
            removeTable(table.getInternalNamespace(), table.getFullName(),
                true);
        }
    }

    private void removeTable(String namespace,
                             String tableName,
                             boolean shouldSucceed)
        throws Exception {

        TableTestUtils.removeTable(namespace, tableName, shouldSucceed, cs,
                                   store);
    }

    private void removeIndex(TableImpl table, String indexName,
                            boolean shouldSucceed)
        throws Exception {

        TableTestUtils.removeIndex(table.getInternalNamespace(),
                                   table.getFullName(),
                                   indexName,
                                   shouldSucceed, cs, store);
    }

    private void createTestTable(final Shell shell,
                                 final String tableName,
                                 final String parentTable)
        throws Exception {

        List<String> createTableArgs = new ArrayList<String>();
        createTableArgs.add(createSub);
        createTableArgs.add(TableCommand.TABLE_NAME_FLAG);
        if (parentTable != null) {
            createTableArgs.add(parentTable + "." + tableName);
        } else {
            createTableArgs.add(tableName);
        }
        execShellCommand(shell, new TableCommand.TableCreateSub(),
            createTableArgs.toArray(new String[createTableArgs.size()]));
        ShellCommand currentCmd = shell.getCurrentCommand();

        /* tableName-> add-field -name id -type integer */
        String[] cmdArgs = new String[] {
            createSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "id",
            TableCommand.TYPE_FLAG, "Integer"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> add-field -name name -type String */
        cmdArgs = new String[] {
            createSub, buildAddFieldSub,
            TableCommand.FIELD_NAME_FLAG, "name",
            TableCommand.TYPE_FLAG, "String"
        };
        execShellCommand(shell, currentCmd, cmdArgs);


        /* tableName-> primary-key -field id*/
        cmdArgs = new String[] {
            createSub, buildPrimaryKeySub,
            TableCommand.FIELD_FLAG, "id"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> shard-key -field id*/
        cmdArgs = new String[] {
            createSub, buildShardKeySub,
            TableCommand.FIELD_FLAG, "id"
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* tableName-> exit */
        cmdArgs = new String[] {
            createSub, buildExitSub
        };
        execShellCommand(shell, currentCmd, cmdArgs);

        /* kv-> plan add-table -id tableName */
        cmdArgs = new String[] {
            "add-table",
            PlanCommand.AddTableSub.TABLE_NAME_FLAG, tableName,
            "-wait"
        };
        execShellCommand(shell, new PlanCommand.AddTableSub(), cmdArgs);
    }


    private void execBuildCancelSub(Shell shell, String cmd)
        throws Exception {

        ShellCommand currentCmd = shell.getCurrentCommand();
        String[] cmdArgs = new String[] {
            cmd, buildCancelSub
        };
        execShellCommand(shell, currentCmd, cmdArgs);
    }

    private void execBuildExitSub(Shell shell, String cmd)
        throws Exception {

        execBuildExitSub(shell, cmd, null);
    }

    private void execBuildExitSub(Shell shell, String cmd, Integer iErrMsg)
        throws Exception {

        ShellCommand currentCmd = shell.getCurrentCommand();
        String[] cmdArgs = new String[] {
            cmd, buildExitSub
        };
        execShellCommand(shell, currentCmd, cmdArgs, iErrMsg);
    }

    private void execPrimaryKeySub(Shell shell, String ... fields)
        throws Exception {

        execPrimaryKeySub(shell, null, fields);
    }

    private void execPrimaryKeySub(Shell shell, Integer iErrMsg,
                                   String ... fields)
        throws Exception {

        ShellCommand currentCmd = shell.getCurrentCommand();
        List<String> cmdArgs = new ArrayList<String>();
        cmdArgs.add(createSub);
        cmdArgs.add(buildPrimaryKeySub);
        for (String field: fields) {
            cmdArgs.add(TableCommand.FIELD_FLAG);
            cmdArgs.add(field);
        }
        execShellCommand(shell, currentCmd,
                         cmdArgs.toArray(new String[cmdArgs.size()]),
                         iErrMsg);
    }

    private void execShardKeySub(Shell shell, String... field)
        throws Exception {

        execShardKeySub(shell, null, field);
    }

    private void execShardKeySub(Shell shell, Integer iErrMsg,
                                 String ... fields)
        throws Exception {

        ShellCommand currentCmd = shell.getCurrentCommand();

        List<String> cmdArgs = new ArrayList<String>();
        cmdArgs.add(createSub);
        cmdArgs.add(buildShardKeySub);
        for (String field: fields) {
            cmdArgs.add(TableCommand.FIELD_FLAG);
            cmdArgs.add(field);
        }
        execShellCommand(shell, currentCmd,
                         cmdArgs.toArray(new String[cmdArgs.size()]),
                         iErrMsg);
    }


    /* execute add-map-field command */
    private void execAddMapFieldSub(Shell shell, String name, String type)
        throws Exception {

        execAddMapFieldSub(shell, name, type, null);
    }

    private void execAddMapFieldSub(Shell shell, String name,
                                    String type, Integer iErrMsg)
        throws Exception {

        execAddComplexFieldSub(shell, "map", name, type, iErrMsg);
    }

    /* execute add-array-field command */
    private void execAddArrayFieldSub(Shell shell, String name, String type)
        throws Exception {

        execAddArrayFieldSub(shell, name, type, null);
    }

    private void execAddArrayFieldSub(Shell shell, String name,
                                     String type, Integer iErrMsg)
        throws Exception {

        execAddComplexFieldSub(shell, "array", name, type, iErrMsg);
    }

    /* execute add-record-field command */
    private void execAddRecordFieldSub(Shell shell, String name, String type)
        throws Exception {

        execAddRecordFieldSub(shell, name, type, null);
    }

    private void execAddRecordFieldSub(Shell shell, String name,
                                       String type, Integer iErrMsg)
        throws Exception {

        execAddComplexFieldSub(shell, "record", name, type, iErrMsg);
    }

    private void execAddComplexFieldSub(Shell shell, String type,
                                        String name, String InnerFieldType,
                                        Integer iErrMsg)
        throws Exception {

        ShellCommand currentCmd = shell.getCurrentCommand();
        String subCmd = null;
        if (type.toLowerCase().equals("map")) {
            subCmd = buildAddMapSub;
        } else if (type.toLowerCase().equals("array")) {
            subCmd = buildAddArraySub;
        } else if (type.toLowerCase().equals("record")) {
            subCmd = buildAddRecordSub;
        } else {
            throw new Exception("Unsupported complex type: " + type);
        }

        String[] cmdArgs = new String[] {
            createSub, subCmd,
            TableCommand.FIELD_NAME_FLAG, name
        };
        /* add-map-field */
        execShellCommand(shell, currentCmd, cmdArgs, iErrMsg);
        /* add-field */
        execAddFieldSub(shell, name, InnerFieldType, iErrMsg);
        /* exit */
        cmdArgs = new String[] {
            subCmd, buildExitSub
        };
        execShellCommand(shell, shell.getCurrentCommand(), cmdArgs, iErrMsg);
    }

    /* execute add-field command */
    private void execAddFieldSub(Shell shell, String name, String type)
        throws Exception {

        execAddFieldSub(shell, name, type, null, null);
    }

    private void execAddFieldSub(Shell shell, String name,
                                 String type, Integer iErrMsg)
        throws Exception {

        execAddFieldSub(shell, name, type, null, iErrMsg);
    }

    /* execute add-field -type enum */
    private void execAddEnumFieldSub(final Shell shell,
                                     final String name,
                                     final String values)
        throws Exception {

        execAddFieldSub(shell, name, "enum", values, null);
    }


    private void execAddFieldSub(final Shell shell,
                                 final String name,
                                 final String type,
                                 final String values,
                                 final Integer iErrMsg)
        throws Exception {

        ShellCommand currentCmd = shell.getCurrentCommand();
        String[] cmdArgs = null;
        if (type.toLowerCase().endsWith("enum")) {
            cmdArgs = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, name,
                TableCommand.TYPE_FLAG, type,
                TableCommand.ENUM_VALUE_FLAG, values
            };
        } else {
            cmdArgs = new String[] {
                createSub, buildAddFieldSub,
                TableCommand.FIELD_NAME_FLAG, name,
                TableCommand.TYPE_FLAG, type
            };
        }
        execShellCommand(shell, currentCmd, cmdArgs, iErrMsg);
    }

    private void execShellCommand(final Shell shell,
                                  final ShellCommand shellCommand,
                                  final String[] args)
        throws Exception {
        execShellCommand(shell, shellCommand, args, null);
    }

    private void execShellCommand(final Shell shell,
                                  final ShellCommand shellCommand,
                                  final String[] args,
                                  final Integer iErrMsg)
        throws Exception {
        /*String errMsg = null;*/
        boolean expectedOK = true;
        if (iErrMsg != null) {
            expectedOK = false;
            /*if (verifyExceptionMsg) {
                errMsg = exceptionMsgs[iErrMsg];
            }*/
        }
        try {
            shellCommand.execute(args, shell);
            if (!expectedOK) {
                fail("Expected to get ShellException but OK.");
            }
        } catch (ShellException se) {
            if (expectedOK) {
                throw se;
            } /*else if (errMsg != null) {
                if (!se.getMessage().startsWith(errMsg)) {
                    System.out.println("expected: " + errMsg);
                    System.out.println("but actual: " + se.getMessage());
                }
                assertTrue(se.getMessage().startsWith(errMsg));
            }*/
        }
    }

    private void doExecuteUnknownArg(final ShellCommand subObj,
                                     final String cmd)
        throws Exception {

        doExecuteUnknownArg(subObj, new String[]{cmd}, null);
    }

    private void doExecuteUnknownArg(final ShellCommand subObj,
                                     final String[] cmds,
                                     final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException("Unknown argument: " + arg, subObj);

        String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        if (cmdShell == null) {
            cmdShell  = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but " + arg + " was known");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private void doExecuteBadArgCount(final ShellCommand subObj,
                                      final String cmd,
                                      final String[] cmds,
                                      final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final String arg = "UNKNOWN_ARG";
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Incorrect number of arguments for command: " + cmd,
                subObj);

        String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        if (cmdShell == null) {
            cmdShell  = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but correct number of args.");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private void doExecuteFlagRequiredArg(final ShellCommand subObj,
                                          final String cmd,
                                          final String[] args)
        throws Exception {

        doExecuteFlagRequiredArg(subObj, new String[]{cmd}, args, null);
    }

    private void doExecuteFlagRequiredArg(final ShellCommand subObj,
                                          final String[] cmds,
                                          final String[] args,
                                          final Shell shell)
        throws Exception {

        for (String arg: args) {
            doExecuteFlagRequiredArg(subObj, cmds, arg, shell);
        }
    }

    private void doExecuteFlagRequiredArg(final ShellCommand subObj,
                                          final String[] cmds,
                                          final String arg,
                                          final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final ShellUsageException expectedException =
            new ShellUsageException("Flag " + arg + " requires an argument",
                                    subObj);
        final String[] args = new String[cmds.length + 1];
        System.arraycopy(cmds, 0, args, 0, cmds.length);
        args[args.length - 1] = arg;

        if (cmdShell == null) {
            cmdShell = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but flag " + arg + " exists");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private void doExecuteRequiredArgs(final ShellCommand subObj,
                                       final String cmd,
                                       final Map<String, String> argsMap,
                                       final String[] requiredArgs,
                                       final String requiredArgName)
        throws Exception {

        doExecuteRequiredArgs(subObj, new String[]{cmd}, argsMap,
                              requiredArgs, requiredArgName, null);
    }

    private void doExecuteRequiredArgs(final ShellCommand subObj,
                                       final String[] cmds,
                                       final Map<String, String> argsMap,
                                       final String[] requiredArgs,
                                       final String requiredArgName,
                                       final Shell shell)
        throws Exception {

        Shell cmdShell = shell;
        final ShellUsageException expectedException =
            new ShellUsageException(
                "Missing required argument (" + requiredArgName +
                ") for command: " + cmds[cmds.length - 1], subObj);

        final String[] args = getArgsArrayMinusRequiredArgs(cmds, argsMap,
                                                            requiredArgs);
        if (shell == null) {
            cmdShell = new CommandShell(System.in, System.out);
        }
        /* Run the test and verify the results. */
        try {
            subObj.execute(args, cmdShell);
            fail("ShellUsageException expected, but required arg input");
        } catch (ShellUsageException e) {
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private String[] getArgsArrayMinusRequiredArgs(
                         final String[] cmds,
                         final Map<String, String> argsMap,
                         final String[] requiredArgs) {

        final List<String> requiredArgsList =
            (requiredArgs == null)? null:Arrays.asList(requiredArgs);
        final List<String> argsList = new ArrayList<String>();
        argsList.addAll(Arrays.asList(cmds));
        for (Map.Entry<String, String> argsPair : argsMap.entrySet()) {
            final String argName = argsPair.getKey();
            final String argVal = argsPair.getValue();
            if (requiredArgsList != null &&
                requiredArgsList.contains(argName)) {
                continue;
            }
            if (argName.equals(argVal)) {
                argsList.add(argName);
                continue;
            }
            argsList.add(argName);
            argsList.add(argVal);
        }
        return argsList.toArray(new String[argsList.size()]);
    }
}
