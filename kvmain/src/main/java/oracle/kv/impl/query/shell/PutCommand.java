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

import static oracle.kv.impl.util.CommandParser.FILE_FLAG;
import static oracle.kv.impl.util.CommandParser.JSON_FLAG;
import static oracle.kv.impl.util.CommandParser.TABLE_FLAG;

import static oracle.kv.util.shell.LoadTableUtils.loadCSVFromFile;
import static oracle.kv.util.shell.LoadTableUtils.loadJsonFromFile;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.Version;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/*
 * Put command
 */
public class PutCommand extends ShellCommand {
    final static String NAME = "put";

    final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
    final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";
    final static String FILE_FLAG_DESC = FILE_FLAG + " <file>";

    final static String TYPE_JSON = "JSON";
    final static String TYPE_CSV = "CSV";

    final static String SYNTAX = "put " + TABLE_FLAG_DESC + " " +
        CommandParser.optional(JSON_FLAG_DESC + " | " + FILE_FLAG_DESC + " " +
            CommandParser.optional(TYPE_JSON + " | " + TYPE_CSV));

    final static String DESCRIPTION =
        "Put row(s) into the named table." + eolt +
        "The row can be either constructed from JSON string or loaded" + eolt +
        "from a file that contains records in JSON or CSV format, if" + eolt +
        "format is not specified JSON is assumed." + eolt +
        TABLE_FLAG + " is used to specify the name of target table." + eolt +
        JSON_FLAG + " is used to specify a JSON string." + eolt +
        FILE_FLAG + " is used to specify the file that contains records to" +
        eolt + "load.";

    PutCommand() {
        super(NAME, 3);
        overrideJsonFlag = true;
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        Shell.checkHelp(args, this);

        String tableName = null;
        String jsonString = null;
        String fileName = null;
        boolean isJsonFile = true;

        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (TABLE_FLAG.equals(arg)) {
                tableName = Shell.nextArg(args, i++, this);
            } else if (JSON_FLAG.equals(arg)) {
                jsonString = Shell.nextArg(args, i++, this);
            } else if (FILE_FLAG.equals(arg)) {
                fileName = Shell.nextArg(args, i++, this);
                if (!new File(fileName).exists()) {
                    throw new ShellArgumentException("File not found: " +
                                                     fileName);
                }
                if (i + 1 < args.length) {
                   arg = Shell.nextArg(args, i, this);
                   if (arg.equalsIgnoreCase(TYPE_CSV)) {
                       isJsonFile = false;
                       i++;
                   } else if (arg.equalsIgnoreCase(TYPE_JSON)) {
                       isJsonFile = true;
                       i++;
                   }
                }
            } else {
                shell.unknownArgument(arg, this);
            }
        }

        if (tableName == null) {
            shell.requiredArg(TABLE_FLAG, this);
        }
        if (jsonString == null && fileName == null) {
            shell.requiredArg(JSON_FLAG + " | " + FILE_FLAG, this);
        }

        final OnqlShell sqlShell = ((OnqlShell)shell);
        final TableAPI tableImpl = sqlShell.getStore().getTableAPI();

        Table table = tableImpl.getTable(sqlShell.getNamespace(), tableName);
        if (table == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Table not found: ").append(tableName);
            if (sqlShell.getNamespace() != null) {
                sb.append(" in namespace: ").append(sqlShell.getNamespace());
            }
            throw new ShellArgumentException(sb.toString());
        }

        final WriteOptions wro =
            new WriteOptions(sqlShell.getStoreDurability(),
                             sqlShell.getRequestTimeout(),
                             TimeUnit.MILLISECONDS);

        if (jsonString != null) {
            return putRow(tableImpl, table, jsonString, wro);
        }
        return loadFromFile(tableImpl, table, fileName, wro, isJsonFile);
    }

    /* Construct a row from JSON and put it to the target table */
    private String putRow(TableAPI tableImpl, Table table,
                          String jsonString, WriteOptions wro)
        throws ShellException {

        try {
            final Row row = table.createRowFromJson(jsonString, false);
            ReturnRow rr = table.createReturnRow(Choice.VALUE);
            Version v = tableImpl.put(row, rr, wro);
            return (v == null) ? "Put failed." :
                                 "Put successful, row " +
                                     (rr.isEmpty() ? "inserted." : "updated.");
        } catch (IllegalArgumentException ioe) {
            throw new ShellException(ioe.getMessage(), ioe);
        } catch (RuntimeException rte) {
            throw new ShellException(rte.getMessage(), rte);
        }
    }

    /* Load records from the specified file to the target table */
    private String loadFromFile(final TableAPI tableImpl,
                                final Table table,
                                final String fileName,
                                final WriteOptions wro,
                                final boolean isJson)
        throws ShellException {

        Map<String, Long> results = null;
        try {
            if (isJson) {
                results = loadJsonFromFile(tableImpl, table, fileName, wro);
            } else {
                results = loadCSVFromFile(tableImpl, table, fileName, wro);
            }
        } catch (IOException ioe) {
            throw new ShellException(ioe.getMessage(), ioe);
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        } catch (RuntimeException rte) {
            throw new ShellException(rte.getMessage(), rte);
        }

        Long num = results.get(table.getFullName());
        return String.format("Loaded %d %s to table %s",
                             num, (num > 1 ? "rows" : "row"),
                             table.getFullName());
    }

    @Override
    public String getCommandSyntax() {
        return SYNTAX;
    }

    @Override
    public String getCommandDescription() {
        return DESCRIPTION;
    }
}
