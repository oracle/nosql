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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.util.CommandParser;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.shell.LoadTableUtils;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

import static oracle.kv.impl.util.CommandParser.FILE_FLAG;
import static oracle.kv.impl.util.CommandParser.TABLE_FLAG;

/* Import command */
public class ImportCommand extends ShellCommand {

    private final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
    private final static String FILE_FLAG_DESC = FILE_FLAG + " <name>";

    private final static String TYPE_JSON = "JSON";
    private final static String TYPE_CSV = "CSV";

    final static String NAME = "import";

    final static String SYNTAX = NAME + " " +
        CommandParser.optional(TABLE_FLAG_DESC) + " " + FILE_FLAG_DESC + " " +
        CommandParser.optional(TYPE_JSON + " | " + TYPE_CSV);

    final static String DESCRIPTION =
        "Imports records from the specified file into the named table." + eolt +
        "The records can be in either JSON or CSV format. If format is" + eolt +
        "not specified JSON is assumed." + eolt +
        "-table is used to specify the name of table to which the records" +
        eolt + "are loaded, the alternative way to specify the table is to " +
        "add " + eolt + "the table specification \"Table: <name>\" before " +
        "its records in " + eolt + "the file, e.g. A file contains the " +
        "records of 2 tables \"users\"" + eolt + "and \"email\":" + eolt +
        Shell.tab +
        "Table: users" + eolt + Shell.tab +
        "<records of users>" + eolt + Shell.tab +
        "..." + eolt + Shell.tab +
        "Table: emails" + eolt + Shell.tab +
        "<records of emails>" + eolt + Shell.tab +
        "..." ;

    public ImportCommand() {
        super(NAME, 3);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        Shell.checkHelp(args, this);

        String tableName = null;
        String fileName = null;
        Boolean isJson = null;
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (TABLE_FLAG.equals(arg)) {
                tableName = Shell.nextArg(args, i++, this);
            } else if (FILE_FLAG.equals(arg)) {
            	fileName = Shell.nextArg(args, i++, this);
            } else if (TYPE_JSON.equals(arg.toUpperCase())) {
                isJson = true;
            } else if (TYPE_CSV.equals(arg.toUpperCase())) {
                isJson = false;
            } else {
                shell.unknownArgument(arg, this);
            }
        }

        if (fileName == null ) {
            shell.requiredArg(FILE_FLAG, this);
        }
        if (isJson == null) {
            isJson = true;
        }

        final OnqlShell sqlShell = (OnqlShell)shell;
        final TableAPI tableImpl = sqlShell.getStore().getTableAPI();
        Table table = null;
        if (tableName != null) {
            table = tableImpl.getTable(tableName);
            if (table == null) {
                throw new ShellArgumentException("Table not found: " +
                                                 tableName);
            }
        }

        if (!new File(fileName).exists()) {
            throw new ShellArgumentException("File not found: " + fileName);
        }

        final WriteOptions wro = new WriteOptions(sqlShell.getStoreDurability(),
                                                  sqlShell.getRequestTimeout(),
                                                  TimeUnit.MILLISECONDS);
        Map<String, Long> results = null;
        try {
            if (isJson) {
                results = LoadTableUtils.loadJsonFromFile(tableImpl, table,
                                                          fileName, wro);
            } else {
                results = LoadTableUtils.loadCSVFromFile(tableImpl, table,
                                                         fileName, wro);
            }
        } catch (IOException ioe) {
            throw new ShellException(ioe.getMessage(), ioe);
        } catch (RuntimeException rte) {
            throw new ShellException(rte.getMessage(), rte);
        }
        return getRetString(results);
    }

    private String getRetString(Map<String, Long> results) {
        if (results.isEmpty()) {
            return "Loaded 0 rows";
        }

        final StringBuilder sb = new StringBuilder();
        for (Entry<String, Long> entry : results.entrySet()) {
            final String tableName = entry.getKey();
            final long count = entry.getValue();
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(count);
            sb.append(" rows to ");
            sb.append(tableName);
        }
        sb.append(".");
        return "Loaded " + sb.toString();
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
