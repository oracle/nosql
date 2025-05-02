/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.query.shell;

import static oracle.kv.impl.util.CommandParser.FILE_FLAG;
import static oracle.kv.impl.util.CommandParser.JSON_FLAG;
import static oracle.kv.impl.util.CommandParser.TABLE_FLAG;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.util.shell.Shell;

import org.junit.Test;

public class PutCommandTest extends ShellTestBase {

    final static String TABLE_USERS = "users";
    final static String CREATE_TABLE_USERS =
        "CREATE TABLE IF NOT EXISTS " + TABLE_USERS +
        "(" +
        "  id INTEGER, " +
        "  name STRING, " +
        "  age INTEGER, " +
        "  primary key (id)" +
        ")";

    @Test
    public void testGetCommandDescription()
        throws Exception {

        final PutCommand putObj = new PutCommand();
        final String expectedResult = PutCommand.DESCRIPTION;
        assertEquals(expectedResult, putObj.getCommandDescription());
    }

    @Test
    public void testGetCommandSyntax()
        throws Exception {

        final PutCommand putObj = new PutCommand();
        final String expectedResult = PutCommand.SYNTAX;
        assertEquals(expectedResult, putObj.getCommandSyntax());
    }

    @Test
    public void testExecuteInvalidArgument()
        throws Exception {

        final Shell shell = getTestShell();
        executeDDL(CREATE_TABLE_USERS);

        /* Required flag: -table */
        final PutCommand cmdObj = new PutCommand();
        String[] args = {PutCommand.NAME};
        runWithRequiredFlag(shell, cmdObj, args, TABLE_FLAG);

        /* Required flag: -json|-file */
        args = new String[] {PutCommand.NAME, TABLE_FLAG, TABLE_USERS};
        runWithRequiredFlag(shell, cmdObj, args, JSON_FLAG + " | " + FILE_FLAG);

        /* Table not found. */
        args = new String[]{PutCommand.NAME, TABLE_FLAG, "invalidTable",
                            JSON_FLAG, "{\"id\":1}"};
        runWithInvalidArgument(shell, cmdObj, args);

        /* File not found. */
        args = new String[]{PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                            FILE_FLAG, "invalidFile"};
        runWithInvalidArgument(shell, cmdObj, args);
    }

    @Test
    public void testPutRow() {

        executeDDL(CREATE_TABLE_USERS);
        Table table = getTable(TABLE_USERS);

        final Shell shell = getTestShell();
        final PutCommand cmdObj = new PutCommand();

        Row row = createUserRow(table, 0);

        String jsonString = row.toJsonString(false);
        String[] args = new String[] {PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                                      JSON_FLAG, jsonString};
        runCommand(shell, cmdObj, args, true, "Put successful, row inserted");
        runCommand(shell, cmdObj, args, true, "Put successful, row updated");
    }

    @Test
    public void testPutFromFile() {
        final String dataFile = getTempFileName("users", ".data");
        final Shell shell = getTestShell();
        final PutCommand cmdObj = new PutCommand();

        executeDDL(CREATE_TABLE_USERS);
        Table table = tableAPI.getTable(TABLE_USERS);

        /* Load JSON records from a file */
        writeRecordsToFile(TABLE_USERS, dataFile, 0, 30, true);
        String[] args = new String[] {PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                                      FILE_FLAG, dataFile};
        runCommand(shell, cmdObj, args, true, "Loaded 30 rows to table users");
        assertEquals(30, countRecords(table));

        writeRecordsToFile(TABLE_USERS, dataFile, 30, 30, true);
        args = new String[] {PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                             FILE_FLAG, dataFile, "JSON"};
        runCommand(shell, cmdObj, args, true, "Loaded 30 rows to table users");
        assertEquals(60, countRecords(table));

        /* Load CSV records from a file */
        writeRecordsToFile(TABLE_USERS, dataFile, 60, 30, false);
        args = new String[] {PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                             FILE_FLAG, dataFile, "CSV"};
        runCommand(shell, cmdObj, args, true, "Loaded 30 rows to table users");
        assertEquals(90, countRecords(table));
    }

    @Test
    public void testExecuteFailed() {
        final Shell shell = getTestShell();
        executeDDL(CREATE_TABLE_USERS);

        final PutCommand cmdObj = new PutCommand();

        /* Invalid JSON. */
        String[] args = new String[]{PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                                     JSON_FLAG, "{\"int\":1}"};
        runCommand(shell, cmdObj, args, false);

        /* Invalid JSONs in file. */
        final String dataFile = getTempFileName("users", ".data");
        writeRecordsToFile(TABLE_USERS, dataFile, 0, 2, false);
        args = new String[] {PutCommand.NAME, TABLE_FLAG, TABLE_USERS,
                             FILE_FLAG, dataFile};
        runCommand(shell, cmdObj, args, false);
    }

    private Row createUserRow(Table table, int id) {
        Row row = table.createRow();
        row.put("id", id);
        if (id % 10 == 9) {
            row.putNull("name");
            row.putNull("age");
            return row;
        }
        row.put("name", "name" + id);
        row.put("age", 20 + (id % 50));
        return row;
    }

    private void writeRecordsToFile(final String tblName,
                                    final String outFile,
                                    final int idFrom,
                                    final int nRows,
                                    final boolean isJson) {

        final Table table = getTable(tblName);
        final File file = new File(outFile);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file, false);
            String line;
            for (int i = idFrom; i < idFrom + nRows; i++) {
                final Row row = createUserRow(table, i);
                if (isJson) {
                    line = row.toJsonString(false) + "\n";
                } else {
                    line = rowToCSVString(row);
                }
                fos.write(line.getBytes());
            }
        } catch (IOException e) {
            fail("Failed to write to file " + file.getAbsolutePath());
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private String rowToCSVString(Row row) {

        StringBuilder sb = new StringBuilder();
        sb.append(row.get("id").asInteger().get());
        sb.append(",");

        FieldValue val = row.get("name");
        if (!val.isNull()) {
            sb.append("\"");
            sb.append(val.asString().get());
            sb.append("\"");
        }
        sb.append(",");

        val = row.get("age");
        if (!val.isNull()) {
            sb.append(val.asInteger().get());
        }
        sb.append('\n');
        return sb.toString();
    }

    private int countRecords(Table table) {
        TableIterator<PrimaryKey> iter =
            tableAPI.tableKeysIterator(table.createPrimaryKey(), null, null);

        int count = 0;
        while(iter.hasNext()) {
            iter.next();
            count++;
        }
        return count;
    }
}
