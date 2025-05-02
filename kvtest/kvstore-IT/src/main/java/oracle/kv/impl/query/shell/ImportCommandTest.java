/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.query.shell;

import static oracle.kv.impl.util.CommandParser.FILE_FLAG;
import static oracle.kv.impl.util.CommandParser.TABLE_FLAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.util.shell.Shell;

import org.junit.Test;

/* Unit tests for import command */
public class ImportCommandTest extends ShellTestBase {

    final static String TABLE_SIMPLE = "simple";
    final static String CREATE_TABLE_SIMPLE =
        "CREATE TABLE IF NOT EXISTS " + TABLE_SIMPLE +
        "(" +
        "  id INTEGER, " +
        "  fint INTEGER, " +
        "  flong LONG, " +
        "  fstring STRING," +
        "  fdouble DOUBLE," +
        "  ffloat FLOAT," +
        "  fboolean BOOLEAN," +
        "  fenum ENUM(value1, value2, value3)," +
        "  fbinary BINARY," +
        "  ffixed BINARY(10)," +
        "  primary key (id)" +
        ")";

    final static String TABLE_COMPLEX = "complex";
    final static String CREATE_TABLE_COMPLEX =
        "CREATE TABLE IF NOT EXISTS " + TABLE_COMPLEX +
        "(" +
        "  id INTEGER, " +
        "  farray ARRAY(STRING), " +
        "  fmap MAP(INTEGER), " +
        "  frecord RECORD(frid INTEGER, frstring STRING), "+
        "  primary key (id)" +
        ")";

    final static String DROP_TABLE_SIMPLE = "DROP TABLE IF EXISTS simple";
    final static String DROP_TABLE_COMPLEX = "DROP TABLE IF EXISTS complex";

    private static enum Format {JSON, JSON_PRETTY, CSV}

    @Test
    public void testGetCommandDescription()
        throws Exception {

        final ImportCommand importObj = new ImportCommand();
        final String expectedResult = ImportCommand.DESCRIPTION;
        assertEquals(expectedResult, importObj.getCommandDescription());
    }

    @Test
    public void testGetCommandSyntax()
        throws Exception {

        final ImportCommand importObj = new ImportCommand();
        final String expectedResult = ImportCommand.SYNTAX;
        assertEquals(expectedResult, importObj.getCommandSyntax());
    }

    @Test
    public void testExecuteInvalidArgument()
        throws Exception {

        executeInvalidArgument();
    }

    private void executeInvalidArgument()
        throws Exception {

        final Shell shell = getTestShell();
        executeDDL(CREATE_TABLE_SIMPLE);

        /* Required flag: -file <name> */
        final ImportCommand importObj = new ImportCommand();
        String[] args = {ImportCommand.NAME};
        runWithRequiredFlag(shell, importObj, args, FILE_FLAG);

        /* Invalid argument for format type */
        args = new String[]{ImportCommand.NAME, TABLE_FLAG, "test",
                            FILE_FLAG, "a.out", "INVALID"};
        runWithUnknownArgument(shell, importObj, args);

        /* Table not found. */
        args = new String[]{ImportCommand.NAME, TABLE_FLAG, "invalidTable",
                            FILE_FLAG, "a.out"};
        runWithInvalidArgument(shell, importObj, args);

        /* File not found. */
        args = new String[]{ImportCommand.NAME, TABLE_FLAG, "simple",
                            FILE_FLAG, "INVALID_FILE"};
        runWithInvalidArgument(shell, importObj, args);
    }

    @Test
    public void testLoadJsons() {

        createTable(CREATE_TABLE_SIMPLE, TABLE_SIMPLE);
        createTable(CREATE_TABLE_COMPLEX, TABLE_COMPLEX);

        loadJsons(false);

        deleteTable(TABLE_SIMPLE);
        deleteTable(TABLE_COMPLEX);
        loadJsons(true);
    }

    private void loadJsons(boolean pretty) {
        final Format format = pretty ? Format.JSON_PRETTY : Format.JSON;
        final String cmdName = ImportCommand.NAME;
        final Shell shell = getTestShell();
        final String simpleJsonFile = getTempFileName(TABLE_SIMPLE, ".json");
        final String complexJsonFile = getTempFileName(TABLE_COMPLEX, ".json");
        final String jsonFile = getTempFileName("table", ".json");
        final int nRows = 50;

        writeRecordsToFile(TABLE_SIMPLE, simpleJsonFile, format, nRows);
        writeRecordsToFile(TABLE_COMPLEX, complexJsonFile, format, nRows);

        /*
         * The file contains below row ids for 2 tables:
         *  simple: 1 ~ 50
         *  complex: 1 ~ 50
         *  simple: 1 ~ 100 (the first 50 rows are duplicated rows)
         */
        writeRecordsToFile(TABLE_SIMPLE, jsonFile, format, nRows, true);
        writeRecordsToFile(TABLE_COMPLEX, jsonFile, format, nRows, true);
        writeRecordsToFile(TABLE_SIMPLE, jsonFile, format, 2*nRows, true);

        final ImportCommand cmdObj = new ImportCommand();

        /* Load JSON records to table "simple" */
        String[] args = {cmdName, FILE_FLAG, simpleJsonFile,
                         TABLE_FLAG, TABLE_SIMPLE};
        runCommand(shell, cmdObj, args, TABLE_SIMPLE,
                   String.valueOf(nRows));
        verifyRecords(TABLE_SIMPLE, nRows);
        deleteTable(TABLE_SIMPLE);

        /* Load JSON records to table "complex" */
        args = new String[] {cmdName, FILE_FLAG, complexJsonFile,
                             TABLE_FLAG, TABLE_COMPLEX};
        runCommand(shell, cmdObj, args, TABLE_COMPLEX, String.valueOf(nRows));
        verifyRecords(TABLE_COMPLEX, nRows);
        deleteTable(TABLE_COMPLEX);

        /* Load JSON records to table "simple" and "complex" */
        args = new String[] {cmdName, FILE_FLAG, jsonFile};
        runCommand(shell, cmdObj, args, TABLE_SIMPLE, String.valueOf(3 * nRows),
                   TABLE_COMPLEX, String.valueOf(nRows));
        verifyRecords(TABLE_COMPLEX, nRows);
        verifyRecords(TABLE_SIMPLE, 2 * nRows);
    }

    @Test
    public void testLoadCSVs() {

        final String cmdName = ImportCommand.NAME;
        final Shell shell = getTestShell();
        final String simpleCSVFile = getTempFileName(TABLE_SIMPLE, ".csv");
        final int nRows = 50;

        createTable(CREATE_TABLE_SIMPLE, TABLE_SIMPLE);
        writeRecordsToFile(TABLE_SIMPLE, simpleCSVFile, Format.CSV, nRows);

        final ImportCommand cmdObj = new ImportCommand();

        /* Load CSV records to table "simple" */
        String[] args = new String[] {cmdName, FILE_FLAG, simpleCSVFile,
                             TABLE_FLAG, TABLE_SIMPLE, "CSV"};
        runCommand(shell, cmdObj, args, TABLE_SIMPLE,
                   String.valueOf(nRows));
        verifyRecords(TABLE_SIMPLE, nRows);
        deleteTable(TABLE_SIMPLE);

        /* Test if space is trimmed for each element */
        final Row row = createRow(getTable(TABLE_SIMPLE), 0);
        String line = rowToCSVString(row);
        line = addSpaceToElement(line);
        writeToFile(simpleCSVFile, line, false);
        runCommand(shell, cmdObj, args, TABLE_SIMPLE, String.valueOf(1));
        verifyRecords(TABLE_SIMPLE, 1);
    }

    private String addSpaceToElement(String line) {
        final String[] tokens = line.split(",");
        final StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(" ");
            sb.append(token);
            sb.append(" ");
        }
        return sb.toString();
    }

    @Test
    public void testExecuteFailed() {
        final String cmdName = ImportCommand.NAME;
        final Shell shell = getTestShell();
        final String simpleCSVFile = getTempFileName(TABLE_SIMPLE, ".csv");
        final String tempFile = getTempFileName("temp", ".data");
        final int nRows = 10;

        createTable(CREATE_TABLE_SIMPLE, TABLE_SIMPLE);
        createTable(CREATE_TABLE_COMPLEX, TABLE_COMPLEX);
        writeRecordsToFile(TABLE_SIMPLE, simpleCSVFile, Format.CSV, nRows);

        final ImportCommand cmdObj = new ImportCommand();

        /* The given file doesn't exit */
        String[] args = {cmdName, FILE_FLAG, "invalidFile",
                         TABLE_FLAG, TABLE_SIMPLE};
        runCommand(shell, cmdObj, args, false);

        /* CSV records is not supported to load table with complex field */
        writeToFile(tempFile, "1,NULL,NULL,NULL", false);
        args = new String[] {cmdName, FILE_FLAG, tempFile,
                             TABLE_FLAG, TABLE_COMPLEX, "CSV"};
        runCommand(shell, cmdObj, args, false);

        /*
         * No target table specified and miss table specification before
         * records
         */
        clearFile(tempFile);
        writeToFile(tempFile, "{\"id\":1}", false);
        args = new String[] {cmdName, FILE_FLAG, tempFile};
        runCommand(shell, cmdObj, args, false);
    }

    @Test
    public void testExceuteExitOnFailure() {
        final String cmdName = ImportCommand.NAME;
        final Shell shell = getTestShell();
        final String simpleCSVFile = getTempFileName(TABLE_SIMPLE, ".csv");
        final String tempFile = getTempFileName("temp", ".data");
        final int nRows = 10;

        createTable(CREATE_TABLE_SIMPLE, TABLE_SIMPLE);
        writeRecordsToFile(TABLE_SIMPLE, simpleCSVFile, Format.CSV, nRows);

        final ImportCommand cmdObj = new ImportCommand();

        /* Null value for primary key field. */
        writeToFile(simpleCSVFile,
                    "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL",
                    true);
        String[] args = new String[] {cmdName, FILE_FLAG, simpleCSVFile,
                                      TABLE_FLAG, TABLE_SIMPLE, "CSV"};
        runCommand(shell, cmdObj, args, false, "Failed to create row");

        String invalidFixed = TableJsonUtils.encodeBase64(getBinaryData(20));
        String[] invalidTypeLines = {
            /* Invalid integer value for an integer field */
            "1,abc,,,,,,,,",
            /* Invalid long value for a long field  */
            "1,,abc,,,,,,,",
            /* Invalid double value for a double field  */
            "1,,,,abc,,,,,",
            /* Invalid float value for a float field  */
            "1,,,,,abc,,,,",
            /* Invalid enum value for an enum field */
            "1,,,,,,,\"abc\",,",
            /* Invalid value for a fixed binary field */
            "1,,,,,,,,," + invalidFixed,
        };

        for (String line : invalidTypeLines) {
            clearFile(tempFile);
            writeToFile(tempFile, line, false);
            args = new String[] {cmdName, FILE_FLAG, tempFile,
                                TABLE_FLAG, TABLE_SIMPLE, "CSV"};
            runCommand(shell, cmdObj, args, false, "Failed to create row");
        }

        /* Invalid JSON */
        String[] invalidJsons = {
            "\"id\" : 1",
            "{\"id\" : 1",
            "\"id\" : 1}",
            "{\"id\" : } 1",
        };

        for (String line : invalidJsons) {
            clearFile(tempFile);
            writeToFile(tempFile, line, false);
            args = new String[] {cmdName, FILE_FLAG, tempFile,
                                TABLE_FLAG, TABLE_SIMPLE};
            runCommand(shell, cmdObj, args, false, "Invalid JSON string");
        }

        /* Skip the rows of the table that not existed */
        clearFile(tempFile);
        writeRecordsToFile(TABLE_SIMPLE, tempFile, Format.CSV, nRows);
        writeToFile(tempFile, "\nTable: invalidTable", true);
        writeToFile(tempFile, "\n1,2,3,4", true);
        args = new String[] {cmdName, FILE_FLAG, tempFile,
                            TABLE_FLAG, TABLE_SIMPLE, "CSV"};
        runCommand(shell, cmdObj, args, false,
                   "Table invalidTable", "not found");
    }

    private Row createRow(Table table, int id) {
        if (table.getFullName().equalsIgnoreCase("simple")) {
            return createSimpleRow(table, id);
        }
        return createComplexRow(table, id);
    }

    private Row createSimpleRow(Table table, int id) {

        if (useNulls(id)) {
            return createRowWithNullValues(table, id);
        }

        final Row row = table.createRow();
        FieldDef fdef = table.getField("fenum");
        final String[] enumValues = fdef.asEnum().getValues();
        final int nFixed = table.getField("ffixed").asFixedBinary().getSize();
        row.put("id", id);
        row.put("fint", id);
        row.put("flong", Long.valueOf(id));
        row.put("fdouble", Double.valueOf(id + 1));
        row.put("ffloat", Float.valueOf(id + 1));
        row.put("fstring", getStringValue(id));
        row.putEnum("fenum", enumValues[id % enumValues.length]);
        row.put("fboolean", (id % 2 == 0 ? true : false));
        row.put("fbinary", getBinaryData(id % 20 + 1));
        row.putFixed("ffixed", getBinaryData(nFixed));
        return row;
    }

    private Row createComplexRow(Table table, int id) {

        if (useNulls(id)) {
            return createRowWithNullValues(table, id);
        }

        final Row row = table.createRow();
        row.put("id", id);
        final ArrayValue arVal = row.putArray("farray");
        for (int i = 0; i < 3; i++) {
            arVal.add("array value " + id + "_" + i);
        }

        final MapValue mapVal = row.putMap("fmap");
        for (int i = 0; i < 3; i++) {
            mapVal.put("map value " + id + "_" + i, i);
        }

        final RecordValue recVal = row.putRecord("frecord");
        recVal.put("frid", 1);
        recVal.put("frstring", "record value " + id);
        return row;
    }

    private boolean useNulls(int id) {
        return id % 11 == 10;
    }

    private boolean isEmptyString(int id) {
        return id % 11 == 5;
    }

    private boolean containEmbeddedQuotes(int id) {
        return id % 11 == 7;
    }

    private String getStringValue(int id) {
        if (isEmptyString(id)) {
            return "";
        }
        if (containEmbeddedQuotes(id)) {
            return "string \"embedded quote\"" + id;
        }
        return "string " + id;
    }

    private Row createRowWithNullValues(final Table table, final int id) {
        final List<String> fields = table.getFields();
        final Row row = table.createRow();
        row.put("id", id);
        for (String field : fields) {
            if (!field.equalsIgnoreCase("id")) {
                row.putNull(field);
            }
        }
        return row;
    }

    private byte[] getBinaryData(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    private void deleteTable(final String tblName) {
        final Table table = getTable(tblName);
        final TableIterator<Row> iterator =
            tableAPI.tableIterator(table.createPrimaryKey(), null, null);
        try {
            while (iterator.hasNext()) {
                final Row row = iterator.next();
                if (!tableAPI.delete(row.createPrimaryKey(), null, null)) {
                    fail("Failed to delete row from table \'" + tblName +
                         "\':" + row.toJsonString(false));
                }
            }
        } catch (Exception e) {
            fail("Failed to delete row: " + e.getMessage());
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    private void writeRecordsToFile(final String tblName,
                            final String outFile,
                            final Format format,
                            final int nRows) {
        writeRecordsToFile(tblName, outFile, format, nRows, false, false);
    }

    private void writeRecordsToFile(final String tblName,
                                    final String outFile,
                                    final Format format,
                                    final int nRows,
                                    final boolean append) {
        writeRecordsToFile(tblName, outFile, format, nRows, true, append);
    }

    private void writeRecordsToFile(final String tblName,
                                    final String outFile,
                                    final Format format,
                                    final int nRows,
                                    final boolean addTableSpecification,
                                    final boolean append) {
        final Table table = getTable(tblName);
        final File file = new File(outFile);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file, append);
            String line;
            if (addTableSpecification) {
                line = "Table: " + tblName + "\n";
                fos.write(line.getBytes());
            }
            for (int i = 0; i < nRows; i++) {
                final Row row = createRow(table, i);
                line = rowToString(row, format) + "\n";
                if (format == Format.JSON_PRETTY) {
                    line += "\n";
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

    private void writeToFile(final String outFile,
                             final String line,
                             final boolean append) {
        final File file = new File(outFile);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file, append);
            fos.write(line.getBytes());
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

    private String rowToString(final Row row, final Format format) {
        if (format == Format.JSON) {
            return row.toJsonString(false);
        } else if (format == Format.JSON_PRETTY) {
            return row.toJsonString(true);
        }
        return rowToCSVString(row);
    }

    private String rowToCSVString(final Row row) {
        final StringBuilder sb = new StringBuilder();
        final Table table = row.getTable();
        final List<String> fields = table.getFields();

        for (String fname : fields) {
            final FieldDef def = table.getField(fname);
            if (def.isComplex()) {
                fail("Complex type is not supported to be CSV format");
            }
            final FieldValue fval = row.get(fname);
            String sval;
            if (fval.isNull()) {
                sval = "";
            } else {
                sval = fval.toString();
                if (def.isString() || def.isEnum()) {
                    sval = "\"" + sval + "\"";
                }
            }
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(sval);
        }
        return sb.toString();
    }

    private void verifyRecords(final String tblName, final int expNumRows) {
        final Table table = getTable(tblName);
        final TableIterator<Row> iterator =
            tableAPI.tableIterator(table.createPrimaryKey(), null, null);
        int nRows = 0;
        try {
            while (iterator.hasNext()) {
                final Row row = iterator.next();
                final int id = row.get("id").asInteger().get();
                final Row expRow = createRow(table, id);
                if (!expRow.equals(row)) {
                    fail("Expected to get row " + expRow.toJsonString(false) +
                         ", but get " + row.toJsonString(false));
                }
                nRows++;
            }
        } catch (Exception e) {
            fail("Verify records failed: " + e.getMessage());
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        assertEquals("The row count is wrong, expected " + nRows +
                     " but get " + expNumRows, expNumRows, nRows);
    }

    private void clearFile(String fileName) {
        PrintWriter writer;
        try {
            writer = new PrintWriter(fileName);
            writer.print("");
            writer.close();
        } catch (FileNotFoundException e) {
            fail("File not found: " + fileName);
        }
    }
}
