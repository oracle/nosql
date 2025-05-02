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

package oracle.kv.util.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.FaultException;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;

/**
 * A utility class that encapsulates functions to load records from a file to
 * table(s), the support formats of record are JSON and CSV.
 *
 * The target table to be loaded into can be specified either by the input
 * argument, or by adding a comment line "Table: <name>" ahead of  the
 * records to load in the file, see below example:
 *
 * e.g. 2 tables and their records included in data.txt file.
 *
 * Table: users
 * <records of users>
 * ...
 *
 * Table: emails
 * <records of emails>
 * ...
 *
 * The table name specified in the file take the precedence to be used.
 */
public class LoadTableUtils {

    private final static char LEFT_CURLY_BRACE = '{';
    private final static char RIGHT_CURLY_BRACE = '}';
    private final static char DOUBLE_QUOTE = '\"';
    private final static char ESCAPE = '\\';

    private static enum Type {JSON, CSV}

    /**
     * Load JSONs from a file to table(s), the target table(s) should be
     * specified in the file with a line of "Table: <name>" before records of
     * the table.
     *
     * @param tableImpl the TableAPI interface for the store.
     * @param fileName the file that contains JSON records.
     * @param options the write options used for put operation.
     *
     * @return a map of table name and its corresponding record count loaded.
     */
    public static Map<String, Long> loadJsonFromFile(final TableAPI tableImpl,
                                                     final String fileName,
                                                     final WriteOptions options)
        throws IllegalArgumentException, IOException, FaultException {

        return loadJsonFromFile(tableImpl, null, fileName, options);
    }

    /**
     * Load JSONs from a file to the specified table.
     *
     * A overloaded {@link #loadJsonFromFile(TableAPI, String, WriteOptions)}
     * method, the difference is that 2nd argument "table" is provided to
     * specify the target table to load records to.
     */
    public static Map<String, Long> loadJsonFromFile(final TableAPI tableImpl,
                                                     final Table table,
                                                     final String fileName,
                                                     final WriteOptions options)
        throws IllegalArgumentException, IOException, FaultException {

        return new Loader(tableImpl).loadJsonToTables(table, fileName,
                                                      options, true);
    }

    /**
     * Load CSVs from a file to table(s), the target table(s) should be
     * specified in the file with a line of "Table: <name>" before records of
     * the table.
     *
     * @param tableImpl the TableAPI interface for the store.
     * @param fileName the file that contains CSV records to load.
     * @param options the write options used for put operation.
     *
     * @return a map of table name and its corresponding record count loaded.
     */
    public static Map<String, Long> loadCSVFromFile(final TableAPI tableImpl,
                                                    final String fileName,
                                                    final WriteOptions options)
        throws IllegalArgumentException, IOException, FaultException {

        return loadCSVFromFile(tableImpl, null, fileName, options);
    }

    /**
     * Load JSONs from a file to the specified table.
     *
     * A overloaded {@link #loadCSVFromFile(TableAPI, String, WriteOptions)}
     * method, the difference is that 2nd argument "table" is provided to
     * specify the target table to load records to.
     */
    public static Map<String, Long> loadCSVFromFile(final TableAPI tableImpl,
                                                    final Table table,
                                                    final String fileName,
                                                    final WriteOptions options)
        throws IllegalArgumentException, IOException, RuntimeException {

        return new Loader(tableImpl).loadCSVToTables(table, fileName,
                                                     options, true);
    }

    /**
     * The loader class implements methods to load JSON or CSV records from a
     * file to table(s).
     */
    public static class Loader {

        private final TableAPI tableImpl;

        public Loader(final TableAPI tableImpl) {
            this.tableImpl = tableImpl;
        }

        /**
         * Load JSON records from a file to tables.
         *
         * @param table the initial table to which JSON records are loaded.
         * @param fileName the file contains JSON records.
         * @param options the WriteOptions used to put records.
         * @param exitOnFailure the flag indicates if exits if a record is
         *        failed to put.
         *
         * @return A map of table name and count of records loaded.
         */
        public Map<String, Long> loadJsonToTables(Table table,
                                                  String fileName,
                                                  WriteOptions options,
                                                  boolean exitOnFailure)
            throws IllegalArgumentException, IOException, FaultException {

            return loadRecordsFromFile(table, fileName, Type.JSON,
                                       options, false, exitOnFailure);
        }

        /**
         * Load JSON records from a file to the specified table.
         *
         * @param table the target table to which JSON records are loaded, the
         *        records of other tables will be skipped.
         * @param fileName the file contains JSON records.
         * @param options the WriteOptions used to put records.
         * @param exitOnFailure the flag indicates if exits if a record is
         *        failed to put.
         *
         * @return The total number of records loaded to the target table.
         */
        public long loadJsonToTable(Table table,
                                    String fileName,
                                    WriteOptions options,
                                    boolean exitOnFailure)
            throws IllegalArgumentException, IOException, FaultException {

            Map<String, Long> results = loadRecordsFromFile(table,
                                                            fileName,
                                                            Type.JSON,
                                                            options,
                                                            true,
                                                            exitOnFailure);
            if (results.isEmpty()) {
                return 0;
            }
            final String tableName = table.getFullName();
            assert(results.containsKey(tableName));
            return results.get(tableName);
        }

        /**
         * Load CSV records from a file to the specified table.
         *
         * @param table the target table to which CSV records are loaded, the
         *        records of other tables will be skipped.
         * @param fileName the file contains CSV records.
         * @param options the WriteOptions used to put records.
         * @param exitOnFailure the flag indicates if exits if a record is
         *        failed to put.
         *
         * @return A map of table name and count of records loaded.
         */
        public Map<String, Long> loadCSVToTables(Table table,
                                                 String fileName,
                                                 WriteOptions options,
                                                 boolean exitOnFailure)
            throws IllegalArgumentException, IOException, FaultException {

            return loadRecordsFromFile(table, fileName, Type.CSV,
                                       options, false, exitOnFailure);
        }

        /**
         * Load records from a file to table(s)
         *
         * @param table the target table to which the records are loaded.
         * @param fileName the file that contains the records.
         * @param type the type of record: JSON or CSV.
         * @param options the WriteOptions used to performance put operation.
         * @param targetTableOnly a flag to indicate that only load record to
         *        the specified target {@code table}, the records for other
         *        tables are skipped.
         * @param exitOnFailure a flag to indicate if terminate the load process
         *        if any failure.
         *
         * @return A map of table name and count of records loaded.
         *
         * @throws IOException
         * @throws RuntimeException
         */
        private Map<String, Long>
            loadRecordsFromFile(final Table table,
                                final String fileName,
                                final Type type,
                                final WriteOptions options,
                                final boolean targetTableOnly,
                                final boolean exitOnFailure)
            throws IllegalArgumentException, IOException, FaultException {

            assert (!targetTableOnly || table != null);

            if (!new File(fileName).exists()) {
                throw new FileNotFoundException("File not found: " + fileName);
            }

            if (type == Type.CSV) {
                for (String fname : table.getFields()) {
                    final FieldDef fdef = table.getField(fname);
                    if (fdef.isComplex()) {
                        final String fmt = "The table \"%s\" contains a complex " +
                            "field \"%s\" that cannot be imported as CSV format";
                        throw new IllegalArgumentException(
                            String.format(fmt, table.getFullName(), fname));
                    }
                }
            }

            BufferedReader br = null;
            try {
                Table target = table;
                Row row = null;
                final StringBuilder buffer = new StringBuilder();
                String line;
                long nLoaded = 0;
                long nLine = 0;
                int nCurlyBrace = 0;
                boolean skipLine = false;
                Map<String, Long> result = new HashMap<String, Long>();
                br = new BufferedReader(new FileReader(fileName));

                while((line = br.readLine()) != null) {
                    nLine++;

                    /* Skip comment line or empty line. */
                    if (isComment(line)) {
                        continue;
                    }

                    /*
                     * Check if the line includes table specification with the
                     * format: Table: <name>.
                     */
                    final String name = parseTableName(line);
                    if (name != null) {
                        if (targetTableOnly) {
                            skipLine =
                                !target.getFullName().equalsIgnoreCase(name);
                        } else {
                            Table newTable = tableImpl.getTable(name);
                            if (newTable != null) {
                                if (target != null) {
                                    tallyCount(result, target, nLoaded);
                                    nLoaded = 0;
                                }
                                target = newTable;
                                skipLine = false;
                            } else {
                                if (exitOnFailure) {
                                    final String fmt = "Table %s (at line %s)" +
                                        " not found.";
                                    throw new IllegalArgumentException(
                                        String.format(fmt, name, nLine));
                                }
                                skipLine = true;
                            }
                        }
                        continue;
                    }

                    if (skipLine) {
                        continue;
                    }

                    if (target == null) {
                        final String fmt = "Missing table specification " +
                            "\"Table: <name>\" before record at line %d of %s";
                        throw new IllegalArgumentException(
                            String.format(fmt, nLine, fileName));
                    }

                    String rowLine;
                    if (type == Type.JSON) {
                        buffer.append(line);
                        /*
                         * If curly braces are matched in pairs, then the JSON
                         * record is considered as completed one.
                         */
                        nCurlyBrace += getUnmatchedCurlyBraces(line);
                        if (nCurlyBrace != 0) {
                            continue;
                        }
                        rowLine = buffer.toString().trim();
                        if (!isValidJsonString(rowLine)) {
                            if (exitOnFailure) {
                                final String fmt = "Invalid JSON string at " +
                                		"line %d of %s: %s";
                                throw new IllegalArgumentException(
                                    String.format(fmt, nLine, fileName,
                                                  rowLine));
                            }
                        }
                        buffer.setLength(0);
                    } else {
                        rowLine = line;
                    }

                    try {
                        row = createRow(target, rowLine, type);
                        if (doPut(tableImpl, row, options)) {
                            ++nLoaded;
                        } else {
                            if (exitOnFailure) {
                                break;
                            }
                        }
                    } catch (RuntimeException rte) {
                        if (exitOnFailure) {
                            final String fmt = "Failed to import JSON row at " +
                                "line %d of file, %s: " + rte.getMessage();
                            throw new RuntimeException(
                                String.format(fmt, nLine, fileName), rte);
                        }
                        /* Ignore the exception if exitOnFailure is false. */
                    }
                }

                /* Handle the left string in buffer which is not a valid JSON */
                if (buffer.length() > 0) {
                    assert(type == Type.JSON);
                    if (exitOnFailure) {
                        throw new IllegalArgumentException
                        ("Invalid JSON string: " + buffer.toString());
                    }
                }

                if (target != null) {
                    tallyCount(result, target, nLoaded);
                }
                return result;
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        /**
         * Creates a row from a string.
         */
        private Row createRow(Table table, String str, Type type) {
            if (type == Type.JSON) {
                return createRowFromJson(table, str);
            }
            return createRowFromCSV(table, str);
        }

        /**
         * Creates row from JSON doc for the specified table.
         */
        public Row createRowFromJson(Table table, String str) {

            return table.createRowFromJson(str, false);
        }

        /**
         * Creates row from CVS record for the specified table.
         */
        public Row createRowFromCSV(Table table, String str) {
            final String[] values = splitCSVValues(str);
            final List<String> fnames = table.getFields();
            if (values.length != fnames.size()) {
                final String fmt = "Invalid record for table %s, the number " +
                    "of values is expected to be %d but %d: %s";
                throw new IllegalArgumentException(
                    String.format(fmt, table.getFullName(), fnames.size(),
                                  values.length, str));
            }

            final Row row = table.createRow();
            for (int i = 0; i < values.length; i++) {
                String sval = values[i];
                final String fname = fnames.get(i);
                final FieldDef fdef = table.getField(fname);

                assert(!fdef.isComplex());

                if (isCSVNullValue(sval)) {
                    row.putNull(fname);
                } else {
                    /* Trim double quotes for string type value */
                    if (fdef.isString() || fdef.isEnum()) {
                        sval = trimQuotes(sval);
                    }
                    final FieldValue fval =
                        FieldDefImpl.createValueFromString(sval, fdef);
                    row.put(fname, fval);
                }
            }
            return row;
        }

        /**
         * Executes put operation.
         */
        public boolean doPut(TableAPI tableAPIImpl, Row row, WriteOptions wro) {
            return (tableAPIImpl.put(row, null, wro) != null);
        }

        /**
         * Tally the count of records for a table.
         */
        private static void tallyCount(final Map<String, Long> result,
                                       final Table table,
                                       final long count) {
            if (table == null || count == 0) {
                return;
            }
            final String name = table.getFullName();
            if (result.containsKey(name)) {
                long total = result.get(name) + count;
                result.put(name, total);
            } else {
                result.put(name, count);
            }
        }

        /**
         * Returns true if the line is comment line or empty line.
         */
        private boolean isComment(String line) {
            final String str = line.trim();
            if (str.length() == 0) {
                return true;
            }

            final int commentIdx = str.indexOf(Shell.COMMENT_MARK);
            if (commentIdx == 0) {
                return true;
            }
            return false;
        }

        /**
         * Parses the table name from the comment line, the format of
         * table specification is "Table: <name>", e.g. Table: users
         */
        private static String parseTableName(final String line) {
            final String[] tokens = {"table", ":"};
            String str = line.trim();
            for (String token : tokens) {
                if (!str.toLowerCase().startsWith(token)) {
                    return null;
                }
                str = str.substring(token.length()).trim();
            }
            return str;
        }


        /**
         * Returns true if the string value represents null value in CSV format
         */
        private static boolean isCSVNullValue(final String value) {
            return value.isEmpty();
        }

        /**
         * Split the line into token by comma but ignoring the embedded comma in
         * quotes.
         */
        private static String[] splitCSVValues(String line) {
            final String tline = line.trim();
            String[] values = tline.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            for (int i = 0; i < values.length; i++) {
                values[i] = values[i].trim();
            }
            return values;
        }

        /**
         * Trim leading and trailing double quote of a string
         */
        private static String trimQuotes(final String value) {
            String str = value.trim();
            if (str.startsWith("\"") && str.endsWith("\"")) {
                str = str.substring(1, str.length() - 1);
            }
            return str;
        }

       /**
        * Return the number of unmatched curly braces. If unmatched curly
        * brace(s) are left brace(s), then return a positive number. If
        * unmatched curly brace(s) are right brace(s), then return
        * negative number.
        */
        private static int getUnmatchedCurlyBraces(String line) {
            int cnt = 0;
            boolean inQuotes = false;
            char prev = 0;
            for (int i = 0; i < line.length(); i++) {
                final char ch = line.charAt(i);
                switch (ch) {
                    case LEFT_CURLY_BRACE:
                        if (!inQuotes) {
                            cnt++;
                        }
                        break;
                    case RIGHT_CURLY_BRACE:
                        if (!inQuotes) {
                            cnt--;
                        }
                        break;
                    case DOUBLE_QUOTE:
                        if (prev != ESCAPE) {
                            inQuotes = !inQuotes;
                        }
                        break;
                    default:
                        break;
                }
                prev = ch;
            }
            return cnt;
        }

        /**
         * Check if the given string is valid JSON string.
         *
         * 1.JSON string should start with left curly brace '{' and end with
         *   the right curly brace '}'.
         */
        private boolean isValidJsonString(final String str) {
            assert(str != null && !str.isEmpty());

            if (str.indexOf(LEFT_CURLY_BRACE) != 0) {
                return false;
            }
            if (str.lastIndexOf(RIGHT_CURLY_BRACE) !=  str.length() - 1) {
                return false;
            }
            return true;
        }
    }
}
