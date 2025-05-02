/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.nosql.common.qtf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A abstract class that encapsulates functions to load records from a file to
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
/**
 * The loader class implements methods to load JSON or CSV records from a
 * file to table(s).
 */
public abstract class JsonLoader {

    private final static char LEFT_CURLY_BRACE = '{';
    private final static char RIGHT_CURLY_BRACE = '}';
    private final static char DOUBLE_QUOTE = '\"';
    private final static char ESCAPE = '\\';
    private final static String COMMENT_MARK = "#";

    public static enum Type {JSON, CSV}

    /**
     * Load records from a file to table(s)
     *
     * @param table the target table to which the records are loaded.
     * @param fileName the file that contains the records.
     * @param type the type of record: JSON or CSV.
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
    public Map<String, Long>
        loadRecordsFromFile(final Object table,
                            final String fileName,
                            final Type type,
                            final boolean targetTableOnly,
                            final boolean exitOnFailure)
        throws IllegalArgumentException, IOException {

        assert (!targetTableOnly || table != null);

        if (!new File(fileName).exists()) {
            throw new FileNotFoundException("File not found: " + fileName);
        }

        if (type == Type.CSV) {
            if (table != null)
                checkValidFieldForCSV(table);
        }

        BufferedReader br = null;
        try {
            Object target = table;
            final StringBuilder buffer = new StringBuilder();
            String line;
            long nLoaded = 0;
            long nLine = 0;
            int nCurlyBrace = 0;
            boolean skipTable = false;
            Map<String, Long> result = new HashMap<String, Long>();
            br = new BufferedReader(new FileReader(fileName));

            while((line = br.readLine()) != null) {
                nLine++;

                /**
                 * Skip comment line or empty line.
                 *
                 * nCurlyBrace != 0 indicates the line may be part of the JSON
                 * that spans multiple lines, in which case the line should be
                 * preserved even though it contains only white spaces or starts
                 * with a comment mark '#'.
                 */
                if (nCurlyBrace == 0 && isComment(line)) {
                    continue;
                }

                /*
                 * Check if the line includes table specification with the
                 * format: Table: <name>.
                 */
                final String name = parseTableName(line);
                if (name != null) {
                    if (targetTableOnly) {
                        skipTable = checkSkipTable(target, name);
                    } else {
                        Object newTable = getTargetTable(name);
                        if (newTable != null) {
                            if (target != null) {
                                tallyCount(result, target, nLoaded);
                                nLoaded = 0;
                            }
                            target = newTable;
                            skipTable = false;
                            if (type == Type.CSV) {
                                checkValidFieldForCSV(target);
                            }
                        } else {
                            if (exitOnFailure) {
                                final String fmt = "Table %s (at line %s)" +
                                    " not found.";
                                throw new IllegalArgumentException(
                                    String.format(fmt, name, nLine));
                            }
                            skipTable = true;
                        }
                    }
                    continue;
                }

                if (skipTable) {
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
                    if (putRecord(target, rowLine, type)) {
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
            tallyCount(result, target, nLoaded);
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
     * get Target Table for kv or TableResult for cloud
     * @param name the table Name
     * @return Table for kv or TableResult for cloud, return null if table not
     *         exist in store.
     */
    public abstract Object getTargetTable(String name);

    /**
     * check whether we could skip the line for table
     * @param table Table for kv or TableResult for cloud
     * @param name table Name
     * @return true if table name is not matched
     */
    public abstract boolean checkSkipTable(Object table, String name);

    /**
     * check whether field is valid for CSV format
     * @param tableObj Table for kv or TableResult for cloud
     * @throws IllegalArgumentException if it's invalid
     */
    public abstract void checkValidFieldForCSV(Object tableObj)
            throws IllegalArgumentException;

    /**
     * put one record into table
     * @param target Table for kv or TableResult for cloud
     * @param rowLine the json string for the record
     * @param type JSON or CSV
     * @return true if put operation succeeded
     * @throws RuntimeException
     */
    public abstract boolean putRecord(Object target, String rowLine, Type type)
            throws RuntimeException;

    /**
     * tally the count of records for a table.
     * @param result result of tally count
     * @param table Table for kv or TableResult for cloud
     * @param count the count
     */
    public abstract void tallyCount(final Map<String, Long> result,
                                    final Object table,
                                    final long count);

    /**
     * Returns true if the line is comment line or empty line.
     */
    private boolean isComment(String line) {
        final String str = line.trim();
        if (str.length() == 0) {
            return true;
        }

        final int commentIdx = str.indexOf(COMMENT_MARK);
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
