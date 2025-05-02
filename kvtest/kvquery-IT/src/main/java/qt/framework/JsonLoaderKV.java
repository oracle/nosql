/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt.framework;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.nosql.common.qtf.JsonLoader;

public class JsonLoaderKV extends JsonLoader{

    private static int METADATA_NOT_FOUND_RETRY = 5;

    private final TableAPI tableImpl;

    private WriteOptions option;

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

        return new JsonLoaderKV(tableImpl).loadJsonToTables(table, fileName,
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

        return new JsonLoaderKV(tableImpl).loadCSVToTables(table, fileName,
                                                           options, true);
    }

    public JsonLoaderKV(final TableAPI tableImpl) {
        this.tableImpl = tableImpl;
    }

    private void setWriteOptions(WriteOptions option) {
        this.option = option;
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

        setWriteOptions(options);
        return loadRecordsFromFile(table, fileName, Type.JSON,
                                   false, exitOnFailure);
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

        setWriteOptions(options);
        return loadRecordsFromFile(table, fileName, Type.CSV,
                                   false, exitOnFailure);
    }

    @Override
    public void tallyCount(final Map<String, Long> result,
                           final Object table,
                           final long count) {
        if (table == null || count == 0) {
            return;
        }
        final String name = ((Table)table).getFullName();
        if (result.containsKey(name)) {
            long total = result.get(name) + count;
            result.put(name, total);
        } else {
            result.put(name, count);
        }
    }

    @Override
    public Object getTargetTable(String name) {
        Table newTable = tableImpl.getTable(name);
        return newTable;
    }

    @Override
    public boolean checkSkipTable(Object table, String name) {
        boolean ret = false;
        if (table != null) {
            ret = !((Table)table).getFullName().equalsIgnoreCase(name);
        }
        return ret;
    }

    @Override
    public void checkValidFieldForCSV(Object tableObj)
            throws IllegalArgumentException {
        Table table = (Table) tableObj;
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

    @Override
    public boolean putRecord(Object target, String rowLine, Type type)
            throws RuntimeException {

        Row row = createRow((Table)target, rowLine, type);

        int retry = 0;
        while (true) {
            try {
                return (tableImpl.put(row, null, option) != null);
            } catch (MetadataNotFoundException mnfe) {
                /*
                 * Retry when caught MetadataNotFoundException which may be
                 * related to asynchronous propagation of schema information.
                 */
                if (retry++ < METADATA_NOT_FOUND_RETRY) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                    continue;
                }
                throw mnfe;
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
}
