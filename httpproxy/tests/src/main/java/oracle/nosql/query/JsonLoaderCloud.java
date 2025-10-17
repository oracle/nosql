/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */
package oracle.nosql.query;

import java.io.IOException;
import java.util.Map;

import oracle.nosql.common.qtf.JsonLoader;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.JsonOptions;

public class JsonLoaderCloud extends JsonLoader {

    private final NoSQLHandle handle;

    private PutRequest.Option option;

    private String dataFile;

    /**
     * Load JSONs from a file to table(s), the target table(s) should be
     * specified in the file with a line of "Table: <name>" before records of
     * the table.
     *
     * @param handle the NoSQLHandle interface.
     * @param fileName the file that contains JSON records.
     * @param options PutRequest.Option used for put operation.
     *
     * @return a map of table name and its corresponding record count loaded.
     */
    public static Map<String, Long> loadJsonFromFile(final NoSQLHandle handle,
                                                     final String fileName,
                                                     final PutRequest.Option
                                                     options)
        throws IllegalArgumentException, IOException {

        return loadJsonFromFile(handle, null, fileName, options);
    }

    /**
     * Load JSONs from a file to the specified table.
     *
     * A overloaded
     * {@link #loadJsonFromFile(NoSQLHandle, String, PutRequest.Option)}
     * method, the difference is that 2nd argument "table" is provided to
     * specify the target table to load records to.
     */
    public static Map<String, Long> loadJsonFromFile(final NoSQLHandle handle,
                                                     final TableResult table,
                                                     final String fileName,
                                                     final PutRequest.Option
                                                     options)
        throws IllegalArgumentException, IOException {

        return new JsonLoaderCloud(handle).loadJsonToTables(table, fileName,
                                                            options, true);
    }

    public JsonLoaderCloud(final NoSQLHandle handle) {
        this.handle = handle;
    }

    private void setWriteOptions(PutRequest.Option option) {
        this.option = option;
    }
    /**
     * Load JSON records from a file to tables.
     *
     * @param table the initial table to which JSON records are loaded.
     * @param fileName the file contains JSON records.
     * @param options PutRequest.Option used to put records.
     * @param exitOnFailure the flag indicates if exits if a record is
     *        failed to put.
     *
     * @return A map of table name and count of records loaded.
     */
    public Map<String, Long> loadJsonToTables(TableResult table,
                                              String fileName,
                                              PutRequest.Option options,
                                              boolean exitOnFailure)
        throws IllegalArgumentException, IOException  {

        setWriteOptions(options);
        dataFile = fileName;

        return loadRecordsFromFile(table, fileName, Type.JSON,
                                   false, exitOnFailure);
    }

    /**
     * Load JSON records from a file to the specified table.
     *
     * @param table the target table to which JSON records are loaded, the
     *        records of other tables will be skipped.
     * @param fileName the file contains JSON records.
     * @param options the PutRequest.Optio used to put records.
     * @param exitOnFailure the flag indicates if exits if a record is
     *        failed to put.
     *
     * @return The total number of records loaded to the target table.
     */
    public long loadJsonToTable(TableResult table,
                                String fileName,
                                PutRequest.Option options,
                                boolean exitOnFailure)
        throws IllegalArgumentException, IOException {

        setWriteOptions(options);
        Map<String, Long> results = loadRecordsFromFile(table,
                                                        fileName,
                                                        Type.JSON,
                                                        true,
                                                        exitOnFailure);
        if (results.isEmpty()) {
            return 0;
        }
        final String tableName = table.getTableName();
        assert(results.containsKey(tableName));
        return results.get(tableName);
    }

    @Override
    public void tallyCount(final Map<String, Long> result,
                                   final Object table,
                                   final long count) {
        if (table == null || count == 0) {
            return;
        }
        final String name = ((TableResult)table).getTableName();
        if (result.containsKey(name)) {
            long total = result.get(name) + count;
            result.put(name, total);
        } else {
            result.put(name, count);
        }
    }

    @Override
    public Object getTargetTable(String name) {
        try {
            GetTableRequest tableReq =
                    new GetTableRequest().setTableName(name);
            TableResult tableRes = handle.getTable(tableReq);
            return tableRes;
        } catch(Exception e) {
            return null;
        }
    }

    @Override
    public boolean checkSkipTable(Object table, String name) {
        boolean ret = false;
        if (table != null) {
            ret = !((TableResult)table).getTableName().equalsIgnoreCase(name);
        }
        return ret;
    }

    @Override
    public void checkValidFieldForCSV(Object tableObj)
            throws IllegalArgumentException {
        throw new IllegalArgumentException("spartakv tool does not support "
                + "CSV type for now");
    }

    @Override
    public boolean putRecord(Object target, String rowLine, Type type)
            throws RuntimeException {

        JsonOptions options = 
                new JsonOptions().setAllowNonNumericNumbers(true);

        String tableName = ((TableResult)target).getTableName();
        PutRequest putReq = new PutRequest().
            setTableName(tableName).
            setValueFromJson(rowLine, options);

        if (option != null) {
            putReq = putReq.setOption(option);
        }

        if (dataFile.contains("row_metadata")) {
            FieldValue info = putReq.getValue().get("info");
            if (info == null) {
                info = JsonNullValue.getInstance();
            }
            String metadata = info.toJson();
            putReq.setRowMetadata(metadata);
        }

        PutResult putRes = handle.put(putReq);
        //On a successful operation the value returned by getVersion()
        //is non-null. On failure that value is null.
        return putRes.getVersion() == null ? false: true;
    }
}
