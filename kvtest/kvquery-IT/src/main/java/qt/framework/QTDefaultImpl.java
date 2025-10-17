/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt.framework;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.nosql.common.qtf.FileUtils;
import oracle.nosql.common.qtf.QTAfter;
import oracle.nosql.common.qtf.QTBefore;
import oracle.nosql.common.qtf.QTOptions;

public class QTDefaultImpl implements QTBefore, QTAfter {

    protected QTOptions opts;
    File configFile;
    protected Properties configProperties;

    @Override
    public void setOptions(QTOptions opts) {
        this.opts = opts;
    }

    @Override
    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }

    @Override
    public void setConfigProperties(Properties configProperties) {
        this.configProperties = configProperties;
    }

    @Override
    public void before() {
        opts.verbose("Before:  default impl for " +
            opts.relativize(configFile));

        try {
            String beforeDmlProp = configProperties.getProperty("before-ddl-file");
            if (beforeDmlProp != null) {

                File dmlFile = new File(configFile.getParentFile(),
                    beforeDmlProp);
                if (!dmlFile.exists() || !dmlFile.isFile())
                    throw new IllegalArgumentException("Property before-ddl-file" +
                        " doesn't reference a valid file.");

                System.out.println("Executing suite: " + configFile);
                opts.verbose("Executing  before-ddl-file: " + beforeDmlProp);
                List<String> stmts = extractStatements(dmlFile);
                executeStatements(stmts);
            }

            String beforeDataProp = configProperties.getProperty("before-data-file");
            if (beforeDataProp != null) {

                File dataFile = new File(configFile.getParentFile(),
                                         beforeDataProp);
                if (!dataFile.exists() || !dataFile.isFile())
                    throw new IllegalArgumentException("Property before-data-file" +
                        " doesn't reference a valid file.");

                opts.verbose("Executing  before-data-file: " + beforeDataProp);
                JsonLoaderKV.loadJsonFromFile(QTest.store.getTableAPI(),
                                              dataFile.getCanonicalPath(),
                                              null);
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    void storeRecords(List<String> records) {

        String tableName;
        TableAPI tableAPI = QTest.store.getTableAPI();
        Table table = null;

        for (String record : records) {
            if (record.trim().length() == 0)
                continue;

            int ci = record.indexOf(':');
            if (ci >= 5 &&
                record.substring(0, ci).trim().toLowerCase().equals("table")) {
                tableName = record.substring(ci + 1).trim();
                table = tableAPI.getTable(tableName);
                if (table == null)
                    throw new IllegalArgumentException("Invalid table name.");
            } else {
                if (table == null)
                    throw new IllegalArgumentException("No table name was " +
                        "provided.");

                Row row = table.createRowFromJson(record, true);
                tableAPI.put(row, null, null);
            }
        }
    }

    private String addRegionsForMRTable(String stmt) {
        if (TestBase.mrTableMode && stmt.toLowerCase().contains("create table")) {
            //TODO: remove the following check when MRTables
            // support ideneity column and ttl.
            if (!stmt.toLowerCase().contains("as identity") &&
                !stmt.toLowerCase().contains("ttl")) {
                /* add a remote region when create tables. */
                stmt += " IN REGIONS " + TestBase.REMOTE_REGION;
            } else {
                opts.verbose("Identity columns and ttl are not " +
                    "allowed for multi-region tables. ");
            }
        }
        return stmt;
    }

    protected void executeStatements(List<String> stmts) {
        for(String stmt : stmts) {
            executeStatement(stmt);
        }
    }

    protected void executeStatement(String stmt) {
        addRegionsForMRTable(stmt);
        opts.verbose("Executing: " + stmt);
        StatementResult sr = QTest.store.executeSync(stmt);
        if (!sr.isSuccessful())
            throw new IllegalArgumentException(
                "Error while executing statement: " + stmt +
                    " message: " + sr.getErrorMessage());

        opts.verbose("   Successful");
    }

    protected List<String> extractStatements(File dmlFile)
        throws IOException {

        // a statement can be on multiple lines, statements are split by an
        // empty line (using String.trim() definition).

        String dmlStr = FileUtils.readFileToString(dmlFile).trim();
        String[] dmlLines = dmlStr.split("\n");
        List<String> stmts = new ArrayList<String>();
        String currentStmt = "";
        for (int i = 0; i < dmlLines.length; i++) {
            String dmlLine = dmlLines[i].trim();
            if (dmlLine.startsWith("#"))
                continue;

            if (dmlLine.length() > 0) {
                if (TestBase.mrTableMode) {
                    /*
                     * There are some string operators for the statements
                     * in mrtable mode, so remove any redundant spaces. */
                    String[] dmlArr = dmlLine.split("\\s+");
                    dmlLine = String.join(" ", dmlArr) + " ";
                }
                currentStmt = currentStmt + dmlLine;
            } else {
                if (currentStmt.length() > 0)
                    stmts.add(currentStmt);
                currentStmt = "";
            }
        }
        if (currentStmt.length() > 0)
            stmts.add(currentStmt);
        return stmts;
    }

    protected List<String> extractLines(File dataFile)
        throws IOException {
        // use the same technique
        return extractStatements(dataFile);
    }

    @Override
    public void after() {
        opts.verbose("After: default impl for " +
                     opts.relativize(configFile));

        String afterDmlProp = configProperties.getProperty("after-ddl-file");
        if (afterDmlProp != null) {
            try {
                File dmlFile = new File(configFile.getParentFile(),
                                        afterDmlProp);
                if (!dmlFile.exists() || !dmlFile.isFile())
                    throw new IllegalArgumentException("Property " +
                        "after-ddl-file" +
                        " doesn't reference a valid file.");

                List<String> stmts = extractStatements(dmlFile);
                executeStatements(stmts);
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }
    }
}
