/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.nosql.common.qtf.FileUtils;
import oracle.nosql.common.qtf.QTAfter;
import oracle.nosql.common.qtf.QTBefore;
import oracle.nosql.common.qtf.QTOptions;
import oracle.nosql.driver.IndexExistsException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.TableExistsException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;

/**
 * Class implements QTBefore and QTAfter.
 */
public class QTDefaultImpl
    implements QTBefore, QTAfter {

    private static Pattern tableNamePattern = Pattern.compile(
            "(?is)\\b(?:create+\\s+table+\\s+if+\\s+not+\\s+exists|" +
            "create+\\s+table|alter+\\s+table)\\s+" +
            "(.*?)(?=\\s|\\(|$)");

    protected QTOptions opts;
    protected File configFile;
    protected Properties configProperties;

    private enum DdlType {
        CREATE_TABLE,
        DROP_TABLE,
        ALTER_TABLE,
        CREATE_INDEX
    }

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

        System.out.println("Executing: " + opts.relativize(configFile));
        try {
            String beforeDmlProp =
                    configProperties.getProperty("before-ddl-file");
            if (beforeDmlProp != null) {

                File dmlFile = new File(configFile.getParentFile(),
                    beforeDmlProp);
                if (!dmlFile.exists() || !dmlFile.isFile())
                    throw new IllegalArgumentException("Property " +
                        " before-ddl-file doesn't reference a valid file.");

                opts.verbose("Executing  before-ddl-file: " + beforeDmlProp);

                List<String> stmts = extractStatements(dmlFile);
                executeStatements(stmts);
            }

            String beforeDataProp =
                    configProperties.getProperty("before-data-file");
            if (beforeDataProp != null) {

                File dataFile = new File(configFile.getParentFile(),
                    beforeDataProp);
                if (!dataFile.exists() || !dataFile.isFile())
                    throw new IllegalArgumentException("Property " +
                        "before-data-file doesn't reference a valid file.");

                opts.verbose("Executing  before-data-file: " + beforeDataProp);
                JsonLoaderCloud.loadJsonFromFile(QTest.getHandle(),
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
        catch (Throwable t) {
            t.printStackTrace();
            throw new IllegalArgumentException(t);
        }
    }

    protected void executeStatements(List<String> stmts) {
        final int waitMillis = 100000;
        final int delayMillis = 1000;
        final TableLimits defLimits = new TableLimits(200000, 200000, 30);
        final int maxRetries = 3;

        for(String stmt : stmts) {
            opts.verbose("Executing: " + stmt);
            String s = stmt.toLowerCase();

            if (s.contains("namespace")) {
                doSysOp(QTest.getHandle(), stmt, waitMillis, delayMillis);
            } else {
                TableLimits limits = defLimits;
                /*
                 * This is a hack to make sure that limits are not attached
                 * to anything but create table ops. QTF does not support
                 * altering limits so alter table doesn't have them either.
                 */
                DdlType type = null;
                if (s.contains("table")) {
                    if (s.contains("drop") || s.contains("alter")) {
                        limits = null;
                        type = s.contains("drop") ? DdlType.DROP_TABLE :
                                                    DdlType.ALTER_TABLE;
                    } else {
                        type = DdlType.CREATE_TABLE;
                        if (isCreateChildTable(stmt)) {
                            limits = null;
                        }
                    }
                }
                if (s.contains("index")) {
                    limits = null;
                    type = DdlType.CREATE_INDEX;
                }

                retryTableOp(QTest.getHandle(), stmt, type, limits, waitMillis,
                             delayMillis, maxRetries);
            }

            opts.verbose("   Successful");
        }
    }

    private void retryTableOp(NoSQLHandle nosqlHanel,
                              String stmt,
                              DdlType type,
                              TableLimits limits,
                              int waitMs,
                              int delayMs,
                              int maxRetries) {
        int retries = 0;
        while (true) {
            try {
                doTableOp(QTest.getHandle(), stmt, limits, waitMs, delayMs);
                break;
            } catch (RequestTimeoutException ex) {
                if (retries++ < maxRetries) {
                    opts.verbose(" retry executing '" + stmt + "' " +
                                 (retries + 1) + " times");
                    continue;
                }
                throw ex;
            } catch (TableNotFoundException ex) {
                if (retries > 0 && type == DdlType.DROP_TABLE) {
                    opts.verbose(" got TableNotFoundException, ignore this " +
                                 "error when retry dropping table: " + stmt);
                    break;
                }
                throw ex;
            } catch (TableExistsException ex) {
                if (retries > 0 && type == DdlType.CREATE_TABLE) {
                    opts.verbose(" got TableExistsException, ignore this " +
                                 "error when retry creating table: " + stmt);
                    break;
                }
                throw ex;
            } catch (IndexExistsException ex) {
                if (retries > 0 && type == DdlType.CREATE_INDEX) {
                    opts.verbose(" got IndexExistsException, ignore this " +
                                 "error when retry creating index: " + stmt);
                    break;
                }
                throw ex;
            }
        }
    }

    private static TableResult doTableOp(NoSQLHandle nosqlHanel,
                                         String stmt,
                                         TableLimits limits,
                                         int waitMs,
                                         int delayMs) {

        TableRequest tableRequest = new TableRequest()
            .setStatement(stmt)
            .setTimeout(60000);
        if (limits != null) {
            tableRequest = tableRequest.setTableLimits(limits);
        }

        TableResult tres = QTest.getHandle().tableRequest(tableRequest);
        tres.waitForCompletion(QTest.getHandle(), waitMs, delayMs);
        return tres;
    }

    private static SystemResult doSysOp(NoSQLHandle handle,
                                        String stmt,
                                        int waitMs,
                                        int delayMs) {

        return handle.doSystemRequest(stmt, waitMs, delayMs);
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

    /* Check if the statement is to create child table */
    private boolean isCreateChildTable(String statement) {
        Matcher matcher = tableNamePattern.matcher(statement.toLowerCase());
        if (!matcher.find() || matcher.groupCount() != 1) {
            /*
             * unable to find table name or multiple occurrences in statement,
             * return the original statement.
             */
            return false;
        }

        int start = matcher.start(1);
        int end = matcher.end(1);
        String tableName = statement.substring(start, end);
        return tableName.contains(".");
    }
}
