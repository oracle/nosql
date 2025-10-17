/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import static oracle.nosql.proxy.ProxyTestBase.getEffectiveMaxReadKB;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import oracle.nosql.common.qtf.FileUtils;
import oracle.nosql.common.qtf.QTCase;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;
import oracle.nosql.proxy.ProxyTestBase;

/**
 * Class representing a single test case.
 */
public class QTCaseCloud extends QTCase {

    NoSQLHandle handle;

    boolean isOnPrem;

    void setHandle(NoSQLHandle handle) {
        this.handle = handle;
    }

    void setIsOnPrem() {
        isOnPrem = true;
    }

    @Override
    public void run()
        throws IOException {
        boolean r = runQuery();
        Assert.assertTrue("QTCase failure: " + this, r);
    }

    @Override
    public boolean runQuery()
        throws IOException {

        String query = FileUtils.readFileToString(queryFile).trim();
        String queryName = opt.relativize(getQueryFile());

        String updQuery = null;
        int updPos = query.indexOf("update");
        int insPos = query.indexOf("insert");
        int delPos = query.indexOf("delete");
        int selPos = query.indexOf("select");
        boolean isUpdateQuery = (updPos >= 0);

        updPos = (insPos >= 0 ? insPos : (delPos >= 0 ? delPos : updPos));

        if (updPos >= 0 && selPos >= 0 && selPos > updPos) {
            updQuery = query.substring(0, selPos);
            query = query.substring(selPos);
        }

        String expectedResult = null;
        boolean haveExpectedResult = false;
        boolean generatedExpectedResult = false;

        if (resultFile.exists()) {
            haveExpectedResult = true;
            expectedResult = skipComments(
                FileUtils.readFileToString(getResultFile()));
        }

        boolean checkQueryPlan = run.compileOnly;

        //System.out.println("Executing query: " + queryName);
        opt.progress("Executing query: " + queryName);
        opt.verbose("   Query: '" + query);

        if (handle == null) {
            throw new IllegalStateException("No handle available.");
        }

        boolean ret = false;
        boolean finished = false;

        PreparedStatement selPrep = null;
        PreparedStatement updPrep = null;
        PreparedStatement prep = null;

        QueryRequest selReq = new QueryRequest();
        selReq.setConsistency(Consistency.ABSOLUTE);
        selReq.setQueryName(queryName);
        selReq.setTraceLevel(3/*opt.getTraceLevel()*/);
        selReq.setLogFileTracing(false);
        selReq.setInTestMode(true);
        selReq.setMaxServerMemoryConsumption(200*1024);
        /*
        if (queryFile.getPath().contains("inner_joins/q/q23")) {
            selReq.setLogFileTracing(true);
            selReq.setTraceLevel(3);
        }
        */
        if (isOnPrem) {
            /*
             * The fix to KVSTORE-1228 was included into 21.3.1 but not in
             * earlier versions, the configured kv in proxy is still 21.2.19,
             * setting the batch limit may lead to some unnest test failures,
             * so set the limit only when run against kv >= 21.3.1
             */
            if (ProxyTestBase.checkKVVersion(21, 3, 1)) {
                if (!isUpdateQuery) {
                    selReq.setLimit(opt.getBatchSize());
                }
            }
        } else if (opt.getReadKBLimit() == 0) {
            /* Use specific read limit, which exhibits the bug fixed in
             * SR [#27735] */
            if (queryFile.getPath().contains("prim_index_sort/q/sort1")) {
                selReq.setMaxReadKB(135);
            } else {
                if (!isUpdateQuery) {
                    selReq.setMaxReadKB(15);
                }
            }
        } else {
            if (!isUpdateQuery) {
                selReq.setMaxReadKB(opt.getReadKBLimit());
            }
            //System.out.println("Executing query: " + queryName +
            //                   " with readKBLimit " + opt.getReadKBLimit());
        }

        if (queryFile.getPath().contains("offsetlimit")) {
            selReq.setMaxMemoryConsumption(600000);
        } else if (queryFile.getPath().contains("prim_index_sort")) {
            selReq.setMaxMemoryConsumption(250000);
        } else if (queryFile.getPath().contains("nonulls_idx")) {
            selReq.setMaxMemoryConsumption(1000000);
        } else if (queryFile.getPath().contains("idc_geojson")) {
            selReq.setMaxMemoryConsumption(650000);
        } else if (queryFile.getPath().contains("join")) {
            selReq.setMaxMemoryConsumption(600000);
            selReq.setTimeout(10000);
        } else {
            selReq.setMaxMemoryConsumption(60000);
        }

        QueryRequest updReq = null;
        if (updQuery != null) {
            updReq = new QueryRequest();
            updReq.setTraceLevel(opt.getTraceLevel());
            if (!isUpdateQuery) {
                updReq.setMaxReadKB(opt.getReadKBLimit());
            }
            selReq.setConsistency(Consistency.ABSOLUTE);
        }

        PrepareRequest preq = new PrepareRequest();
        preq.setGetQueryPlan(true);
        PrepareResult pres;

        try {
            try {
                if (updQuery != null && updReq != null) {
                    preq.setStatement(updQuery);
                    pres = handle.prepare(preq);
                    updPrep = pres.getPreparedStatement();
                    updReq.setPreparedStatement(updPrep);

                    preq.setStatement(query);
                    pres = handle.prepare(preq);
                    selPrep = pres.getPreparedStatement();
                    selReq.setPreparedStatement(selPrep);

                    prep = updPrep;
                } else {
                    preq.setStatement(query);
                    pres = handle.prepare(preq);
                    selPrep = pres.getPreparedStatement();
                    selReq.setPreparedStatement(selPrep);

                    prep = selPrep;
                }
            } catch (Exception e) {
                if (haveExpectedResult) {
                    ret = checkException(true, expectedResult, e);
                    finished = true;
                } else {
                    opt.failure("Missing expected result file and exception " +
                        "during prepare: " + e.getMessage() +
                        "\n" + getStackTrace(e));
                    throw e;
                }
            }

            if (prep != null && checkQueryPlan) {

                if (!haveExpectedResult) {
                    String queryPlan = prep.getQueryPlan();

                    genTmpFile(COMPILED_QUERY_PLAN, queryPlan);

                    generatedExpectedResult = true;
                } else {
                    ret = checkQueryPlan(prep, expectedResult);
                }

                finished = true;
            }

            if (!finished) {
                bindVars(updQuery, query, prep);

                if (!haveExpectedResult) {
                    StringBuffer sb = new StringBuffer();

                    do {
                        QueryResult res = handle.query(selReq);
                        List<MapValue> list = res.getResults();
                        for (MapValue val : list) {
                            sb.append(val.toString()).append("\n");
                        }
                    } while (!selReq.isDone());

                    genTmpFile(UNORDERED_RESULT, sb.toString());

                    generatedExpectedResult = true;
                } else {
                    ret = checkResults(updReq, selReq, expectedResult);
                }
            }

            if (ret) {
                opt.progress("  Passed");

            } else if (generatedExpectedResult) {
                File tmpResultFile =
                    new File(resultFile.getAbsolutePath() + ".tmp");
                opt.failure("Missing result file: " +
                            opt.relativize(resultFile) +
                            " generated expected file " +
                            opt.relativize(tmpResultFile) + ".");

            } else {
                opt.failure("  FAILED !!! results don't match.");
                if (prep != null && !checkQueryPlan) {
                    opt.failure("\tQuery Plan:\n" + prep);
                }
                if (!checkQueryPlan) {
                    if (updReq != null) {
                       updReq.printTrace(System.out);
                    } else {
                        selReq.printTrace(System.out);
                    }
                }
            }

            return ret;

        } catch (Exception e) {
            String st = getStackTrace(e);
            opt.failure("  FAILED !!! with exception.");
            opt.failure("    Exception: " + e.getMessage() + "\n" + st +
                        "\n\n\tQuery plan:\n" + prep);
            genActualFile("FAILED !!! with exception:", "Exception: " +
                          e.getMessage() + "\n" + st + "\n\n\tQuery plan:\n" + prep);
            return false;
        } catch (Throwable e) {
            opt.failure("  FAILED !!! with throwable.");
            opt.failure("    Throwable: " + e.getMessage() +
                        "\n\n\tQuery plan:\n" + prep);
            e.printStackTrace();
            return false;
        } finally {
            if (queryFile.getPath().contains("delete")) {
                getRun().suite.after();
                getRun().suite.before();
            }
        }
    }

    private void bindVars(
        String updQuery,
        String query,
        PreparedStatement prep) {

        if (updQuery != null) {
            query = updQuery;
        }

        int insPos = query.indexOf("insert");
        if (insPos < 0) {
            insPos = query.indexOf("update");
        }

        for (Map.Entry<String, String> entry : run.suite.vars.entrySet()) {

            String qtfName = entry.getKey();
            String varName = qtfName;
            int varPos = -1;

            if (varName.startsWith("$$")) {

                int pos = varName.indexOf('_');
                String queryName = varName.substring(pos+1);

                if (!queryName.equals(queryFile.getName())) {
                    continue;
                }

                String varPosStr = varName.substring(2, pos);
                varPos = Integer.parseInt(varPosStr);

                varName = varName.substring(0, pos);

            } else {
                int idx = query.indexOf(varName);
                if (idx < 0 ||
                    (insPos < 0 &&
                     query.charAt(idx + varName.length()) != ' ')) {
                    continue;
                }
            }

            String strValue = run.suite.vars.get(qtfName);
            String strType = ((QTSuiteCloud)run.suite).varsType.get(qtfName);
            String strDeclType = run.suite.varsDeclType.get(qtfName);
            FieldValue.Type type = null;
            FieldValue value;

            if (strDeclType != null) {
                if (strDeclType.equals("int")) {
                    type = Type.INTEGER;
                } else if (strDeclType.equals("long")) {
                    type = Type.LONG;
                } else if (strDeclType.equals("double")) {
                    type = Type.DOUBLE;
                } else if (strDeclType.equals("number")) {
                    type = Type.NUMBER;
                } else if (strDeclType.equals("string")) {
                    type = Type.STRING;
                } else if (strDeclType.equals("boolean")){
                    type = Type.BOOLEAN;
                } else if (strDeclType.equals("json")) {
                    type = Type.MAP;
                } else {
                    throw new IllegalArgumentException(
                        "Unknown bind variable type in test.config file. " +
                        "Variable name: " + varName + " type: " + strDeclType);
                }

                value = createValueFromString(strValue, type);

            } else {
                if (strType == null) {
                    type = Type.NULL;
                } else if (strType.equals("jnull")) {
                    type = Type.JSON_NULL;
                } else if (strType.equals("integer")){
                    type = Type.INTEGER;
                } else if (strType.equals("long")){
                    type = Type.LONG;
                } else if (strType.equals("double")){
                    type = Type.DOUBLE;
                } else if (strType.equals("number")) {
                    type = Type.NUMBER;
                } else if (strType.equals("json")) {
                    type = Type.MAP;
                } else if (strType.equals("array")) {
                    type = Type.ARRAY;
                } else if (strType.equals("boolean")){
                    type = Type.BOOLEAN;
                } else {
                    type = Type.STRING;
                }

                value = createValueFromString(strValue, type);
            }

            opt.verbose(" Bind var: " + varName + " = " + value);

            try {
                if (varPos >= 0) {
                    prep.setVariable(varPos + 1, value);
                } else {
                    prep.setVariable(varName, value);
                }
            } catch (IllegalArgumentException e) {
                continue;
            }
        }
    }

    private boolean checkQueryPlan(
        PreparedStatement ps,
        String expectedResult) throws IOException {

        expectedResult = skipComments(expectedResult);

        // find first line of the rest
        String resultType;

        do {
            int i = expectedResult.indexOf(EOL);
            if (i < 0) {
                throw new IllegalArgumentException("Invalid result file: " +
                    getOpt().relativize(resultFile));
            }

            resultType = expectedResult.substring(0, i).trim().toLowerCase();
            expectedResult = expectedResult.substring(i + 1);
        } while (resultType.length() == 0);

        String expected = expectedResult.trim();

        if (!COMPILED_QUERY_PLAN.equals(resultType)) {
            throw new IllegalStateException("checkQueryPlan() should be " +
                "called only for results with 'compiled-query-plan'");
        }

        String actual = ps.getQueryPlan().trim();
        boolean pass = expected.equals(actual);
        String act = actual;
        String exp = expected;
        String aLine = null;
        String eLine = null;

        if (!pass) {
            pass = true;
            // check line by line
            do {
                int aIndex = act.indexOf(EOL);
                int eIndex = exp.indexOf(EOL);

                if (aIndex == eIndex && aIndex < 0) {
                    break;
                } else if (aIndex < 0 || eIndex < 0) {
                    pass = false;
                    break;
                }

                aLine = act.substring(0, aIndex);
                eLine = exp.substring(0, eIndex);

                if (!aLine.trim().equals(eLine.trim())) {
                    pass = false;
                    break;
                }
                act = act.substring(aIndex + 1);
                exp = exp.substring(eIndex + 1);
            } while (true);

            if (!pass) {

                if (opt.isUpdateQueryPlans()) {
                    updateQueryPlanInResultFile(actual);
                    pass = true;
                } else {
                    opt.failure("Test FAILED: " + opt.relativize(queryFile));

                    opt.failure("Query compilation plan not matching: " +
                            "\nEXPECTED:\n" + expected + "\n" +
                            "\nACTUAL:\n" + actual + "\n");

                    if (aLine != null && eLine != null) {
                        opt.failure("\nexpected line: " + eLine +
                                "\nactual line:   " + aLine + "\n");
                    }
                    genActualFile(COMPILED_QUERY_PLAN, actual);
                }
            }
        }

        return pass;
    }

    private boolean checkResults(
        QueryRequest updReq,
        QueryRequest selReq,
        String expectedResult) {

        expectedResult = skipComments(expectedResult);

        // find first line of the rest
        int i = expectedResult.indexOf(EOL);
        if (i < 0) {
            throw new IllegalArgumentException("Invalid result file: " +
                getOpt().relativize(resultFile));
        }

        String resultType = expectedResult.substring(0, i).trim()
            .toLowerCase();
        String results = expectedResult.substring(i + 1).trim();

        if (UNORDERED_RESULT.equals(resultType)) {
            return checkUnorderedResults(updReq, selReq, results);

        } else if (ORDERED_RESULT.equals(resultType)) {
            return checkOrderedResults(updReq, selReq, results);

        } else if (RUNTIME_EXCEPTION.equals(resultType)) {

            QueryRequest req = (updReq != null ? updReq : selReq);

            do {
                try {
                    QueryResult res = handle.query(req);
                    res.getResults();
                } catch (Exception e) {
                    return checkException(false, expectedResult, e);
                }
            } while (!req.isDone());

            opt.failure("  Test " + this + " FAILED:" +
                "\nA runtime exception was expected, but none was raised.");
            return false;

        } else {
            throw new IllegalArgumentException(
                "Invalid result file: " +
                getOpt().relativize(resultFile) +
                "\nResult type: " + resultType);
        }
    }

    private boolean checkUnorderedResults(
        QueryRequest updReq,
        QueryRequest selReq,
        String expectedResult) {
        return checkEachResult(updReq, selReq, expectedResult, false);
    }

    private boolean checkOrderedResults(
        QueryRequest updReq,
        QueryRequest selReq,
        String expectedResult) {
        return checkEachResult(updReq, selReq, expectedResult, true);
    }

    private boolean checkEachResult(
        QueryRequest updReq,
        QueryRequest selReq,
        String expectedResult,
        boolean ordered) {

        String[] expected = expectedResult.split(EOL);
        int numExpected = expected.length;

        if (expected.length == 1 && expected[0].equals("")) {
            expected[0] = null;
            numExpected = 0;
        }

        MapValue[] expectedVals = new MapValue[expected.length];

        JsonOptions options = new JsonOptions();
        options.setAllowNonNumericNumbers(true);
        options.setMaintainInsertionOrder(true);

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != null) {
                expectedVals[i] = (MapValue)
                    JsonUtils.createValueFromJson(expected[i], options);
            }
        }

        String completeResult = "";
        int numMatched = 0;
        boolean failed = false;
        int totalReadKB = 0;
        int j = 0;

        if (updReq != null) {
            do {
                QueryResult res = handle.query(updReq);
                res.getResults();
            } while (!updReq.isDone());
        }

        /* These tests have non-deterministic results. We can only check that
         * the number of results is the expected one */
        if (queryFile.getPath().contains("gb/q/noidx09") ||
            queryFile.getPath().contains("gb/q/noidx12") ||
            queryFile.getPath().contains("gb/q/noidx15") ||
             queryFile.getPath().contains("gb/q/distinct02")) {

            int numActual = 0;

            do {
                QueryResult res = handle.query(selReq);
                List<MapValue> list = res.getResults();
                numActual += list.size();
            } while (!selReq.isDone());

            if (numActual != numExpected) {
                opt.failure(
                "  Test " + this + " FAILED:" +
                    "\n    Unexpected number of results: " +
                    "\n    expected size: " + numExpected +
                    "\n    actual size:   " + numMatched);
                failed = true;
            }

            return !failed;
        }

        do {
            QueryResult res = handle.query(selReq);
            List<MapValue> list = res.getResults();

            totalReadKB += res.getReadKB();

            /* This assert assumes that the max row size is 3KB. If larger rows
             * are ever used, the assert needs to be modified */
            int maxReadKB;
            if (queryFile.getPath().contains("join")) {
                maxReadKB = getEffectiveMaxReadKB(selReq) + 5;
            } else if (queryFile.getPath().contains("idc_geojson")) {
                /* The max row size of idc_geojson is 13KB */
                maxReadKB = getEffectiveMaxReadKB(selReq) + 13;
            } else {
                maxReadKB = getEffectiveMaxReadKB(selReq) + 3;
            }
            assertTrue("maxReadKB=" + maxReadKB + ", readKB=" + res.getReadKB(),
                       res.getReadKB() <= maxReadKB);

            for (MapValue val1 : list) {

                MapValue val2 = (MapValue)JsonUtils.createValueFromJson(
                    val1.toString(), options);

                boolean found = false;

                if (!ordered) {
                    for (int i = 0; i < expectedVals.length; ++i) {

                        if (expectedVals[i] != null) {
                            int ret = -1;
                            try {
                                ret = val2.compareTo(expectedVals[i]);
                            } catch (Exception e) {}

                            if (ret == 0) {
                                expectedVals[i] = null;
                                ++numMatched;
                                found = true;
                                break;
                            }
                        }
                    }
                } else {
                    int ret = -1;
                    try {
                        ret = val2.compareTo(expectedVals[j]);
                    } catch (Exception e) {}

                    if (ret == 0) {
                        expectedVals[j] = null;
                        ++numMatched;
                        found = true;
                    }
                    ++j;
                }

                if (!failed && !found) {
                    opt.failure(
                        "  Test " + this + " FAILED:" +
                        "\n    unexpected actual result:  '" + val2 + "'" );
                    failed = true;
                }

                completeResult =
                    completeResult +
                    (completeResult.length() == 0 ? "" : "\n") +
                    val2;
            }

        } while (!selReq.isDone());
/*
        if (!opt.getOnPrem() &&
            (queryFile.getPath().contains("sec_index/q/sort11") ||
             queryFile.getPath().contains("sec_index/q/q16") ||
             queryFile.getPath().contains("prim_index_sort/q/sort5") ||
             queryFile.getPath().contains("prim_index/q/q18"))) {
            assert(totalReadKB == 1);
        }
*/
        if (opt.getTraceLevel() > 0 && !selReq.getLogFileTracing()) {
            selReq.printTrace(System.out);
        }

        if (!failed && numMatched != numExpected) {
            opt.failure(
                "  Test " + this + " FAILED:" +
                    "\n    Fewer than expected results: " +
                    "\n    expected size: " + numExpected +
                    "\n    actual size:   " + numMatched);
            failed = true;
        }

        if (failed) {
            opt.failure(
                "    expected unordered: '\n" + expectedResult + "'" +
                    "\n    actual:           '\n" + completeResult + "'" +
                    "\n    not matching: " + (numExpected - numMatched));
            genActualFile(UNORDERED_RESULT, completeResult);
        }

        return !failed;
    }

    public static FieldValue createValueFromString(
        String value,
        final Type type) {

        final InputStream jsonInput;

        switch (type) {
        case JSON_NULL:
            return JsonNullValue.getInstance();
        case NULL:
            return NullValue.getInstance();
        case STRING:
            return new StringValue(value);
        case INTEGER:
            return new IntegerValue(Integer.parseInt(value));
        case LONG:
            return new LongValue(Long.parseLong(value));
        case DOUBLE:
            return new DoubleValue(Double.parseDouble(value));
        case NUMBER:
            return new NumberValue(new BigDecimal(value));
        case BOOLEAN:
            return BooleanValue.getInstance(Boolean.parseBoolean(value));
        case BINARY:
            return new BinaryValue(value.getBytes());
        case TIMESTAMP:
            return new TimestampValue(value);
        case ARRAY:
        case MAP:
            jsonInput =  new ByteArrayInputStream(value.getBytes());
            return FieldValue.createFromJson(jsonInput, new JsonOptions().
                    setMaintainInsertionOrder(true));
        default:
            throw new IllegalArgumentException(
                "Type not yet implemented: " + type);
        }
    }
}
