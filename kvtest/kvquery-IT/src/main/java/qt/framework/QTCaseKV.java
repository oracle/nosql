/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package qt.framework;

//import java.io.ByteArrayInputStream;
import java.io.File;
//import java.io.InputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

//import com.fasterxml.jackson.core.JsonParser;

import oracle.kv.Consistency;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.JsonCollectionRowImpl;
import oracle.kv.impl.api.table.NullJsonValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.RecordValue;
import oracle.nosql.common.qtf.FileUtils;
import oracle.nosql.common.qtf.QTCase;

import org.junit.Assert;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Class representing a single test case.
 */
public class QTCaseKV extends QTCase {

    private static DisplayFormatter dfmt;
    /*
     * TODO:
     * Use customized DisplayFormatter with timestampWithZone = false to print
     * result to json string, this is because "Z" has been appended to default
     * timestamp string, but we can't change the strings in expected results.
     * Because the test cases are used by proxy as well and appending final "Z"
     * can't be propagated to proxy as driver like python does not accept such
     * strings.
     */
    static {
        dfmt = new DisplayFormatter(0 /* increment */,
                                    false /* pretty */,
                                    false /* timestampWithZone*/);
    }

    KVStoreImpl store;

    void setStore(KVStoreImpl store) {
        this.store = store;
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

        String queryName = opt.relativize(getQueryFile());

        String query = FileUtils.readFileToString(getQueryFile()).trim();

        String updQuery = null;
        int updPos = query.indexOf("update");
        int insPos = query.indexOf("insert");
        int delPos = query.indexOf("delete");
        int selPos = query.indexOf("select");

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

        boolean useAsync = opt.useAsync();
        boolean checkQueryPlan = run.compileOnly;

        if (useAsync) {
            //System.out.println("Executing async query: " + queryName);
            opt.progress("Executing async query: " + queryName);
        } else {
            //System.out.println("Executing query: " + queryName);
            opt.progress("Executing query: " + queryName);
        }
        opt.verbose("   Query: '" + query + "'");

        if (store == null) {
            throw new IllegalStateException("No store available.");
        }

        ExecuteOptions options = new ExecuteOptions();

        options.setMaxMemoryConsumption(200 * 1024);
        options.setMaxServerMemoryConsumption(1024);
        options.setTraceLevel(opt.getTraceLevel());
        options.setDeleteLimit(5);
        options.setAsync(false);
        options.setQueryName(queryName);
        options.setDoLogFileTracing(false);
        options.setInTestMode(true);

        if (queryName.contains("delete")) {
            options.setConsistency(Consistency.ABSOLUTE);
        }
        /*
        if (queryName.contains("joins4/q/d07")) {
            options.setTraceLevel((byte)3);
        }
        */
        options.setResultsBatchSize(opt.getBatchSize());

        PreparedStatement qps = null;
        PreparedStatement ups = null;
        PreparedStatementImpl ps = null;
        StatementResult qsr = null;
        boolean res = false;
        boolean finished = false;

        try {

            try {
                if (updQuery != null) {
                    ups = store.prepare(updQuery, options);
                    qps = store.prepare(query, options);
                    ps = (PreparedStatementImpl)ups;
                } else {
                    qps = store.prepare(query, options);
                    ps = (PreparedStatementImpl)qps;
                }
            } catch (Exception e) {
                if (haveExpectedResult) {
                    res = checkException(true, expectedResult, e);
                    finished = true;
                } else {
                    opt.failure("Missing expected result file and exception " +
                        "during prepare: " + e.getMessage() +
                        "\n" + getStackTrace(e));
                    throw e;
                }
            }

            if (ps != null && qps != null && checkQueryPlan) {

                if (!haveExpectedResult) {
                    String queryPlan = displayQueryPlan(ps);
                    genTmpFile(COMPILED_QUERY_PLAN, queryPlan);
                    generatedExpectedResult = true;
                } else {
                    res = checkQueryPlan(ps, expectedResult);
                }

                finished = true;
            }

            if (!finished) {

                @SuppressWarnings("null")
                BoundStatement bs = ps.createBoundStatement();
                Publisher<RecordValue> pub = null;

                //long t = System.currentTimeMillis();
                //System.out.println("Query " + queryName + "starting at time: ");
                //System.out.println(java.time.Clock.systemUTC().instant());

                try {
                    bindVariables(bs);

                    if (updQuery != null) {
                        store.executeSync(bs, options);
                        qsr = store.executeSync(qps, options);
                    } else if (useAsync && haveExpectedResult) {
                        pub = store.executeAsync(bs, options);
                    } else {
                        qsr = store.executeSync(bs, options);
                    }

                } catch (Exception e) {
                    if (haveExpectedResult) {
                        res = checkException(false, expectedResult, e);
                        finished = true;
                    } else {
                        //t = System.currentTimeMillis() - t;
                        //System.out.println("Query " + queryName +
                        //                   " completed in " + t + " msec.\n");

                        opt.failure("Missing expected result file and exception " +
                            "during execute: " + e.getMessage() +
                            "\n" + getStackTrace(e));
                        throw e;
                    }
                }

                if (!finished) {
                    if (qsr != null && !haveExpectedResult) {

                        StringBuffer sb = new StringBuffer();

                        Iterator<RecordValue> iter = qsr.iterator();

                        while (iter.hasNext()) {
                            RecordValue rec = iter.next();
                            sb.append(rec.toJsonString(false)).append("\n");
                        }

                        //t = System.currentTimeMillis() - t;
                        //System.out.println("Query " + queryName +
                        //                   " completed in " + t + " msec.\n");

                        genTmpFile(UNORDERED_RESULT, sb.toString());
                        generatedExpectedResult = true;

                    } else {
                        res = checkResults(qsr, pub, expectedResult);
                    }
                }
            }

            if (qsr != null && options.getTraceLevel() > 0) {
                qsr.printTrace(System.out);
            }

            if (res) {
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
                if (ps != null && !checkQueryPlan) {
                    opt.failure("\tQuery Plan:\n" + ps);
                }
            }

            return res;

        } catch (Exception e) {
            String st = getStackTrace(e);
            opt.failure("FAILED !!! with exception: \n");
            opt.failure(e.getMessage() + "\n" + st);
            if (ps != null) {
                opt.failure("\n\n\tQuery plan:\n" + ps.getQueryPlan().display(true));
            }
            genActualFile("FAILED !!! with exception:", "Exception: " +
                          e.getMessage() + "\n" + st + "\n\n\tQuery plan:\n" + ps);
            return false;

        } finally {
            if (queryFile.getPath().contains("delete")) {
                getRun().suite.after();
                getRun().suite.before();
            }
        }
    }

    private void bindVariables(BoundStatement bs) {

        Map<String, FieldDef> vars = bs.getVariableTypes();

        for(Map.Entry<String, FieldDef> entry : vars.entrySet()) {

            String origName = entry.getKey();
            String name = origName;
            int pos = -1;

            if (name.startsWith("$$")) {
                name = name + "_" + queryFile.getName();
                String posStr = origName.substring(2);
                pos = Integer.parseInt(posStr);
            }

            if (!run.suite.vars.containsKey(name) ) {
                throw new IllegalArgumentException("Variable: " +
                    name + " not defined in the test suite.");
            }

            String strValue = run.suite.vars.get(name);
            String strType = run.suite.varsType.get(name);
            String strDeclType = run.suite.varsDeclType.get(name);
            FieldDefImpl type;
            FieldValue value;

            if (strValue == null) {
                value = NullValueImpl.getInstance();

            } else if (strType.equals("jnull")) {
                value = NullJsonValueImpl.getInstance();

            } else if (strDeclType != null) {
                if (strDeclType.equals("int")) {
                    type = FieldDefImpl.Constants.integerDef;
                } else if (strDeclType.equals("long")) {
                    type = FieldDefImpl.Constants.longDef;
                } else if (strDeclType.equals("float")) {
                    type = FieldDefImpl.Constants.floatDef;
                } else if (strDeclType.equals("double")) {
                    type = FieldDefImpl.Constants.doubleDef;
                } else if (strDeclType.equals("number")) {
                    type = FieldDefImpl.Constants.numberDef;
                } else if (strDeclType.equals("string")) {
                    type = FieldDefImpl.Constants.stringDef;
                } else if (strDeclType.equals("boolean")){
                    type = FieldDefImpl.Constants.booleanDef;
                } else if (strDeclType.equals("json")) {
                    type = FieldDefImpl.Constants.jsonDef;
                } else {
                    throw new IllegalArgumentException(
                        "Unknown bind variable type in test.config file. " +
                        "Variable name: " + name + " type: " + strDeclType);
                }

                value = FieldDefImpl.createValueFromString(strValue, type);

            } else {

                type = (FieldDefImpl)entry.getValue();

                if (type.isAny()) {
                    if (strType.equals("number")) {
                        type = FieldDefImpl.Constants.numberDef;
                    } else if (strType.equals("string")) {
                        type = FieldDefImpl.Constants.stringDef;
                    } else if (strType.equals("json")) {
                        type = FieldDefImpl.Constants.jsonDef;
                    } else if (strType.equals("array")) {
                        type = FieldDefImpl.Constants.jsonDef;
                    } else if (strType.equals("boolean")){
                        type = FieldDefImpl.Constants.booleanDef;
                    } else {
                        type = FieldDefImpl.Constants.stringDef;
                    }
                }

                value = FieldDefImpl.createValueFromString(strValue, type);
            }

            if (pos >= 0) {
                bs.setVariable(pos+1, value);
            } else {
                bs.setVariable(origName, value);
            }
        }
    }


    private boolean checkQueryPlan(
        PreparedStatementImpl ps,
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

        String actual = displayQueryPlan(ps).trim();

        boolean pass = expected.equals(actual);
        String act = actual;
        String exp = expected;
        String aLine = null;
        String eLine = null;
        int lno = 1;

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
                ++lno;
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
                                    "\nactual line:   " + aLine + "\n" +
                                    "at line " + lno + "\n");
                    }
                    genActualFile(COMPILED_QUERY_PLAN, actual);
                }
            }
        }

        return pass;
    }

    private String displayQueryPlan(PreparedStatementImpl ps) {

        String queryPlan = ps.getQueryPlan().display(false).trim();

        StringBuilder sb = new StringBuilder();
        sb.append("{\n\"query file\" : \"");
        sb.append(opt.relativize(queryFile)).append("\",\n");
        sb.append("\"plan\" : \n").append(queryPlan);
        sb.append("\n}\n");

        queryPlan = sb.toString();

        /*
        InputStream planStream = new ByteArrayInputStream(queryPlan.getBytes());
        try {
            JsonParser parser = JsonUtils.createJsonParser(planStream);
            parser.readValueAsTree();
        } catch (Throwable e) {
            System.out.println("Failed to parse query plan.\n" + e);
        }
        */
        return queryPlan;
    }

    private boolean checkResults(
        StatementResult sr,
        Publisher<RecordValue> pub,
        String expectedResult) {

        expectedResult = skipComments(expectedResult);

        // find first line of the rest
        int i = expectedResult.indexOf(EOL);
        if (i < 0) {
            throw new IllegalArgumentException("Invalid result file: " +
                getOpt().relativize(resultFile));
        }

        String expResultType = expectedResult.substring(0, i).trim()
            .toLowerCase();
        String expResults = expectedResult.substring(i + 1).trim();

        if (UNORDERED_RESULT.equals(expResultType)) {

            if (pub != null) {
                return checkResultsAsync(pub, false, false, expResults);
            }
            return checkUnorderedResults(sr, expResults);

        } else if (ORDERED_RESULT.equals(expResultType)) {

            if (pub != null) {
                return checkResultsAsync(pub, true, false, expResults);
            }
            return checkOrderedResults(sr, expResults);

        } else if (RUNTIME_EXCEPTION.equals(expResultType)) {

            if (pub != null) {
                return checkResultsAsync(pub, false, true, expResults);
            }

            try {
                Iterator<RecordValue> iter = sr.iterator();
                while (iter.hasNext()) {
                    iter.next();
                }
            } catch (Exception e) {
                return checkException(false, expectedResult, e);
            }

            opt.failure("  Test " + this + " FAILED:" +
                        "\nA runtime exception was expected, but none was raised.");
            return false;

        } else {
            throw new IllegalArgumentException(
                "Invalid result file: " +
                getOpt().relativize(resultFile) +
                "\n    Expected result type: " + expResultType +
                "\n    Actual result success: " + sr.isSuccessful());
        }
    }

    /**
     * @return true if result consistent with expected, false otherwise
     */
    private boolean checkUnorderedResults(
        StatementResult sr,
        String expectedResult) {

        checkSuccessAndQueryKind(sr);

        String[] expected = expectedResult.split(EOL);
        int numExpected = expected.length;
        int numActual = 0;

        if (expected.length == 1 && expected[0].equals("")) {
            expected[0] = null;
            numExpected = 0;
        }

        String result, completeResult = "";
        int numMatched = 0;
        boolean failed = false;
        String queryName = opt.relativize(getQueryFile());

        /* These tests have non-deterministic results. We can only check that
         * the number of results is the expected one */
        if (queryName.contains("gb/q/noidx09") ||
            queryName.contains("gb/q/noidx12") ||
            queryName.contains("gb/q/noidx15") ||
            queryName.contains("gb/q/distinct02")) {

            for (RecordValue value : sr) {
                result = value.toString();
                ++numActual;
            }

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

        for (RecordValue value : sr) {

            result = toJsonString(value);
            ++numActual;
            boolean orderedFields = (value instanceof JsonCollectionRowImpl) ?
                false : true;
            boolean found = false;
            for (int i = 0; i < expected.length; ++i) {
                if (expected[i] != null &&
                    compareJson(result, expected[i].trim(), orderedFields)) {
                    expected[i] = null;
                    ++numMatched;
                    found = true;
                    break;
                }
            }

            if (!failed && !found) {
                opt.failure("  Test " + this + " FAILED:" +
                            "\n    unexpected actual result:  '" + result + "'" );
                failed = true;
            }

            completeResult =
                completeResult +
                (completeResult.length() == 0 ? "" : "\n") +
                result;
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

    /**
     * @return true if result consistent with expected, false otherwise
     */
    private boolean checkOrderedResults(
        StatementResult sr,
        String expectedResult) {

        checkSuccessAndQueryKind(sr);

        String[] expected = expectedResult.split(EOL);
        int numExpected = expected.length;

        if (expected.length == 1 && expected[0].equals("")) {
            expected[0] = null;
            numExpected = 0;
        }

        String result, completeResult = "";
        int numMatched = 0;
        boolean failed = false;
        int i = 0;

        for (RecordValue value : sr) {

            result = toJsonString(value);
            boolean orderedFields = (value instanceof JsonCollectionRowImpl) ?
                false : true;

            if (i < numExpected &&
                expected[i] != null &&
                compareJson(result, expected[i].trim(), orderedFields)) {
                expected[i] = null;
                ++numMatched;
            } else {
                opt.failure(
                    "  Test " + this + " FAILED:" +
                    "\n    unexpected actual result:  '" + result + "'" );
                failed = true;
            }

            completeResult = completeResult +
                (completeResult.length() == 0 ? "" : "\n") + result;
            ++i;
        }

        if (!failed && numMatched != numExpected) {
            opt.failure(
                "  Test " + this + " FAILED:" +
                    "\n    More than expected results: " +
                    "\n    expected size: " + numExpected +
                    "\n    actual size:   " + numMatched);
            failed = true;
        }

        if (failed) {
            opt.failure(
                "    expected ordered: '" + expectedResult + "'" +
                    "\n    actual:         '" + completeResult + "'" +
                    "\n    not matching: " + (numExpected -
                    numMatched));
            genActualFile(ORDERED_RESULT, completeResult);
        }

        return !failed;
    }

    private boolean checkResultsAsync(
        final Publisher<RecordValue> publisher,
        boolean ordered,
        boolean failureExpected,
        String expectedResult) {

        final QTCaseKV qtCase = this;

        final String[] expected = expectedResult.split(EOL);
        final int numExpected;

        if (expected.length == 1 && expected[0].equals("")) {
            expected[0] = null;
            numExpected = 0;
        } else if (!failureExpected) {
            numExpected = expected.length;
        } else {
            numExpected = 0;
        }

        final boolean checkNumResultsOnly;
        if (queryFile.getPath().contains("gb/q/noidx09") ||
            queryFile.getPath().contains("gb/q/noidx12") ||
            queryFile.getPath().contains("gb/q/noidx15") ||
            queryFile.getPath().contains("gb/q/distinct02")) {
            checkNumResultsOnly = true;
        } else {
            checkNumResultsOnly = false;
        }

        class QTFSubscriber implements Subscriber<RecordValue> {

            final static int theResultBatchSize = 8;

            Subscription theSubscription;

            int theNumResults;

            int theNumMatched;

            String theFullResultStr = "";

            boolean theDone;

            boolean theHaveUnexpectedResult;

            boolean theFailed;

            @Override
            public void onSubscribe(Subscription s) {

                theSubscription = s;
                s.request(theResultBatchSize);
            }

            @Override
            public void onNext(RecordValue result) {

                if (theNumResults % theResultBatchSize == 0) {
                    theSubscription.request(theResultBatchSize);
                }

                if (checkNumResultsOnly) {
                    ++theNumResults;
                    ++theNumMatched;
                    return;
                }

                if (failureExpected) {
                    return;
                }

                String resultStr = toJsonString(result);
                boolean orderedFields =
                    (result instanceof JsonCollectionRowImpl) ? false : true;

                theFullResultStr = (theFullResultStr +
                                    (theFullResultStr.length() == 0 ?
                                     "" : "\n") + resultStr);

                if (ordered) {
                    if (theNumResults < numExpected &&
                        expected[theNumResults] != null &&
                        compareJson(resultStr,
                                    expected[theNumResults].trim(),
                                    orderedFields)) {
                        expected[theNumResults] = null;
                        ++theNumMatched;
                    } else {
                        opt.failure(
                            "Test " + qtCase + " FAILED:\n" +
                            "unexpected actual result: " + result +
                            "\nexpected result         : " +
                            expected[theNumResults]);
                        theHaveUnexpectedResult = true;
                    }
                } else {
                    boolean found = false;
                    for (int i = 0; i < expected.length; ++i) {
                        if (expected[i] != null &&
                            compareJson(resultStr,
                                        expected[i].trim(),
                                        orderedFields)) {
                            expected[i] = null;
                            ++theNumMatched;
                            found = true;
                            break;
                        }
                    }

                    if (!theHaveUnexpectedResult && !found) {
                        opt.failure("Test " + qtCase + " FAILED:\n" +
                                    "unexpected actual result: " +  result);
                        theHaveUnexpectedResult = true;
                    }
                }

                ++theNumResults;
            }

            @Override
            public synchronized void onError(Throwable e) {

                if (!failureExpected) {
                    theFailed = true;
                    opt.failure("Test " + qtCase +
                                " FAILED due to unexpected exception" + e);
                    //e.printStackTrace();
                }

                theDone = true;
                notifyAll();
            }

            @Override
            public synchronized void onComplete() {

                if (failureExpected) {
                    theFailed = true;
                    opt.failure("Test " + qtCase + " FAILED:\n" +
                                "A runtime exception was expected, but none was raised.");

                } else if (theHaveUnexpectedResult) {
                    theFailed = true;
                    opt.failure("Test " + qtCase + " FAILED:\n" +
                                "expected unordered: \n" + expectedResult +
                                "\nactual:\n" + theFullResultStr +
                                "\nnot matching: " + (numExpected - theNumMatched));
                    genActualFile(UNORDERED_RESULT, theFullResultStr);

                } else if (theNumMatched != numExpected) {
                    theFailed = true;
                    opt.failure(
                        "Test " + qtCase + " FAILED:\n" +
                        "Fewer than expected results: expected size = " +
                        numExpected + " actual size = " + theNumMatched);
                    genActualFile(UNORDERED_RESULT, theFullResultStr);
                }

                theDone = true;
                notifyAll();
            }
        }

        QTFSubscriber subscriber = new QTFSubscriber();

        publisher.subscribe(subscriber);

        synchronized (subscriber) {

            int numSecs = 0;
            while (!subscriber.theDone && numSecs < 5) {
                try {
                    subscriber.wait(1000);
                    ++numSecs;
                } catch (InterruptedException e) {
                    opt.failure("QTCase thread interrupted while waiting " +
                                "for async query");
                    subscriber.theFailed = true;
                    break;
                }

                if (!subscriber.theDone) {
                    System.out.println(
                        "Waiting for another 1 sec for the test case to finish");
                }
            }

            if (numSecs >= 5) {
                subscriber.theFailed = true;
                opt.failure("Test case took more than 5 secs to finish");
            }
        }

        return !subscriber.theFailed;
    }

    /*
     * Compare JSON strings. If orderedFields is false, use
     * an unordered acomparison; otherwise compare strings
     */
    private static boolean compareJson(String json1,
                                       String json2,
                                       boolean orderedFields) {

        if (orderedFields) {
            return json1.equals(json2);
        }

        FieldValue val1 = FieldValueFactory.createValueFromJson(
            FieldDefImpl.Constants.jsonDef, json1);
        FieldValue val2 =
            FieldValueFactory.createValueFromJson(
                FieldDefImpl.Constants.jsonDef, json2);
        return val1.equals(val2);
    }

    private void checkSuccessAndQueryKind(StatementResult sr) {

        if ( !sr.isSuccessful() ) {
            opt.failure(
                "  Test FAILED:  " + getOpt().relativize(queryFile) +
                "\n    StatementResult.isSuccessful()" +
                "\n    expected: true" +
                "\n    actual:   false");
            genActualFile("StatementResult.isSuccessful() == false");
        }

        if ( !sr.getKind().equals(StatementResult.Kind.QUERY) ) {
            opt.failure(
                "  Test FAILED:  " + getOpt().relativize(queryFile) +
                "\n    StatementResult.getKind()" +
                "\n    expected: '" + StatementResult.Kind.QUERY.name() + "'" +
                "\n    actual:   '" + sr.getKind().name() + "'");
            genActualFile("StatementResult.getKind() == " + sr.getKind().name());
        }
    }

    private String toJsonString(RecordValue value) {
        StringBuilder sb = new StringBuilder(128);
        ((FieldValueImpl)value).toStringBuilder(sb, dfmt);
        return sb.toString();
    }
}
