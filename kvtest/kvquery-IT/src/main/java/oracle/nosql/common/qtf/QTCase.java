/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;


/**
 * Class representing a single test case.
 */
public abstract class QTCase {

    public final static String EOL = "\n";
    public static final String COMPILED_QUERY_PLAN = "compiled-query-plan";
    public static final String COMPILE_EXCEPTION = "compile-exception";
    public static final String RUNTIME_EXCEPTION = "runtime-exception";
    public static final String UNORDERED_RESULT = "unordered-result";
    public static final String ORDERED_RESULT = "ordered-result";

    protected QTRun run;
    protected File queryFile;
    protected File resultFile;
    protected QTOptions opt;

    public abstract void run() throws IOException;

    public abstract boolean runQuery() throws IOException;

    protected void updateQueryPlanInResultFile(String actualQueryPlan)
            throws IOException {

        String oldFileContent = FileUtils.readFileToString(resultFile);

        int index = oldFileContent.indexOf(COMPILED_QUERY_PLAN);
        if ( index == -1 ) {
            throw new IllegalArgumentException("No '" + COMPILED_QUERY_PLAN
                + "' directive found in file: " + opt.relativize(resultFile));
        }

        String header = oldFileContent.substring(0, index);

        try {
            FileOutputStream fos = new FileOutputStream(resultFile);
            Writer w =
                new OutputStreamWriter(fos);
            w.write(header);
            w.write(COMPILED_QUERY_PLAN + "\n\n");
            w.write(actualQueryPlan);
            w.flush();
            fos.flush();
            w.close();
            fos.close();
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        opt.progress("UPDATE query plan: " + opt.relativize(resultFile));
    }

    protected String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    protected boolean checkException(
        boolean compile,
        String expectedResult,
        Exception e) {

        String expected = skipComments(expectedResult);
        int i;

        // find first line of the rest
        i = expected.indexOf(EOL);
        if (i < 0) {
            throw new IllegalArgumentException("Invalid result file: " +
                getOpt().relativize(resultFile));
        }

        String actualHeader = compile ? COMPILE_EXCEPTION : RUNTIME_EXCEPTION;
        String expectedHeader = expected.substring(0, i).trim().toLowerCase();
        String expectedMsg = expected.substring(i + 1).trim();
        String actualMsg = e.getMessage();

        if (! expectedHeader.equals(actualHeader)) {
            opt.failure("  Test FAILED:  " + opt.relativize(queryFile));
            opt.failure("    expected: '" + expectedHeader + "'" +
                        "\n    actual:   '" + actualHeader + "' : " +
                        getStackTrace(e));
            genActualFile(actualHeader, actualMsg + "\n" + getStackTrace(e));
            return false;
        }

        if ( (i + 1 < 0) || (i + 1 >= expected.length()) ) {
            // no expected msg to be checked
            return true;
        }

        if (actualMsg == null || !actualMsg.contains(expectedMsg)) {
            opt.failure("  Test FAILED:  " + opt.relativize(queryFile));
            opt.failure("\n    expected: '" + expectedMsg + "'" +
                "\n    actual:   '" + actualMsg + "' : " +
                getStackTrace(e));
            genActualFile(actualHeader, actualMsg);
            return false;
        }

        return true;
    }

    protected String skipComments(String expectedResult) {
        // skip comments
        String s = expectedResult;
        int i;
        int p;
        do {
            i = s.indexOf(EOL);
            if (i >= 0 && i + 1 <= s.length())
            {
                p = s.indexOf('#');

                if ( !(i >= 0 && p >= 0 && p < i &&
                    s.substring(0, p).trim().length() == 0))
                {
                    break;
                }
                s = s.substring(i + 1);
            } else {
                break;
            }
        } while (true);

        return s;
    }

    protected void genActualFile(String header) {
        genActualFile(header, "");
    }

    protected void genActualFile(String header, String content) {

        if (!opt.isGenActualFiles()) {
            return;
        }

        genFile(".actual", header, content);
    }

    protected void genTmpFile(String header, String content) {
        genFile(".tmp", header, content);
    }

    protected void genFile(String suffix, String header, String content) {

        File file = new File(resultFile.getPath() + suffix);
        try {
            FileOutputStream fos = new FileOutputStream(file);
            Writer w =
                new OutputStreamWriter(fos);
            w.write(header);
            w.write("\n");
            w.write(content);
            w.flush();
            fos.flush();
            w.close();
            fos.close();
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        opt.verbose("Generate actual file: " + opt.relativize(file));
    }

    @Override
    public String toString() {
        return getOpt().relativize(getQueryFile());
    }

    public File getQueryFile() {
        return queryFile;
    }

    public void setQueryFile(File queryFile) {
        this.queryFile = queryFile;
    }

    public File getResultFile() {
        return resultFile;
    }

    public void setResultFile(File resultFile) {
        this.resultFile = resultFile;
    }

    public QTSuite getSuite() {
        return run.suite;
    }

    public QTRun getRun() {
        return run;
    }

    public void setRun(QTRun run) {
        this.run = run;
    }

    public QTOptions getOpt() {
        return opt;
    }

    public void setOpt(QTOptions opt) {
        this.opt = opt;
    }
}
