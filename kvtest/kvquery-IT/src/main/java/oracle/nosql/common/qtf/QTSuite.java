/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*
 * QTSuite represents a group of queries. Logically, each such group (suite) is
 * defined by a test.config file, which specifies:
 *
 * (a) What tables and indexes to create before the queries in the suite are
 *     executed.
 * (b) What data to load into these tables
 * (c) The subdir containing the query files (one .q file per query)
 * (d) Whether the queries should just be compiled (to check for changes in
 *     their execution plans) or just run (to check for changes in their
 *     result sets) or both.
 * (e) The subdir containing the expected query results (one .r file per query)
 * (f) The subdir containing the expected query execution plans (one .r file per
 *     query)
 * (g) Values for any bind variables used by the queries
 * (h) what to do after all queries in the suite are done (basically drop the
 *     tables)
 *
 * (a) and (b) may be specified in terms of files containing DDL commands and
 * data in json format, or in terms of a java class that will create and load
 * the tables programatically (most suites use files, but a few (like the
 * prim_index one) use classes).
 *
 * QTSuite includes code that parses a test.config file.
 */
public class QTSuite {

    public static final String RUN_PREFIX = "run-";
    public static final String VAR_PREFIX = "var-";
    public static final String COMPILE_PREFIX = "compile-";
    public static final String DEFAULT_PKG = "qt.framework.";
    public static final String DEFAULT_BEFORE_PKG = "qt.";

    public QTOptions opts;
    public File configFile;

    public QTBefore beforeInstance;
    public QTAfter afterInstance;

    public List<QTRun> runs;

    public Map<String, String> vars;
    public Map<String, String> varsType;
    public Map<String, String> varsDeclType;

    public String packageName;

    public QTSuite(QTOptions opts, File configFile) {
        this(opts, configFile, DEFAULT_PKG);
    }

    public QTSuite(QTOptions opts, File configFile, String packageName) {

        this.opts = opts;
        this.configFile = configFile;
        runs = new ArrayList<QTRun>();
        vars = new HashMap<String, String>();
        varsType = new HashMap<String, String>();
        varsDeclType = new HashMap<String, String>();
        this.packageName = packageName;

        parseConfig();
    }

    private void parseConfig()
    {
        Properties props = new Properties();

        try {
            props.load(new FileInputStream(configFile));

            String beforeClassStr = props.getProperty("before-class");

            if (beforeClassStr != null) {
                Class<?> beforeClass = Class.forName(
                    getBeforeClassPackageName() + beforeClassStr);

                beforeInstance = (QTBefore)beforeClass.
                    getDeclaredConstructor().newInstance();

                opts.verbose("before-class: " + getBeforeClassPackageName() +
                             beforeClassStr);
            } else {
                beforeInstance = getQTBeforeImpl();
            }

            beforeInstance.setOptions(opts);
            beforeInstance.setConfigFile(configFile);
            beforeInstance.setConfigProperties(props);

            String afterClassStr = props.getProperty("after-class");
            if (afterClassStr != null) {
                Class<?> afterClass = Class.forName(
                    getBeforeClassPackageName() + afterClassStr);

                afterInstance = (QTAfter)afterClass.
                    getDeclaredConstructor().newInstance();

                opts.verbose("  after-class: " + getBeforeClassPackageName()
                                               + afterClassStr);
            } else {
                afterInstance = getQTAfterImpl();
            }

            afterInstance.setOptions(opts);
            afterInstance.setConfigFile(configFile);
            afterInstance.setConfigProperties(props);

            for (String prop : props.stringPropertyNames()) {

                if (getRunCase(prop)) {
                    runs.add(QTRun.parse(this, prop, props.getProperty(prop),
                                         opts, configFile));
                } else if (prop.startsWith(VAR_PREFIX)) {
                    String varName = prop.substring(VAR_PREFIX.length());
                    if (varName.length() == 0)
                        throw new IllegalArgumentException(
                            "No name for " + VAR_PREFIX + " entry.");

                    String varTypVal = props.getProperty(prop).trim();
                    String varType = null;
                    String varVal = null;

                    if (varTypVal.startsWith("type:")) {
                        int valPos = varTypVal.indexOf(":", 5);
                        varType = varTypVal.substring(5, valPos);
                        varVal = varTypVal.substring(valPos + 1);
                    } else {
                        varVal = varTypVal;
                    }

                    parseVariable(varName, varType, varVal);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private String getBeforeClassPackageName() {
        return packageName == DEFAULT_PKG ? DEFAULT_BEFORE_PKG : packageName;
    }

    private QTBefore getQTBeforeImpl() {
        return (QTBefore)getQTDefaultImpl();
    }

    private QTAfter getQTAfterImpl() {
        return (QTAfter)getQTDefaultImpl();
    }

    private Object getQTDefaultImpl() {
        try {
            return Class.forName(packageName + "QTDefaultImpl").newInstance();
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    protected boolean getRunCase(String prop) {
        return prop.startsWith(RUN_PREFIX) || prop.startsWith(COMPILE_PREFIX);
    }

    protected void parseVariable(
        String varName,
        String varStrTyp,
        String varStrVal) {

        if (varStrVal == null ||
            varStrVal.length() == 0 ||
            varStrVal.toLowerCase().equals("null")) {
            vars.put(varName, null);
            varsType.put(varName, null);
            return;
        }

        if (varStrVal.toLowerCase().equals("jnull")) {
            vars.put(varName, "jnull");
            varsType.put(varName, "jnull");
            return;
        }

        int l = varStrVal.length();
        String value = null;
        String type = null;

        switch (varStrVal.charAt(0)) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        case '+':
        case '-':
            // must be a number
            value = varStrVal;
            int pos = varName.indexOf("-");
            if (pos > 0) {
                type = varName.substring(0, pos).toLowerCase();
                varName = varName.substring(pos + 1, varName.length());
            } else {
                type = "number";
            }
            break;
        case '"':
            // must be a string
            if (varStrVal.charAt(l - 1) != '"')
                throw new IllegalArgumentException("Value not a valid " +
                    "string: '" + varStrVal + "'");
            value = varStrVal.substring(1, l - 1);
            type = "string";
            break;
        case '{':
            // be json complex
            if (varStrVal.charAt(l - 1) != '}')
                throw new IllegalArgumentException("Value not a valid " +
                    "JSON object: '" + varStrVal + "'");
            value = varStrVal;
            type = "json";
            break;
        case '[':
            // must be json array
            if (varStrVal.charAt(l - 1) != ']')
                throw new IllegalArgumentException("Value not a valid " +
                    "JSON array: '" + varStrVal + "'");
            value = varStrVal;
            type = "array";
            break;
        case 't':
        case 'T':
            // must be true
            if ( varStrVal.toLowerCase().equals("true") ) {
                value = varStrVal;
                type = "boolean";
            }
            break;
        case 'f':
        case 'F':
            // must be false
            if ( varStrVal.toLowerCase().equals("false") ) {
                value = varStrVal;
                type = "boolean";
            }
            break;
        case 'n':
        case 'N':
            // must be json null
            if ( varStrVal.toLowerCase().equals("null") )
                throw new IllegalStateException("Null values should have " +
                    "already handled: '" + varStrVal + "'");
            break;
        }

        if (value == null || type == null) {
            throw new IllegalArgumentException("Illegal var " +
                "value: " + varStrVal.substring(0));
        }

        opts.verbose("  Var: " + varName + " = '" + value + "'");
        vars.put(varName, value);
        varsType.put(varName, type);
        varsDeclType.put(varName, varStrTyp);
    }

    public String getRelPath() {
        return opts.relativize(configFile.getParentFile());
    }

    public void addCases(Collection<QTCase> cases) {
        for (QTRun r : runs) {
            r.addCases(cases);
        }
    }

    public void before() {
        if (beforeInstance != null) {
            beforeInstance.before();
        }
    }

    public void after() {
        if (afterInstance != null) {
            afterInstance.after();
        }
    }

    /*
     * Called only when tests are run as a java app (not via unit test)
     */
    public void run() throws IOException {

        if (runs.size() > 0) {
            before();
        }

        // todo run in parallel on multiple threads if they have the
        // same dependencies
        for (QTRun run : runs) {
            run.run();
        }

        if (runs.size() > 0) {
            after();
        }
    }
}
