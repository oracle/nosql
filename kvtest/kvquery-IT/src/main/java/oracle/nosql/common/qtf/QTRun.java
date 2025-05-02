/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A QTRun obj represents a run-* or compile-* command in a test.config file.
 * For example: run-array-index = q() = expres
 */
public class QTRun {

    public QTSuite suite;

    /*
     * The value of the command. Its format is
     * queryDir(depDir1, depDir2, ...) = resultDir
     */
    public String runString;

    public QTOptions opt;

    /* The path to the directory containing the test.config file */
    public String dirPath;

    /* The test.config file */
    public File configFile;

    /* The directory containing the queries */
    public File queriesDir;

    /* The directory containing the expected results */
    public File resultsDir;

    /* True if the command is compile-* */
    public boolean compileOnly;

    static QTRun parse(
        QTSuite suite,
        String commandName,
        String commandValue,
        QTOptions opt,
        File configFile) {

        // commandValue should look like:  queryDir(depDir1, depDir2)=resultDir

        QTRun res = QTestBase.factory.createQTRun();
        res.suite = suite;
        res.runString = commandValue;
        res.opt = opt;
        res.configFile = configFile;
        res.dirPath = opt.relativize(configFile.getParentFile());

        res.compileOnly = (commandName.startsWith("compile-"));

        String queriesDirStr;
        String configFileStr = opt.relativize(configFile);

        int leftP = commandValue.indexOf('(');
        if (leftP > 0) {
            int rightP = commandValue.indexOf(')');
            if (rightP < 0 || rightP < leftP + 1)
                throw new IllegalArgumentException("Missing ')' " +
                    "in '" + commandValue + "' : " + configFileStr );

            queriesDirStr = commandValue.substring(0, leftP).trim();

            res.queriesDir = new File(configFile.getParentFile(),
                                      queriesDirStr);

            if (!res.queriesDir.exists() || !res.queriesDir.isDirectory())
                throw new IllegalArgumentException("Missing query " +
                    "directory '" + queriesDirStr + "' : " + configFileStr);

            int eqIndx = commandValue.indexOf('=');
            if (eqIndx < rightP)
                throw new IllegalArgumentException("Missing " +
                    "= in '" + commandValue + "' : " +
                    configFileStr);

            String resDirStr = commandValue.substring(eqIndx + 1).trim();
            res.resultsDir = new File(configFile.getParentFile(), resDirStr);

            if (!res.resultsDir.exists() || !res.resultsDir.isDirectory())
                throw new IllegalArgumentException("Missing " +
                    "results directory '" + resDirStr + "' : " +
                    configFileStr );

        } else {
            throw new IllegalArgumentException("Missing '(' " + "in '" +
                commandValue + "' : " + configFileStr);
        }

        return res;
    }

    /*
     * Creates a QTCase instance for each query file in this.queriesDir.
     * The created QTCases are returned in the "cases" param.
     */
    public void addCases(Collection<QTCase> cases) {

        File[] queryFiles = queriesDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name != null && name.endsWith(".q");
            }
        });

        List<File> queryFilesList = Arrays.asList(queryFiles);
        Collections.sort(queryFilesList);
        String filterOutList = opt.getFilterOut();

        for (File queryFile : queryFilesList) {

            String queryRelPath = opt.relativize(queryFile);
            if (opt.getFilter() != null &&
                !queryRelPath.contains(opt.getFilter()))
                continue;

            boolean find = false;
            if (filterOutList != null) {
                String[] elems = filterOutList.split(";");
                for (String s : elems) {
                    if (s != null && (!s.trim().isEmpty()) &&
                        queryRelPath.contains(s.trim())) {
                        find = true;
                        break;
                    }
                }
                if (find) {
                    continue;
                }
            }

            String queryFileName = queryFile.getName();

            String resultFileName = queryFileName.
                                    substring(0, queryFileName.length() - 1) +
                                    "r";
            File resultFile = new File(resultsDir, resultFileName);

            QTCase c = QTestBase.factory.createQTCase();
            c.setQueryFile(queryFile);
            c.setResultFile(resultFile);
            c.setRun(this);
            c.setOpt(opt);

            cases.add(c);
        }
    }

    /*
     * Called only when tests are run as a java app (not via unit test)
     */
    public void run()
        throws IOException {

        opt.verbose("Running " + runString + " in " + suite.getRelPath());

        File[] queryFiles = queriesDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name != null && name.endsWith(".q");
            }
        });

        List<File> queryFilesList = Arrays.asList(queryFiles);
        Collections.sort(queryFilesList);

        for (File queryFile : queryFilesList) {

            String queryFileName = queryFile.getName();
            String resultFileName = queryFileName.substring(0, queryFileName
                .length() - 1) + "r";
            File resultFile = new File(resultsDir, resultFileName);

            if (!resultFile.exists() || !resultFile.isFile())
                throw new IllegalArgumentException("Missing result file: " +
                    opt.relativize(resultsDir) + " : " + resultFileName);

            QTCase c = QTestBase.factory.createQTCase();
            c.setQueryFile(queryFile);
            c.setResultFile(resultFile);
            c.setRun(this);
            c.setOpt(opt);

            c.run();
        }
    }
}
