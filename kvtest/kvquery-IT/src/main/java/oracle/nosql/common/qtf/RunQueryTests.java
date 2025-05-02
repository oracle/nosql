/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RunQueryTests {

    public QTOptions opt = new QTOptions();

    public List<QTSuite> suites = new ArrayList<QTSuite>();

    public List<QTCase> getCases() {

        List<QTCase> cases = new ArrayList<QTCase>();
        for(QTSuite s : suites) {
            s.addCases(cases);
        }

        return cases;
    }

    /**
     * This method is used only when running QTF as a standalone application
     * program
     */
    public int init(String[] args) throws IOException {

        String baseDirStr = null;
        String filter = null;
        String filterOut = null;
        boolean verbose = false;
        boolean updateQueryPlans = false;
        boolean genActualFiles = false;

        int argc = 0;
        int nArgs = args.length;

        while (argc < nArgs) {

            String thisArg = args[argc++];

            if (thisArg.equals("-base-dir")) {

                if (argc < nArgs) {
                    baseDirStr = args[argc++];
                } else {
                    System.err.println("Flag -base-dir requires and argument" +
                        ".");
                    return -1;
                }

            } else if (thisArg.equals("-filter")) {

                if (argc < nArgs) {
                    filter = args[argc++];
                } else {
                    System.err.println("Flag -filter requires and argument.");
                    return -1;
                }
            } else if (thisArg.equals("-filterOut")) {

                if (argc < nArgs) {
                    filterOut = args[argc++];
                } else {
                    System.err.println("Flag -filterOut requires and argument" +
                        ".");
                    return -1;
                }
            } else if (thisArg.equals("-verbose")) {
                verbose = true;
            } else if (thisArg.equals("-updateQueryPlans")) {
                updateQueryPlans = true;
            } else if (thisArg.equals("-actual")) {
                genActualFiles = true;
            } else {
                System.out.println("Unknown argument: '" + thisArg +"'.\n");
                System.out.println("Usage: ");
                System.out.println("  JVM arg: -Dtestdestdir=" +
                    "./kvstore/build/kvsandbox must use store dest directory");
                System.out.println("\t-base-dir <dir-path> Base directory of" +
                    " the query tests, usualy ./kvstore/test/query/cases.");
                System.out.println("\t-filter <string> Filter relative full " +
                    " test name to contain the given string.");
                System.out.println("\t-filterOut <string> Filter out relative" +
                    " full test name to contain the given string. Default " +
                    "value is \"idc_\".");
                System.out.println("\t-updateQueryPlans Update the query " +
                    "plans in expected result files.");
                System.out.println("\t-actual Generate .actual files next to" +
                    " expected result files with the actual output.");
                System.out.println("\t-verbose Enable verbose output.");
            }
        }

        if (baseDirStr == null) {
            baseDirStr = ".";
            opt.verbose("No -base-dir flag, it's assumed '.' : " +
                new File(baseDirStr).getCanonicalPath());
        }

        File baseDir = new File(baseDirStr).getCanonicalFile();

        if ( !baseDir.isDirectory() ) {
            System.err.println(
                "Not a valid directory:" + baseDirStr);
            return -1;
        }

        // Filter out idc_ tests by default, unless filter is "idc_".
        if ( (!"idc_".equals(filter)) && filterOut == null ) {
            filterOut = "idc_";
        }

        opt.setBaseDir(baseDir);
        opt.setFilter(filter);
        opt.setFilterOut(filterOut);
        opt.setVerbose(verbose);
        opt.setUpdateQueryPlans(updateQueryPlans);
        opt.setGenActualFiles(genActualFiles);

        return 0;
    }
}
