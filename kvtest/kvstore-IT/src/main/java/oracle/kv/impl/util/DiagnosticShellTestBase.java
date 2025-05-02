/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import oracle.kv.TestBase;

/**
 * Base class for tests of DiagnosticShell commands.
 */
public class DiagnosticShellTestBase extends TestBase {
    protected File outputDir;

    /* Assign it as true when the test runs under windows platform */
    protected boolean isWindows;

    /**
     * Call DiagnosticShell, and return any output it prints as a String.
     * @param args The command line arguments
     * @return The command's output
     * @throws Exception
     */
    String runCLICommand(String... args)
        throws Exception {

        /* Arrange to capture stdout. */
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);

        /* Run the command. */
        final DiagnosticShell shell = new DiagnosticShell(System.in, cmdOut);
        /* allow hidden commands to be run */
        shell.parseArgs(args);
        shell.start();
        return outStream.toString();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        /*
         * Create a output directory to store configuration file for setup
         * command
         */

        outputDir = new File("outputDir");
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

        isWindows = false;
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("win") >= 0) {
            isWindows = true;
        }
    }

    @Override
    public void tearDown() throws Exception {
        /* Delete the configuration file and the output directory */
        deleteDirectory(outputDir);
        super.tearDown();
    }

    /**
     * All files and directories under the specified path;
     * @param path
     */
    protected void deleteDirectory(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (File file : files) {
            deleteDirectory(file);
        }
        path.delete();
    }
}
