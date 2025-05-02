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
import oracle.kv.impl.security.util.PasswordReader;

/**
 * Base class for tests of SecurityShell commands.
 */
public class SecurityShellTestBase extends TestBase {

    /**
     * Call SecurityShell, and return any output it prints as a String.
     * @param args The command line arguments
     * @return The command's output
     * @throws Exception
     */
    public static String runCLICommand(PasswordReader reader, String... args)
        throws Exception {

        /* Arrange to capture stdout. */
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final PrintStream cmdOut = new PrintStream(outStream);
        /* Run the command. */
        final SecurityShell shell =
            new SecurityShell(System.in, cmdOut, reader);
        /* allow hidden commands to be run */
        shell.enableHidden();
        shell.parseArgs(args);
        shell.start();
        return outStream.toString();
    }

    /**
     * Deletes the specified directory and its contents
     * deleteLimit provides a sanity check that we aren't doing something
     * stupid.  If more than deleteLimit files + directories would be deleted
     * by the action, we throw an exception.
     */
    void removeDirectory(File dir, int deleteLimit) {
        final int deleteCount = removeDirectoryCheck(dir);
        if (deleteCount > deleteLimit) {
            throw new IllegalArgumentException(
                "deleting the specified directory: " + dir +
                " would cause the deletion of " + deleteCount +
                " files/directories");
        }
        removeDirectoryExecute(dir);
    }

    private int removeDirectoryCheck(File dir) {
        int count = 0;
        if (dir.exists() && dir.isDirectory()) {
            final File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        count += removeDirectoryCheck(file);
                    } else {
                        count++;
                    }
                }
            }
            count++; /* the directory itself */
        }
        return count;
    }

    private void removeDirectoryExecute(File dir) {
        if (dir.exists() && dir.isDirectory()) {
            final File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        removeDirectoryExecute(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
}
