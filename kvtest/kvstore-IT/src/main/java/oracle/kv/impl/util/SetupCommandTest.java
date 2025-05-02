/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Assume;
import org.junit.Test;

/**
 * Test the setup command-line interface.
 */
public class SetupCommandTest extends DiagnosticShellTestBase {

    /**
     * Test setup -add command
     * @throws Exception
     */
    @Test
    public void testAdd() throws Exception {
        /* Positive test without user name */
        String username = System.getProperty("user.name");
        String s = runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "localhost",
                "-sshusername", username,
                "-rootdir", "~/kvroot",
                "-configdir", outputDir.getAbsolutePath()});

        /*
         * The expected output is empty string when setup -add command
         * completes successfully
         */
        assertEquals(s, "");

        /* Check whether the content of configuration file is as expected */
        File configFile = new File(outputDir, "sn-target-list");
        BufferedReader br = new BufferedReader(new FileReader(configFile));
        String line = br.readLine();
        br.close();
        /* The user name is system user name when does not give -user */

        assertEquals(line, "mystore|sn1|" + username + "@localhost|~/kvroot");

        /* Duplicated SNA info test */
        s = runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "localhost",
                "-sshusername", username,
                "-rootdir", "~/kvroot",
                "-configdir", outputDir.getAbsolutePath()});

        /* Check the result when add duplicated SNA info */
        assertTrue(s.indexOf("Duplicated line") != -1);

        /* Missing argument test */
        s = runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "localhost",
                "-rootdir", "~/kvroot",
                "-configdir", outputDir.getAbsolutePath()});

        /* Check the result when miss a required argument */
        assertTrue(s.indexOf("Missing required argument") != -1);

        /* Invalid host test */
        s = runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "xxx",
                "-sshusername", username,
                "-rootdir", "~/kvroot",
                "-configdir", outputDir.getAbsolutePath()});

        /* Check the result when the host is unreachable */
        assertTrue(s.indexOf("Specified -host xxx not reachable") != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"setup",
                "-add",
                "-xxx", "localhost"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* -help flag test */
        s = runCLICommand(new String[] {"setup",
                "-add",
                "-help"});
        assertTrue(s.indexOf("Add a descriptor for this storage node to the " +
                        "sn-target-list file.") != -1);

        /* Test without specified the output directory of configuration file */
        s = runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "localhost",
                "-sshusername", username,
                "-rootdir", "~/kvroot"});
        assertEquals(s, "");

        /* Check whether the content of configuration file is as expected */
        File newConfigFile = new File(System.getProperty("user.dir") +
                                      File.separator + "sn-target-list");

        br = new BufferedReader(new FileReader(newConfigFile));
        line = br.readLine();
        while (line.isEmpty()) {
            line = br.readLine();
        }
        br.close();
        /* Delete the new configuration file */
        if (newConfigFile.exists()) {
            newConfigFile.delete();
        }

        assertEquals(line, "mystore|sn1|" + username + "@localhost|~/kvroot");
    }

    /**
     * Test setup -delete command
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        /* Add SNA info into configuration file */
        for (int i = 0; i < 10; i++) {
            runCLICommand(new String[] {"setup",
                    "-add",
                    "-store", "mystore" + i,
                    "-sn", "sn" + (i / 5),
                    "-host", "localhost",
                    "-sshusername", "tests" + i,
                    "-rootdir", "~/kvroot" + i,
                    "-configdir", outputDir.getAbsolutePath()});
        }

        /* Delete a line SNA info from configuration file */
        String s = runCLICommand(new String[] {"setup",
                "-delete",
                "-sshusername", "tests0",
                "-configdir", outputDir.getAbsolutePath()});
        assertTrue(s.indexOf("1 line has been deleted from") != -1);

        /* Delete 0 line SNA info from configuration file */
        s = runCLICommand(new String[] {"setup",
                "-delete",
                "-rootdir", "~/kvroot", /*No rootdir is ~/kvroot existing*/
                "-configdir", outputDir.getAbsolutePath()});
        assertTrue(s.indexOf("0 line has been deleted from") != -1);

        /* Delete multiple lines SNAs info from configuration file */
        s = runCLICommand(new String[] {"setup",
                "-delete",
                "-sn", "sn0",
                "-configdir", outputDir.getAbsolutePath()});

        assertTrue(s.indexOf("lines have been deleted from") != -1);

        /* Help flag test */
        s = runCLICommand(new String[] {"setup", "-delete", "-help"});
        assertTrue(s.indexOf("Delete SNA descriptors from the " +
                "sn-target-list file") != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"setup",
                "-delete",
                "-xxx", "localhost"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* No SNA info configuration file existing test */
        s = runCLICommand(new String[] {"setup",
                "-delete",
                "-host", "localhost"});

        /* No existing configuration file, FileNotFoundException is expected */
        assertTrue(s.indexOf("Cannot find SN target file") != -1);

        /* Delete all lines SNAs info in configuration file */
        s = runCLICommand(new String[] {"setup",
                "-delete",
                "-host", "localhost",
                "-configdir", outputDir.getAbsolutePath()});
        assertTrue(s.indexOf("lines have been deleted from") != -1);
    }

    /**
     * Test setup -clear command
     * @throws Exception
     */
    @Test
    public void testClear() throws Exception {
        /* Add SNA info into configuration file */
        for (int i = 0; i < 10; i++) {
            runCLICommand(new String[] {"setup",
                    "-add",
                    "-store", "mystore" + i,
                    "-sn", "sn" + (i / 5),
                    "-host", "localhost",
                    "-sshusername", "tests" + i,
                    "-rootdir", "~/kvroot" + i,
                    "-configdir", outputDir.getAbsolutePath()});
        }

        /* Help flag test */
        String s = runCLICommand(new String[] {"setup", "-clear", "-help"});
        assertTrue(s.indexOf("Clear info of all SNAs from " +
                        "configuration file") != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"setup",
                "-clear", "-xxx"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* No existing configuration file test */
        s = runCLICommand(new String[] {"setup", "-clear"});

        /* It is allowed to clear non-existing configuration file */
        assertEquals(s, "");

        /* Clear all lines test */
        s = runCLICommand(new String[] {"setup", "-clear",
                "-configdir", outputDir.getAbsolutePath()});
        assertEquals(s, "");
    }

    /**
     * Test setup -list command
     * @throws Exception
     */
    @Test
    public void testList() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        /* Add SNA info into configuration file */
        for (int i = 0; i < 3; i++) {
            runCLICommand(new String[] {"setup",
                    "-add",
                    "-store", "mystore",
                    "-sn", "sn" + i,
                    "-host", "localhost",
                    "-sshusername", System.getProperty("user.name"),
                    "-rootdir", "~/kvroot" + i,
                    "-configdir", outputDir.getAbsolutePath()});
        }

        /* Help flag test */
        String s = runCLICommand(new String[] {"setup", "-list", "-help"});
        assertTrue(s.indexOf("Display and validate all storage nodes") != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"setup", "-list", "-xxx"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* No existing configuration file test */
        s = runCLICommand(new String[] {"setup", "-list"});

        /* No existing configuration file */
        assertTrue(s.indexOf("Cannot find file") != -1);

        /* Create temporary files */
        File rootdir0 = new File(System.getProperty("user.home"), "kvroot0");
        if (!rootdir0.exists()) {
            rootdir0.mkdir();
        }

        /* List all lines test */
        s = runCLICommand(new String[] {"setup", "-list",
                "-configdir", outputDir.getAbsolutePath()});

        assertTrue(s.indexOf("Total: 3    Completed: 3    Status: done") != -1);

        /* Delete all temporary directories */
        rootdir0.delete();
    }
}
