/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Helper methods for running a kvstore utility that takes the form of
 * java -jar kvstore.jar XXX as a separate process. Useful for unit tests that
 * are process dependent, such as tests that look for exit codes.
 */
public class ProcessSetupUtils {

    /**
     * Helper for creating the command line for running a utility as a process
     *
     * @returns a command list that sets the classpath appropriately for
     * a "java -jar kvstore.jar ... " command.
     */
    public static List<String> setupJavaJarKVStore() {

        List<String> command = new ArrayList<String>();
        command.add("java");
        String cp = System.getProperty("java.class.path");
        String kvstore_regex = "kvstore-[0-9.]+(?:-SNAPSHOT)?\\.jar";
        assert(Pattern.compile(kvstore_regex).matcher(cp).find()) :
        "Classpath does not include kvstore.jar: " + cp;
        command.add("-cp");
        command.add(cp);
        command.add("oracle.kv.impl.util.KVStoreMain");
        return command;
    }

    /**
     * Runs the command as a process
     * @param command - the command line
     * @param output is the System.out from the process
     * @return the process exit code
     */
    public static int runProcess(List<String> command, StringBuilder output)
        throws IOException {

        JVMSystemUtils.insertZingJVMArgs(command);
        if ("java".equals(command.get(0))) {
            command.add(1, "-Doracle.kv.async=" + TestUtils.useAsync());
        }
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        Process process = null;

        try {
            process = builder.start();
            BufferedReader br = new BufferedReader
                (new InputStreamReader(process.getInputStream()));
            String line = null;
            while ( (line = br.readLine()) != null) {
                output.append(line);
                output.append(System.getProperty("line.separator"));
            }
            process.waitFor();
            return process.exitValue();
        } catch (RuntimeException re) {
            throw re;
        } catch (IOException ioe) {
            throw ioe;
        } catch (InterruptedException ie) {
            System.err.println("Process Interrupted with exception: " +
                               LoggerUtils.getStackTrace(ie));
        } finally {
            if (process != null) {
                process.destroy();
            }
        }

        throw new IllegalStateException
            ("Should have returned with success or thrown");
    }
}
