/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Provide a mechanism to allow System.out and System.err to be captured,
 * and optionally displayed.
 */
public class OutputCapture {
    /* The original values of System.out and System.err */
    private PrintStream systemOut = null;
    private PrintStream systemErr = null;

    /* The capturing components of the output streams */
    private final ByteArrayOutputStream baosOut =
        new ByteArrayOutputStream();
    private final ByteArrayOutputStream baosErr =
        new ByteArrayOutputStream();

    /* The replacement PrintStreams */
    private final PrintStream printOut = new PrintStream(baosOut);
    private final PrintStream printErr = new PrintStream(baosErr);

    private boolean capturing = false;

    /**
     * Constructor.
     */
    public OutputCapture() {
    }

    /**
     * Install capturing PrintStreams in System.out and System.err.
     */
    public synchronized void start() {

        if (!capturing) {
            systemOut = System.out;
            System.setOut(printOut);

            systemErr = System.err;
            System.setErr(printErr);

            capturing = true;
        }
    }

    /**
     * Restore the PrintStreams that were in place at the time that the
     * constructor ran.  This method only has any effect the first time it is
     * called.
     */
    public synchronized void restore() {

        if (capturing) {
            capturing = false;

            System.setOut(systemOut);
            System.setErr(systemErr);
        }
    }

    /**
     * Display the captured out and err lines to System.out and System.err
     */
    public void showCapture() {
        showLines(systemErr, getErrLines());
        showLines(systemOut, getOutLines());
    }

    /**
     * Return an array of Strings, which represent the captured output sent
     * to System.out.
     */
    public String[] getOutLines() {
        printOut.flush();
        try {
            baosOut.flush();
        } catch (IOException ioe) {
        }
        return getLines(baosOut);
    }

    /**
     * Return an array of Strings, which represent the captured output sent
     * to System.err.
     */
    public String[] getErrLines() {
        printErr.flush();
        try {
            baosErr.flush();
        } catch (IOException ioe) {
        }
        return getLines(baosErr);
    }

    private void showLines(PrintStream output, String[] lines) {
        for (String line : lines) {
            output.println(line);
        }
    }

    private String[] getLines(ByteArrayOutputStream baos) {
        final byte[] captured = baos.toByteArray();
        final ByteArrayInputStream bais =
            new ByteArrayInputStream(captured);
        final BufferedReader reader = 
            new BufferedReader(new InputStreamReader(bais));
        List<String> lines = new ArrayList<String>();

        while (true) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                lines.add(line);
            } catch (IOException ioe) {
                /*
                 * Shouldn't happen, but probably safe to ignore. Treat as
                 * an EOF.
                 */
                break;
            }
        }

        return lines.toArray(new String[lines.size()]);
    }
}
