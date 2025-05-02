/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

import oracle.kv.TestBase;

/**
 * Tests JavaVersionVerifier.
 */
public class JavaVersionVerifierTest extends TestBase {
    private PrintStream stdErr;
    private ByteArrayOutputStream baos;

    @Override
    public void setUp()
        throws Exception {
        super.setUp();
        stdErr = System.err;
        baos = new ByteArrayOutputStream();
        System.setErr(new PrintStream(baos));
    }

    @Override
    public void tearDown()
        throws Exception {
        System.setErr(stdErr);
        super.tearDown();
    }

    @Test
    public void testVerify() throws IOException {
        /*
         * Test the return of verify method of JavaVersionVerifier
         * when the parameter returnOnError is set as true, which is the
         * standard way of running the verifier.
         */
        JavaVersionVerifier verifier =
                new JavaVersionVerifier(true, "-force");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier = new JavaVersionVerifier(true, "-force");
        /* Test when java vendor and java version are supported */
        verifier.setJKDVersionInfo("Oracle Corporation", "1.8.0_01");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("Oracle Corporation", "1.8.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("Oracle Corporation", "1.9.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("IBM Corporation", "1.8.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("IBM Corporation", "1.9.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("Red Hat, Inc.", "1.8.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("Red Hat, Inc.", "11.0.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("GraalVM Community", "1.8.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        verifier.setJKDVersionInfo("GraalVM Community", "11.0.0");
        assertTrue(verifier.verify());
        baos.flush();
        assertEquals("", baos.toString());
        baos.reset();

        /*
         * Test when java vendor is supported and java version is not supported
         */
        verifier.setJKDVersionInfo("Oracle Corporation", "1.7.1");
        assertFalse(verifier.verify());
        baos.flush();
        assertTrue("Output: " + baos, baos.toString().contains("-force"));
        baos.reset();

        verifier.setJKDVersionInfo("IBM Corporation", "1.6.1");
        assertFalse(verifier.verify());
        baos.flush();
        assertTrue("Output: " + baos, baos.toString().contains("-force"));
        baos.reset();

        /* Test when java vendor is not supported */
        verifier.setJKDVersionInfo("XXXCompany", "1.7.1");
        assertFalse(verifier.verify());
        baos.flush();
        assertTrue("Output: " + baos, baos.toString().contains("-force"));
        baos.reset();
    }

    @Test
    public void testForce() throws IOException {
        /* Set returnOnError as false */
        JavaVersionVerifier verifier =
            new JavaVersionVerifier(false, "-force");
        /* Test when Java version is not supported */
        verifier.setJKDVersionInfo("Oracle Corporation", "1.7.0");
        assertTrue(verifier.verify());
        baos.flush();

        /* The output message should not contain "-flag" */
        String filOut = baos.toString();
        assertTrue("Output: " + filOut, filOut.indexOf("-force") == -1);

        /* Test when Java vendor is not supported */
        baos.reset();
        verifier.setJKDVersionInfo("Unknown vendor", "1.8.0");
        assertTrue(verifier.verify());
        baos.flush();

        /* The output message should not contain "-flag" */
        filOut = baos.toString();
        assertTrue("Output: " + filOut, filOut.indexOf("-force") == -1);
    }
}
