/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic.ssh;

import oracle.kv.TestBase;

/**
 * Tests SSHClient.
 */
public class ClientTestBase extends TestBase {
    /* Assign it as true when the test runs under windows platform */
    protected boolean isWindows;

    @Override
    public void setUp()
        throws Exception {
        isWindows = false;
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("win") >= 0) {
            isWindows = true;
        }
    }

    @Override
    public void tearDown()
        throws Exception {
    }
}
