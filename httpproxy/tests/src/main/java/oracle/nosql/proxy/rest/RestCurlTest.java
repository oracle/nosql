/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.lang.ProcessBuilder;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.nosql.proxy.ProxyTestBase;

public class RestCurlTest extends ProxyTestBase {
    private static final String shell = getProxyBase() + "/oracle/nosql/proxy/rest/curl_smoke_test.sh ";

    /* Note this overrides the parent BeforeClass method */
    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        /* don't start kvlite/proxy if not in cloudsim mode */
        Assume.assumeTrue(
            "Skipping RestCurlTest if not cloudsim test",
            !Boolean.getBoolean(ONPREM_PROP) &&
            !Boolean.getBoolean(USEMC_PROP) &&
            !Boolean.getBoolean(USECLOUD_PROP));

        staticSetUp(tenantLimits);
    }

    @Test
    public void restSmokeTest() throws Exception {

        /* this only runs on cloudsim mode */
        assumeTrue(onprem == false);
        assumeTrue(cloudRunning == false);
        /* should output go to stdout? default no/silent */
        // boolean curl_verbose = Boolean.getBoolean("test.curl.verbose");

        /* invoke a shell script that uses curl to to REST tests */
        /* TODO: check for executable first */
        final String verb = (verbose) ? "-v " : "";
        String sh = shell + verb + getProxyPort();
        Process p = new ProcessBuilder()
                        .inheritIO()
                        .command("/bin/bash", "-c", sh)
                        .start();
        int retCode = p.waitFor();
        if (retCode != 0) {
            fail("Error executing rest smoke test");
        }
    }
}
