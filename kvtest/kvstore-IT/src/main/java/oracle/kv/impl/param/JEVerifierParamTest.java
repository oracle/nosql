/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.param;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import oracle.kv.TestBase;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Unit test to check whether JE data verifier is disabled
 */
public class JEVerifierParamTest extends TestBase {
    private CreateStore createStore = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        suppressSystemError();
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown(false);
        }

        resetSystemError();
        super.tearDown();
    }

    @Test
    /**
     * Check whether the default value for je.env.runVerifier is false. There is
     * no explicit way of retrieving the value of the parameter, so this method
     * is to grep the log file to check the value.
     * @throws Exception
     */
    public void testBasic() throws Exception {
        /* Start 3*3 KVStore */
        createStore = new CreateStore("kvtest-JEVerifierParamTest",
                                      5000,
                                      3, /* Storage nodes */
                                      3, /* Replication factor */
                                      9, /* Partitions */
                                      3, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      false,
                                      null);
        createStore.start();

        /* Grep the log file to check JE data verifier is disabled or not */
        File rgRNLog = new File(TestUtils.getTestDir() +
                "/kvtest-JEVerifierParamTest/log/rg1-rn1_0.log");


        assertTrue("Awaiting Results", new PollCondition(500, 100000) {

            @Override
            protected boolean condition() {
                /* je.env.runVerifier=false mean verifier is disabled */
                return grepLog(rgRNLog, "je.env.runVerifier=false");
            }
        }.await());

        assertFalse("Awaiting Results", new PollCondition(500, 100000) {

            @Override
            protected boolean condition() {
                /*
                 * je.env.runVerifier=true mean verifier is enabled.
                 * After starting, the default value should be false, so there
                 * is no true value for je.env.runVerifier=true;
                 */
                return grepLog(rgRNLog, "je.env.runVerifier=true");
            }
        }.await());
    }

    /**
     * Grep the target file to check whether existing the pattern
     * @return return true when find the pattern or return false
     */
    private boolean grepLog(File logFile, String pattern) {
        BufferedReader br = null;
        try {
             br = new BufferedReader(new FileReader(logFile));

             String line;
             while ((line = br.readLine()) != null) {
                 if (line.indexOf(pattern) > -1) {
                     return true;
                 }
             }
        } catch (IOException ex) {
            return false;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    return false;
                }
            }
        }
        return false;
    }
}
