/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.server;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;

import oracle.kv.TestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.JENotifyHooks.RecoveryListener;
import oracle.kv.impl.util.server.JENotifyHooks.RedirectHandler;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * This class tests that JE-level information (logging, Recovery progress) are
 * propagated to the KV world.  Ideally we would test that SEVERE-level events
 * are turned into critial events as well.  However, there's no good way to
 * induce such severe events, so for now we will limit testing to just the log
 * files.  In addition, SyncUp progress is also currently untested.
 */
public class JENotifyTest extends TestBase {

    /* Basic store parameters */
    private static final int START_PORT = 5000;

    private CreateStore createStore;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
        }
        TestUtils.clearTestDirectory();
        super.tearDown();
    }

    /**
     * A simple FileFilter that recognizes log files for a particular
     * kvstore component.
     */
    class LogFileFilter implements FileFilter {

        private final String logBase;

        /**
         * @param logBase The base portion of the log file name, normally the
         * resourceId name of the logging component
         */
        LogFileFilter(String logBase) {
            this.logBase = logBase;
        }

        @Override
        public boolean accept(File pathname) {

            /* Split the pathname into individual components */
            String pathComps[] = pathname.toString().split(File.separator);

            /* The component we care about is the final one */
            String lastComp = pathComps[pathComps.length-1];

            if (lastComp.toString().startsWith(logBase) &&
                lastComp.toString().endsWith("." + FileNames.LOG_FILE_SUFFIX)) {
                return true;
            }
            return false;
        }
    }

    @Test
    public void testJELogRedirect()
        throws Exception {

        try {
            createStore =
                new CreateStore(kvstoreName,
                                START_PORT,
                                1,     /* Storage nodes */
                                1,     /* Replication factor */
                                100,   /* Partitions */
                                1    /* Capacity */);

            createStore.start();
        } catch (Exception E) {
            fail("unexpected exception: " + E);
        }

        File loggingDir =
            FileNames.getLoggingDir(TestUtils.getTestDir(), kvstoreName);

        /* The list of logs to be examined */
        String[] logTgts =
            new String[] { new AdminId(1).toString(),       // admin
                           new RepNodeId(1, 1).toString(),  // rep node
                           kvstoreName };                   // store-wide

        /*
         * For each of the logTgts entries, we search for lines containing
         * well-known prefixes used by the notify hooks and count to see how
         * many we have.
         */
        for (String logTgt : logTgts) {
            int lineCountJE = 0;
            int lineCountJERecovery = 0;

            File[] logTgtFiles =
                loggingDir.listFiles(new LogFileFilter(logTgt));

            for (File logFile : logTgtFiles) {
                BufferedReader br = new BufferedReader(new FileReader(logFile));

                do {
                    String line = br.readLine();
                    if (line == null) {
                        break;
                    }

                    if (line.indexOf(RedirectHandler.PREFIX) >= 0) {
                        lineCountJE++;
                    }

                    if (line.indexOf(RecoveryListener.PREFIX) >= 0) {
                        lineCountJERecovery++;
                    }
                } while (true);

                br.close();
            }

            /*
             * It is certainly possible that you could set up an environment
             * in which all the JE logging is suppressed, but we don't expect
             * that in our unit test environment.
             */
            assertTrue(lineCountJE > 0);
            assertTrue(lineCountJERecovery > 0);
        }
   }
}

