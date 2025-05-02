/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import oracle.kv.TestBase;

import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;
import org.junit.Assert;
import org.junit.Test;

public class MakeBootConfigTest extends TestBase {

    @Test
    public void testBasic() throws Exception {
        File testDir = TestUtils.getTestDir();
        File rootDir = new File(testDir.getAbsolutePath() + File.separator +
            "kvroot");
        rootDir.mkdirs();
        String[] args ={"makebootconfig",
            "-root", rootDir.getAbsolutePath(),
            "-host", "localhost",
            "-port", "8000",
            "-harange", "8010,8020",
            "-capacity", "0",
            "-store-security", "none"};
        KVStoreMain.main(args);
    }

    /**
     * Runs basic test with -storagedir but without -storagedirsize. This
     * should not throw an exception. It should print a warning message, which
     * can be checked manually.
     */
    @Test
    public void testMissingStorageDirSize() throws Exception {
        File testDir = TestUtils.getTestDir();
        File rootDir = new File(testDir.getAbsolutePath() + File.separator +
                                "kvroot");
        rootDir.mkdirs();
        String[] args ={"makebootconfig",
                        "-root", rootDir.getAbsolutePath(),
                        "-storagedir", rootDir.getAbsolutePath(),
                        "-host", "localhost",
                        "-port", "8000",
                        "-harange", "8010,8020",
                        "-capacity", "0",
                        "-store-security", "none"};
        System.err.println("Warning message below is expected.");
        KVStoreMain.main(args);
    }

    /**
     * Validates the -storageType parameter that is used in the
     * makebootconfig command, ensures it is written to disk in config.xml.
     * The supported storage types are HD, SSD, and NVME.
     * Type names are tested in BootConfigVerifierTest
     */
    @Test
    public void testStorageType() throws Exception {
        File testDir = TestUtils.getTestDir();
        final String testDirPath = testDir.getAbsolutePath() +
                File.separator + "kvroot";
        File rootDir = new File(testDirPath);
        rootDir.mkdirs();

        final String configFilePath = testDirPath + File.separator +
                "config.xml";
        final String securityFilePath = testDirPath + File.separator +
                "security.policy";

        String[] args ={"makebootconfig",
                "-root", rootDir.getAbsolutePath(),
                "-host", "localhost",
                "-port", "8000",
                "-harange", "8010,8020",
                "-capacity", "0",
                "-store-security", "none",
                "-storage-type", null
        };

        /* test every valid storage type for an SN (NVME, HD, SSD, and UNKNOWN) */
        for (StorageType type : StorageType.values()) {
            args[args.length - 1] = type.name();
            KVStoreMain.main(args);

            /* get storageType that was written to config.xml */
            File configFile = new File(configFilePath);
            LoadParameters lp = LoadParameters.getParametersByType(configFile);
            ParameterMap map = lp.getMap(ParameterState.BOOTSTRAP_PARAMS,
                                         ParameterState.BOOTSTRAP_TYPE);
            String typeParam = map.get(ParameterState.SN_STORAGE_TYPE)
                                       .asString();

            Assert.assertEquals(type, StorageType.parseType(typeParam));

            /* delete configuration files in kvroot for next test */
            File securityFile = new File(securityFilePath);
            if (!configFile.delete()) {
                fail("config.xml not deleted successfully");
            }
            if (!securityFile.delete()) {
                fail("security.policy not deleted successfully.");
            }
        }
    }

    /**
     * Test the validation check on host name.
     */
    @Test
    public void testInvalidHostname() {

        final String[] invalidHosts = {
            "nosql_test", "nosq(", "nosql\'test", "-nosql", "---"
        };

        for (String host : invalidHosts) {
            String[] args = new String[] {"-host", host};
            TestParser parser = new TestParser(args);
            parser.parseArgs();

            assertTrue("Expect to get error message for invalid hostname",
                parser.getErrorMsg().contains("Invalid hostname"));
        }
    }

    /**
     * An extension of CommandParser to verify the error message passed into
     * the CommandParser.usage() method when encounter error during parsing
     * arguments.
     */
    private static class TestParser extends CommandParser {

        private final StringBuilder msg = new StringBuilder();

        public TestParser(String[] args) {
            super(args);
        }

        @Override
        public void usage(String errorMsg) {
            msg.setLength(0);
            msg.append(errorMsg);
        }

        public String getErrorMsg() {
            return msg.toString();
        }

        @Override
        protected boolean checkArg(String arg) {
            return false;
        }

        @Override
        protected void verifyArgs() {
        }
    }
}
