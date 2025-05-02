/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertTrue;

import java.io.File;

import oracle.kv.TestBase;

import org.junit.Test;

public class CommandParserTest extends TestBase {
    private static final Parser parser = new Parser();

    /** Test CommandParser.validateHostname */
    @Test
    public void testValidateHostname() {
        validateHostname("localhost", false);
        validateHostname("some-host", false);

        final File dir = TestUtils.getTestDir();
        final String file = dir + "/non-existent";

        checkException(() -> validateHostname("unix_domain:" + file, false),
                       IllegalArgumentException.class,
                       "Invalid hostname");
        checkException(() -> validateHostname("unix_domain:", false),
                       IllegalArgumentException.class,
                       "Invalid hostname");

        checkException(() -> validateHostname("foo:" + file, false),
                       IllegalArgumentException.class,
                       "Invalid hostname");

        validateHostname("localhost", true);
        validateHostname("some-host", true);

        validateHostname("unix_domain:" + file, true);
        validateHostname("unix_DOMAIN:" + file, true);
        validateHostname("unix_domain:", true);
        checkException(() -> validateHostname("foo:" + file, true),
                       IllegalArgumentException.class,
                       "Invalid hostname");

        final File subdir = new File(dir, "testValidateHostname");
        final String subfile = subdir + "/abc";

        assertTrue(subdir.mkdir());
        assertTrue(subdir.setWritable(false));
        checkException(() -> validateHostname("unix_domain:" + subfile, true),
                       IllegalArgumentException.class,
                       "socket directory not writable");

        assertTrue(subdir.delete());
        checkException(() -> validateHostname("unix_domain:" + subfile, true),
                       IllegalArgumentException.class,
                       "socket directory not found");
   }

    private void validateHostname(String hostname, boolean allowUnixDomain) {
        parser.validateHostname(hostname, allowUnixDomain);
    }

    /** Create a minimal command parser we can use for testing */
    private static class Parser extends CommandParser {
        Parser() {
            super(new String[0]);
        }
        @Override
        protected boolean checkArg(String arg) { return true; }
        @Override
        protected void verifyArgs() { }
        @Override
        public void usage(String errorMsg) {
            throw new IllegalArgumentException("Usage: " + errorMsg);
        }
    }
}
