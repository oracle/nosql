/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.util;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;

import oracle.kv.TestBase;

import org.junit.Test;

public class PasswordReaderTest extends TestBase {
    private static String PASSWORD = "NoSql00__testpwd";
    private static String JLINE_DISABLE = "oracle.kv.shell.jline.disable";
    private static String SYS_CONSOLE_DISABLE =
        "oracle.kv.shell.sys.console.disable";

    @Override
    public void setUp() {
        final ByteArrayInputStream bais =
            new ByteArrayInputStream(PASSWORD.getBytes());
        System.setIn(bais);
        System.setProperty(JLINE_DISABLE, "true");
        System.setProperty(SYS_CONSOLE_DISABLE, "true");
    }

    @Override
    public void tearDown() {
        System.setIn(System.in);
        System.setProperty(JLINE_DISABLE, "false");
        System.setProperty(SYS_CONSOLE_DISABLE, "false");
    }

    @Test
    public void testShellPasswordReader()
        throws Exception {

        final ShellPasswordReader pwdReader = new ShellPasswordReader();
        testReadPassword(pwdReader);
    }

    @Test
    public void testConsolePasswordReader()
        throws Exception {

        ConsolePasswordReader pwdReader =
            ConsolePasswordReader.createTestReader();
        testReadPassword(pwdReader);
    }

    private void testReadPassword(PasswordReader pwdReader)
        throws Exception {

        final char[] pwd = pwdReader.readPassword("");
        assertArrayEquals(pwd, PASSWORD.toCharArray());
    }
}
