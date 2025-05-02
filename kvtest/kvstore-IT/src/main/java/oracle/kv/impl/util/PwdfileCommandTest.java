/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.TestUtils.assertMatch;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

/**
 * Test the PwdfileCommand command-line interface.
 * This borrows heavily from the WalletCommandTest class, but is modified
 * for the specifics of the FileStore implementation
 */
public class PwdfileCommandTest extends SecurityShellTestBase {

    private String pwdfileLoc = null;
    private String nonexistentDirFile = null;
    private String existingFile = null;

    private final String secret1 = "secret-1";
    private final String secret2 = "secret-2";

    TestPasswordReader pwReader = new TestPasswordReader(null);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        pwdfileLoc = new File(TestUtils.getTestDir(), "pwdfile").getPath();
        clearFile();

        String nonexistentDir =
            new File(TestUtils.getTestDir(), "non-existent").getPath();
        nonexistentDirFile = new File(nonexistentDir, "pwdfile").getPath();

        existingFile = new File(TestUtils.getTestDir(), "aFile").getPath();
        new File(existingFile).createNewFile();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        clearFile();
        new File(pwdfileLoc).delete();
        new File(existingFile).delete();
    }

    /**
     * Create an autologin password store.
     */
    @Test
    public void createAutologinStore()
        throws Exception {

        clearFile();

        /* Auto-login password stores need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        String s = runCLICommand(pwReader,
                                 new String[] {"pwdfile",
                                               "create",
                                               "-file",
                                               nonexistentDirFile});

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a writable directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "create",
                                        "-file",
                                        "/makeanewfile"});

        assertTrue(s.indexOf("is not writable") != -1);

        /**
         * existing file already
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "create",
                                        "-file",
                                        existingFile});

        assertTrue(s.indexOf("already exists") != -1);

        /**
         * valid directory - this one should work
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "create",
                                        "-file",
                                        pwdfileLoc});
        assertTrue(s.indexOf("Created") != -1);

    }

    /**
     * Open an autologin password store.
     */
    @Test
    public void openAutologinStore()
        throws Exception {

        clearFile();

        /* Auto-login password stores need no passphrase */
        pwReader.setPassword(null);

        /**
         * Non-password file detection
         */
        final String s = runCLICommand(pwReader,
                                       new String[] {"pwdfile",
                                                     "secret",
                                                     "-file",
                                                     existingFile,
                                                     "-list"});

        assertTrue(s.indexOf("contain a password store") != -1);
    }

    /**
     * Test secret commands in an autologin store.
     */
    @Test
    public void testAutologinSecrets()
        throws Exception {

        clearFile();

        /*
         * Auto-login stores need no passphrase, but we do want to provide
         * a secret
         */
        pwReader.setPassword(secret1);

        /**
         * valid directory
         */
        String s = runCLICommand(pwReader,
                                 new String[] {"pwdfile",
                                               "create",
                                               "-file",
                                               pwdfileLoc});
        assertTrue(s.indexOf("Created") != -1);

        /**
         * create a secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "secret",
                                        "-file",
                                        pwdfileLoc,
                                        "-set",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("created") != -1);

        /**
         * update the secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "secret",
                                        "-file",
                                        pwdfileLoc,
                                        "-set",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("updated") != -1);

        /**
         * list the secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "secret",
                                        "-file",
                                        pwdfileLoc,
                                        "-list"});
        assertTrue(s.indexOf("alias1") != -1);

        /**
         * delete the secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "secret",
                                        "-file",
                                        pwdfileLoc,
                                        "-delete",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("deleted") != -1);

        /**
         * delete the secret again
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "secret",
                                        "-file",
                                        pwdfileLoc,
                                        "-delete",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("not exist") != -1);

        /**
         * create a secret without prompting
         */
        pwReader.setPassword(null);
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "secret",
                                        "-file",
                                        pwdfileLoc,
                                        "-set",
                                        "-alias",
                                        "alias2",
                                        "-secret",
                                        secret2 });
        assertTrue(s.indexOf("created") != -1);

    }

    /**
     * Test login commands in an autologin store.
     */
    @Test
    public void testAutologinLogins()
        throws Exception {

        clearFile();

        /*
         * Auto-login stores need no passphrase, but we do want to provide
         * a secret for the login
         */
        pwReader.setPassword(secret1);

        /**
         * valid directory
         */
        String s = runCLICommand(pwReader,
                                 new String[] {"pwdfile",
                                               "create",
                                               "-file",
                                               pwdfileLoc});
        assertTrue(s.indexOf("Created") != -1);

        /**
         * create a login
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "login",
                                        "-file",
                                        pwdfileLoc,
                                        "-set",
                                        "-db",
                                        "db1",
                                        "-user",
                                        "user1"});
        assertTrue(s.indexOf("created") != -1);

        /**
         * update the login
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "login",
                                        "-file",
                                        pwdfileLoc,
                                        "-set",
                                        "-db",
                                        "db1",
                                        "-user",
                                        "user1"});
        assertTrue(s.indexOf("updated") != -1);

        /**
         * list the login
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "login",
                                        "-file",
                                        pwdfileLoc,
                                        "-list"});
        assertTrue(s.indexOf("db1") != -1);

        /**
         * delete the login
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "login",
                                        "-file",
                                        pwdfileLoc,
                                        "-delete",
                                        "-db",
                                        "db1"});
        assertTrue(s.indexOf("deleted") != -1);

        /**
         * delete the login again
         */
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "login",
                                        "-file",
                                        pwdfileLoc,
                                        "-delete",
                                        "-db",
                                        "db1"});
        assertTrue(s.indexOf("not exist") != -1);

        /**
         * create a login without prompting
         */
        pwReader.setPassword(null);
        s = runCLICommand(pwReader,
                          new String[] {"pwdfile",
                                        "login",
                                        "-file",
                                        pwdfileLoc,
                                        "-set",
                                        "-db",
                                        "db2",
                                        "-user",
                                        "user2",
                                        "-secret",
                                        secret2 });
        assertTrue(s.indexOf("created") != -1);
    }

    @Test
    public void testWhitespace() throws Exception {
        clearFile();
        pwReader.setPassword(null);

        assertMatch(
            "Created\n",
            runCLICommand(pwReader,
                          "pwdfile", "create", "-file", pwdfileLoc));

        assertMatch(
            "Error handling command pwdfile: " +
            "Leading and trailing whitespace are not permitted for alias\n",
            runCLICommand(pwReader,
                          "pwdfile", "secret", "-file", pwdfileLoc,
                          "-set", "-alias", " spacealias",
                          "-secret", "mysecret"));

        assertMatch(
            "Error handling command pwdfile: " +
            "Leading and trailing whitespace are not permitted for secret\n",
            runCLICommand(pwReader,
                          "pwdfile", "secret", "-file", pwdfileLoc,
                          "-set", "-alias", "myalias",
                          "-secret", " spacesecret"));

        assertMatch(
            "Error handling command pwdfile: " +
            "Leading and trailing whitespace are not permitted for db\n",
            runCLICommand(pwReader,
                          "pwdfile", "login", "-file", pwdfileLoc,
                          "-set", "-db", " spacedb", "-user", "myuser",
                          "-secret", "mysecret"));

        assertMatch(
            "Error handling command pwdfile: " +
            "Leading and trailing whitespace are not permitted for user\n",
            runCLICommand(pwReader,
                          "pwdfile", "login", "-file", pwdfileLoc,
                          "-set", "-db", "mydb", "-user", " spaceuser",
                          "-secret", "mysecret"));

        assertMatch(
            "Error handling command pwdfile: " +
            "Leading and trailing whitespace are not permitted for secret\n",
            runCLICommand(pwReader,
                          "pwdfile", "login", "-file", pwdfileLoc,
                          "-set", "-db", "mydb", "-user", "myuser",
                          "-secret", " spacesecret"));
    }

    private void clearFile() {
        final File pwdfile = new File(pwdfileLoc);
        pwdfile.delete();
    }
}
