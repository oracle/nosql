/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

/**
 * Test the WalletCommand command-line interface.
 */
public class WalletCommandTest extends SecurityShellTestBase {

    private String walletDir = null;
    private String existingFile = null;
    private final String validPassphrase = "jn34pr12";
    private final String validPassphrase2 = "jn34pr13";

    private final String secret1 = "secret-1";
    private final String secret2 = "secret-2";

    TestPasswordReader pwReader = new TestPasswordReader(null);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        walletDir = new File(TestUtils.getTestDir(), "wallet").getPath();
        clearDirectory();

        existingFile = new File(TestUtils.getTestDir(), "aFile").getPath();
        new File(existingFile).createNewFile();
    }

    @Override
    public void tearDown() throws Exception {

        clearDirectory();
        new File(existingFile).delete();
    }

    /**
     * Create an autologin wallet (currently, all are autologin).
     */
    @Test
    public void createAutologinWallet()
        throws Exception {

        clearDirectory();

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        String s = runCLICommand(pwReader,
                                 new String[] {"wallet",
                                               "create",
                                               "-directory",
                                               "/nonexistent/directory"});

        assertTrue(s.indexOf("Unable to create") != -1);

        /**
         * not a writable directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "create",
                                        "-directory",
                                        "/"});

        assertTrue(s.indexOf("Unable to create") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "create",
                                        "-directory",
                                        existingFile});

        assertTrue(s.indexOf("not a directory") != -1);

        /**
         * valid directory - this one should work
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "create",
                                        "-directory",
                                        walletDir});
        assertTrue(s.indexOf("Created") != -1);

    }

    /**
     * Test secret commands in an autologin wallet.
     */
    @Test
    public void testAutologinSecrets()
        throws Exception {

        clearDirectory();

        /*
         * Auto-login wallets need no passphrase, but we do want to provide
         * a secret
         */
        pwReader.setPassword(secret1);

        /**
         * valid directory
         */
        String s = runCLICommand(pwReader,
                                 new String[] {"wallet",
                                               "create",
                                               "-directory",
                                               walletDir});
        assertTrue(s.indexOf("Created") != -1);

        /**
         * create a secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "secret",
                                        "-directory",
                                        walletDir,
                                        "-set",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("created") != -1);

        /**
         * update the secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "secret",
                                        "-directory",
                                        walletDir,
                                        "-set",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("updated") != -1);

        /**
         * list the secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "secret",
                                        "-directory",
                                        walletDir,
                                        "-list"});
        assertTrue(s.indexOf("alias1") != -1);

        /**
         * delete the secret
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "secret",
                                        "-directory",
                                        walletDir,
                                        "-delete",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("deleted") != -1);

        /**
         * delete the secret again
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "secret",
                                        "-directory",
                                        walletDir,
                                        "-delete",
                                        "-alias",
                                        "alias1"});
        assertTrue(s.indexOf("not exist") != -1);

        /**
         * create a secret without prompting
         */
        pwReader.setPassword(null);
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "secret",
                                        "-directory",
                                        walletDir,
                                        "-set",
                                        "-alias",
                                        "alias2",
                                        "-secret",
                                        secret2 });
        assertTrue(s.indexOf("created") != -1);


    }

    /**
     * Test login commands in an autologin wallet.
     */
    @Test
    public void testAutologinLogins()
        throws Exception {

        clearDirectory();

        /*
         * Auto-login wallets need no passphrase, but we do want to provide
         * a secret for the login
         */
        pwReader.setPassword(secret1);

        /**
         * valid directory
         */
        String s = runCLICommand(pwReader,
                                 new String[] {"wallet",
                                               "create",
                                               "-directory",
                                               walletDir});
        assertTrue(s.indexOf("Created") != -1);

        /**
         * create a login
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "login",
                                        "-directory",
                                        walletDir,
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
                          new String[] {"wallet",
                                        "login",
                                        "-directory",
                                        walletDir,
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
                          new String[] {"wallet",
                                        "login",
                                        "-directory",
                                        walletDir,
                                        "-list"});
        assertTrue(s.indexOf("db1") != -1);

        /**
         * delete the login
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "login",
                                        "-directory",
                                        walletDir,
                                        "-delete",
                                        "-db",
                                        "db1"});
        assertTrue(s.indexOf("deleted") != -1);

        /**
         * delete the login again
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "login",
                                        "-directory",
                                        walletDir,
                                        "-delete",
                                        "-db",
                                        "db1"});
        assertTrue(s.indexOf("not exist") != -1);

        /**
         * create a login without prompting
         */
        pwReader.setPassword(null);
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "login",
                                        "-directory",
                                        walletDir,
                                        "-set",
                                        "-db",
                                        "db2",
                                        "-user",
                                        "user2",
                                        "-secret",
                                        secret2 });
        assertTrue(s.indexOf("created") != -1);
    }

    /**
     * Test passphrase commands.
     */
    @Test
    public void testPassphrases()
        throws Exception {

        clearDirectory();

        pwReader.setPassword(null);
        String s = runCLICommand(pwReader,
                                 new String[] {"wallet",
                                               "create",
                                               "-directory",
                                               walletDir});
        assertTrue(s.indexOf("Created") != -1);

        /**
         * Null change to autologin
         */
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "passphrase",
                                        "-directory",
                                        walletDir,
                                        "-autologin"});
        assertTrue(s.indexOf("Set to autologin") != -1);

        /**
         * Change to passphrase
         */
        pwReader.setPassword(validPassphrase);
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "passphrase",
                                        "-directory",
                                        walletDir});
        assertTrue(s.indexOf("Passphrase set") != -1);

        /**
         * Change the passphrase
         */
        pwReader.setPasswords(new String[] { validPassphrase,
                                             validPassphrase2 });
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "passphrase",
                                        "-directory",
                                        walletDir});
        assertTrue(s.indexOf("Passphrase set") != -1);

        /**
         * Change back to autologin
         */
        pwReader.setPassword(validPassphrase2);
        s = runCLICommand(pwReader,
                          new String[] {"wallet",
                                        "passphrase",
                                        "-directory",
                                        walletDir,
                                        "-autologin"});
        assertTrue(s.indexOf("Set to autologin") != -1);
    }

    private void clearDirectory() {
        removeDirectory(new File(walletDir), 20);
    }
}
