/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.PasswordStore.LoginId;
import oracle.kv.impl.security.wallet.WalletManager;
import oracle.kv.impl.security.wallet.WalletStore;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test the WalletStore API.
 */
public class WalletStoreTest extends TestBase {

    private String walletDir = null;

    private final char[] validPassphrase = "jn34pr12".toCharArray();
    private final char[] validPassphrase2 = "jn34pr13".toCharArray();
    private final char[] invalidPassphrase = "x".toCharArray();
    private final char[] nullPassphrase = null;

    private final char[] secret1 =
        ("secret#1" + ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL_DEFAULT).
        toCharArray();
    private final char[] secret2 =
        ("secret#2" + ParameterState.SEC_PASSWORD_ALLOWED_SPECIAL_DEFAULT).
        toCharArray();
    private final char[] dbSecret1 = "dbsecret#1".toCharArray();
    private final char[] dbSecret2 = "dbsecret#2".toCharArray();

    @Override
    public void setUp()
        throws Exception {

        walletDir = new File(TestUtils.getTestDir(), "wallet").getPath();
        clearDirectory();
    }

    @Override
    public void tearDown()
        throws Exception {

        clearDirectory();
        new File(walletDir).delete();
    }

    @Test
    public void testBasic()
        throws Exception {

        /* Test autologin */
        testBasicWallet(nullPassphrase);

        /* Test with passphrase */
        testBasicWallet(validPassphrase);
    }

    /**
     * A handy interface for running test sequences.
     */
    interface WalletTester {
        void test(PasswordStore wallet) throws Exception;
    }

    /**
     * Test basic wallet operation, either with or without a passphrase.
     */
    public void testBasicWallet(char[] passphrase)
        throws Exception {

        clearDirectory();

        final PasswordManager pwdMgr = PasswordManager.load(
            "oracle.kv.impl.security.wallet.WalletManager");
        assertNotNull(pwdMgr);

        final File walletLoc = new File(walletDir);
        PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);

        /*
         * Wallet should not yet exist
         */
        assertTrue(!wallet.exists());

        if (passphrase != null) {
            /*
             * And isValidPassphrase() should work
             */
            assertTrue(wallet.isValidPassphrase(passphrase));
        }

        /*
         * Create the wallet
         */

        wallet.create(passphrase);

        /*
         * Wallet should exist now
         */
        assertTrue(wallet.exists());

        /*
         * And requiresPassphrase() should work
         */
        assertTrue(wallet.requiresPassphrase() == (passphrase != null));

        /*
         * Initially, there should be no secrets defined
         */

        Collection<String> aliases = wallet.getSecretAliases();
        assertNotNull(aliases);
        assertTrue(aliases.isEmpty());

        /*
         * and no logins defined
         */

        Collection<LoginId> logins = wallet.getLogins();
        assertNotNull(logins);
        assertTrue(logins.isEmpty());

        /*
         * Create 2 secrets
         */
        wallet.setSecret("secret1", secret1);
        wallet.setSecret("secret2", secret2);

        /*
         * Create 2 logins
         */
        wallet.setLogin(new LoginId("db1", "user1"), dbSecret1);
        wallet.setLogin(new LoginId("db2", "user2"), dbSecret2);

        /*
         * We should now have 2 secrets
         */

        aliases = wallet.getSecretAliases();
        assertNotNull(aliases);
        assertEquals(2, aliases.size());

        /*
         * and 2 logins
         */

        logins = wallet.getLogins();
        assertNotNull(logins);
        assertEquals(2, logins.size());

        /*
         * Check that the secrets are recorded and equal, but with
         * different identity.
         */
        char[] secret = wallet.getSecret("secret1");
        assertNotNull(secret);
        assertTrue(secret != secret1);
        assertArrayEquals(secret, secret1);

        secret = wallet.getSecret("secret2");
        assertNotNull(secret);
        assertTrue(secret != secret2);
        assertArrayEquals(secret, secret2);

        /*
         * Check that the logins are recorded and equal, but with
         * different identity for the secrets.
         */
        LoginId loginId = wallet.getLoginId("db1");
        assertNotNull(loginId);
        assertEquals("db1", loginId.getDatabase());
        assertEquals("user1", loginId.getUser());

        secret = wallet.getLoginSecret("db1");
        assertNotNull(secret);
        assertTrue(secret != dbSecret1);
        assertArrayEquals(secret, dbSecret1);

        loginId = wallet.getLoginId("db2");
        assertNotNull(loginId);
        assertEquals("db2", loginId.getDatabase());
        assertEquals("user2", loginId.getUser());

        secret = wallet.getLoginSecret("db2");
        assertNotNull(secret);
        assertTrue(secret != dbSecret2);
        assertArrayEquals(secret, dbSecret2);

        /*
         * Delete a secret
         */
        wallet.deleteSecret("secret1");

        /*
         * Delete a login
         */
        wallet.deleteLogin("db1");

        /*
         * The following sequence will be repeated, so wrap it for re-use.
         */

        WalletTester tester = new WalletTester() {
                @Override
                public void test(PasswordStore testWallet)
                    throws Exception {

                    /*
                     * We should have 1 secret
                     */
                    final Collection<String> ialiases =
                        testWallet.getSecretAliases();
                    assertNotNull(ialiases);
                    assertEquals(1, ialiases.size());
                    char[] isecret = testWallet.getSecret("secret2");
                    assertNotNull(isecret);
                    assertArrayEquals(isecret, secret2);

                    /*
                     * and 1 login
                     */
                    final Collection<LoginId> ilogins = testWallet.getLogins();
                    assertNotNull(ilogins);
                    assertEquals(1, ilogins.size());

                    final LoginId iloginId = testWallet.getLoginId("db2");
                    assertNotNull(iloginId);
                    assertEquals("db2", iloginId.getDatabase());
                    assertEquals("user2", iloginId.getUser());

                    isecret = testWallet.getLoginSecret("db2");
                    assertNotNull(isecret);
                    assertArrayEquals(isecret, dbSecret2);
                }
            };

        /* Test the secret and login */
        tester.test(wallet);

        /* Test save */
        wallet.save();

        /* Secret and login should be intact */
        tester.test(wallet);

        /* discard */
        wallet.discard();

        /* Create a new handle and open */
        wallet = pwdMgr.getStoreHandle(walletLoc);

        /*
         * The wallet should already exist
         */
        assertTrue(wallet.exists());

        /*
         * And requiresPassphrase() should work
         */
        assertTrue(wallet.requiresPassphrase() == (passphrase != null));

        wallet.open(passphrase);

        /*
         * The wallet should still exist
         */
        assertTrue(wallet.exists());

        /* Secret and login should be intact */
        tester.test(wallet);

        /* Change the passphrase several times */

        final char[][] testPassphrases = new char[][] {
            validPassphrase, validPassphrase2, nullPassphrase };

        for (char[] newPassphrase : testPassphrases) {

            wallet.setPassphrase(newPassphrase);

            /* Secret and login should be intact */
            tester.test(wallet);

            /* Discard and re-open */
            wallet.discard();
            wallet = pwdMgr.getStoreHandle(walletLoc);

            /*
             * And requiresPassphrase() should work
             */
            assertTrue(wallet.requiresPassphrase() == (newPassphrase != null));

            wallet.open(newPassphrase);

            /* Secret and login should be intact */
            tester.test(wallet);
        }

        /* Wallet APIs should not add security provider implictly */
        assertNull(Security.getProvider(WalletStore.ORACLE_PKI_PROVIDER));
    }

    @Test
    public void testEmptySecret() throws Exception {

        clearDirectory();

        final String alias = "secret1";

        final PasswordManager pwdMgr = PasswordManager.load(
            "oracle.kv.impl.security.wallet.WalletManager");
        assertNotNull(pwdMgr);

        final File walletLoc = new File(walletDir);
        PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);

        wallet.create(null);
        wallet.setSecret(alias, "".toCharArray());
        wallet.save();
        wallet.discard();

        wallet = pwdMgr.getStoreHandle(walletLoc);
        wallet.open(null);
        assertEquals(new String(wallet.getSecret(alias)), "");
        wallet.discard();
    }

    /*
     * This test reproduces problem in SR25990.
     */
    @Test
    public void testReopenWallet() throws Exception {

        clearDirectory();

        /* Create wallet dir */
        final PasswordManager pwdMgr =
            PasswordManager.load(
                "oracle.kv.impl.security.wallet.WalletManager");
        final File walletLoc = new File(walletDir);
        PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
        wallet.create(null);
        assertTrue(wallet.exists());

        /* Set wallet secret */
        wallet.setSecret("secret1", secret1);
        wallet.save();
        wallet.discard();

        ArrayList<OpenWalletThread> list =
            new ArrayList<OpenWalletThread>();
        /* Multiple threads to open the same wallet */
        for (int i = 0; i < 10; i++) {
            OpenWalletThread thread = new OpenWalletThread();
            thread.start();
            list.add(thread);
        }

        for (OpenWalletThread thread : list) {
            thread.join();
            /* Throw exception in main thread, indicate test failure */
            if (thread.getFailException() != null) {
                throw thread.getFailException();
            }
        }
    }

    @Test
    public void testErrors()
        throws Exception {

        clearDirectory();

        final PasswordManager pwdMgr = PasswordManager.load(
            "oracle.kv.impl.security.wallet.WalletManager");
        assertNotNull(pwdMgr);

        /*
         * Non-existent directory path
         */
        File walletLoc = new File("/This/path/should/not/exist");
        final PasswordStore neWallet = pwdMgr.getStoreHandle(walletLoc);

        assertTrue(!neWallet.exists());

        /* Can't open */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.open(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /* Can't create */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.create(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /*
         * Non-existent relative path
         */
        walletLoc = new File("thisPathShouldNotExist");
        assertFalse(walletLoc.exists());
        final PasswordStore nerelWallet = pwdMgr.getStoreHandle(walletLoc);
        assertFalse(nerelWallet.exists());

        /* Can't open and throws exception with absolute path */
        checkException(() -> nerelWallet.open(nullPassphrase),
                       IOException.class,
                       walletLoc.getAbsolutePath());

        /*
         * Non-writable directory path
         */
        walletLoc = new File("/mywallet");
        final PasswordStore nwWallet = pwdMgr.getStoreHandle(walletLoc);

        assertTrue(!nwWallet.exists());

        /* Can't open */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.open(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /* Can't create */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.create(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /*
         * Work with a valid path
         */
        walletLoc = new File(walletDir);

        /*
         * Invalid passphrase
         */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            assertTrue(!wallet.isValidPassphrase(invalidPassphrase));
            wallet.create(invalidPassphrase);
            fail("should not have been able to create");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            assertTrue(!wallet.isValidPassphrase(invalidPassphrase));
            wallet.open(invalidPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /*
         * Most operations invalid until open
         */

        /* getSecret() */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.getSecret("x");
            fail("should not have been able to call getSecret");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getSecrets() */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.getSecretAliases();
            fail("should not have been able to call getSecrets");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* deleteSecret() */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.deleteSecret("x");
            fail("should not have been able to call deleteSecret");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getLoginId() */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.getLoginId("x");
            fail("should not have been able to call getLoginId");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getLoginSecret() */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.getLoginSecret("x");
            fail("should not have been able to call getLoginSecret");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getLogins() */
        try {
            final PasswordStore wallet = pwdMgr.getStoreHandle(walletLoc);
            wallet.getLogins();
            fail("should not have been able to call getLogins");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }
    }

    /**
     * Delete the wallet directory and its contents
     */
    private void clearDirectory() {
        final File dir = new File(walletDir);
        if (dir.exists() && dir.isDirectory()) {
            final File[] files = dir.listFiles();
            if (files != null) {
                for (File walletFile : files) {
                    walletFile.delete();
                }
            }
            dir.delete();
        }
    }

    /* Thread to open wallet at the same location */
    class OpenWalletThread extends Thread {

        private Exception failException;

        public Exception getFailException() {
            return failException;
        }

        @Override
        public void run() {
            /* Try 100 times of the open operation */
            for (int i = 0; i < 100; i++) {
                try {
                    final WalletManager storeMgr = new WalletManager();
                    final PasswordStore fileStore =
                       storeMgr.getStoreHandle(new File(walletDir));
                    fileStore.open(null);
                } catch (Exception e) {
                    failException = e;
                }
            }
        }
    }
}
