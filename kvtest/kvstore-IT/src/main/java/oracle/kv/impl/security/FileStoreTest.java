/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static oracle.kv.impl.security.filestore.FileStore.checkNameIsValid;
import static oracle.kv.impl.security.filestore.FileStore.checkSecretIsValid;
import static oracle.kv.impl.security.filestore.FileStore.hasLeadingTrailingWhitespace;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.PasswordStore.LoginId;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test the FileStore API.
 */
public class FileStoreTest extends TestBase {

    private String pwdStoreFile = null;

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

    private static final String FILE_STORE_MANAGER_CLASS =
        "oracle.kv.impl.security.filestore.FileStoreManager";


    @Override
    public void setUp() throws Exception {

        super.setUp();
        pwdStoreFile = new File(TestUtils.getTestDir(), "pwdfile").getPath();
        clearFile();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        clearFile();
    }

    /**
     * A handy interface for running test sequences.
     */
    interface FileTester {
        void test(PasswordStore pwdStore) throws Exception;
    }

    /**
     * Test basic file store operation, either with or without a passphrase.
     */
    @Test
    public void testBasic()
        throws Exception {

        clearFile();

        final PasswordManager pwdMgr =
            PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        assertNotNull(pwdMgr);

        final File pwdStoreLoc = new File(pwdStoreFile);
        PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);

        /*
         * File should not yet exist
         */
        assertTrue(!pwdStore.exists());

        /*
         * And isValidPassphrase() should work
         */
        assertTrue(pwdStore.isValidPassphrase(nullPassphrase));
        assertTrue(!pwdStore.isValidPassphrase(invalidPassphrase));

        /*
         * Create the pwdStore
         */

        pwdStore.create(nullPassphrase);

        /*
         * File should exist now
         */
        assertTrue(pwdStore.exists());

        /*
         * And requiresPassphrase() should work
         */
        assertTrue(!pwdStore.requiresPassphrase());

        /*
         * Initially, there should be no secrets defined
         */

        Collection<String> aliases = pwdStore.getSecretAliases();
        assertNotNull(aliases);
        assertTrue(aliases.isEmpty());

        /*
         * and no logins defined
         */

        Collection<LoginId> logins = pwdStore.getLogins();
        assertNotNull(logins);
        assertTrue(logins.isEmpty());

        /*
         * Create 2 secrets
         */
        pwdStore.setSecret("secret1", secret1);
        pwdStore.setSecret("secret2", secret2);

        /*
         * Create 2 logins
         */
        pwdStore.setLogin(new LoginId("db1", "user1"), dbSecret1);
        pwdStore.setLogin(new LoginId("db2", "user2"), dbSecret2);

        /*
         * We should now have 2 secrets
         */

        aliases = pwdStore.getSecretAliases();
        assertNotNull(aliases);
        assertEquals(2, aliases.size());

        /*
         * and 2 logins
         */

        logins = pwdStore.getLogins();
        assertNotNull(logins);
        assertEquals(2, logins.size());

        /*
         * Check that the secrets are recorded and equal, but with
         * different identity.
         */
        char[] secret = pwdStore.getSecret("secret1");
        assertNotNull(secret);
        assertNotSame(secret, secret1);
        assertArrayEquals(secret, secret1);

        secret = pwdStore.getSecret("secret2");
        assertNotNull(secret);
        assertNotSame(secret, secret2);
        assertArrayEquals(secret, secret2);

        /*
         * Check that the logins are recorded and equal, but with
         * different identity for the secrets.
         */
        LoginId loginId = pwdStore.getLoginId("db1");
        assertNotNull(loginId);
        assertEquals("db1", loginId.getDatabase());
        assertEquals("user1", loginId.getUser());

        secret = pwdStore.getLoginSecret("db1");
        assertNotNull(secret);
        assertNotSame(secret, dbSecret1);
        assertArrayEquals(secret, dbSecret1);

        loginId = pwdStore.getLoginId("db2");
        assertNotNull(loginId);
        assertEquals("db2", loginId.getDatabase());
        assertEquals("user2", loginId.getUser());

        secret = pwdStore.getLoginSecret("db2");
        assertNotNull(secret);
        assertNotSame(secret, dbSecret2);
        assertArrayEquals(secret, dbSecret2);

        /*
         * Delete a secret
         */
        pwdStore.deleteSecret("secret1");

        /*
         * Delete a login
         */
        pwdStore.deleteLogin("db1");

        /*
         * The following sequence will be repeated, so wrap it for re-use.
         */

        FileTester tester = new FileTester() {
                @Override
                public void test(PasswordStore testFile)
                    throws Exception {

                    /*
                     * We should have 1 secret
                     */
                    final Collection<String> ialiases =
                        testFile.getSecretAliases();
                    assertNotNull(ialiases);
                    assertEquals(1, ialiases.size());
                    char[] isecret = testFile.getSecret("secret2");
                    assertNotNull(isecret);
                    assertArrayEquals(isecret, secret2);

                    /*
                     * and 1 login
                     */
                    final Collection<LoginId> ilogins = testFile.getLogins();
                    assertNotNull(ilogins);
                    assertEquals(1, ilogins.size());

                    final LoginId iloginId = testFile.getLoginId("db2");
                    assertNotNull(iloginId);
                    assertEquals("db2", iloginId.getDatabase());
                    assertEquals("user2", iloginId.getUser());

                    isecret = testFile.getLoginSecret("db2");
                    assertNotNull(isecret);
                    assertArrayEquals(isecret, dbSecret2);
                }
            };

        /* Test the secret and login */
        tester.test(pwdStore);

        /* Test save */
        pwdStore.save();

        /* Secret and login should be intact */
        tester.test(pwdStore);

        /* discard */
        pwdStore.discard();

        /* Create a new handle and open */
        pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);

        /*
         * The pwdStore should already exist
         */
        assertTrue(pwdStore.exists());

        /*
         * And requiresPassphrase() should work
         */
        assertTrue(!pwdStore.requiresPassphrase());

        pwdStore.open(nullPassphrase);

        /*
         * The pwdStore should still exist
         */
        assertTrue(pwdStore.exists());

        /* Secret and login should be intact */
        tester.test(pwdStore);

        /* Check the setPassphrase API */

        try {
            pwdStore.setPassphrase(invalidPassphrase);
            fail("setPassphrase should have failed");
        } catch (UnsupportedOperationException uoe) {
            assertTrue(true); /* ignore */
        }

        pwdStore.setPassphrase(nullPassphrase);

        /* Secret and login should be intact */
        tester.test(pwdStore);

        /* Discard and re-open */
        pwdStore.discard();
        pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);

        /*
         * And requiresPassphrase() should work
         */
        assertTrue(!pwdStore.requiresPassphrase());

        pwdStore.open(nullPassphrase);

        /* Secret and login should be intact */
        tester.test(pwdStore);
    }

    @Test
    public void testEmptySecret() throws Exception {

        clearFile();

        final String alias = "emptySecret";

        try (PrintWriter out = new PrintWriter(pwdStoreFile)) {
            out.println("Password Store:");
            out.println("secret." + alias + "=");
        }

        final File pwdStoreLoc = new File(pwdStoreFile);
        final PasswordManager pwdMgr =
            PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
        pwdStore.open(null);
        assertNull(pwdStore.getSecret(alias));
        pwdStore.discard();
    }

    @Test
    public void testErrors()
        throws Exception {

        clearFile();

        final PasswordManager pwdMgr =
            PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        assertNotNull(pwdMgr);

        /*
         * Non-existent directory path
         */
        File pwdStoreLoc = new File("/This/path/should/not/exist");
        final PasswordStore neFile = pwdMgr.getStoreHandle(pwdStoreLoc);

        assertTrue(!neFile.exists());

        /* Can't open */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.open(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /* Can't create */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.create(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /*
         * Non-existent relative path
         */
        pwdStoreLoc = new File("thisPathShouldNotExist");
        assertFalse(pwdStoreLoc.exists());
        final PasswordStore nerelFile = pwdMgr.getStoreHandle(pwdStoreLoc);
        assertFalse(nerelFile.exists());

        /* Can't open and throws exception with absolute path */
        checkException(() -> nerelFile.open(nullPassphrase),
                       IOException.class,
                       pwdStoreLoc.getAbsolutePath());

        /*
         * Non-writable directory path
         */
        pwdStoreLoc = new File("/mypwdStore");
        final PasswordStore nwFile = pwdMgr.getStoreHandle(pwdStoreLoc);

        assertTrue(!nwFile.exists());

        /* Can't open */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.open(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /* Can't create */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.create(nullPassphrase);
            fail("should not have been able to open");
        } catch (IOException ioe) {
            assertTrue(true); /* ignore */
        }

        /*
         * Work with a valid path
         */
        pwdStoreLoc = new File(pwdStoreFile);

        /* Can't create with non-null passphrase*/
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.create(invalidPassphrase);
            fail("should not have been able to create");
        } catch (UnsupportedOperationException uoe) {
            assertTrue(true); /* ignore */
        }

        /*
         * Most operations invalid until open
         */

        /* getSecret() */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.getSecret("x");
            fail("should not have been able to call getSecret");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getSecrets() */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.getSecretAliases();
            fail("should not have been able to call getSecrets");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* deleteSecret() */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.deleteSecret("x");
            fail("should not have been able to call deleteSecret");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getLoginId() */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.getLoginId("x");
            fail("should not have been able to call getLoginId");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getLoginSecret() */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.getLoginSecret("x");
            fail("should not have been able to call getLoginSecret");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* getLogins() */
        try {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
            pwdStore.getLogins();
            fail("should not have been able to call getLogins");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }
    }

    @Test
    public void testHasLeadingTrailingWhitespace() throws Exception {
        assertFalse(hasLeadingTrailingWhitespace(""));
        assertTrue(hasLeadingTrailingWhitespace(" "));
        assertTrue(hasLeadingTrailingWhitespace("  "));
        assertFalse(hasLeadingTrailingWhitespace("a"));
        assertFalse(hasLeadingTrailingWhitespace("a b"));
        assertTrue(hasLeadingTrailingWhitespace(" a"));
        assertTrue(hasLeadingTrailingWhitespace("a "));
        assertTrue(hasLeadingTrailingWhitespace(" a "));
        assertTrue(hasLeadingTrailingWhitespace("\n"));
        assertTrue(hasLeadingTrailingWhitespace("\na"));
        assertTrue(hasLeadingTrailingWhitespace("a\n"));
        assertTrue(hasLeadingTrailingWhitespace("\t"));
        assertTrue(hasLeadingTrailingWhitespace("\ta"));
        assertTrue(hasLeadingTrailingWhitespace("a\t"));

        /* Test a whitespace character that is not recognized by trim */
        final int paraSeparator = 0x2029;
        assertFalse(hasLeadingTrailingWhitespace(string(paraSeparator)));
        assertFalse(
            hasLeadingTrailingWhitespace(
                string(paraSeparator, paraSeparator)));
        assertTrue(hasLeadingTrailingWhitespace(string(' ', paraSeparator)));
        assertTrue(hasLeadingTrailingWhitespace(string(paraSeparator, ' ')));
        assertTrue(
            hasLeadingTrailingWhitespace(string(' ', paraSeparator, ' ')));

        /* Test a character that is represented by two UTF-16 chars */
        final int smiley = 0x1f600;
        assertFalse(hasLeadingTrailingWhitespace(string(smiley)));
        assertFalse(hasLeadingTrailingWhitespace(string(smiley, smiley)));
        assertTrue(hasLeadingTrailingWhitespace(string(' ', smiley)));
        assertTrue(hasLeadingTrailingWhitespace(string(smiley, ' ')));
        assertTrue(hasLeadingTrailingWhitespace(string(' ', smiley, ' ')));
        assertTrue(
            hasLeadingTrailingWhitespace(string(smiley, smiley, ' ')));
    }

    @Test
    public void testCheckNameIsValid() {
        checkNameIsValid("usrname", "abc");
        checkException(() -> checkNameIsValid("usrname", " def"),
                       IllegalArgumentException.class,
                       "whitespace .* not permitted for usrname");
    }

    @Test
    public void testCheckSecretIsValid() {
        checkSecretIsValid("passwd", new char[] { 'a', 'b', 'c' });
        checkException(
            () -> checkSecretIsValid(
                "passwd", new char[] { ' ', 'd', 'e', 'f' }),
            IllegalArgumentException.class,
            "whitespace .* not permitted for passwd");
    }

    @Test
    public void testWhitespaceValues() throws Exception {
        clearFile();
        final PasswordManager pwdMgr =
            PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        assertNotNull(pwdMgr);
        final File pwdStoreLoc = new File(pwdStoreFile);
        final PasswordStore pwdStore = pwdMgr.getStoreHandle(pwdStoreLoc);
        pwdStore.create(null);

        checkException(
            () -> pwdStore.setSecret(" spacealias", "mysecret".toCharArray()),
            IllegalArgumentException.class,
            "whitespace .* not permitted for alias");
        assertNull(pwdStore.getSecret(" spacealias"));

        checkException(
            () -> pwdStore.setSecret("myalias", " spacesecret".toCharArray()),
            IllegalArgumentException.class,
            "whitespace .* not permitted for secret");
        assertNull(pwdStore.getSecret("myalias"));

        checkException(
            () -> pwdStore.setLogin(
                new LoginId(" spacedb", "myuser"), "mypasswd".toCharArray()),
            IllegalArgumentException.class,
            "whitespace .* not permitted for db");
        assertNull(pwdStore.getLoginId(" spacedb"));
        assertNull(pwdStore.getLoginSecret(" spacedb"));

        checkException(
            () -> pwdStore.setLogin(
                new LoginId("mydb", " spaceuser"), "mypasswd".toCharArray()),
            IllegalArgumentException.class,
            "whitespace .* not permitted for user");
        assertNull(pwdStore.getLoginId("mydb"));
        assertNull(pwdStore.getLoginSecret("mydb"));

        checkException(
            () -> pwdStore.setLogin(
                new LoginId("mydb", "myuser"), " spacepasswd".toCharArray()),
            IllegalArgumentException.class,
            "whitespace .* not permitted for secret");
        assertNull(pwdStore.getLoginId("mydb"));
        assertNull(pwdStore.getLoginSecret("mydb"));
    }

    /**
     * Create a string from a series of codepoints, each possibly requiring
     * more than a single char.
     */
    private static String string(int... codePoints) {
        return new String(codePoints, 0, codePoints.length);
    }

    /**
     * Delete the password file
     */
    private void clearFile() {
        new File(pwdStoreFile).delete();
    }
}
