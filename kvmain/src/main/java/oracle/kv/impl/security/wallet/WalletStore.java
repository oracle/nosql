/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */
package oracle.kv.impl.security.wallet;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.Security;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.security.pki.OracleSecretStore;
import oracle.security.pki.OracleSecretStoreException;
import oracle.security.pki.OracleWallet;

/**
 * An implementation of PasswordStore based on Oracle Wallet.
 */
public class WalletStore implements PasswordStore {

    /* The Wallet directory */
    private final File storeLocation;

    /* The Wallet object, initialized during open or create */
    private OracleWallet wallet = null;

    /* The SecretStore object, which manages the logins and secrets within
     * the wallet. */
    private OracleSecretStore sstore = null;

    /* If true, the wallet requires no passphrase for access */
    private boolean autoLogin = false;

    /* If true, the SecretStore needs to be written back to the wallet */
    private boolean modified = false;

    /**
     * The following constants are conventions established for the oracle
     * wallet.  Logins (aka Credentials) are a triple of secrets, where
     * the username and connect string (database) aren't really secret.
     * The connections look like
     *     oracle.security.client.connect_stringX
     *     oracle.security.client.usernameX
     *     oracle.security.client.passwordX
     * where X is a numeric index assigned by the wallet code when a
     * credential is added.
     */
    private static final String LOGIN_PREFIX = "oracle.security.client.";
    private static final String LOGIN_CONNECT_PREFIX =
        "oracle.security.client.connect_string";
    private static final String LOGIN_USER_PREFIX =
        "oracle.security.client.username";
    private static final String LOGIN_PW_PREFIX =
        "oracle.security.client.password";
    /*
     * Lock to synchronize for multiple threads to open wallet.
     */
    private static final Object openWalletLock = new Object();

    /*
     * The following error indicators were identified by reverse engineering
     * of the wallet code and may be relevant here.
     * PKI-01002: Invalid password.
     * PKI-01003: Passwords did not match.
     * PKI-02001: A wallet already exists at:
     * PKI-02002: Unable to open the wallet. Check password.
     * PKI-02003: Unable to load the wallet at:
     * PKI-02004: Unable to verify the wallet.
     * PKI-02005: Unable to delete the wallet at:
     * PKI-02006: The specified directory does not exist:
     * PKI-02007: The specified location is not a directory:
     * PKI-02008: Unable to modify a read-only Auto-login wallet.
     * PKI-02009: Unable to create directory.
     * PKI-02010: Invalid MAC for Wallet. Wallet verification failed.
     * PKI-02011: Unable to set file permissions for wallet at
     * PKI-03001: The entry already exists:
     * PKI-03002: No entry found for the alias:
     * PKI-03003: Secrets did not match.
     * PKI-03004: Unable to load the secret store.
     * PKI-03005: Unable to load the Java keystore.
     */

    /**
     * The name of security provider is added by Oracle Wallet create.
     */
    public static final String ORACLE_PKI_PROVIDER = "OraclePKI";

    /**
     * Constructor.
     * Prepare for access to a wallet, which might not yet exist.
     */
    WalletStore(File storeDirectory) {
        this.storeLocation = storeDirectory.getAbsoluteFile();
    }

    /**
     * Create a new wallet.  The storeLocation where the wallet will be stored
     *        must be either empty, or non-existent
     * @param passphrase a passphrase for the store.  If null, this will be
     *        an auto-login wallet.  If non-null, the password must meet
     *        the password criteria.
     * @throws IllegalStateException if this WalletStore has already had
     *         a create or open operation executed.
     * @throws IOException if there are IO errors accessing the wallet
     * @throws PasswordStoreException if there errors in the password store
     *         itself.
     */
    @Override
    public boolean create(char[] passphrase) throws IOException {

        assertNotInitialized();

        final boolean oraclePKINotInstalled =
            (Security.getProperty(ORACLE_PKI_PROVIDER) == null);
        final OracleWallet newWallet = new OracleWallet();
        try {
            if (passphrase != null) {
                newWallet.create(passphrase);
            } else {
                newWallet.createSSO();
            }
            newWallet.saveAs(storeLocation.getPath());
        } catch (IOException ioe) {
            if (exceptionContains(ioe, "PKI-01002")) {
                throw new PasswordStoreException(
                    "The specified passphrase is not valid", ioe);
            }
            if (exceptionContains(ioe, "PKI-02009")) {
                throw new PasswordStoreException(
                    "Unable to create the wallet directory", ioe);
            }
            if (exceptionContains(ioe, "Permission denied")) {
                throw new PasswordStoreException(
                    "Unable to create the wallet file", ioe);
            }
            if (exceptionContains(ioe, "PKI-")) {
                throw new PasswordStoreException(
                    "Error creating the wallet", ioe);
            }
            throw ioe;
        } finally {
            if (oraclePKINotInstalled) {
                /*
                 * Wallet create() adds OraclePKI as the most preferred
                 * security provider implicitly, remove it after use if
                 * it's not installed before calling this method. Leaving
                 * OracleKPI as the preferred security provider could have
                 * introduced unexpected issues in NoSQL applications that used
                 * a wallet with the NoSQL API because the OraclePKI provider
                 * doesn't support all of the features and algorithms of
                 * standard Java Sun provider.
                 *
                 * This issue was uncovered in the KVLocalTest with PKCS12
                 * KeyStore. OraclePKI provider has issue loading PKCS12
                 * KeyStore created by Java Keytool.
                 */
                Security.removeProvider(ORACLE_PKI_PROVIDER);
            }
        }

        this.autoLogin = (passphrase == null);
        final File walletFile = findWalletFile(false);
        if (!SecurityUtils.makeOwnerAccessOnly(walletFile)) {
            throw new PasswordStoreException(
                "Unable to set access permissions for file. " +
                "Correct manually before using the password store");
        }

        try {
            this.sstore = newWallet.getSecretStore();
        } catch (OracleSecretStoreException osse) {
            /* Not clear how this could happen with a newly created wallet */
            throw new PasswordStoreException(
                "Error retrieving secret store from wallet", osse);
        }

        this.autoLogin = (passphrase == null);
        this.wallet = newWallet;
        return true;
    }

    /**
     * Open an existing wallet.
     * @throws IllegalStateException if this WalletStore has already had
     *         a create or open operation executed.
     * @throws IOException if there are IO errors accessing the wallet
     */
    @Override
    public boolean open(char[] passphrase) throws IOException {

        assertNotInitialized();

        final OracleWallet newWallet = new OracleWallet();
        try {
            /*
             * Concurrent attempts to open the wallet will produce an
             * IOException thrown from inside the wallet implementation.
             * Sync the operations on the static lock.
             */
            synchronized (openWalletLock) {
                newWallet.open(storeLocation.getPath(), passphrase);
            }
        } catch (IOException ioe) {
            /* PKI-02002 indicates a bad passphrase - handle this specially */
            if (exceptionContains(ioe, "PKI-02002")) {
                throw new PasswordStoreException(
                    "Error accessing the wallet.  Check your passphrase",
                    ioe);
            }
            /*
             * PKI-02006 indicates non-exist path
             *
             * Since Oraclpki 23.4, opening a wallet with a non-existent path
             * throws IOException with PKI-02006, handle it specially by
             * rethrowing it directly to match the exception thrown by previous
             * releases in this case and to preserve the original error message,
             * which has the full path.
             */
            if (exceptionContains(ioe, "PKI-02006")) {
               throw ioe;
            }
            if (exceptionContains(ioe, "PKI-")) {
                throw new PasswordStoreException(
                    "Error accessing the wallet.", ioe);
            }

            throw ioe;
        }
        try {
            this.sstore = newWallet.getSecretStore();
        } catch (OracleSecretStoreException osse) {
            throw new IOException("Error retrieving secret store from wallet",
                                  osse);
        }

        this.autoLogin = (passphrase == null);
        this.wallet = newWallet;

        return true;
    }

    @Override
    public Collection<String> getSecretAliases() throws IOException {

        assertInitialized();

        final Set<String> secretAliases = new HashSet<String>();
        try {
            @SuppressWarnings("rawtypes")
            final Enumeration e = sstore.internalAliases();
            while (e.hasMoreElements()) {
                final String alias = (String) e.nextElement();
                if (!alias.startsWith(LOGIN_PREFIX)) {
                    secretAliases.add(alias);
                }
            }
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "Error retrieving secret store from wallet", osse);
        }

        return secretAliases;
    }

    @Override
    public char[] getSecret(String alias) throws IOException {

        assertInitialized();

        try {
            return sstore.getSecret(alias);
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "error retrieving secret from wallet", osse);
        }
    }

    @Override
    public boolean setSecret(String alias, char[] secret)throws IOException  {

        assertInitialized();

        try {
            final boolean updated = sstore.containsAlias(alias);
            sstore.setSecret(alias, secret);
            modified = true;
            return updated;
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "error modifying secret store", osse);
        }
    }

    @Override
    public boolean deleteSecret(String alias) throws IOException {

        assertInitialized();

        try {
            if (sstore.containsAlias(alias)) {
                sstore.deleteSecret(alias);
                modified = true;
                return true;
            }
            return false;
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "error modifying secret store", osse);
        }
    }

    @Override
    public Collection<LoginId> getLogins() throws IOException {

        assertInitialized();

        final Set<LoginId> logins = new HashSet<LoginId>();
        try {
            @SuppressWarnings("rawtypes")
            final Enumeration e = sstore.internalAliases();
            while (e.hasMoreElements()) {
                final String alias = (String) e.nextElement();
                if (alias.startsWith(LOGIN_CONNECT_PREFIX)) {
                    final String index =
                        alias.substring(LOGIN_CONNECT_PREFIX.length());
                    final String db = new String(sstore.getSecret(alias));
                    final String userKey = LOGIN_USER_PREFIX + index;
                    final String user = new String(sstore.getSecret(userKey));
                    logins.add(new LoginId(db, user));
                }
            }
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "error retrieving secret store from wallet", osse);
        }

        return logins;
    }

    @Override
    public boolean setLogin(LoginId login, char[] password) throws IOException {

        assertInitialized();

        try {
            final Collection<LoginId> logins = getLogins();
            LoginId existing = null;
            for (LoginId lid : logins) {
                if (databasesEqual(lid.getDatabase(), login.getDatabase())) {
                    existing = lid;
                    break;
                }
            }

            /* suppress SecretStore debug code */
            final OutputCapture output = new OutputCapture();
            try {
                if (existing != null) {
                    sstore.modifyCredential(login.getDatabase().toCharArray(),
                                            login.getUser().toCharArray(),
                                            password);
                } else {
                    sstore.createCredential(login.getDatabase().toCharArray(),
                                            login.getUser().toCharArray(),
                                            password);
                }
            } finally {
                output.restore();
            }
            modified = true;
            return existing != null;
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "error modifying secret store", osse);
        }
    }

    @Override
    public LoginId getLoginId(String database) throws IOException {

        assertInitialized();

        final Collection<LoginId> logins = getLogins();
        for (LoginId lid : logins) {
            if (databasesEqual(lid.getDatabase(), database)) {
                return lid;
            }
        }

        return null;
    }

    @Override
    public char[] getLoginSecret(String database) throws IOException {

        assertInitialized();

        try {
            @SuppressWarnings("rawtypes")
            final Enumeration e = sstore.internalAliases();
            while (e.hasMoreElements()) {
                final String alias = (String) e.nextElement();
                if (alias.startsWith(LOGIN_CONNECT_PREFIX)) {
                    final String db = new String(sstore.getSecret(alias));
                    if (databasesEqual(db, database)) {
                        final String index =
                            alias.substring(LOGIN_CONNECT_PREFIX.length());
                        final String pwKey = LOGIN_PW_PREFIX + index;
                        return sstore.getSecret(pwKey);
                    }
                }
            }
            return null;
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "Error accessing secret store", osse);
        }
    }

    @Override
    public boolean deleteLogin(String db) {

        assertInitialized();

        try {
            /* suppress SecretStore debug code */
            final OutputCapture output = new OutputCapture();
            try {
                sstore.deleteCredential(db.toCharArray());
            } finally {
                output.restore();
            }
            modified = true;
            return true;
        } catch (OracleSecretStoreException osse) /* CHECKSTYLE:OFF */ {
            /* Normally this is because the login doesn't exist */
        }/* CHECKSTYLE:ON */

        return false;
    }

    @Override
    public boolean setPassphrase(char[] passphrase)
        throws IOException {

        assertInitialized();

        if (passphrase == null && autoLogin) {
            return true;
        }

        File originalWalletFile = findWalletFile(false);
        final boolean oraclePKINotInstalled =
            (Security.getProperty(ORACLE_PKI_PROVIDER) == null);
        final OracleWallet newWallet = new OracleWallet();

        try {
            if (autoLogin) {
                /* convert from autologin to passphrase-protected */
                newWallet.create(passphrase);
            } else if (passphrase == null) {
                /* convert from passphrase-protected to autologin */
                newWallet.createSSO();
            } else {
                /* change (presumably) the passphrase */
                newWallet.create(passphrase);
                originalWalletFile = null;
            }
        } finally {
            if (oraclePKINotInstalled) {
                /* Same as create(char[]) */
                Security.removeProvider(ORACLE_PKI_PROVIDER);
            }
        }

        /*
         * Seems like there might be an easier way to do this, but the obvious
         * attempt of storing the old secret store in the new wallet throws an
         * error.
         */

        OracleSecretStore newSstore = null;
        try {
            newSstore = newWallet.getSecretStore();
        } catch (OracleSecretStoreException osse) {
            throw new IOException("error retrieving secret store from wallet",
                                  osse);
        }

        try {
            @SuppressWarnings("rawtypes")
            final Enumeration e = sstore.internalAliases();
            while (e.hasMoreElements()) {
                final String alias = (String) e.nextElement();
                final char[] secret = sstore.getSecret(alias);
                newSstore.setSecret(alias, secret);
            }
        } catch (OracleSecretStoreException osse) {
            throw new IOException("error transferring secrets",
                                  osse);
        }

        /*
         * Put the secretStore in the new wallet
         */
        try {
            newWallet.setSecretStore(newSstore);
        } catch (OracleSecretStoreException osse) {
            throw new PasswordStoreException(
                "Error modifying secret store", osse);
        }

        /*
         * If converting between sso and p12, remove the old file.
         * We need to do this before saving the new wallet, or the wallet
         * code will decide not to write the .p12 file.  A better solution
         * might be to rename the file, do the write, and then only delete
         * the renamed file if the write suceeds.
         */
        if (originalWalletFile != null) {
            originalWalletFile.delete();
        }

        /*
         * Save the new file
         */
        newWallet.saveAs(storeLocation.getPath());
        modified = false;
        sstore = newSstore;
        wallet = newWallet;
        autoLogin = (passphrase == null);

        /*
         * Update protections on the file
         */
        final File walletFile = findWalletFile(false);
        if (!SecurityUtils.makeOwnerAccessOnly(walletFile)) {
            throw new PasswordStoreException(
                "Unable to set access permissions for file. " +
                "Correct manually before using the password store");
        }

        return true;
    }

    /**
     * Save a modified wallet.
     */
    @Override
    public void save() throws IOException {

        assertInitialized();

        if (wallet == null) {
            throw new IllegalStateException("wallet has not been initialized");
        }

        if (modified) {
            try {
                wallet.setSecretStore(sstore);
            } catch (OracleSecretStoreException osse) {
                throw new PasswordStoreException(
                    "Error saving secret store", osse);
            }
            wallet.saveAs(storeLocation.getPath());
            modified = false;
        }
    }

    /**
     * Discard this password store.
     */
    @Override
    public void discard() {
        wallet = null;
        sstore = null;
    }

    /**
     * Check whether a passphrase is required to access the store.
     * @return true if the wallet requires a passphrase.
     * @throws IOException if there was a problem accessing the wallet
     */
    @Override
    public boolean requiresPassphrase() throws IOException {

        if (wallet != null) {
            return !autoLogin;
        }

        final File f = findWalletFile(true);
        if (f.getName().endsWith(".sso")) {
            return false;
        }

        return true;
    }

    /**
     * Check whether a passphrase is valid to use.
     * @return true if the passphrase is valid.
     */
    @Override
    public boolean isValidPassphrase(char[] passphrase) {
        return OracleWallet.isValidPassword(passphrase);
    }

    @Override
    public boolean exists() throws IOException {
        return findWalletFile(true) != null;
    }

    private File findWalletFile(boolean any) throws IOException {
        if (!storeLocation.exists()) {
            return null;
        }
        if (!storeLocation.isDirectory()) {
            throw new IOException("Wallet location is not a directory");
        }
        final File[] storeFiles = storeLocation.listFiles();
        if (storeFiles == null) {
            /* Directory does not exist (already checked that it does) or
             * an IO error occurred.  We probably don't have permission
             * to access */
            throw new IOException(
                "Unable to access wallet. Check access permissions.");
        }
        for (File f : storeFiles) {
            if (((any || autoLogin) && f.getName().endsWith(".sso")) ||
                ((any || !autoLogin) && f.getName().endsWith(".p12"))) {
                return f;
            }
        }
        return null;
    }

    private static boolean exceptionContains(Exception e, String match) {
        final String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        return msg.contains(match);
    }

    private boolean databasesEqual(String db1, String db2) {
        return db1.equals(db2);
    }

    private void assertNotInitialized() {
        if (wallet != null) {
            throw new IllegalStateException("wallet already initialized");
        }
    }

    private void assertInitialized() {
        if (wallet == null) {
            throw new IllegalStateException("wallet not yet initialized");
        }
    }

    /**
     * Provides a mechanism to allow System.out to be captured and
     * suppressed.
     */
    private final class OutputCapture {
        /* The original values of System.out */
        private PrintStream systemOut = null;

        /* The capturing components of the output stream */
        private final ByteArrayOutputStream baosOut =
            new ByteArrayOutputStream();

        /* The replacement PrintStream */
        private final PrintStream printOut = new PrintStream(baosOut);

        private boolean capturing = false;

        /**
         * Constructor.
         */
        private OutputCapture() {
            start();
        }

        /**
         * Install capturing PrintStreams in System.out and System.err.
         */
        private void start() {

            if (!capturing) {
                systemOut = System.out;
                if (!EmbeddedMode.isEmbedded()) {
                    System.setOut(printOut);
                }
                capturing = true;
            }
        }

        /**
         * Restore the PrintStream that was in place at the time that the
         * constructor ran.  This method only has any effect the first time
         * it is called.
         */
        private void restore() {

            if (capturing) {
                capturing = false;
                if (!EmbeddedMode.isEmbedded()) {
                    System.setOut(systemOut);
                }
            }
        }
    }
}
