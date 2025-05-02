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
package oracle.kv.impl.security.filestore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * Open-source implementation of PasswordStore.
 * The FileStore uses a single file to house a PasswordStore.  The store
 * content is currently saved as clear text.
 */
public class FileStore implements PasswordStore {

    /* The File containing the password store */
    private final File storeLocation;

    /* The data structure containing the secrets */
    private SecretHash secretHash = null;

    /* If true, the store needs to be written back to the file */
    private boolean modified = false;

    /**
     * There are two types of information stored in a password store:
     * secrets and logins
     * A "secret" is a named password, whereas a login is a pair of
     * user/password associated with a database as a unique key.
     *
     * The following constants are conventions established to allow
     * tracking of both logins and secrets in the same hash structure.
     *
     * The login keys look like
     *     login.user.<database>
     *     login.password.<database>
     * and secrets look like this:
     *     secret.<user-specified-alias>
     */
    private static final String LOGIN_USER_PREFIX = "login.user.";
    private static final String LOGIN_PW_PREFIX = "login.password.";
    private static final String SECRET_PREFIX = "secret.";

    /**
     * Constructor
     * Prepare for access to a file store, which might not yet exist.
     */
    FileStore(File storeFile) {
        this.storeLocation = storeFile.getAbsoluteFile();
    }

    /**
     * Create a new file store.  The file where the store will be stored
     *        must not exist yet.
     * @param passphrase a passphrase for the store.  This option is not
     *        currently supported and results in an
     *        UnsupportedOperationException.
     * @throws IllegalStateException if this FileStore has already had
     *         a create or open operation executed.
     * @throws UnsupportedOperationException if a non-null passphrase is
     *         specified
     * @throws IOException if there are IO errors accessing the file store
     * @throws PasswordStoreException if there errors in the password store
     *         itself.
     */
    @Override
    public boolean create(char[] passphrase) throws IOException {

        assertNotInitialized();

        if (passphrase != null) {
            throw new UnsupportedOperationException(
                "Passphrases are not supported");
        }

        if (storeLocation.exists()) {
            throw new PasswordStoreException(
                "A file already exists at this location");
        }

        File parentDir = storeLocation.getParentFile();
        if (parentDir == null) {
            parentDir = new File(".");
        }

        if (!parentDir.exists() || !parentDir.isDirectory()) {
            throw new PasswordStoreException(
                "The directory for the password file does not exist");
        }

        if (!parentDir.canWrite()) {
            throw new PasswordStoreException(
                "The directory for the password file is not writable");
        }

        final SecretHash newSecretHash = new SecretHash();

        try {
            newSecretHash.write(storeLocation);
        } catch (IOException ioe) {
            // TBD: special interpretation of any exceptions?
            throw ioe;
        }

        if (!SecurityUtils.makeOwnerAccessOnly(storeLocation)) {
            throw new PasswordStoreException(
                "Unable to set access permissions for file. " +
                "Correct manually before using the password store");
        }

        /* Make sure we can read it back */
        try {
            newSecretHash.read(storeLocation);
        } catch (IOException ioe) {
            /* Not clear how this could happen with a newly created store */
            throw new PasswordStoreException(
                "Error retrieving passwords from file", ioe);
        }

        this.secretHash = newSecretHash;
        return true;
    }

    /**
     * Open an existing filestore.
     * @throws IllegalStateException if this FileStore has already had
     *         a create or open operation executed.
     * @throws UnsupportedOperationException if a non-null passphrase is
     *         specified
     * @throws IOException if there are IO errors accessing the store
     */
    @Override
    public boolean open(char[] passphrase) throws IOException {

        assertNotInitialized();

        if (!storeLocation.exists()) {
            throw new PasswordStoreException(
                "No file exists at this location: " + storeLocation);
        }

        if (passphrase != null) {
            throw new UnsupportedOperationException(
                "Passphrases are not supported by this implementation");
        }

        final SecretHash newSecretHash = new SecretHash();
        try {
            newSecretHash.read(storeLocation);
        } catch (IOException ioe) {
            throw ioe;
        }
        this.secretHash = newSecretHash;

        return true;
    }

    @Override
    public Collection<String> getSecretAliases() throws IOException {

        assertInitialized();

        final Set<String> secretAliases = new HashSet<String>();
        final Iterator<String> e = secretHash.aliases();
        while (e.hasNext()) {
            String alias = e.next();
            if (alias.startsWith(SECRET_PREFIX)) {
                alias = alias.substring(SECRET_PREFIX.length());
                secretAliases.add(alias);
            }
        }

        return secretAliases;
    }

    @Override
    public char[] getSecret(String alias) throws IOException {

        assertInitialized();

        return secretHash.getSecret(SECRET_PREFIX + alias);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the values of the alias or the
     * secret are not valid
     */
    @Override
    public boolean setSecret(String alias, char[] secret) throws IOException  {

        assertInitialized();
        checkNameIsValid("alias", alias);
        checkSecretIsValid("secret", secret);

        final String internalAlias = SECRET_PREFIX + alias;
        final boolean updated = secretHash.containsAlias(internalAlias);
        secretHash.setSecret(internalAlias, secret);
        modified = true;
        return updated;
    }

    @Override
    public boolean deleteSecret(String alias) throws IOException {

        assertInitialized();

        final String internalAlias = SECRET_PREFIX + alias;
        if (secretHash.containsAlias(internalAlias)) {
            secretHash.deleteSecret(internalAlias);
            modified = true;
            return true;
        }
        return false;
    }

    @Override
    public Collection<LoginId> getLogins() throws IOException {

        assertInitialized();

        final Set<LoginId> logins = new HashSet<LoginId>();

        final Iterator<String> e = secretHash.aliases();
        while (e.hasNext()) {
            final String alias = e.next();
            if (alias.startsWith(LOGIN_USER_PREFIX)) {
                final String db = alias.substring(LOGIN_USER_PREFIX.length());
                final String user = new String(secretHash.getSecret(alias));
                logins.add(new LoginId(db, user));
            }
        }

        return logins;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the values of the login database,
     * login user, or the password are not valid
     */
    @Override
    public boolean setLogin(LoginId login, char[] password) throws IOException {

        assertInitialized();
        final String db = login.getDatabase();
        final String user = login.getUser();
        checkNameIsValid("db", db);
        checkNameIsValid("user", user);
        /* The PwdfileCommand has a '-secret' flag, so all it "secret" */
        checkSecretIsValid("secret", password);

        final String dbAlias = LOGIN_USER_PREFIX + db;
        final boolean exists = secretHash.containsAlias(dbAlias);
        secretHash.setSecret(dbAlias, user.toCharArray());
        secretHash.setSecret(LOGIN_PW_PREFIX + db, password);
        modified = true;
        return exists;
    }

    @Override
    public LoginId getLoginId(String database) throws IOException {

        assertInitialized();

        final char[] user = secretHash.getSecret(LOGIN_USER_PREFIX + database);
        if (user == null) {
            return null;
        }
        return new LoginId(database, new String(user));
    }

    @Override
    public char[] getLoginSecret(String database) throws IOException {

        assertInitialized();

        return secretHash.getSecret(LOGIN_PW_PREFIX + database);
    }

    @Override
    public boolean deleteLogin(String db) {

        assertInitialized();

        if (secretHash.getSecret(LOGIN_USER_PREFIX + db) == null &&
            secretHash.getSecret(LOGIN_PW_PREFIX + db) == null) {
            return false;
        }

        secretHash.deleteSecret(LOGIN_USER_PREFIX + db);
        secretHash.deleteSecret(LOGIN_PW_PREFIX + db);
        modified = true;
        return true;
    }

    /**
     * Provides an implementation for setPassphrase().  However, only
     * null passphrases are accepted by this implementation.
     * @throws UnsupportedOperationException if a non-null passphrase is
     *         specified
     */
    @Override
    public boolean setPassphrase(char[] passphrase)
        throws IOException {

        assertInitialized();

        if (passphrase != null) {
            throw new UnsupportedOperationException(
                "Passphrases are not supported");
        }

        return true;
    }

    /**
     * Save a modified password store.
     */
    @Override
    public void save() throws IOException {

        assertInitialized();

        if (secretHash == null) {
            throw new IllegalStateException(
                "Password store has not been initialized");
        }

        if (modified) {
            secretHash.write(storeLocation);
            modified = false;
        }
    }

    /**
     * Discard this store.
     */
    @Override
    public void discard() {
        if (secretHash != null) {
            secretHash.discard();
        }
    }

    /**
     * Check whether a passphrase is required to access the store.
     * @return true if a passphrase is required.  The current implementation
     * returns false in all cases
     */
    @Override
    public boolean requiresPassphrase() throws IOException {

        return false;
    }

    /**
     * Check whether a passphrase is valid to use.
     * @return true if the passphrase is valid.  The current implementation
     * does not support passphrases, so this returns false unless the supplied
     * passphrase is null.
     */
    @Override
    public boolean isValidPassphrase(char[] passphrase) {
        return passphrase == null;
    }

    /**
     * Check whether a secret alias or user name is valid. Valid names must not
     * have leading or trailing whitespace, as determined by String.trim.
     *
     * @param type the type of name being checked, for example "secret" or
     * "user"
     * @param name the name to check
     * @throws IllegalArgumentException if the name is not valid
     */
    public static void checkNameIsValid(String type, String name) {
        checkLeadingTrailingWhitespace(type, name);
    }

    /**
     * Check whether a secret or password is valid. Valid values must not have
     * leading or trailing whitespace, as determined by String.trim.
     *
     * @param type the type of value being checked, for example "secret" or
     * "password"
     * @param value the value to check
     * @throws IllegalArgumentException if the value is not valid
     */
    public static void checkSecretIsValid(String type, char[] value) {
        checkLeadingTrailingWhitespace(type, CharBuffer.wrap(value));
    }

    private static void checkLeadingTrailingWhitespace(String type,
                                                       CharSequence value) {
        if (hasLeadingTrailingWhitespace(value)) {
            throw new IllegalArgumentException(
                "Leading and trailing whitespace are not permitted for " +
                type);
        }
    }

    /**
     * Return whether the character sequence starts or ends with a character
     * that would be removed by String.trim, that is, a space character
     * (U+0020) or lower, with support for multi-character Unicode code points.
     */
    /* Public for testing */
    public static boolean hasLeadingTrailingWhitespace(CharSequence chars) {
        final AtomicInteger index = new AtomicInteger(-1);
        final AtomicInteger first = new AtomicInteger();
        final AtomicInteger last = new AtomicInteger();
        chars.codePoints().forEach(
            i -> {
                if (index.incrementAndGet() == 0) {
                    first.set(i);
                } else {
                    last.set(i);
                }
            });
        if ((index.get() >= 0) && (first.get() <= 0x20)) {
            return true;
        }
        if ((index.get() > 0) && (last.get() <= 0x20)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean exists() throws IOException {
        return storeLocation.exists();
    }

    private void assertNotInitialized() {
        if (secretHash != null) {
            throw new IllegalStateException(
                "Password store already initialized");
        }
    }

    private void assertInitialized() {
        if (secretHash == null) {
            throw new IllegalStateException(
                "Password store not yet initialized");
        }
    }

    private static final class SecretHash {
        private HashMap<String, char[]> secretData =
            new HashMap<String, char[]>();

        private static final String PASSWORD_STORE_KEY = "Password Store:";

        /**
         * Constructor.
         */
        SecretHash() {
        }

        /**
         * Return an iterator over the secretData keys.
         */
        private Iterator<String> aliases() {
            return secretData.keySet().iterator();
        }

        /**
         * Create or update a mapping of the specified key to the
         * specified secret value.
         */
        private void setSecret(String alias, char[] secret) {
            secretData.put(alias,  Arrays.copyOf(secret, secret.length));
        }

        /**
         * Return the current secret associated with the alias.
         */
        private char[] getSecret(String alias) {
            final char[] secret = secretData.get(alias);
            if (secret == null) {
                return null;
            }

            /* return a copy */
            return Arrays.copyOf(secret, secret.length);
        }

        /*
         * Check whether a secret exists named by the specified alias
         */
        private boolean containsAlias(String alias) {
            return secretData.containsKey(alias);
        }

        /* Remove the secret mapping for the specified alias. */
        private boolean deleteSecret(String alias) {
            final char[] secret = secretData.remove(alias);
            if (secret == null) {
                return false;
            }
            Arrays.fill(secret, ' ');
            return true;
        }

        private void discard() {
            discardSecretData();
        }

        /* "Zero" out all of the entries as a precaution. */
        private void discardSecretData() {
            final Iterator<char[]> iter = secretData.values().iterator();
            while (iter.hasNext()) {
                final char[] secret = iter.next();
                Arrays.fill(secret, ' ');
            }
        }

        /**
         * Read a password store into memory
         */
        synchronized void read(File f) throws IOException {
            discardSecretData();

            final BufferedReader br = new BufferedReader(new FileReader(f));
            final String keyLine = br.readLine();

            try {
                if (keyLine == null ||
                    !keyLine.startsWith(PASSWORD_STORE_KEY)) {
                    throw new PasswordStoreException(
                        "The file does not appear to contain a password store");
                }

                while (true) {
                    final String input = br.readLine();
                    if (input == null) {
                        break;
                    }

                    final int firstEqual = input.indexOf("=");

                    /*
                     * After the first equal mark, the trimmed value is the
                     * password, if the value is empty, don't process this
                     * line.
                     */

                    if (firstEqual != -1 && firstEqual + 1 < input.length()) {
                        final String alias =
                            input.substring(0, firstEqual).trim();
                        final char[] secret =
                            input.substring(firstEqual + 1, input.length()).
                            trim().toCharArray();
                        secretData.put(alias, secret);
                    }
                }
            } finally {
                br.close();
            }
        }

        synchronized void write(File f) throws IOException {
            final PrintWriter writer = new PrintWriter(f);

            writer.println(PASSWORD_STORE_KEY);

            final Set<String> keys = secretData.keySet();
            for (String alias : keys) {
                final char[] secret = secretData.get(alias);
                writer.print(alias + "=");
                /*
                 * Keep separate to take advantange of char[] support.
                 * Simple string concatenation doesn't print the chars
                 */
                writer.println(secret);
            }

            writer.close();
        }
    }
}
