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
package oracle.kv.impl.security;

import static oracle.kv.KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_WALLET_PROPERTY;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * The base class of all NoSQL password managers.  Password managers provide
 * the ability to store and retrieve passwords to and from a file or directory.
 */
public abstract class PasswordManager {
    /**
     * @hidden
     *
     * The parameter can used to specify the name of the customized
     * implementation of password manager. It's not currently public
     * or documented.
     */
    public static final String PWD_MANAGER = "oracle.kv.auth.pwdfile.manager";

    /**
     * The name of the standard open-source implementation of password manager
     * included in NoSQL.
     */
    public static final String FILE_STORE_MANAGER_CLASS =
        "oracle.kv.impl.security.filestore.FileStoreManager";

    /**
     * The name of the Oracle proprietary implementation of password manager
     * based on the Oracle Wallet code.  This is available only in the NoSQL
     * DB EE version.
     */
    public static final String WALLET_MANAGER_CLASS =
        "oracle.kv.impl.security.wallet.WalletManager";

    /**
     * List of known implementation classes in order of preference.
     */
    private static final String[] preferredImplementations =
        new String[] { WALLET_MANAGER_CLASS, FILE_STORE_MANAGER_CLASS };

    /**
     * Attempt to load an instance of the specified class as a PasswordManager.
     *
     * @param className the name of the class that extends PasswordManager.
     * @return an instance of PasswordManager
     * @throws ClassNotFoundException if the named class cannot be found
     * @throws ExceptionInInitializerError if an exception was thrown during
     *         class initialization
     * @throws IllegalAccessException if the class or default constructor
     *         are inaccessible
     * @throws IllegalArgumentException if the constructor arguments are
     *         incorrect
     * @throws InstantiationException if the class has no default constructor
     *         or is not an instantiable class.
     * @throws InvocationTargetException if the constructor throws an exception
     * @throws NoSuchMethodException if the construct is not found
     */
    public static PasswordManager load(String className)
        throws ClassNotFoundException, IllegalAccessException,
               InstantiationException, InvocationTargetException,
               NoSuchMethodException {

        final Class<?> pwdMgrClass = Class.forName(className);
        return (PasswordManager)
            pwdMgrClass.getDeclaredConstructor().newInstance();
    }

    /**
     * Report the preferred PasswordManager class name for this install.
     * @return the preferred password manager implementation for this
     *   installation.
     */
    public static String preferredManagerClass() {

        for (String s : preferredImplementations) {
            try {
                Class.forName(s);
                return s;
            } catch (ClassNotFoundException cnfe) /* CHECKSTYLE:OFF */ {
                /* Ignore */
            } /* CHECKSTYLE:ON */
        }
        return null;
    }

    /**
     * Try to retrieve the password from the password store constructed from
     * the security properties. If wallet.dir is set, the password will be
     * fetched using wallet store, otherwise file password store is used.
     */
    public static char[] retrievePassword(final String alias,
                                          final Properties securityProps) {
        if (alias == null || securityProps == null) {
            return null;
        }

        PasswordManager pwdManager = null;
        PasswordStore pwdStore = null;

        try {
            final String walletDir =
                securityProps.getProperty(AUTH_WALLET_PROPERTY);
            if (walletDir != null && !walletDir.isEmpty()) {
                pwdManager = PasswordManager.load(WALLET_MANAGER_CLASS);
                pwdStore = pwdManager.getStoreHandle(new File(walletDir));
            } else {
                String mgrClass = securityProps.getProperty(PWD_MANAGER);
                if (mgrClass == null || mgrClass.isEmpty()) {
                    mgrClass = FILE_STORE_MANAGER_CLASS;
                }
                final String pwdFile =
                    securityProps.getProperty(AUTH_PWDFILE_PROPERTY);
                if (pwdFile == null || pwdFile.isEmpty()) {
                    return null;
                }
                pwdManager = PasswordManager.load(mgrClass);
                pwdStore = pwdManager.getStoreHandle(new File(pwdFile));
            }
            pwdStore.open(null); /* must be autologin */
            return pwdStore.getSecret(alias);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (pwdStore != null) {
                pwdStore.discard();
            }
        }
    }

    /**
     * Get a handle to a password store. This store might not exist yet, which
     * is fine.  The returned handle allows the caller to query for existence
     * and access requiremnets for the store.
     *
     * @param storeLocation an abstract file identifying the location of the
     *   store.  This abstract file may be an actual file or a directory,
     *   as is appropriate for the PasswordManager implementation.
     * @return a PasswordStore handle based on the store location.
     */
    public abstract PasswordStore getStoreHandle(File storeLocation);

    /**
     * Indicate whether the password store implementation requires a directory
     * (to allow for multiple files) or just a file for its storage.
     *
     * @return true if the storeLocation passed to getStoreHandle should be a
     *   directory and false if the storeLocation should be a file.
     */
    public abstract boolean storeLocationIsDirectory();

}
