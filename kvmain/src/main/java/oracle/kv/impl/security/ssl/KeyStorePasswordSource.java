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
package oracle.kv.impl.security.ssl;

import static oracle.kv.KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_WALLET_PROPERTY;
import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static oracle.kv.impl.security.PasswordManager.PWD_MANAGER;
import static oracle.kv.impl.security.PasswordManager.WALLET_MANAGER_CLASS;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import oracle.kv.KVSecurityConstants;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;

import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.PasswordSource;

/**
 * Provides keystore password retrieval capability from a PasswordStore.
 * This class and its derived classes are designed to allow use directly within
 * the KVStore code, but also to support indirect instantiation by the
 * JE HA code.
 */
public abstract class KeyStorePasswordSource implements PasswordSource {

    private static final String DEF_KEYSTORE_PASSWORD_ALIAS =  "keystore";

    /**
     * For access by concrete subclasses.
     */
    protected KeyStorePasswordSource() {
    }

    /**
     * Retrieve the keystore password from the PasswordStore.
     */
    @Override
    public char[] getPassword() {
        final PasswordStore pwdStore = getPasswordStore();
        final String pwdAlias = getPasswordAlias();

        try {
            final char[] secret = pwdStore.getSecret(pwdAlias);
            return secret;
        } catch (IOException e) {
            throw new IllegalStateException(
                "Unable to retrieve password from password store");
        }
    }

    /**
     * Returns an instance of a password store that contains the keystore
     * password.  For use by this class.
     */
    protected abstract PasswordStore getPasswordStore();

    /**
     * Returns the alias of the secret within the password store.
     * For use in configuring JE HA through properties.
     */
    protected abstract String getPasswordAlias();

    /**
     * Returns the constructor parameter string that would be used by JE HA to
     * re-instantiate this instance.
     */
    public abstract String getParamString();

    /**
     * Creates a KeyStorePassword source from the SecurityParams instance
     * provided.
     */
    public static KeyStorePasswordSource create(SecurityParams sp) {

        /*
         * Look for an alias in the password store
         */
        String pwdAlias = sp.getKeystorePasswordAlias();
        if (pwdAlias == null) {
            pwdAlias = DEF_KEYSTORE_PASSWORD_ALIAS;
        }

        /*
         * First look for a wallet implementation *
         */
        final String walletDir = sp.getWalletDir();
        if (walletDir != null && walletDir.length() > 0) {
            return new WalletPasswordSource(sp.resolveFile(walletDir).getPath(),
                                            pwdAlias);
        }

        /*
         * Then look to see if we have a password file configuration
         */
        final String pwdFile = sp.getPasswordFile();
        if (pwdFile != null && pwdFile.length() > 0) {

            String pwdClass = sp.getPasswordClass();

            if (pwdClass == null || pwdClass.length() == 0) {
                pwdClass = FILE_STORE_MANAGER_CLASS;
            }

            return new FilePasswordSource(sp.resolveFile(pwdFile).getPath(),
                                          pwdClass, pwdAlias);
        }

        return null;
    }

    /**
     * Creates a KeyStorePassword source from the Properties provided.
     */
    public static KeyStorePasswordSource create(Properties sp) {
        final String pwdAlias = sp.getProperty(
            KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_ALIAS_PROPERTY);
        if (pwdAlias == null || pwdAlias.isEmpty()) {
            /* alias not specified */
            return null;
        }

        final String walletDir = sp.getProperty(AUTH_WALLET_PROPERTY);
        if (walletDir != null && !walletDir.isEmpty()) {
            return new WalletPasswordSource(walletDir, pwdAlias);
        }
        final String pwdFile = sp.getProperty(AUTH_PWDFILE_PROPERTY);
        if (pwdFile != null && !pwdFile.isEmpty()) {
            String mgrClass = sp.getProperty(PWD_MANAGER);
            if (mgrClass == null || mgrClass.isEmpty()) {
                mgrClass = FILE_STORE_MANAGER_CLASS;
            }
            return new FilePasswordSource(pwdFile, mgrClass, pwdAlias);
        }

        throw new IllegalArgumentException(
            "Property " +
            KVSecurityConstants.SSL_TRUSTSTORE_PASSWORD_ALIAS_PROPERTY +
            " specified but none of these two properties is specified: " +
            AUTH_WALLET_PROPERTY + ", " + AUTH_PWDFILE_PROPERTY);
    }

    /**
     * Implementation of a KeyStorePasswordSource based on oracle Wallet
     * functionality.
     */
    public static class WalletPasswordSource extends KeyStorePasswordSource {

        private final File walletDir;
        private final String alias;

        /**
         * For reflection-based instantiation by the SSLChannelFactory class
         * by JE HA.
         * @throws IllegalArgumentException if the constructor params string
         * yielded by params.getClassParams() in not in the correct format
         */
        public WalletPasswordSource(InstanceParams params)
            throws IllegalArgumentException {

            final String[] paramSplits = decodeParams(params.getClassParams());
            this.alias = paramSplits[0];
            this.walletDir = new File(paramSplits[1]);
        }

        /**
         * For internal instantiation.
         */
        public WalletPasswordSource(String walletDir, String alias) {

            this.walletDir = new File(walletDir);
            this.alias = alias;
        }

        @Override
        protected PasswordStore getPasswordStore() {
            try {
                final PasswordManager pwdMgr =
                    PasswordManager.load(WALLET_MANAGER_CLASS);
                final PasswordStore pwdStore =
                    pwdMgr.getStoreHandle(walletDir);
                pwdStore.open(null); /* must be autologin */
                return pwdStore;
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Unable to access the configured wallet store", e);
            }
        }

        @Override
        protected String getPasswordAlias() {
            return alias;
        }

        @Override
        public String getParamString() {
            return alias + ":" + walletDir.getPath();
        }

        /**
         * Decodes a string produced by getParamString to return a directory
         * and an alias.
         * @return  a 2-element array with the first element being
         * the alias and the second being the store location
         * @throw IllegalArgumentException if the params string is not
         * formatted correctly
         */
        private static String[] decodeParams(String params)
            throws IllegalArgumentException {

            final String[] splits = params.split(":", 2);
            if (splits.length != 2) {
                throw new IllegalArgumentException(
                    "params does not have valid format");
            }
            return splits;
        }

    }

    /**
     * Implementation of a KeyStorePasswordSource based on FileStore
     * functionality.
     */
    public static class FilePasswordSource extends KeyStorePasswordSource {

        private final File storeFile;
        private final String managerClassName;
        private final String alias;

        /**
         * For reflection-based instantiation by the SSLChannelFactory class
         * by JE HA.
         * @throws IllegalArgumentException if the constructor params string
         * yielded by params.getClassParams() in not in the correct format
         */
        public FilePasswordSource(InstanceParams params)
            throws IllegalArgumentException {

            final String[] paramSplits = decodeParams(params.getClassParams());
            this.alias = paramSplits[0];
            this.managerClassName = paramSplits[1];
            this.storeFile = new File(paramSplits[2]);
        }

        /**
         * For internal instantiation or testing.
         */
        public FilePasswordSource(String storeFile,
                                  String managerClassName,
                                  String alias) {

            this.storeFile = new File(storeFile);
            this.managerClassName = managerClassName;
            this.alias = alias;
        }

        @Override
        protected PasswordStore getPasswordStore() {
            try {
                final PasswordManager pwdMgr =
                    PasswordManager.load(managerClassName);
                final PasswordStore pwdStore = pwdMgr.getStoreHandle(storeFile);
                pwdStore.open(null); /* must be autologin */
                return pwdStore;
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Unable to access the configured password store", e);
            }
        }

        @Override
        protected String getPasswordAlias() {
            return alias;
        }

        @Override
        public String getParamString() {
            return alias + ":" + managerClassName + ":" + storeFile.getPath();
        }

        /**
         * Decodes a string produced by getParamString to return a directory
         * and an alias. The string must be encoded in the format provided by
         * the getParamString() method.
         *
         * @return  a 3-element array with the first element being the alias,
         * the second being the file manager class name, and the third
         * being the store location
         * @throw IllegalArgumentException if the params string is not
         * properly encoded
         */
        private static String[] decodeParams(String params) {
            final String[] splits = params.split(":", 3);
            if (splits.length != 3) {
                throw new IllegalArgumentException(
                    "params does not have valid format");
            }
            return splits;
        }

    }
}
