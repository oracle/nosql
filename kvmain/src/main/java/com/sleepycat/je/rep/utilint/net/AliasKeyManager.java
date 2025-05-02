/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.utilint.net;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.net.ssl.X509ExtendedKeyManager;

import com.sleepycat.je.rep.net.InstanceLogger;

/**
 * An implementation of X509ExtendedKeyManager which delegates most operations
 * to an underlying implementation and supports explicit selection of the
 * alias.
 */
public class AliasKeyManager extends AbstractAliasKeyManager {

    private final X509ExtendedKeyManager delegateKeyManager;

    /**
     * Create an instance of this class with a fixed delegate.
     *
     * @param delegateKeyManager the underlying key manager to fulfill key
     * retrieval requests
     * @param serverAlias the alias to return for server context requests
     * @param clientAlias the alias to return for client context requests
     */
    public AliasKeyManager(X509ExtendedKeyManager delegateKeyManager,
                           String serverAlias,
                           String clientAlias) {
        super(serverAlias, clientAlias);
        this.delegateKeyManager = delegateKeyManager;
    }

    /**
     * Creates a key manager which delegates operations to an underlying
     * implementation that it refreshes if the underlying keystore file
     * changes.
     *
     * @param serverAlias the alias to return for server context requests
     * @param clientAlias the alias to return for client context requests
     * @param keystorePath the pathname of the file that contains the
     * underlying keystore
     * @param createDelegate a function that uses the contents of the
     * associated keystore file to create a key manager to delegate operations
     * to
     * @param logger to log messages
     * @return the key manager
     * @throws IOException if an I/O failure occurs reading the keystore file
     * @throws GeneralSecurityException if there is a problem with the keystore
     */
    public static X509ExtendedKeyManager createRefresh(
        String serverAlias,
        String clientAlias,
        String keystorePath,
        KeyStoreFunction<X509ExtendedKeyManager> createDelegate,
        InstanceLogger logger)
        throws IOException, GeneralSecurityException
    {
        return new Refresh(serverAlias, clientAlias, keystorePath,
                           createDelegate, logger);
    }

    /** Implement key manager that uses a KeyStoreCache to do refreshes. */
    private static class Refresh extends AbstractAliasKeyManager {
        private final KeyStoreCache cache;
        private volatile X509ExtendedKeyManager delegate;
        Refresh(String serverAlias,
                String clientAlias,
                String keystorePath,
                KeyStoreFunction<X509ExtendedKeyManager> createDelegate,
                InstanceLogger logger)
            throws GeneralSecurityException, IOException
        {
            super(serverAlias, clientAlias);
            cache = new KeyStoreCache(
                keystorePath,
                is -> delegate = createDelegate.apply(is),
                logger);
        }
        @Override
        protected X509ExtendedKeyManager getDelegateKeyManager() {
            cache.check();
            return delegate;
        }
    }

    @Override
    protected X509ExtendedKeyManager getDelegateKeyManager() {
        return delegateKeyManager;
    }
}
