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
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import com.sleepycat.je.rep.net.InstanceLogger;

/**
 * An implementation of X509ExtendedTrustManager which delegates operations to
 * an underlying implementation that it refreshes if the underlying truststore
 * file changes.
 */
public class DelegatingTrustManager extends X509ExtendedTrustManager {

    private final KeyStoreCache trustStoreCache;
    private volatile X509ExtendedTrustManager delegate;

    /**
     * Create an instance of this class.
     *
     * @param truststorePath the pathname of the file that contains the
     * underlying truststore
     * @param createDelegate a function that uses the contents of the
     * associated truststore file to create a trust manager to delegate
     * operations to
     * @param logger to log messages
     * @throws GeneralSecurityException if there is a problem associated with
     * the truststore
     * @throws IOException if there is a problem reading data from the
     * truststore file
     */
    public DelegatingTrustManager(
        String truststorePath,
        KeyStoreFunction<X509ExtendedTrustManager> createDelegate,
        InstanceLogger logger)
        throws GeneralSecurityException, IOException
    {
        trustStoreCache =
            new KeyStoreCache(truststorePath,
                              is -> delegate = createDelegate.apply(is),
                              logger);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain,
                                   String authType,
                                   Socket socket) throws CertificateException {
        getDelegate().checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain,
                                   String authType,
                                   Socket socket) throws CertificateException {
        getDelegate().checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain,
                                   String authType,
                                   SSLEngine engine)
        throws CertificateException
    {
        getDelegate().checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain,
                                   String authType,
                                   SSLEngine engine)
        throws CertificateException
    {
        getDelegate().checkServerTrusted(chain, authType, engine);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException
    {
        getDelegate().checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException
    {
        getDelegate().checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return getDelegate().getAcceptedIssuers();
    }

    /**
     * Return the trust manager to delegate operations to after checking to see
     * if it needs to be updated because the truststore file has changed.
     */
    private X509ExtendedTrustManager getDelegate() {
        trustStoreCache.check();
        return delegate;
    }
}
