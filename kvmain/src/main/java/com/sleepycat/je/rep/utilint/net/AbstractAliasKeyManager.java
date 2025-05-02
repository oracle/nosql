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

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

/**
 * An implementation of X509ExtendedKeyManager which delegates most operations
 * and supports explicit selection of the alias.
 */
abstract class AbstractAliasKeyManager extends X509ExtendedKeyManager {

    private final String serverAlias;
    private final String clientAlias;

    /**
     * Create an instance of this class.
     *
     * @param serverAlias the alias to return for server context requests
     * @param clientAlias the alias to return for client context requests
     */
    protected AbstractAliasKeyManager(String serverAlias, String clientAlias) {
        this.serverAlias = serverAlias;
        this.clientAlias = clientAlias;
    }

    /** Return the key manager that operations should be delegated to. */
    protected abstract X509ExtendedKeyManager getDelegateKeyManager();

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
    	return getDelegateKeyManager().getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(
        String[] keyType, Principal[] issuers, Socket socket) {
        if (clientAlias != null) {
            return clientAlias;
        }

        return getDelegateKeyManager().chooseClientAlias(
            keyType, issuers, socket);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return getDelegateKeyManager().getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(
        String keyType, Principal[] issuers, Socket socket) {

        if (serverAlias != null) {
            return serverAlias;
        }

        return getDelegateKeyManager().chooseServerAlias(
            keyType, issuers, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        return getDelegateKeyManager().getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        return getDelegateKeyManager().getPrivateKey(alias);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType,
                                          Principal[] issuers,
                                          SSLEngine engine) {
        if (clientAlias != null) {
            return clientAlias;
        }
        return getDelegateKeyManager()
            .chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public String chooseEngineServerAlias(String keyType,
                                          Principal[] issuers,
                                          SSLEngine engine) {
        if (serverAlias != null) {
            return serverAlias;
        }
        return getDelegateKeyManager()
            .chooseEngineServerAlias(keyType, issuers, engine);
    }
}
