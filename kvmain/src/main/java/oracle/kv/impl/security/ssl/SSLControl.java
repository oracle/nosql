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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * SSL policy control information.
 */
public class SSLControl {

    private final SSLParameters sslParameters;
    private final SSLContext sslContext;
    private final SSLAuthenticator sslAuthenticator;
    private final HostnameVerifier sslHostVerifier;

    public SSLControl(SSLParameters sslParameters,
                      SSLContext sslContext,
                      HostnameVerifier sslHostVerifier,
                      SSLAuthenticator sslAuthenticator) {
        this.sslParameters = sslParameters;
        this.sslContext = sslContext;
        this.sslHostVerifier = sslHostVerifier;
        this.sslAuthenticator = sslAuthenticator;
    }

    public SSLParameters sslParameters() {
        return this.sslParameters;
    }

    public SSLContext sslContext() {
        return this.sslContext;
    }

    public SSLAuthenticator peerAuthenticator() {
        return this.sslAuthenticator;
    }

    public HostnameVerifier hostVerifier() {
        return this.sslHostVerifier;
    }

    public void applySSLParameters(SSLSocket sslSocket) {
        if (sslParameters != null) {
            /* Apply sslParameter-selected policies */
            if (sslParameters.getCipherSuites() != null) {
                sslSocket.setEnabledCipherSuites(
                    sslParameters.getCipherSuites());
            }

            if (sslParameters.getProtocols() != null) {
                sslSocket.setEnabledProtocols(
                    sslParameters.getProtocols());
            }

            /* These are only applicable to the server side */
            if (sslParameters.getNeedClientAuth()) {
                sslSocket.setNeedClientAuth(true);
            }
        }
    }

    /*
     * Override hashCode() and equals() to give us a better chance to
     * reduce socket usage.
     */
    @Override
    public int hashCode() {
        int result = 17;
        if (sslParameters != null) {
            result = result * 31 + sslParameters.hashCode();
        }
        if (sslContext != null) {
            result = result * 31 + sslContext.hashCode();
        }
        if (sslAuthenticator != null) {
            result = result * 31 + sslAuthenticator.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final SSLControl other = (SSLControl) obj;

        if (sslParameters != other.sslParameters) {
            return false;
        }

        if (sslContext != other.sslContext) {
            return false;
        }

        if (sslAuthenticator != other.sslAuthenticator) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "SSLControl[" +
            "sslParameters=" + sslParameters +
            " sslContext=" + sslContext +
            " sslAuthenticator=" + sslAuthenticator + "]";
    }
}
