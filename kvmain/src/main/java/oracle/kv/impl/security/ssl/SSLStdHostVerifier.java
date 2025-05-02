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

import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * SSL socket authentication implementation using standard server host
 * certificate authentication.
 */
public class SSLStdHostVerifier implements HostnameVerifier {

    private static final int ALTNAME_DNS = 2;
    private static final int ALTNAME_IP  = 7;

    public SSLStdHostVerifier() {
    }

    @Override
    public boolean verify(String targetHost, SSLSession sslSession) {

        if (targetHost == null) {
            return false;
        }

        Principal principal = null;
        Certificate[] peerCerts = null;
        try {
            principal = sslSession.getPeerPrincipal();
            peerCerts = sslSession.getPeerCertificates();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (principal != null && principal instanceof X500Principal) {
            final X500Principal x500Principal = (X500Principal) principal;
            final String name = x500Principal.getName("RFC1779");
            if (targetHost.equalsIgnoreCase(name)) {
                return true;
            }
        }

        /* Check for SubjectAlternativeNames */
        if (peerCerts[0] instanceof java.security.cert.X509Certificate) {

            final X509Certificate peerCert =
                (java.security.cert.X509Certificate) peerCerts[0];

            Collection<List<?>> altNames = null;
            try {
                altNames = peerCert.getSubjectAlternativeNames();
            } catch (CertificateParsingException cpe) /* CHECKSTYLE:OFF */ {
                // TODO: think about communicating this error
                // Should add a logger to the constructor
            } /* CHECKSTYLE:ON */

            if (altNames != null) {
                for (List<?> altName : altNames) {
                    /*
                     * altName will be a 2-element list, with the first being
                     * the name type and the second being the "name".  For
                     * DNS and IP entries, the "name" will be a string.
                     */
                    final int nameType = ((Integer) altName.get(0)).intValue();
                    if (nameType == ALTNAME_IP || nameType == ALTNAME_DNS) {
                        final String nameValue = (String) altName.get(1);
                        if (targetHost.equals(nameValue)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
