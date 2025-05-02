/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.net.HttpURLConnection;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.util.logging.Level;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.util.HttpRequest.ConnectionHandler;

public class SSLConnectionHandler {

    private static final String OCI_CERT_CONNECTION_HANDLER =
        "oracle.nosql.util.OCICertConnectionHandler";

    /*
     * Returns SSL connection handler using OCI CA certificate, or null if
     * any error.
     */
    public static ConnectionHandler getOCICertHandler(SkLogger logger) {
        try {
            Class<?> clazz = Class.forName(OCI_CERT_CONNECTION_HANDLER);
            return (ConnectionHandler)clazz.getConstructor().newInstance();
        } catch (ClassNotFoundException cnfe) {
            logger.log(Level.WARNING,
                       "Unable to find class " + OCI_CERT_CONNECTION_HANDLER,
                       cnfe);
        } catch (Throwable th) {
            logger.log(Level.WARNING,
                       "Unable to create " + OCI_CERT_CONNECTION_HANDLER +
                       " instance", th);
        }

        return null;
    }

    /*
     * Returns SSL connection handler using internal SSL certificate, or null if
     * any error.
     */
    public static ConnectionHandler getInternalCertHandler(SkLogger logger) {
        try {
            return new SSLHanlder("InternalServiceCert",
                                  SSLConfig.getInternalSSLCert());
        } catch (Throwable th) {
            logger.log(Level.WARNING,
                       "Initializing internal SSL ConnectionHandler failed",
                       th);
        }

        return null;
    }

    /*
     * SSLConnectionHandler used to configure SSL connection, which uses the
     * fixed SSLSocketFactory initialized with a self-signed certificate.
     *
     * public for test purpose.
     */
    public static class SSLHanlder implements ConnectionHandler {

        private final SSLSocketFactory sslSocketFactory;

        public SSLHanlder(String alias, File cert) {
            if (alias == null) {
                throw new IllegalArgumentException(
                    "Alias name must not be null");
            }
            if (cert == null) {
                throw new IllegalArgumentException(
                    "Certificate must not be null");
            }
            if (!cert.exists() || !cert.isFile() ) {
                throw new IllegalArgumentException(
                    "The certificate does not exist or is not a file: " + cert);
            }
            sslSocketFactory = initSSLSocketFactory(alias, cert);
        }

        private SSLSocketFactory initSSLSocketFactory(String alias, File cert) {
            try {
                KeyStore trustStore = KeyStore.getInstance("JKS");
                trustStore.load(null, null);
                loadCert(trustStore, alias, cert);

                TrustManagerFactory tmf = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);

                /*
                 * WARNING: while running with BCFIPS, only TLSv1.2 can work.
                 */
                SSLContext context = SSLContext.getInstance("TLSv1.2");

                boolean fipsApproved = Boolean.getBoolean(
                    "org.bouncycastle.fips.approved_only");
                context.init(null, tmf.getTrustManagers(),
                             (fipsApproved ?
                              SecureRandom.getInstance("DEFAULT", "BCFIPS") :
                              new SecureRandom()));
                return context.getSocketFactory();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Unable to initialize SSLSocketFactory [alias=" + alias +
                    ", cert=" + cert + "]", e);
            }
        }

        private void loadCert(KeyStore ts, String alias, File sslCert)
            throws Exception {
            try (FileInputStream is = new FileInputStream(sslCert)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                ts.setCertificateEntry(alias, cf.generateCertificate(is));
            }
        }

        @Override
        public SSLSocketFactory getSSLSocketFactory() {
            return sslSocketFactory;
        }

        @Override
        public void configureConnection(HttpURLConnection con) {
        }
    }
}
