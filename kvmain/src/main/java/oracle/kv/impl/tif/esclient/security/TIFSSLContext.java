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
package oracle.kv.impl.tif.esclient.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.tif.esclient.httpClient.SSLContextException;

/*
 * TODO : In case FTS does not need it's own Security Configuration,
 * This class should be made to use code from SSLConfig or extend
 * from SSLConfig.
 * Also to reuse that code cleanly, an accessor in StorageNodeParams for
 * kvSecurityDirectory was required.
 *
 * However, this class makes sene as it is expected that in future FTS will
 * have it's own security configuration as it depends on Elasticsearch Cluster
 * SSL Configurations.
 *
 * Also importing Elasticsearch certificates in KVStore truststore
 * may get restricted. And this is another reason that FTS
 * will need it's own SSL Configuration.
 *
 */
public class TIFSSLContext {

    /*
     * TODO: For the first version of secure ES Client, there is no addition to
     * existing security parameters.
     *
     * The below method tries to create SSLContext from existing security
     * parameters of KV Store Security Configuration and adds ES Secure Client
     * specific properties, if any, in a hard coded fashion below.
     *
     * In future, Security Params may get a new transport map.
     *
     * TODO: It will be better if SSLContext for ES Secure Client comes out of
     * SSLTransport. That would need some discussions and will be evaluated in
     * the future.
     */
    public static SSLContext makeSSLContext(SecurityParams sp,
                                            AtomicReference<char[]> ksPwdAR,
                                            Logger logger)
        throws SSLContextException {
        final KeyStorePasswordSource pwdSrc =
                KeyStorePasswordSource.create(sp);
        char[] ksPwd = (pwdSrc == null) ? null : pwdSrc.getPassword();
        if (ksPwd != null) {
            ksPwdAR.set(ksPwd);
        }

        final File keyStoreFile = sp.resolveFile(sp.getKeystoreFile());
        String keyStoreType = sp.getKeystoreType();
        final File trustStoreFile = sp.resolveFile(sp.getTruststoreFile());
        String trustStoreType = sp.getTruststoreType();
        final String ftsAlias = "FTS";

        KeyManager[] kmList = null;
        TrustManager[] tmList = null;

        /* Construct SSL key-related information */

        if (keyStoreFile != null && ksPwd != null) {

            if (keyStoreType == null) {
                keyStoreType = KeyStore.getDefaultType();
            }
            try {

                final KeyStore ks = KeyStore.getInstance(keyStoreType);
                ks.load(new FileInputStream(keyStoreFile), ksPwd);

                /*
                 * FTS certificate should be present in the keystore.
                 */
                if (!ks.containsAlias(ftsAlias)) {
                    throw new IllegalArgumentException("Alias " + ftsAlias
                            + " not found in " + keyStoreFile);
                }

                String x509Name = System
                                  .getProperty("oracle.kv.ssl.x509AlgoName");
                if (x509Name == null || x509Name.isEmpty()) {
                    final String jvmVendor = System.getProperty("java.vendor");
                    if (jvmVendor.startsWith("IBM")) {
                        x509Name = "IbmX509";
                    } else {
                        x509Name = "SunX509";
                    }
                }

                final KeyManagerFactory kmf = KeyManagerFactory
                                             .getInstance(x509Name);
                kmf.init(ks, ksPwd);
                kmList = kmf.getKeyManagers();

                if (trustStoreFile != null) {
                    if (trustStoreType == null) {
                        trustStoreType = KeyStore.getDefaultType();
                    }

                    final KeyStore ts = KeyStore.getInstance(trustStoreType);

                    /*
                     * After switched to PKCS12 as default Java Keystore,
                     * loading truststore now require password. The store
                     * keystore and truststore are using the same password.
                     */
                    ts.load(new FileInputStream(trustStoreFile), ksPwd);

                    /*
                     * Sanity check if truststore is loaded properly, the
                     * store.trust should always have at least one entry.
                     */
                    if (ts.size() == 0) {
                        throw new IllegalArgumentException(
                            trustStoreFile + " is not loaded properly," +
                            " no entry found");
                    }
                    TrustManagerFactory tmf;
                    tmf = TrustManagerFactory.getInstance(x509Name);

                    tmf.init(ts);

                    tmList = tmf.getTrustManagers();
                }

                final SSLContext ctx = SSLContext.getInstance("TLSv1.2");

                ctx.init(kmList, tmList, null /* SecureRandom */);

                return ctx;
            } catch (NoSuchAlgorithmException e) {
                logSevereSSLException(logger, e,
                                      "Do check certificate set up and"
                                              + " certificate contents");
            } catch (KeyManagementException e) {
                logSevereSSLException(logger, e, "");
            } catch (KeyStoreException e) {
                logSevereSSLException(logger, e, "");
            } catch (CertificateException e) {
                logSevereSSLException(logger, e,
                                      "Make sure the KV keystore has the" +
                                      " certificate chain required.");

            } catch (FileNotFoundException e) {
                logSevereSSLException(logger, e,
                                      "Keystore/truststore files are not in "
                                              + "security dir of SN");

            } catch (IOException e) {
                logSevereSSLException(logger, e, "");
            } catch (UnrecoverableKeyException e) {
                logSevereSSLException(logger, e, "");
            }

        }

        return null;
    }

    private static void logSevereSSLException(Logger logger,
                                              Exception e,
                                              String checkMsg)
        throws SSLContextException {

        String message = "SSL Context for ES did not get created due to:"
                + e.getMessage() + " Check:" + checkMsg;

        logger.log(Level.SEVERE, message, e);

        throw new SSLContextException(e);
    }

}
