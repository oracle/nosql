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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.InstanceContext;
import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.net.SSLAuthenticator;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder.CtorArgSpec;

/**
 * A factory class for generating SSLDataChannel instances based on
 * SocketChannel instances.
 */
public class SSLChannelFactory implements DataChannelFactory {

    /*
     * Protocol to use in call to SSLContext.getInstance.  This isn't a
     * protocol per-se.  Actual protocol selection is chosen at the time
     * a connection is established based on enabled protocol settings for
     * both client and server.
     */
    private static final String SSL_CONTEXT_PROTOCOL = "TLS";

    /**
     * A system property to allow users to specify the correct X509 certificate
     * algorithm name based on the JVMs they are using.
     */
    private static final String X509_ALGO_NAME_PROPERTY =
        "je.ssl.x509AlgoName";

    /**
     * The algorithm name of X509 certificate. It depends on the vendor of JVM.
     */
    private static final String X509_ALGO_NAME = getX509AlgoName();

    /**
     * An SSLContext that will hold all the interesting connection parameter
     * information for session creation in server mode.
     */
    private final SSLContext serverSSLContext;

    /**
     * An SSLContext that will hold all the interesting connection parameter
     * information for session creation in client mode.
     */
    private final SSLContext clientSSLContext;

    /**
     * The base SSLParameters for use in channel creation.
     */
    private final SSLParameters baseSSLParameters;

    /**
     * An authenticator object for validating SSL session peers when acting
     * in server mode
     */
    private final SSLAuthenticator sslAuthenticator;

    /**
     * A host verifier object for validating SSL session peers when acting
     * in client mode
     */
    private final HostnameVerifier sslHostVerifier;

    private final InstanceLogger logger;

    /**
     * Constructor for use during creating based on access configuration
     */
    public SSLChannelFactory(InstanceParams params) {
        logger = params.getContext().getLoggerFactory().getLogger(getClass());
        serverSSLContext = constructSSLContext(params, false, logger);
        clientSSLContext = constructSSLContext(params, true, logger);
        baseSSLParameters =
            filterSSLParameters(constructSSLParameters(params),
                                serverSSLContext);
        sslAuthenticator = constructSSLAuthenticator(params);
        sslHostVerifier = constructSSLHostVerifier(params);
    }

    /**
     * Constructor for use when SSL configuration objects have already
     * been constructed.
     */
    public SSLChannelFactory(SSLContext serverSSLContext,
                             SSLContext clientSSLContext,
                             SSLParameters baseSSLParameters,
                             SSLAuthenticator sslAuthenticator,
                             HostnameVerifier sslHostVerifier,
                             InstanceLogger logger) {

        this.serverSSLContext = serverSSLContext;
        this.clientSSLContext = clientSSLContext;
        this.baseSSLParameters =
            filterSSLParameters(baseSSLParameters, serverSSLContext);
        this.sslAuthenticator = sslAuthenticator;
        this.sslHostVerifier = sslHostVerifier;
        this.logger = logger;
    }

    /**
     * Construct a DataChannel wrapping the newly accepted SocketChannel
     */
    @Override
    public DataChannel acceptChannel(SocketChannel socketChannel) {

        final SocketAddress socketAddress =
            socketChannel.socket().getRemoteSocketAddress();
        String host = null;
        if (socketAddress == null) {
            throw new IllegalArgumentException(
                "socketChannel is not connected");
        }

        if (socketAddress instanceof InetSocketAddress) {
            host = ((InetSocketAddress)socketAddress).getAddress().toString();
        }

        final SSLEngine engine =
            serverSSLContext.createSSLEngine(host,
                                             socketChannel.socket().getPort());
        engine.setSSLParameters(baseSSLParameters);
        engine.setUseClientMode(false);
        if (sslAuthenticator != null) {
            if (baseSSLParameters.getNeedClientAuth()) {
                engine.setNeedClientAuth(true);
            } else {
                engine.setWantClientAuth(true);
            }
        }

        return new SSLDataChannel(socketChannel, engine, null, null,
                                  sslAuthenticator, logger);
    }

    /**
     * Construct a DataChannel wrapping a new connection to the specified
     * address using the associated connection options.
     */
    @Override
    public DataChannel connect(InetSocketAddress addr,
                               InetSocketAddress localAddr,
                               ConnectOptions connectOptions)
        throws IOException {

        final SocketChannel socketChannel =
            RepUtils.openSocketChannel(addr, localAddr, connectOptions);

        /*
         * Figure out a good host to specify.  This is used for session caching
         * so it's not critical what answer we come up with, so long as it
         * is relatively repeatable.
         */
        String host = addr.getHostName();
        if (host == null) {
            host = addr.getAddress().toString();
        }

        final SSLEngine engine =
            clientSSLContext.createSSLEngine(host, addr.getPort());
        engine.setSSLParameters(baseSSLParameters);
        engine.setUseClientMode(true);

        return new SSLDataChannel(
            socketChannel, engine, host, sslHostVerifier, null, logger);
    }

    /**
     * Reads the KeyStore configured in the ReplicationNetworkConfig into
     * memory.
     */
    public static KeyStore readKeyStore(InstanceContext context,
                                        InputStream inputStream) {

        KeyStoreInfo ksInfo =
            readStoreInfo(context, inputStream, false /* isTrustStore */);
        try {
            return ksInfo.ks;
        } finally {
            ksInfo.clearPassword();
        }
    }

    /**
     * Checks whether the auth string is a valid authenticator specification
     */
    public static boolean isValidAuthenticator(String authSpec) {
        authSpec = authSpec.trim();

        if (authSpec.equals("") || authSpec.equals("mirror")) {
            return true;
        }

        if (authSpec.startsWith("dnmatch(") && authSpec.endsWith(")")) {
            try {
                SSLDNAuthenticator.validate(authSpec);
                return true;
            } catch(IllegalArgumentException iae) {
                return false;
            }
        }

        return false;
    }

    /**
     * Checks whether input string is a valid host verifier specification
     */
    public static boolean isValidHostVerifier(String hvSpec) {

        hvSpec = hvSpec.trim();

        if (hvSpec.equals("") || hvSpec.equals("mirror") ||
            hvSpec.equals("hostname")) {
            return true;
        }

        if (hvSpec.startsWith("dnmatch(") && hvSpec.endsWith(")")) {
            try {
                SSLDNHostVerifier.validate(hvSpec);
            } catch (IllegalArgumentException iae) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Builds an SSLContext object for the specified access mode.
     *
     * @param params general instantiation information
     * @param clientMode set to true if the SSLContext is being created for
     * the client side of an SSL connection and false otherwise
     * @param logger for logging
     */
    private static SSLContext constructSSLContext(InstanceParams params,
                                                  boolean clientMode,
                                                  InstanceLogger logger) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        /* Determine whether a specific key is supposed to be used */
        String ksAliasProp = clientMode ?
            config.getSSLClientKeyAlias() :
            config.getSSLServerKeyAlias();
        if (ksAliasProp != null && ksAliasProp.isEmpty()) {
            ksAliasProp = null;
        }
        final KeyManager[] kmList =
            buildKeyManagerList(params.getContext(), ksAliasProp, clientMode,
                                logger);

        final TrustManager[] tmList =
            buildTrustManagerList(params.getContext(), logger);

        /*
         * Get an SSLContext object
         */
        SSLContext newContext = null;
        try {
            newContext = SSLContext.getInstance(SSL_CONTEXT_PROTOCOL);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalStateException(
                "Unable to find a suitable SSLContext", nsae);
        }

        /*
         * Put it all together into the SSLContext object
         */
        try {
            newContext.init(kmList, tmList, null);
        } catch (KeyManagementException kme) {
            throw new IllegalStateException(
                "Error establishing SSLContext", kme);
        }

        return newContext;
    }

    /**
     * Builds a list of KeyManagers for incorporation into an SSLContext.
     *
     * @param context the context information
     * @param ksAlias an optional KeyStore alias.  If set, the key manager
     * for X509 certs will always select the certificate with the specified
     * alias.
     * @param clientMode set to true if this is for the client side of
     * an SSL connection and false otherwise
     * @param logger for logging
     */
    private static KeyManager[] buildKeyManagerList(InstanceContext context,
                                                    String ksAlias,
                                                    boolean clientMode,
                                                    InstanceLogger logger) {

        final String keystoreName = getKeyStoreName(context);

        /* If there is no keystore, then there are no key managers */
        if (keystoreName == null) {
            return null;
        }

        try {
            final String serverAlias = clientMode ? null : ksAlias;
            final String clientAlias = clientMode ? ksAlias : null;
            return new KeyManager[] {
                AliasKeyManager.createRefresh(
                    serverAlias, clientAlias, keystoreName,
                    is -> createKeyManager(context, is), logger)
            };
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(
                "Problem with keystore: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Problem reading keystore: " + e.getMessage(), e);
        }
    }

    private static
        X509ExtendedKeyManager createKeyManager(InstanceContext context,
                                                InputStream inputStream)
    {
        final KeyStoreInfo ksInfo = readStoreInfo(context, inputStream,
                                                  false /* isTrustStore */);

        /* Already checked that there is a keystore */
        requireNonNull(ksInfo);
        try {

            /*
             * Get a KeyManagerFactory
             */
            final KeyManagerFactory kmf;
            try {
                kmf = KeyManagerFactory.getInstance(X509_ALGO_NAME);
            } catch (NoSuchAlgorithmException nsae) {
                throw new IllegalStateException(
                    "Unable to find a suitable KeyManagerFactory", nsae);
            }

            /*
             * Initialize the key manager factory
             */
            try {
                kmf.init(ksInfo.ks, ksInfo.ksPwd);
            } catch (KeyStoreException kse) {
                throw new IllegalStateException(
                    "Error processing keystore file " + ksInfo.ksFile,
                    kse);
            } catch (NoSuchAlgorithmException nsae) {
                throw new IllegalStateException(
                    "Unable to find appropriate algorithm for " +
                    "keystore file " + ksInfo.ksFile, nsae);
            } catch (UnrecoverableKeyException uke) {
                throw new IllegalStateException(
                    "Unable to recover key from keystore file " +
                    ksInfo.ksFile, uke);
            }

            /* Find the X509ExtendedKeyManager */
            for (final KeyManager km : kmf.getKeyManagers()) {
                if (km instanceof X509ExtendedKeyManager) {
                    return (X509ExtendedKeyManager) km;
                }
            }
            throw new IllegalStateException(
                "Unable to locate an X509ExtendedKeyManager " +
                "corresponding to keyStore " + ksInfo.ksFile);
        } finally {
            ksInfo.clearPassword();
        }
    }

    /**
     * Builds a TrustManager list for the input Truststore for use in creating
     * an SSLContext.
     */
    private static
        TrustManager[] buildTrustManagerList(InstanceContext context,
                                             InstanceLogger logger)
    {
        final String truststoreName =
            getStoreName(context, true /* isTrustStore */);

        /* If there is no truststore, then there are no trust managers */
        if (truststoreName == null) {
            return null;
        }

        try {
            return new TrustManager[] {
                new DelegatingTrustManager(
                    truststoreName, is -> createTrustManager(context, is),
                    logger)
            };
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(
                "Problem with truststore: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "Problem reading truststore: " + e.getMessage(), e);
        }
    }

    private static
        X509ExtendedTrustManager createTrustManager(InstanceContext context,
                                                    InputStream inputStream)
    {
        final KeyStoreInfo tsInfo = readStoreInfo(context, inputStream,
                                                  true /* isTruststore */);

        /* Already checked that there is a truststore */
        requireNonNull(tsInfo);
        try {
            final TrustManagerFactory tmf;
            try {
                tmf = TrustManagerFactory.getInstance(X509_ALGO_NAME);
            } catch (NoSuchAlgorithmException nsae) {
                throw new IllegalStateException(
                    "Unable to find a suitable TrustManagerFactory", nsae);
            }

            try {
                tmf.init(tsInfo.ks);
            } catch (KeyStoreException kse) {
                throw new IllegalStateException(
                    "Error initializing truststore " + tsInfo.ksFile, kse);
            }

            /* Find the X509ExtendedTrustManager and make it the delegate */
            for (final TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509ExtendedTrustManager) {
                    return (X509ExtendedTrustManager) tm;
                }
            }
            throw new IllegalStateException(
                "Unable to locate an X509ExtendedTrustManager " +
                "corresponding to keyStore " + tsInfo.ksFile);
        } finally {
            tsInfo.clearPassword();
        }
    }

    /**
     * Finds the key or trust store password based on the input config.
     */
    private static char[] getStorePassword(InstanceContext context,
                                           boolean trustStore) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        char[] pwd = null;

        /*
         * Determine the password for the keystore file
         * Try first using a password source, either explicit or
         * constructed.
         */
        PasswordSource pwdSource = trustStore ?
            config.getSSLTrustStorePasswordSource() :
            config.getSSLKeyStorePasswordSource();
        if (pwdSource == null) {
            pwdSource = constructPasswordSource(
                new InstanceParams(context, null), trustStore);
        }

        if (pwdSource != null) {
            pwd = pwdSource.getPassword();
        } else {
            /* Next look for an explicit password setting */
            String pwdProp = trustStore ?
                config.getSSLTrustStorePassword() :
                config.getSSLKeyStorePassword();
            if (pwdProp == null || pwdProp.isEmpty()) {

                /*
                 * Finally, consider the standard Java Keystore
                 * password system property
                 */
                pwdProp = trustStore ?
                    System.getProperty("javax.net.ssl.trustStorePassword") :
                    System.getProperty("javax.net.ssl.keyStorePassword");
            }
            if (pwdProp != null) {
                pwd = pwdProp.toCharArray();
            }
        }

        return pwd;
    }

    /**
     * Reads a KeyStore into memory based on the config.
     */
    private static KeyStoreInfo readStoreInfo(InstanceContext context,
                                              InputStream inputStream,
                                              boolean isTrustStore) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();

        /*
         * Determine what KeyStore file to access
         */
        final String storeName = getStoreName(context, isTrustStore);
        if (storeName == null) {
            return null;
        }

        /*
         * Determine what type of keystore to assume.  If not specified
         * loadStore determines the default
         */
        String storeType = isTrustStore ?
            config.getSSLTrustStoreType() :
            config.getSSLKeyStoreType();

        if (storeType == null || storeType.isEmpty()) {
            storeType = KeyStore.getDefaultType();
        }
        final char[] pwd = getStorePassword(context, isTrustStore);
        try {
            if (pwd == null && !isTrustStore) {
                throw new IllegalArgumentException(
                    "Unable to open keystore without a password");
            }

            /*
             * Get a KeyStore instance
             */
            final String flavor = isTrustStore ? "truststore" : "keystore";
            final KeyStore ks =
                loadStore(storeName, inputStream, pwd, flavor, storeType);

            return new KeyStoreInfo(storeName, ks, pwd);
        } finally {
            if (pwd != null) {
                Arrays.fill(pwd, ' ');
            }
        }
    }

    /**
     * Returns the name of the keystore file associated with the context.
     */
    static String getKeyStoreName(InstanceContext context) {
        return getStoreName(context, false /* isTrustStore */);
    }

    /**
     * Returns the name of the keystore or truststore file associated with the
     * context.
     */
    private static String getStoreName(InstanceContext context,
                                       boolean isTrustStore) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) context.getRepNetConfig();
        final String storeProp = isTrustStore ?
            config.getSSLTrustStore() : config.getSSLKeyStore();
        if (storeProp != null && !storeProp.isEmpty()) {
            return storeProp;
        }
        return isTrustStore ?
            System.getProperty("javax.net.ssl.trustStore") :
            System.getProperty("javax.net.ssl.keyStore");
    }

    /**
     * Create an SSLParameters base on the input configuration.
     */
    private static SSLParameters constructSSLParameters(
        InstanceParams params) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        /*
         * Determine cipher suites configuration
         */
        String cipherSuitesProp = config.getSSLCipherSuites();
        String[] cipherSuites = null;
        if (cipherSuitesProp != null && !cipherSuitesProp.isEmpty()) {
            cipherSuites = cipherSuitesProp.split(",");
        }

        /*
         * Determine protocols configuration
         */
        String protocolsProp = config.getSSLProtocols();
        String[] protocols = null;
        if (protocolsProp != null && !protocolsProp.isEmpty()) {
            protocols = protocolsProp.split(",");
        }

        final SSLParameters result =
            new SSLParameters(cipherSuites, protocols);
        if (config.getNeedClientAuth()) {
            result.setNeedClientAuth(true);
        }
        return result;
    }

    /**
     * Filter SSLParameter configuration to respect the supported
     * configuration capabilities of the context.
     */
    private static SSLParameters filterSSLParameters(
        SSLParameters configParams, SSLContext filterContext)
        throws IllegalArgumentException {

        SSLParameters suppParams = filterContext.getSupportedSSLParameters();

        /* Filter the cipher suite selection */
        String[] configCipherSuites = configParams.getCipherSuites();
        if (configCipherSuites != null) {
            final String[] suppCipherSuites = suppParams.getCipherSuites();
            configCipherSuites =
                filterConfig(configCipherSuites, suppCipherSuites);
            if (configCipherSuites.length == 0) {
                throw new IllegalArgumentException(
                    "None of the configured SSL cipher suites are supported " +
                    "by the environment.");
            }
        }

        /* Filter the protocol selection */
        String[] configProtocols =
            configParams.getProtocols();
        if (configProtocols != null) {
            final String[] suppProtocols = suppParams.getProtocols();
            configProtocols = filterConfig(configProtocols, suppProtocols);
            if (configProtocols.length == 0) {
                throw new IllegalArgumentException(
                    "None of the configured SSL protocols are supported " +
                    "by the environment.");
            }
        }

        final SSLParameters newParams =
            new SSLParameters(configCipherSuites, configProtocols);
        newParams.setWantClientAuth(configParams.getWantClientAuth());
        newParams.setNeedClientAuth(configParams.getNeedClientAuth());
        return newParams;
    }

    /**
     * Return the intersection of configChoices and supported
     */
    private static String[] filterConfig(String[] configChoices,
                                         String[] supported) {

        ArrayList<String> keep = new ArrayList<>();
        for (String choice : configChoices) {
            for (String supp : supported) {
                if (choice.equals(supp)) {
                    keep.add(choice);
                    break;
                }
            }
        }
        return keep.toArray(new String[keep.size()]);
    }

    /**
     * Build an SSLAuthenticator or HostnameVerifier based on property
     * configuration. This method looks up a class of the specified name,
     * then finds a constructor that has a single argument of type
     * InstanceParams and constructs an instance with that constructor, then
     * validates that the instance extends or implements the mustImplement
     * class specified.
     *
     * @param params the parameters for constructing this factory.
     * @param checkerClassName the name of the class to instantiate
     * @param checkerClassParams the value of the configured String params
     *                           argument
     * @param mustImplement a class denoting a required base class or
     *   required implemented interface of the class whose name is
     *   specified by checkerClassName.
     * @param miDesc a descriptive term for the class to be instantiated
     * @return an instance of the specified class
     */
    private static Object constructSSLChecker(
        InstanceParams params,
        String checkerClassName,
        String checkerClassParams,
        Class<?> mustImplement,
        String miDesc) {

        InstanceParams objParams =
            new InstanceParams(params.getContext(), checkerClassParams);

        return DataChannelFactoryBuilder.constructObject(
            checkerClassName, mustImplement, miDesc,
            /* class(InstanceParams) */
            new CtorArgSpec(
                new Class<?>[] { InstanceParams.class },
                new Object[]   { objParams }));
    }

    /**
     * Builds an SSLAuthenticator based on the input configuration referenced
     * by params.
     */
    private static SSLAuthenticator constructSSLAuthenticator(
        InstanceParams params)
        throws IllegalArgumentException {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        final String authSpec = config.getSSLAuthenticator();
        final String authClassName = config.getSSLAuthenticatorClass();

        /* check for conflicts */
        if (authSpec != null && !authSpec.equals("") &&
            authClassName != null && !authClassName.equals("")) {

            throw new IllegalArgumentException(
                "Cannot specify both authenticator and authenticatorClass");
        }

        if (authSpec != null && !authSpec.equals("")) {
            /* construct an authenticator of a known type */
            return constructStdAuthenticator(params, authSpec);
        }

        if (authClassName == null || authClassName.equals("")) {
            return null;
        }

        /* construct an authenticator using the specified class */
        final String authParams = config.getSSLAuthenticatorParams();

        return (SSLAuthenticator) constructSSLChecker(
            params, authClassName, authParams, SSLAuthenticator.class,
            "authenticator");
    }

    /**
     * Builds an SSLAuthenticator of a known type.
     */
    private static SSLAuthenticator constructStdAuthenticator(
        InstanceParams params, String authSpec)
        throws IllegalArgumentException {

        authSpec = authSpec.trim();
        if (authSpec.startsWith("dnmatch(") && authSpec.endsWith(")")) {
            /* a DN matching authenticator */
            final String match =
                authSpec.substring("dnmatch(".length(),
                                        authSpec.length()-1);
            return new SSLDNAuthenticator(
                new InstanceParams(params.getContext(), match));
        } else if (authSpec.equals("mirror")) {
            /* a mirroring  authenticator */
            return new SSLMirrorAuthenticator(
                new InstanceParams(params.getContext(), null));
        }

        throw new IllegalArgumentException(
            authSpec  + " is not a valid authenticator specification.");
    }

    /**
     * Builds an HostnameVerifier based on the configuration referenced in
     * params.
     */
    private static HostnameVerifier constructSSLHostVerifier(
        InstanceParams params)
        throws IllegalArgumentException {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();
        final String hvSpec = config.getSSLHostVerifier();
        final String hvClassName = config.getSSLHostVerifierClass();

        /* Check for conflicts */
        if (hvSpec != null && !hvSpec.equals("") &&
            hvClassName != null && !hvClassName.equals("")) {

            throw new IllegalArgumentException(
                "Cannot specify both hostVerifier and hostVerifierClass");
        }

        if (hvSpec != null && !hvSpec.equals("")) {
            /* construct a host verifier of a known type */
            return constructStdHostVerifier(params, hvSpec);
        }

        if (hvClassName == null || hvClassName.equals("")) {
            return null;
        }

        /* construct a host verifier using the specified class */
        final String hvParams = config.getSSLHostVerifierParams();

        return (HostnameVerifier) constructSSLChecker(
            params, hvClassName, hvParams, HostnameVerifier.class,
            "hostname verifier");
    }

    /**
     * Builds a HostnameVerifier of a known type.
     */
    private static HostnameVerifier constructStdHostVerifier(
        InstanceParams params, String hvSpec)
        throws IllegalArgumentException {

        hvSpec = hvSpec.trim();
        if (hvSpec.startsWith("dnmatch(") && hvSpec.endsWith(")")) {
            /* a DN matching host verifier */
            final String match = hvSpec.substring("dnmatch(".length(),
                                                       hvSpec.length()-1);
            return new SSLDNHostVerifier(
                new InstanceParams(params.getContext(), match));

        } else if (hvSpec.equals("mirror")) {
            /* a mirroring  host verifier */
            return new SSLMirrorHostVerifier(
                new InstanceParams(params.getContext(), null));

        } else if (hvSpec.equals("hostname")) {
            /* a standard  hostname verifier */
            return new SSLStdHostVerifier(
                new InstanceParams(params.getContext(), null));
        }

        throw new IllegalArgumentException(
            hvSpec  + " is not a valid hostVerifier specification.");
    }

    /**
     * Builds a PasswordSource instance via generic instantiation.
     *
     * @param params the parameters driving the instantiation
     * @param pwdSrcClassName the name of the class to instantiate
     * @param pwSrcParams a possibly null String that has been configured as
     * an argument to the class's constructor.
     * @return the new instance
     */
    private static PasswordSource constructPasswordSource(
        InstanceParams params, String pwdSrcClassName, String pwSrcParams) {

        final InstanceParams objParams =
            new InstanceParams(params.getContext(), pwSrcParams);

        return (PasswordSource)
            DataChannelFactoryBuilder.constructObject(
                pwdSrcClassName, PasswordSource.class, "password source",
                /* class(InstanceParams) */
                new CtorArgSpec(
                    new Class<?>[] { InstanceParams.class },
                    new Object[]   { objParams }));
    }

    /**
     * Build a PasswordSource for the keystore based on the configuration
     * referenced by params.
     */
    private static PasswordSource constructPasswordSource(
        InstanceParams params, boolean trustStore) {

        final ReplicationSSLConfig config =
            (ReplicationSSLConfig) params.getContext().getRepNetConfig();

        final String pwSrcClassName = trustStore ?
            config.getSSLTrustStorePasswordClass() :
            config.getSSLKeyStorePasswordClass();

        if (pwSrcClassName == null || pwSrcClassName.equals("")) {
            return null;
        }

        final String pwSrcParams = trustStore ?
            config.getSSLTrustStorePasswordParams() :
            config.getSSLKeyStorePasswordParams();

        return constructPasswordSource(params, pwSrcClassName, pwSrcParams);
    }

    /**
     * Load a keystore/truststore file into memory
     * @param storeName the name of the store file
     * @param inputStream the keytstore contents
     * @param storeFlavor a descriptive name of store type
     * @param storeType JKS, etc
     * @throws IllegalArgumentException if the specified parameters
     * do now allow a store to be successfully loaded
     */
    private static KeyStore loadStore(String storeName,
                                      InputStream inputStream,
                                      char[] storePassword,
                                      String storeFlavor,
                                      String storeType)
        throws IllegalArgumentException {

        if (storeType == null || storeType.isEmpty()) {
            storeType = KeyStore.getDefaultType();
        }

        final KeyStore ks;
        try {
            ks = KeyStore.getInstance(storeType);
        } catch (KeyStoreException kse) {
            throw new IllegalArgumentException(
                "Unable to find a " + storeFlavor + " instance of type " +
                storeType,
                kse);
        }

        try {
            ks.load(inputStream, storePassword);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error reading from " + storeFlavor + " file " + storeName,
                ioe);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalArgumentException(
                "Unable to check " + storeFlavor + " integrity: " + storeName,
                nsae);
        } catch (CertificateException ce) {
            throw new IllegalArgumentException(
                "Not all certificates could be loaded: " + storeName,
                ce);
        }

        return ks;
    }

    /**
     * Gets a proper algorithm name for the X.509 certificate key manager. If
     * users already specify it via setting the system property of
     * "je.ssl.x509AlgoName", use it directly. Otherwise, for IBM J9 VM, the
     * name is "IbmX509". For Hotspot and other JVMs, the name of "SunX509"
     * will be used.
     *
     * @return algorithm name for X509 certificate manager
     */
    private static String getX509AlgoName() {
        final String x509Name = System.getProperty(X509_ALGO_NAME_PROPERTY);
        if (x509Name != null && !x509Name.isEmpty()) {
            return x509Name;
        }
        final String jvmVendor = System.getProperty("java.vendor");
        if (jvmVendor.startsWith("IBM")) {
            return "IbmX509";
        }
        return "SunX509";
    }

    /**
     * Internal class for communicating a pair of KeyStore and password
     */
    private static class KeyStoreInfo {
        private final String ksFile;
        private final KeyStore ks;
        private final char[] ksPwd;

        private KeyStoreInfo(String ksFile, KeyStore ks, char[] ksPwd) {
            this.ksFile = ksFile;
            this.ks = ks;
            this.ksPwd =
                (ksPwd == null) ? null : Arrays.copyOf(ksPwd, ksPwd.length);
        }

        private void clearPassword() {
            if (ksPwd != null) {
                Arrays.fill(ksPwd, ' ');
            }
        }
    }

    /** Return the client SSLContext, for testing. */
    public SSLContext getClientSSLContext() {
        return clientSSLContext;
    }

    /** Return the server SSLContext, for testing. */
    public SSLContext getServerSSLContext() {
        return serverSSLContext;
    }
}
