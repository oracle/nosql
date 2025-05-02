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

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;

import oracle.kv.impl.admin.param.RMISocketPolicyBuilder;
import oracle.kv.impl.admin.param.RepNetConfigBuilder;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.ssl.SSLConfig.InstanceInfo;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.ssl.SSLServerSocketPolicy;

import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * Factory class for generating RMISocketPolicy instances and configuring
 * JE data channels.
 */
public class SSLTransport
    implements RMISocketPolicyBuilder, RepNetConfigBuilder {

    /**
     * Simple constructor, for use by newInstance(), since these are dynamically
     * constructed based on configuration.
     */
    public SSLTransport() {
    }

    /*
     * RMISocketPolicyBuilder interface methods
     */

    /**
     * Construct an RMISocketPolicy for a transport.
     */
    @Override
    public RMISocketPolicy makeSocketPolicy(SecurityParams sp,
                                            ParameterMap map,
                                            Logger logger)
        throws Exception {

        /* password retrieval may be expensive, so cache across calls */
        final KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);
        final char[] ksPwd = (pwdSrc == null) ? null : pwdSrc.getPassword();

        try {
            final SSLConfig clientConfig = makeSSLConfig(sp, map, ksPwd, false);
            final SSLConfig serverConfig = makeSSLConfig(sp, map, ksPwd, true);

            return new SSLServerSocketPolicy(
                serverConfig.makeSSLControl(true, logger),
                clientConfig.makeSSLControl(false, logger));
        } finally {
            if (ksPwd != null) {
                Arrays.fill(ksPwd, ' ');
            }
        }
    }

    /**
     * Construct a set of properties for client access.
     */
    @Override
    public Properties getClientAccessProperties(SecurityParams sp,
                                                ParameterMap map) {
        final Properties props = getSSLProperties(sp, map,
                                                  false, /* resolveFiles */
                                                  false /* isServer */);

        /*
         * Don't include the keystore if there isn't a keystore alias
         */
        if (props.getProperty(SSLConfig.KEYSTORE_ALIAS) == null) {
            props.remove(SSLConfig.KEYSTORE_FILE);
        }
        return props;
    }

    /*
     * Create SSL config and make up the key manager factory according to
     * the configurations.
     */
    public KeyManagerFactory createKeyManagerFactory(SecurityParams sp,
                                                     ParameterMap map)
        throws Exception {
        final KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);
        final char[] ksPwd = (pwdSrc == null) ? null : pwdSrc.getPassword();
        final SSLConfig config = makeSSLConfig(sp, map, ksPwd, true);
        return config.makeSSLKeyManagerFactory();
    }

    /*
     * RepNetConfigBuilder interface methods
     */

    /**
     * Construct an set of properties for initialization of a
     * ReplicationNetworkConfig for a transport.
     */
    @Override
    public Properties makeChannelProperties(SecurityParams sp,
                                            ParameterMap map) {

        final Properties props = new Properties();
        props.setProperty(ReplicationNetworkConfig.CHANNEL_TYPE, "ssl");

        final String keystoreFile = sp.getKeystoreFile();
        if (keystoreFile != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_KEYSTORE_FILE,
                sp.resolveFile(keystoreFile).getPath());
        }

        final String keystoreType = sp.getKeystoreType();
        if (keystoreType != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_KEYSTORE_TYPE, keystoreType);
        }

        final KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);
        if (pwdSrc != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_CLASS,
                pwdSrc.getClass().getName());
            props.setProperty(
                ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_PARAMS,
                pwdSrc.getParamString());
            props.setProperty(
                ReplicationSSLConfig.SSL_TRUSTSTORE_PASSWORD_CLASS,
                pwdSrc.getClass().getName());
            props.setProperty(
                ReplicationSSLConfig.SSL_TRUSTSTORE_PASSWORD_PARAMS,
                pwdSrc.getParamString());
        }

        final String truststoreFile = sp.getTruststoreFile();
        if (truststoreFile != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_TRUSTSTORE_FILE,
                sp.resolveFile(truststoreFile).getPath());
        }

        final String truststoreType = sp.getTruststoreType();
        if (truststoreType != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_TRUSTSTORE_TYPE, truststoreType);
        }

        final String serverKeyAlias = sp.getTransServerKeyAlias(map);
        if (serverKeyAlias != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_SERVER_KEY_ALIAS, serverKeyAlias);
        }

        final String clientKeyAlias = sp.getTransClientKeyAlias(map);
        if (clientKeyAlias != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_CLIENT_KEY_ALIAS, clientKeyAlias);
        }

        final String serverCipherSuites =
            sp.getTransAllowCipherSuites(map);
        if (serverCipherSuites != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_CIPHER_SUITES, serverCipherSuites);
        }

        final String serverProtocols = sp.getTransAllowProtocols(map);
        if (serverProtocols != null) {
            props.setProperty(
                ReplicationSSLConfig.SSL_PROTOCOLS, serverProtocols);
        }

        final String clntIdentAllowed = sp.getTransClientIdentityAllowed(map);
        if (clntIdentAllowed != null) {
            final InstanceInfo<SSLAuthenticator> authInstInfo =
                SSLConfig.makeAuthenticatorInfo(clntIdentAllowed);
            props.setProperty(
                ReplicationSSLConfig.SSL_AUTHENTICATOR_CLASS,
                authInstInfo.jeImplClass);
            props.setProperty(
                ReplicationSSLConfig.SSL_AUTHENTICATOR_PARAMS,
                authInstInfo.jeImplParams);
        }

        final String srvrIdentAllowed = sp.getTransServerIdentityAllowed(map);
        if (srvrIdentAllowed != null) {
            final InstanceInfo<HostnameVerifier> verifierInstInfo =
                SSLConfig.makeHostVerifierInfo(srvrIdentAllowed);
            props.setProperty(
                ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS,
                verifierInstInfo.jeImplClass);
            props.setProperty(
                ReplicationSSLConfig.SSL_HOST_VERIFIER_PARAMS,
                verifierInstInfo.jeImplParams);
        }

        return props;
    }

    /*
     * Non-interface methods
     */

    /**
     * Build an SSLConfig object suitable for making an SSLControl.
     */
    SSLConfig makeSSLConfig(SecurityParams sp,
                            ParameterMap map,
                            char[] ksPwd,
                            boolean isServer) {

        final Properties props =
            getSSLProperties(sp, map, true /* resolveFiles */, isServer);

        final SSLConfig cfg = new SSLConfig(props);

        /* server key and trust store use same password */
        cfg.setKeystorePassword(ksPwd);
        cfg.setTrustStorePassword(ksPwd);
        return cfg;
    }

    /**
     * Build a Properties that describe the transport configuration.
     */
    Properties getSSLProperties(SecurityParams sp,
                                ParameterMap map,
                                boolean resolveFiles,
                                boolean isServer) {

        final Properties props = new Properties();

        final String allowCipherSuites = sp.getTransAllowCipherSuites(map);
        if (allowCipherSuites != null) {
            props.setProperty(SSLConfig.ENABLED_CIPHER_SUITES,
                              allowCipherSuites);
        }

        final String allowProtocols = sp.getTransAllowProtocols(map);
        if (allowProtocols != null) {
            props.setProperty(SSLConfig.ENABLED_PROTOCOLS,
                              allowProtocols);
        }

        /* For the client, allow override using client-specific config */
        if (!isServer) {
            final String clientAllowCipherSuites =
                sp.getTransClientAllowCipherSuites(map);
            if (clientAllowCipherSuites != null) {
                props.setProperty(SSLConfig.ENABLED_CIPHER_SUITES,
                                  clientAllowCipherSuites);
            }

            final String clientAllowProtocols =
                sp.getTransClientAllowProtocols(map);
            if (clientAllowProtocols != null) {
                props.setProperty(SSLConfig.ENABLED_PROTOCOLS,
                                  clientAllowProtocols);
            }
        }

        final String ksFile = sp.getKeystoreFile();
        if (ksFile != null) {
            props.setProperty(SSLConfig.KEYSTORE_FILE,
                              (resolveFiles ?
                               sp.resolveFile(ksFile).getPath() :
                               ksFile));
            final String ksType = sp.getKeystoreType();
            if (ksType != null) {
                props.setProperty(SSLConfig.KEYSTORE_TYPE, ksType);
            }
        }

        final String tsFile = sp.getTruststoreFile();
        if (tsFile != null) {
            props.setProperty(SSLConfig.TRUSTSTORE_FILE,
                              (resolveFiles ?
                               sp.resolveFile(tsFile).getPath() :
                               tsFile));
            final String tsType = sp.getTruststoreType();
            if (tsType != null) {
                props.setProperty(SSLConfig.TRUSTSTORE_TYPE, tsType);
            }
        }

        if (isServer) {
            final String alias = sp.getTransServerKeyAlias(map);
            if (alias != null) {
                props.setProperty(SSLConfig.KEYSTORE_ALIAS, alias);
            }

            final String clientIdentityAllowed =
                sp.getTransClientIdentityAllowed(map);
            if (clientIdentityAllowed != null) {
                props.setProperty(SSLConfig.CLIENT_AUTHENTICATOR,
                                  clientIdentityAllowed);
            }
        } else {
            final String alias = sp.getTransClientKeyAlias(map);
            if (alias != null) {
                props.setProperty(SSLConfig.KEYSTORE_ALIAS, alias);
            }

            final String serverIdentityAllowed =
                sp.getTransServerIdentityAllowed(map);
            if (serverIdentityAllowed != null) {
                props.setProperty(SSLConfig.SERVER_HOST_VERIFIER,
                                  serverIdentityAllowed);
            }
        }

        return props;
    }
}
