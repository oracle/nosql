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

package oracle.kv.impl.admin.param;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;

import oracle.kv.KVSecurityConstants;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.ClearTransport;
import oracle.kv.impl.security.ssl.SSLTransport;
import oracle.kv.impl.util.registry.ClearSocketPolicy;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;

/**
 * The security configuration properties. Although this class is in the
 * oracle.kv.impl.admin.param package, it isn't actually managed by the admin
 * at this point.  It is managed entirely within the filesystem, using
 * the securityconfig utility.
 */
public class SecurityParams {

    public static final String TRANS_TYPE_FACTORY = "factory";
    public static final String TRANS_TYPE_SSL = "ssl";
    public static final String TRANS_TYPE_CLEAR = "clear";

    /**
     * The KeyStore type of a keystore or truststore whose type was not
     * specified explicitly.  The JKS value matches the default KeyStore type
     * prior to Java 9.
     */
    public static final String MISSING_KEYSTORE_TYPE = "JKS";

    /* Socket policy used for standard access */
    private RMISocketPolicy clientRMISocketPolicy;

    /* Socket policy used for internally authenticated access */
    private RMISocketPolicy trustedRMISocketPolicy;

    /* The main security parameter map */
    private final ParameterMap map;

    /* Transport-type parameter maps */
    private final Map<String, ParameterMap> transportMaps;

    /* The containing directory of this configuration, if known */
    private File configDir;

    /**
     * Basic constructor.
     */
    public SecurityParams() {
        this.map = new ParameterMap();
        this.map.setValidate(true);
        map.setName(ParameterState.SECURITY_PARAMS);
        map.setType(ParameterState.SECURITY_TYPE);
        this.transportMaps = new HashMap<String, ParameterMap>();
    }

    /**
     * constructor with explicit parameter setting.
     */
    public SecurityParams(ParameterMap map) {
        this.map = map;
        map.setName(ParameterState.SECURITY_PARAMS);
        map.setType(ParameterState.SECURITY_TYPE);
        this.transportMaps = new HashMap<String, ParameterMap>();
        this.configDir = null;
    }

    /**
     * constructor for file reading.
     */
    public SecurityParams(LoadParameters lp, File configFile) {
        this.map = lp.getMapByType(ParameterState.SECURITY_TYPE);
        map.setName(ParameterState.SECURITY_PARAMS);
        map.setType(ParameterState.SECURITY_TYPE);

        this.transportMaps = new HashMap<String, ParameterMap>();
        for (ParameterMap pm :
                 lp.getAllMaps(ParameterState.SECURITY_TRANSPORT_TYPE)) {
            this.transportMaps.put(pm.getName(), pm);
        }

        if (configFile != null) {
            final File absConfigFile = configFile.getAbsoluteFile();
            this.configDir = absConfigFile.getParentFile();
        }
    }

    /**
     * Creates a minimal security parameters object that is sufficient
     * to meet the KVStore server requirements, but represents an insecure
     * environment.
     */
    public static SecurityParams makeDefault() {
        final SecurityParams sp = new SecurityParams();

        sp.setSecurityEnabled(false);

        sp.addTransportMap("client");
        sp.setTransType("client", "clear");

        sp.addTransportMap("internal");
        sp.setTransType("internal", "clear");

        sp.addTransportMap("ha");
        sp.setTransType("ha", "clear");

        return sp;
    }

    /**
     * Returns an indication of whether this SecurityParams object actually
     * enables security.
     */
    public boolean isSecure() {
        return getSecurityEnabled();
    }

    public boolean getSecurityEnabled() {
        return map.get(ParameterState.SEC_SECURITY_ENABLED).asBoolean();
    }

    /*
     * Create SSL key manager factory using client-server transportation
     * settings. The key manager factory is used for creating SSL context for
     * admin web service.
     */
    public KeyManagerFactory createSSLKeyManagerFactory() throws Exception {
        if (!isSecure()) {
            return null;
        }
        final String transportName = "client";
        final ParameterMap transportParams =
            findTransportParams(transportName);
        final SSLTransport transport = (SSLTransport)makeTransportFactory(
            transportName, transportParams, RMISocketPolicyBuilder.class);
        return transport.createKeyManagerFactory(this, transportParams);
    }

    public void setSecurityEnabled(boolean enabled) {
        map.setParameter(ParameterState.SEC_SECURITY_ENABLED,
                         Boolean.toString(enabled));
    }

    /**
     * Set configDir for use when not loaded from file
     */
    public void setConfigDir(File cfgDir) {
        this.configDir = cfgDir;
    }

    /**
     * Returns the configuration directory containing the security file.
     * If the SecurityParams is null, the result will be null.
     */
    public File getConfigDir() {
        return configDir;
    }

    /**
     * Accessor for the underlying parameter map.
     */
    public ParameterMap getMap() {
        return map;
    }

    public Collection<ParameterMap> getTransportMaps() {
        return transportMaps.values();
    }

    public void addTransportMap(ParameterMap newMap, String name) {
        newMap.setName(name);
        newMap.setType(ParameterState.SECURITY_TRANSPORT_TYPE);
        transportMaps.put(name, newMap);
    }

    public void addTransportMap(String name) {
        ParameterMap transportMap = transportMaps.get(name);
        if (transportMap == null) {
            transportMap = new ParameterMap();
            transportMap.setValidate(true);
            transportMap.setName(name);
            transportMap.setType(ParameterState.SECURITY_TRANSPORT_TYPE);
            transportMaps.put(name, transportMap);
        }
    }

    public ParameterMap getTransportMap(String name) {
        return transportMaps.get(name);
    }

    /**
     * Whether SSL is disabled on all transports, including transports client,
     * ha and internal.
     * @return true if SSL is disabled on all transports.
     */
    public boolean allTransportSSLDisabled() {
        return !transportSSLEnabled("client") &&
               !transportSSLEnabled("ha") &&
               !transportSSLEnabled("internal");
    }

    /**
     * Whether SSL is enabled on given transport.
     *
     * @param name transport name, valid names: client, ha and internal.
     * @return true if SSL is enabled on given transport, otherwise false
     * if transport isn't enabled SSL or invalid transport is given.
     */
    public boolean transportSSLEnabled(String name) {
        final String transType = getTransType(name);
        final String transFactory = getTransFactory(name);
        return ((transType != null) &&
                transType.equals(TRANS_TYPE_SSL)) ||
                (transFactory != null) &&
                transFactory.equals(SSLTransport.class.getName());
    }

    public File resolveFile(String filename) {
        if (filename == null) {
            return null;
        }
        final File origFile = new File(filename);
        if (!origFile.isAbsolute() && configDir != null) {
            return new File(configDir.getPath(), origFile.getPath());
        }

        return origFile;
    }

    public String getKeystoreFile() {
        return map.get(ParameterState.SEC_KEYSTORE_FILE).asString();
    }

    public void setKeystoreFile(String keystoreFile) {
        map.setParameter(ParameterState.SEC_KEYSTORE_FILE, keystoreFile);
    }

    public String getKeystoreType() {
        final String type =
            map.get(ParameterState.SEC_KEYSTORE_TYPE).asString();

        /*
         * Use JKS if no explicit entry is found, since that was the Java
         * default prior to Java 9.  This check is needed so that we use the
         * proper value when running with Java 9 or later since the default is
         * now PKCS12.  For information on changing the default, see [#26773].
         */
        return ((type != null) && !"".equals(type)) ?
            type :
            MISSING_KEYSTORE_TYPE;
    }

    public void setKeystoreType(String keystoreType) {
        map.setParameter(ParameterState.SEC_KEYSTORE_TYPE, keystoreType);
    }

    public String getKeystoreSigPrivateKeyAlias() {
        final String ksPrivateKeyAlias =
            map.get(ParameterState.SEC_KEYSTORE_SIG_PRIVATE_KEY_ALIAS).
                asString();
        if (ksPrivateKeyAlias != null && !ksPrivateKeyAlias.isEmpty()) {
            return ksPrivateKeyAlias;
        }
        return null;
    }

    public void setKeystoreSigPrivateKeyAlias(String keyAlias) {
        map.setParameter(ParameterState.SEC_KEYSTORE_SIG_PRIVATE_KEY_ALIAS,
                         keyAlias);
    }

    public String getTruststoreFile() {
        return map.get(ParameterState.SEC_TRUSTSTORE_FILE).asString();
    }

    public void setTruststoreFile(String truststoreFile) {
        map.setParameter(ParameterState.SEC_TRUSTSTORE_FILE, truststoreFile);
    }

    public String getTruststoreType() {
        final String type =
            map.get(ParameterState.SEC_TRUSTSTORE_TYPE).asString();

        /* As with the KeyStore type, use JKS if no explicit entry is found */
        return ((type != null) && !"".equals(type)) ?
            type :
            MISSING_KEYSTORE_TYPE;
    }

    public void setTruststoreType(String truststoreType) {
        map.setParameter(ParameterState.SEC_TRUSTSTORE_TYPE, truststoreType);
    }

    public String getTruststoreSigPublicKeyAlias() {
        final String tsPublicKeyAlias =
            map.get(ParameterState.SEC_TRUSTSTORE_SIG_PUBLIC_KEY_ALIAS).
                asString();
        if (tsPublicKeyAlias != null && !tsPublicKeyAlias.isEmpty()) {
            return tsPublicKeyAlias;
        }
        return null;
    }

    public void setTruststoreSigPublicKeyAlias(String keyAlias) {
        map.setParameter(ParameterState.SEC_TRUSTSTORE_SIG_PUBLIC_KEY_ALIAS,
                         keyAlias);
    }

    public String getPasswordFile() {
        return map.get(ParameterState.SEC_PASSWORD_FILE).asString();
    }

    public void setPasswordFile(String passwordFile) {
        map.setParameter(ParameterState.SEC_PASSWORD_FILE, passwordFile);
    }

    public String getPasswordClass() {
        return map.get(ParameterState.SEC_PASSWORD_CLASS).asString();
    }

    public void setPasswordClass(String passwordClass) {
        map.setParameter(ParameterState.SEC_PASSWORD_CLASS, passwordClass);
    }

    public String getWalletDir() {
        return map.get(ParameterState.SEC_WALLET_DIR).asString();
    }

    public void setWalletDir(String walletDir) {
        map.setParameter(ParameterState.SEC_WALLET_DIR, walletDir);
    }

    public String getInternalAuth() {
        return map.get(ParameterState.SEC_INTERNAL_AUTH).asString();
    }

    public void setInternalAuth(String internalAuth) {
        map.setParameter(ParameterState.SEC_INTERNAL_AUTH, internalAuth);
    }

    public String getCertMode() {
        return map.get(ParameterState.SEC_CERT_MODE).asString();
    }

    public void setCertMode(String certMode) {
        map.setParameter(ParameterState.SEC_CERT_MODE, certMode);
    }

    public String getKeystorePasswordAlias() {
        final String ksPwdAlias =
            map.get(ParameterState.SEC_KEYSTORE_PWD_ALIAS).asString();
        if (ksPwdAlias != null && ksPwdAlias.length() > 0) {
            return ksPwdAlias;
        }
        return null;
    }

    public void setKeystorePasswordAlias(String alias) {
        map.setParameter(ParameterState.SEC_KEYSTORE_PWD_ALIAS, alias);
    }

    public String getSignatureAlgorithm() {
        return map.get(ParameterState.SEC_SIGNATURE_ALGO).asString();
    }

    public void setSignatureAlgorithm(String sigAlgo) {
        map.setParameter(ParameterState.SEC_SIGNATURE_ALGO, sigAlgo);
    }

    /* Kerberos-related accessors */

    public String getKerberosServiceName() {
        return map.getOrDefault(
           ParameterState.SEC_KERBEROS_SERVICE_NAME).asString();
    }

    public void setKerberosServiceName(String serviceName) {
        map.setParameter(ParameterState.SEC_KERBEROS_SERVICE_NAME, serviceName);
    }

    public String getKerberosInstanceName() {
        return map.get(ParameterState.SEC_KERBEROS_INSTANCE_NAME).asString();
    }

    public void setKerberosInstanceName(String instanceName) {
        map.setParameter(ParameterState.SEC_KERBEROS_INSTANCE_NAME,
                         instanceName);
    }

    public String getKerberosRealmName() {
        return map.get(ParameterState.SEC_KERBEROS_REALM_NAME).asString();
    }

    public void setKerberosRealmName(String realmName) {
        map.setParameter(ParameterState.SEC_KERBEROS_REALM_NAME, realmName);
    }

    public String getKerberosConfFile() {
        return map.get(ParameterState.SEC_KERBEROS_CONFIG_FILE).asString();
    }

    public void setKerberosConfFile(String confFile) {
        map.setParameter(ParameterState.SEC_KERBEROS_CONFIG_FILE, confFile);
    }

    public String getKerberosKeytabFile() {
        return map.get(ParameterState.SEC_KERBEROS_KEYTAB_FILE).asString();
    }

    public void setKerberosKeytabFile(String keytabFile) {
        map.setParameter(ParameterState.SEC_KERBEROS_KEYTAB_FILE, keytabFile);
    }

    public KrbPrincipalInfo getKerberosPrincipalInfo() {
        return new KrbPrincipalInfo(getKerberosRealmName(),
                                    getKerberosServiceName(),
                                    getKerberosInstanceName());
    }

    /* Transport-related accessors */

    public String getTransType(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransType(transportMap);
    }

    public String getTransType(final ParameterMap transportMap) {
        return transportMap.get(ParameterState.SEC_TRANS_TYPE).asString();
    }

    public void setTransType(String transport, String transType) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransType(transportMap, transType);
    }

    public void setTransType(ParameterMap transportMap, String transType) {
        transportMap.setParameter(ParameterState.SEC_TRANS_TYPE, transType);
    }

    /* Factory is applicable only if transport type == factory */
    public String getTransFactory(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransFactory(transportMap);
    }

    public String getTransFactory(ParameterMap transportMap) {
        return transportMap.get(ParameterState.SEC_TRANS_FACTORY).asString();
    }

    public void setTransFactory(String transport, String factory) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransFactory(transportMap, factory);
    }

    public void setTransFactory(ParameterMap transportMap, String factory) {
        transportMap.setParameter(ParameterState.SEC_TRANS_FACTORY, factory);
    }

    public String getTransServerKeyAlias(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransServerKeyAlias(transportMap);
    }

    public String getTransServerKeyAlias(ParameterMap transportMap) {
        return transportMap.get(ParameterState.SEC_TRANS_SERVER_KEY_ALIAS).
            asString();
    }

    public void setTransServerKeyAlias(String transport, String alias) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransServerKeyAlias(transportMap, alias);
    }

    public void setTransServerKeyAlias(ParameterMap transportMap,
                                       String alias) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_SERVER_KEY_ALIAS, alias);
    }

    public String getTransClientKeyAlias(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransClientKeyAlias(transportMap);
    }

    public String getTransClientKeyAlias(ParameterMap transportMap) {
        return transportMap.
            get(ParameterState.SEC_TRANS_CLIENT_KEY_ALIAS).asString();
    }

    public void setTransClientKeyAlias(String transport, String alias) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransClientKeyAlias(transportMap, alias);
    }

    public void setTransClientKeyAlias(ParameterMap transportMap,
                                       String alias) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_CLIENT_KEY_ALIAS, alias);
    }

    public String getTransAllowCipherSuites(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransAllowCipherSuites(transportMap);
    }

    public String getTransAllowCipherSuites(ParameterMap transportMap) {
        return transportMap.get(
            ParameterState.SEC_TRANS_ALLOW_CIPHER_SUITES).asString();
    }

    public void setTransAllowCipherSuites(String transport,
                                                String allowedSuites) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransAllowCipherSuites(transportMap, allowedSuites);
    }

    public void setTransAllowCipherSuites(ParameterMap transportMap,
                                                String allowedSuites) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_ALLOW_CIPHER_SUITES, allowedSuites);
    }

    public String getTransAllowProtocols(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransAllowProtocols(transportMap);
    }

    public String getTransAllowProtocols(ParameterMap transportMap) {
        return transportMap.getOrDefault(
            ParameterState.SEC_TRANS_ALLOW_PROTOCOLS).asString();
    }

    public void setTransAllowProtocols(String transport,
                                             String allowedProtocols) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransAllowProtocols(transportMap, allowedProtocols);
    }

    public void setTransAllowProtocols(ParameterMap transportMap,
                                             String allowedProtocols) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_ALLOW_PROTOCOLS, allowedProtocols);
    }

    public String getTransClientAllowCipherSuites(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransClientAllowCipherSuites(transportMap);
    }

    public String getTransClientAllowCipherSuites(ParameterMap transportMap) {
        return transportMap.get(
            ParameterState.SEC_TRANS_CLIENT_ALLOW_CIPHER_SUITES).asString();
    }

    public void setTransClientAllowCipherSuites(String transport,
                                                String allowedSuites) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransClientAllowCipherSuites(transportMap, allowedSuites);
    }

    public void setTransClientAllowCipherSuites(ParameterMap transportMap,
                                                String allowedSuites) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_CLIENT_ALLOW_CIPHER_SUITES, allowedSuites);
    }

    public String getTransClientAllowProtocols(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransClientAllowProtocols(transportMap);
    }

    public String getTransClientAllowProtocols(ParameterMap transportMap) {
        return transportMap.get(
            ParameterState.SEC_TRANS_CLIENT_ALLOW_PROTOCOLS).asString();
    }

    public void setTransClientAllowProtocols(String transport,
                                             String allowedProtocols) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransClientAllowProtocols(transportMap, allowedProtocols);
    }

    public void setTransClientAllowProtocols(ParameterMap transportMap,
                                             String allowedProtocols) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_CLIENT_ALLOW_PROTOCOLS, allowedProtocols);
    }

    public String getTransClientIdentityAllowed(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransClientIdentityAllowed(transportMap);
    }

    public String getTransClientIdentityAllowed(
        ParameterMap transportMap) {

        return transportMap.get(
            ParameterState.SEC_TRANS_CLIENT_IDENT_ALLOW).asString();
    }

    public void setTransClientIdentityAllowed(String transport,
                                              String identAllowed) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransClientIdentityAllowed(transportMap, identAllowed);
    }
    public void setTransClientIdentityAllowed(ParameterMap transportMap,
                                              String identAllowed) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_CLIENT_IDENT_ALLOW, identAllowed);
    }

    public boolean getTransClientAuthRequired(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransClientAuthRequired(transportMap);
    }

    public boolean getTransClientAuthRequired(ParameterMap transportMap) {
        return transportMap.get(
            ParameterState.SEC_TRANS_CLIENT_AUTH_REQUIRED).asBoolean();
    }

    public void setTransClientAuthRequired(String transport,
                                           boolean authRequired) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransClientAuthRequired(transportMap, authRequired);
    }

    public void setTransClientAuthRequired(ParameterMap transportMap,
                                           boolean authRequired) {
        transportMap.setParameter(ParameterState.SEC_TRANS_CLIENT_AUTH_REQUIRED,
                                  Boolean.toString(authRequired));
    }

    public String getTransServerIdentityAllowed(String transport) {
        final ParameterMap transportMap = requireTransportMap(transport);
        return  getTransServerIdentityAllowed(transportMap);
    }

    public String getTransServerIdentityAllowed(
        ParameterMap transportMap) {
        return transportMap.get(
            ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW).asString();
    }

    public void setTransServerIdentityAllowed(String transport,
                                              String identAllowed) {
        final ParameterMap transportMap = requireTransportMap(transport);
        setTransServerIdentityAllowed(transportMap, identAllowed);
    }

    public void setTransServerIdentityAllowed(ParameterMap transportMap,
                                              String identAllowed) {
        transportMap.setParameter(
            ParameterState.SEC_TRANS_SERVER_IDENT_ALLOW, identAllowed);
    }

    /*
     * Utility code
     */

    /**
     * Return the standard RMI socket policy.
     *
     * @return the standard RMI socket policy
     */
    public RMISocketPolicy getRMISocketPolicy() {
        if (clientRMISocketPolicy == null) {
            throw new IllegalStateException(
                "No RMI socket policy is in force");
        }

        return clientRMISocketPolicy;
    }

    /**
     * Return the trusted RMI socket policy.
     *
     * @return the trusted RMI socket policy, if available, else null
     */
    public RMISocketPolicy getTrustedRMISocketPolicy() {
        return trustedRMISocketPolicy;
    }

    /**
     * Called by SN components to ensure that an appropriate RMISocketPolicy
     * is in place prior to creating the registryCSF.
     * @throws IllegalStateException if the security configuration is invalid
     */
    public void initRMISocketPolicies(Logger logger)
        throws IllegalStateException {

        if (isSecure()) {
            useRMISocketPolicies(logger);
        } else {
            useRMISocketPolicyDefaults();
        }
    }

    /**
     * Returns a set of properties that enables client communication with
     * the server.
     */
    public Properties getClientAccessProps() {

        final String transportName = "client";
        final ParameterMap transMap = getTransportMap(transportName);

        final RMISocketPolicyBuilder spb = (RMISocketPolicyBuilder)
            makeTransportFactory(transportName, transMap,
                                 RMISocketPolicyBuilder.class);
        final Properties props = spb.getClientAccessProperties(this, transMap);

        final String transportType =
            transMap.get(ParameterState.SEC_TRANS_TYPE).asString();
        if (transportType != null && !transportType.isEmpty()) {
            props.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY,
                              transportType);
        }

        return props;
    }

    /**
     * Called to ensure that an appropriate RMISocketPolicy is in place prior
     * to creating the registryCSF.
     */
    private void useRMISocketPolicies(Logger logger)
        throws IllegalStateException {

        final RMISocketPolicy clientSocketPolicy =
            createClientRMISocketPolicy(logger);
        ClientSocketFactory.setRMIPolicy(clientSocketPolicy);
        clientRMISocketPolicy = clientSocketPolicy;

        final RMISocketPolicy trustedSocketPolicy =
            createTrustedRMISocketPolicy(logger);

        if (trustedSocketPolicy != null) {
            /* No need to supply a store context here */
            trustedSocketPolicy.prepareClient(null /* storeContext */,
                                              null /* clientId */);
            trustedRMISocketPolicy = trustedSocketPolicy;
        }
    }

    private ParameterMap requireTransportMap(String transport) {
        final ParameterMap transportMap = transportMaps.get(transport);
        if (transportMap == null) {
            throw new IllegalStateException(
                "Transport " + transport + " does not exist");
        }
        return transportMap;
    }

    /**
     * Called to ensure that an appropriate RMISocketPolicy is in place prior
     * to creating the registryCSF when there is no security configuration in
     * place.
     */
    private void useRMISocketPolicyDefaults() {
        clientRMISocketPolicy = makeDefaultRMISocketPolicy();
        ClientSocketFactory.setRMIPolicy(clientRMISocketPolicy);
        trustedRMISocketPolicy = null;
    }

    /**
     * Return the RMI socket policy used for normal communication between
     * components.
     */
    private RMISocketPolicy createClientRMISocketPolicy(Logger logger)
        throws IllegalStateException {

        return makeRMISocketPolicy("client", logger);
    }

    /**
     * Return the RMI socket policy used by components when communicating
     * with other components in trusted mode.
     */
    private RMISocketPolicy createTrustedRMISocketPolicy(Logger logger)
        throws IllegalStateException {

        final RMISocketPolicy internalPolicy =
            makeRMISocketPolicy("internal", logger);
        if (!transportSSLEnabled("internal")) {
            return internalPolicy;
        }
        return internalPolicy.isTrustCapable() ? internalPolicy : null;
    }

    private static RMISocketPolicy makeDefaultRMISocketPolicy() {
        return new ClearSocketPolicy();
    }

    /**
     * Construct an RMISocketPolicy for the specified transportName.
     * @param transportName The name of a transport, which must be
     *        present in the list of transports
     * @return An instance of RMISocketPolicy
     */
    private RMISocketPolicy makeRMISocketPolicy(String transportName,
                                                Logger logger)
        throws IllegalStateException {

        final ParameterMap transportParams = findTransportParams(transportName);
        final RMISocketPolicyBuilder spb = (RMISocketPolicyBuilder)
            makeTransportFactory(transportName, transportParams,
                                 RMISocketPolicyBuilder.class);

        try {
            return spb.makeSocketPolicy(this, transportParams, logger);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Error constructing RMISocketPolicy using transport class " +
                "for transport " + transportName, e);
        }
    }

    /**
     * Construct a RepNetConfigBuilder for the specified transport
     * @param transportParams The transport configuration parameters
     * @return An instance of RepNetConfigBuilder
     */
    private RepNetConfigBuilder makeRepNetConfigBuilder(
        ParameterMap transportParams)
        throws IllegalStateException {

        final String transportName = transportParams.getName();
        return (RepNetConfigBuilder)
            makeTransportFactory(transportName, transportParams,
                                 RepNetConfigBuilder.class);
    }

   /**
     * Locate the ParameterMap for the specified transportName.
     *
     * @param transportName The name of a transport, which must be
     *        present in the list of transports
     * @return The parameter map for the transport
     * @throw ConfigurationError if the transport parameter map cannot
     *        be found.
     */
    private ParameterMap findTransportParams(String transportName)
        throws IllegalStateException {

        /* Find the transport parameter map */
        final ParameterMap transportParams = transportMaps.get(transportName);
        if (transportParams == null) {
            throw new IllegalStateException(
                "transport name " + transportName +
                " does not exist in the configuration");
        }
        return transportParams;
    }

    /**
     * Construct a transport fractory for the specified transportName.
     * No assumption is made as to what purpose the transport factory is
     * being created.
     *
     * @param transportName The name of a transport, which must be
     *        present in the list of transports
     * @param transportParams The parameter map for the transport
     * @param factoryInterfaceClass A class or interface to which the
     *        resulting object must be castable.
     * @return An instance of of the factory for the transport name
     */
    private Object makeTransportFactory(String transportName,
                                        ParameterMap transportParams,
                                        Class<?> factoryInterfaceClass)
        throws IllegalStateException {

        /*
         * Get the transport factory class name from the transport
         * parameter map
         */
        final String transportType = getTransType(transportParams);
        String transportFactory = null;
        if (transportType == null || transportType.isEmpty() ||
            TRANS_TYPE_FACTORY.equals(transportType)) {
            transportFactory = getTransFactory(transportParams);
        } else {
            if (TRANS_TYPE_SSL.equals(transportType)) {
                transportFactory = SSLTransport.class.getName();
            } else if (TRANS_TYPE_CLEAR.equals(transportType)) {
                transportFactory = ClearTransport.class.getName();
            } else {
                throw new IllegalStateException(
                    "Transport " + transportName +
                    " has an unrecognized transportType: " + transportType);
            }
        }

        if (transportFactory == null) {
            throw new IllegalStateException(
                "Transport " + transportName +
                " has no transportFactory parameter specified");
        }

        /*
         * Resolve the transport factory class
         */
        Class<?> factoryClass = null;
        try {
            factoryClass = Class.forName(transportFactory);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Error resolving transport class " + transportFactory +
                " for transport " + transportName, e);
        }

        /*
         * Get an instance of the factory class.  It must have an accessible
         * no-argument constructor.
         */
        Object factoryObject = null;
        try {
            factoryObject =
                factoryClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(
                "Error instantiating transport class " + transportFactory +
                " for transport " + transportName, e);
        }

        /*
         * Check that the class must implements the desired interface.
         */
        if (!factoryInterfaceClass.isInstance(factoryObject)) {
            throw new IllegalStateException(
                "Transport factory class " + transportFactory +
                " for transport " + transportName +
                " does not implement " + factoryInterfaceClass.getName());
        }

        return factoryObject;
    }

    /**
     * Get the set of JE properties needed to construct the
     * ReplicationNetworkConfig for JE HA.
     */
    public Properties getJEHAProperties() {

        final ParameterMap transportParams = findTransportParams("ha");
        if (transportParams == null) {
            return new Properties();
        }

        final RepNetConfigBuilder builder =
            makeRepNetConfigBuilder(transportParams);
        return builder.makeChannelProperties(this, transportParams);
    }

    /**
     * The class contains service principal information. This class used for
     * passing principal information via
     * StorageNodeAgentAPI.getKerberosPrincipalInfo
     */
    public static class KrbPrincipalInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String realmName;
        private final String serviceName;
        private final String instanceName;

        KrbPrincipalInfo(String realmName,
                         String serviceName,
                         String instanceName) {
            this.realmName = realmName;
            this.serviceName = serviceName;
            this.instanceName = instanceName;
        }

        public String getRealmName() {
            return realmName;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getInstanceName() {
            return instanceName;
        }
    }
}
