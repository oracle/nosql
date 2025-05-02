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

package oracle.kv.impl.mgmt.jmx;

import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.ssl.SSLServerSocketFactory;

/**
 * The utility class to create a JMXConnectorServer instance.
 */
public class JmxConnectorUtil {
    public static final String JMX_SERVICE_PREFIX = "jmxrmi-";
    public static final String JMX_SSF_NAME = "jmxrmi";
    final static String JMX_CSF_NAME = "jmxrmi";

    private JmxConnectorUtil() {}

    /**
     * Create a {@code JMXConnectorServer}, it sets the correct
     * {@code SSLContext} based on store's truststore
     *
     * @param url the address of the new connector server.  The
     * actual address of the new connector server, as returned by its
     * {@link JMXConnectorServer#getAddress() getAddress} method, will
     * not necessarily be exactly the same.  For example, it might
     * include a port number if the original address did not.
     * @param server the MBean server that this connector server
     * is attached to.
     * @param sp the {@code SecurityParams} of the store
     *
     * @return a {@code JMXConnectorServer} representing the new
     * connector server. Each successful call to this method produces
     * a different object.
     */
    public static JMXConnectorServer createAndStartConnector(
        JMXServiceURL url, MBeanServer server, SecurityParams sp) {

        SSLContext defaultSslCtx = null;
        try {

            Map<String, Object> env = new HashMap<>();
            RMISocketPolicy.SocketFactoryPair jmxSFP = getJMXSFP(
                sp.getRMISocketPolicy());
            if (jmxSFP != null) {
                if (jmxSFP.getServerFactory() != null &&
                    jmxSFP.getClientFactory() != null) {

                    if (jmxSFP.getServerFactory().getClass() ==
                        SSLServerSocketFactory.class)  {

                        if (!EmbeddedMode.isEmbedded()) {
                            /*
                             * Replacing the SSLContext with a new context
                             * based on store truststore and revert back
                             * after JMXServer is started to make sure
                             * SslRMIClientSocketFactory can be initialized
                             * properly for JMXCollectorAgent to connect.
                             *
                             * All SslRMIClientSocketFactory in the same JVM
                             * will use a single instance of SSLSocketFactory to
                             * create sockets and initialized with the default
                             * SSLContext at the first time of createSocket()
                             * invocation.
                             *
                             * Given that JMXCollectorAgent is running within
                             * the same JVM of JMXServer and must use
                             * SslRMIClientSocketFactory to connect JMXServer,
                             * the factory must be initialized with a custom
                             * SSLContext with store truststore.
                             */
                            defaultSslCtx = setSSLContextDefault(sp);
                        }

                        /*
                         * If using SSL, force the CSF to use the standard CSF
                         * class because jconsole won't have access to KVStore
                         * internal ones.
                         */
                        env.put(RMIConnectorServer.
                                    RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                                new SslRMIClientSocketFactory());
                    }
                }
                if (jmxSFP.getServerFactory() != null) {
                    env.put(RMIConnectorServer.
                                RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                            jmxSFP.getServerFactory());

                    if (jmxSFP.getServerFactory().getClass() ==
                        SSLServerSocketFactory.class &&
                        jmxSFP.getClientFactory() != null) {

                        /*
                         * Needed so that we can bind in the SSL registry.
                         * Unfortunately, there doesn't appear to be a
                         * published mechanism for doing this.
                         */
                        env.put("com.sun.jndi.rmi.factory.socket",
                                jmxSFP.getClientFactory());
                    }
                }
            }
            JMXConnectorServer connector = JMXConnectorServerFactory
                    .newJMXConnectorServer(url, env, server);
            connector.start();
            return connector;

        } catch (Exception e) {
            throw new IllegalStateException
                ("Unexpected error creating JMX connector.", e);
        } finally {
            if (defaultSslCtx != null) {
                SSLContext.setDefault(defaultSslCtx);
            }
        }
    }

    /**
     * Construct a URL for the JMX service.
     *
     * @param registryHostName the RMI registry host name
     * @param registryPort the RMI registry port number
     * @param localPort the local port number, if it is 0, use anonymous port
     * @param path the path name of the service URL, in a single registry, each
     * service must register a unique path name
     *
     * @return a {@code JMXServiceURL} representing the new service URL
     */
    public static JMXServiceURL makeUrl(
        String registryHostName, int registryPort, int localPort, String path) {

        StringBuilder sb = new StringBuilder("service:jmx:rmi://");
        if (localPort != 0) {
            sb.append(registryHostName);
            sb.append(":");
            sb.append(localPort);
        }
        sb.append("/jndi/rmi://");
        sb.append(registryHostName);
        sb.append(":");
        sb.append(registryPort);
        sb.append("/");
        sb.append(path);

        try {
            return new JMXServiceURL(sb.toString());
        } catch (MalformedURLException e) {
            throw new IllegalStateException
                ("Unexpected error constructing JMX service URL (" +
                 sb, e);
        }
    }

    private static SocketFactoryPair getJMXSFP(RMISocketPolicy jmxRMIPolicy) {
        if (jmxRMIPolicy == null) {
            return null;
        }

        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(JMX_SSF_NAME).setCsfName(JMX_CSF_NAME);
        return jmxRMIPolicy.getBindPair(args);
    }

    /*
     * Replace the default SSLContext with a context created from security
     * parameters. The context is created with TrustManagers based on the
     * store truststore. This method returns the original default SSLContext,
     * which can be used to reset the default after use.
     */
    private static SSLContext setSSLContextDefault(SecurityParams sp)
        throws Exception {

        SSLContext defaultCtx = null;
        String tsPath = sp.resolveFile(sp.getTruststoreFile()).getPath();
        if (tsPath != null) {
            TrustManager[] tmList = null;
            defaultCtx = SSLContext.getDefault();
            String tsType = sp.getTruststoreType();
            if (tsType == null) {
                tsType = KeyStore.getDefaultType();
            }
            KeyStore ts = KeyStore.getInstance(tsType);
            KeyStorePasswordSource pwdSrc =
                KeyStorePasswordSource.create(sp);
            char[] tsPwd = (pwdSrc == null) ? null : pwdSrc.getPassword();

            try (FileInputStream in = new FileInputStream(tsPath)) {
                SSLConfig.loadKeystore(ts, in, tsPwd);
                TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(SSLConfig.X509_ALGO_NAME);
                tmf.init(ts);
                tmList = tmf.getTrustManagers();
                SSLContext ctx = SSLContext.getInstance("TLS");
                ctx.init(null, tmList, null /* SecureRandom */);

                SSLContext.setDefault(ctx);
            } finally {
                SecurityUtils.clearPassword(tsPwd);
            }
        }
        return defaultCtx;
    }
}
