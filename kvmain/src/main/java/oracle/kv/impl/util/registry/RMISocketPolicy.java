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

package oracle.kv.impl.util.registry;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.util.NonNullByDefault;

/**
 * An RMISocketPolicy implementation is responsible for producing socket
 * factories for RMI. The policy generally produces client and server pairs.
 */
public interface RMISocketPolicy {

    /**
     * A matched pair of socket factories - one for the client and one for
     * the server.
     */
    @NonNullByDefault
    public static final class SocketFactoryPair {
        private final ServerSocketFactory serverFactory;
        private final ClientSocketFactory clientFactory;

        public SocketFactoryPair(ServerSocketFactory serverFactory,
                                 ClientSocketFactory clientFactory) {
            this.serverFactory = checkNull("serverFactory", serverFactory);
            this.clientFactory = checkNull("clientFactory", clientFactory);
        }

        public ClientSocketFactory getClientFactory() {
            return clientFactory;
        }

        public ServerSocketFactory getServerFactory() {
            return serverFactory;
        }
    }

    /**
     * A mechanism for expressing socket and CSF creation options.
     */
    public static final class SocketFactoryArgs {
        private String ssfName;
        private int ssfBacklog;
        private String ssfPortRange;
        private int ssfAcceptMaxActiveConns;
        private String csfName;
        private int csfConnectTimeoutMs;
        private int csfReadTimeoutMs;
        private String kvStoreName;
        private ClientId clientId;

        public SocketFactoryArgs() {
        }

        public String getSsfName() {
            return ssfName;
        }

        public SocketFactoryArgs setSsfName(String newSsfName) {
            this.ssfName = newSsfName;
            return this;
        }

        public int getSsfBacklog() {
            return ssfBacklog;
        }

        public SocketFactoryArgs setSsfBacklog(int newSsfBacklog) {
            this.ssfBacklog = newSsfBacklog;
            return this;
        }

        /**
         * Returns the maximum number of active socket connections permitted
         * for an async server socket.
         */
        public int getSsfAcceptMaxActiveConns() {
            return ssfAcceptMaxActiveConns;
        }

        /**
         * Sets the maximum number of active socket connections permitted for
         * an async server socket.
         */
        public SocketFactoryArgs setSsfAcceptMaxActiveConns(
            int newSsfAcceptMaxActiveConns)
        {
            this.ssfAcceptMaxActiveConns = newSsfAcceptMaxActiveConns;
            return this;
        }

        public String getSsfPortRange() {
            return ssfPortRange;
        }

        public SocketFactoryArgs setSsfPortRange(String newSsfPortRange) {
            this.ssfPortRange = newSsfPortRange;
            return this;
        }

        public String getCsfName() {
            return csfName;
        }

        public SocketFactoryArgs setCsfName(String newCsfName) {
            this.csfName = newCsfName;
            return this;
        }

        public int getCsfConnectTimeout() {
            return csfConnectTimeoutMs;
        }

        public SocketFactoryArgs setCsfConnectTimeout(
            int newCsfConnectTimeoutMs) {

            this.csfConnectTimeoutMs = newCsfConnectTimeoutMs;
            return this;
        }

        public int getCsfReadTimeout() {
            return csfReadTimeoutMs;
        }

        public SocketFactoryArgs setCsfReadTimeout(int newCsfReadTimeoutMs) {
            this.csfReadTimeoutMs = newCsfReadTimeoutMs;
            return this;
        }

        public String getKvStoreName() {
            return kvStoreName;
        }

        public SocketFactoryArgs setKvStoreName(String newKvStoreName) {
            this.kvStoreName = newKvStoreName;
            return this;
        }

        public ClientId getClientId() {
            return clientId;
        }

        public SocketFactoryArgs setClientId(ClientId clientId) {
            this.clientId = clientId;
            return this;
        }
    }

    /**
     * Prepare for use as the standard client policy within an SN component.
     * This is expected to install any state needed to be picked up by
     * client socket factories sent from the server.
     *
     * @param storeContext a null-allowable string that indicates a specific
     *   KVStore name that is being prepared. If null, the prepare is
     *   performed only for unqualified RMI access.
     * @param clientId the client ID or null in a server context
     */
    void prepareClient(String storeContext, ClientId clientId);

    /**
     * Clears client socket factory information installed by {@link
     * #prepareClient} for the client ID associated with a KVStoreImpl that is
     * being shutdown.
     *
     * @param clientId the client ID of the KVStoreImpl
     */
    void clearPreparedClient(ClientId clientId);

    /**
     * Return a server socket factory appropriate for registry creation.
     */
    ServerSocketFactory getRegistrySSF(SocketFactoryArgs args);

    /*
     * Return a Client socket factory for appropriate for registry
     * access by the client
     */
    ClientSocketFactory getRegistryCSF(SocketFactoryArgs args);

    /**
     * Return a Server/Client pair of socket factories appropriate
     * for untrusted object binding.
     */
    SocketFactoryPair getBindPair(SocketFactoryArgs args);

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    boolean isTrustCapable();
}
