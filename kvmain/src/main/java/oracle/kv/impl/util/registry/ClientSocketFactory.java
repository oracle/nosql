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
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;

/**
 * An implementation of RMIClientSocketFactory that permits configuration of
 * the following Socket timeouts:
 * <ol>
 * <li>Connection timeout</li>
 * <li>Read timeout</li>
 * </ol>
 * These are set to allow clients to become aware of possible network problems
 * in a timely manner.
 * <p>
 * CSFs with the appropriate timeouts for a registry are specified on the
 * client side.
 * <p>
 * CSFs for service requests (unrelated to the registry) have default values
 * provided by the server that can be overridden by the client as below:
 * <ol>
 * <li>Server side timeout parameters are set via the KVS admin as policy
 * parameters</li>
 * <li>Client side timeout parameters are set via {@link KVStoreConfig}. When
 * present, they override the parameters set at the server level.</li>
 * </ol>
 * <p>
 * Currently, read timeouts are implemented using a timer thread and the
 * TimeoutTask, which periodically checks open sockets and interrupts any that
 * are inactive and have exceeded their timeout period. We replaced the more
 * obvious approach of using the Socket.setSoTimeout() method with this manual
 * mechanism, because the socket implementation uses a poll system call to
 * enforce the timeout, which was too cpu intensive.
 * <p>
 * The lifetime and data flow of a CSF is complex and worth noting: The CSF is
 * created by the server process exporting the service object, serialized and
 * sent to the registry (hosted by the SNA) when the exported object is first
 * registered by the RMI server. Subsequently, it's serialized and sent out by
 * the RMI registry to each RMI client that needs a handle to the exported
 * object. This means that properties of the client factory, that are
 * client-specific, e.g. connection local address, connection open and read
 * timeouts, etc. must be set when the exported object is deserialized at the
 * client in the updateAfterDeserialization method.
 * <p>
 * Much as with the registries recorded by {@link RegistryUtils}, this class
 * needs to associate information with particular stores, and sometimes with
 * specific services within those stores. The information needed consists of
 * client-side parameters, including security parameters, that are used to
 * configure client socket factories when they are downloaded from the server
 * as part of the service endpoints stored in the registry. Lookups are
 * performed using binding names (for service-specific parameters), or store
 * names (for security parameters), as well as with client IDs. Both types of
 * lookups are used, as well as a global default, for the same reasons
 * mentioned for RegistryUtils, primarily to support tests that mix client and
 * server facilities in the same process.
 * <p>
 * TODO: RMI does not make any provisions for request granularity timeouts, but
 * now that we have implemented our own timeout mechanism, request granularity
 * timeouts could be supported. If request timeouts are implemented, perhaps
 * that should encompass and replace connection and request timeouts.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ClientSocketFactory
        implements FastExternalizable, RMIClientSocketFactory, Serializable
{
    private static final long serialVersionUID = 1L;

    private static volatile Logger logger = null;

    /*
     * Mutual exclusion object for thread-safe synchronized locking, this
     * object is used to create synchronize lock for any update on sockets,
     * timer and timoutTask.
     */
    private static final Object mutex = new Object();

    /*
     * The list of sockets to check for being active.  Access and modification
     * of the list are synchronized, which could be a possible bottleneck if
     * the list is very large.
     * Any update on sockets must be wrapped in synchronized(mutex) block.
     */
    private static final List<TimeoutSocket> sockets = new LinkedList<>();

    /*
     * RMI doesn't let you provide application level context into the
     * socket factory, so the timer and timeout tasks which implement socket
     * timeouts are static, rather than scoped per NoSQL DB service or per
     * RequestDispatcher.
     * Only initialize timer and timeoutTask when socket is created. Free
     * these two objects when there is no active socket. This prevents the
     * JVM from holding an idle thread doing nothing.
     * Any update on timer and timeoutTask must be wrapped in
     * synchronized(mutex) block.
     */
    private static Timer timer = null;
    private static volatile TimeoutTask timeoutTask = null;

    /*
     * The default timer interval in milliseconds.
     * This value should only be changed during testing.
     */
    private static long defaultTimerIntervalMs =
        TimeoutTask.DEFAULT_TIMER_INTERVAL_MS;

    /* Counts of the allocated sockets and socket factories, for unit testing */
    protected transient volatile AtomicInteger socketCount =
        new AtomicInteger(0);
    protected static final AtomicInteger socketFactoryCount =
        new AtomicInteger(0);

    /**
     * Map from service name and client ID to any (optional) client side
     * overrides of the default timeout period.
     */
    private static final Map<ServiceKey, SocketTimeouts> serviceToTimeoutsMap =
        new ConcurrentHashMap<>();

    /**
     * Map from service name and client ID to the local interface to be used
     * for communications with the store. If no entry is present, uses the
     * default local interface.
     */
    private static final Map<ServiceKey, InetSocketAddress>
        serviceToLocalAddrMap = new ConcurrentHashMap<>();

    /*
     * The generation into which new ClientSocketFactories are being born.
     */
    private static final AtomicInteger currCsfGeneration = new AtomicInteger(0);

    /*
     * The ID generator for newly minted ClientSocketFactories.
     */
    private static final AtomicLong nextCsfId =
        new AtomicLong(System.nanoTime());

    /*
     * The RMI socket policy used for general client access.  Synchronize on
     * the class when accessing this field.
     */
    private static RMISocketPolicy clientPolicy;

    /*
     * The SSLConfig used to generate the last SSL socket policy, or null if
     * not using SSL.  Synchronize on the class when accessing this field.
     */
    private static SSLConfig sslConfig;

    /**
     * The client ID of the caller for use during deserialization, or null if
     * called in a server context. The value is specified as a thread local
     * because there is no way to pass it directly during deserialization and,
     * unlike the store name, the value is not available on the server side
     * when the factory is serialized.
     */
    private static final ThreadLocal<ClientId> currentClientId =
        new ThreadLocal<>();

    /**
     * A logger to use for debugging, for cases where we don't have access to
     * the real logger.
     */
    private static final Logger localDebugLogger =
        ClientLoggerUtils.getLogger(ClientSocketFactory.class, "debugging");

    /* The name associated with the CSF. */
    protected final String name;
    protected volatile int connectTimeoutMs;
    protected volatile int readTimeoutMs;

    /**
     * The client ID or null in a server context. The value is specified in the
     * constructor on the client side or is set during deserialization from
     * the value of currentClientId.
     */
    protected transient volatile ClientId clientId;

    /*
     * The "generation" at which this CSF was born, as viewed from the client
     * side of the world. ClientSocketFactories of different generation never
     * compare equal.
     */
    private transient volatile int csfGeneration;

    /*
     * The "id" at which this CSF as viewed from the server side of the world.
     * ClientSocketFactories with different id values never compare equal.
     */
    private final long csfId;

    /*
     * The local address on this machine to which the connection is bound, or
     * null for the default. This value is only set when the CSF instance is
     * deserialized at the client, since the value is client-specific and
     * cannot be a non-transient iv in the CSF itself. The non-null value is
     * used to select from among multiple TCP network interfaces.
     */
    private transient InetSocketAddress localAddr;

    /**
     * Creates the client socket factory.
     *
     * @param name the factory name
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     * @param clientId the client ID or null in a server context
     */
    public ClientSocketFactory(String name,
                               int connectTimeoutMs,
                               int readTimeoutMs,
                               ClientId clientId) {

        this(name, connectTimeoutMs, readTimeoutMs, clientId,
             nextCsfId.getAndIncrement());
    }

    /**
     * Create a client socket factory and specify the CSF ID, for testing.
     */
    public ClientSocketFactory(String name,
                               int connectTimeoutMs,
                               int readTimeoutMs,
                               ClientId clientId,
                               long csfId) {
        this.name = name;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.clientId = clientId;
        this.csfGeneration = currCsfGeneration.get();
        this.csfId = csfId;
    }

    /**
     * Creates an instance using data from an input stream.
     */
    protected ClientSocketFactory(DataInput in, short serialVersion)
        throws IOException {

        name = readString(in, serialVersion);
        connectTimeoutMs = readPackedInt(in);
        readTimeoutMs = readPackedInt(in);
        csfId = readPackedLong(in);
        updateAfterDeserialization();
    }

    /*
     * Force the start of a new generation on the client side.
     */
    public static void newGeneration() {
        currCsfGeneration.incrementAndGet();
    }

    /**
     * For testing only. Need to be careful, update on the sockets needs to be
     * thread-safe. This not guaranteed when the test code updates the sockets.
     */
    static List<TimeoutSocket> getSockets() {
        return sockets;
    }

    /**
     * Generates a factory name that is unique for each KVS, component and
     * service to facilitate timeouts at service granularity.
     *
     * @param kvsName the store name
     * @param compName the component name, the string sn, rn, etc.
     * @param interfaceName the interface name
     *                                  {@link InterfaceType#interfaceName()}
     *
     * @return the name to be used for a factory
     */
    public static String factoryName(String kvsName,
                                     String compName,
                                     String interfaceName) {

        return kvsName + '|' + compName + '|' + interfaceName;
    }

    /**
     * The factory name associated with the SNA's registry.
     */
    public static String registryFactoryName() {
        return "registry";
    }

    public String getBindingName() {
        return name;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public ClientId getClientId() {
        return clientId;
    }

    public InetSocketAddress getLocalAddr() {
        return localAddr;
    }

    /**
     * Returns the number of sockets that have been allocated so far.
     */
    public int getSocketCount() {
        return socketCount.get();
    }

    public static int getSocketFactoryCount() {
        return socketFactoryCount.get();
    }

    public static void setSocketFactoryCount(int count) {
        socketFactoryCount.set(count);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
                ((name == null) ? 0 : name.hashCode());
        result = prime * result + connectTimeoutMs;
        result = prime * result + readTimeoutMs;
        result = prime * result +
                ((localAddr == null) ? 0 : localAddr.hashCode());
        result = prime * result + (int) csfId;
        result = prime * result + csfGeneration;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("<");
        sb.append(getClass().getSimpleName());
        getToStringBody(sb);
        sb.append(">");
        return sb.toString();
    }

    protected void getToStringBody(StringBuilder sb) {
        sb.append(" name=").append(name);
        sb.append(" id=").append(hashCode());
        sb.append(" connectMs=").append(connectTimeoutMs);
        sb.append(" readMs=").append(readTimeoutMs);
        sb.append(" clientId=").append(clientId == null ? "none" : clientId);
        sb.append(" localAddr=").append(
            (localAddr == null) ? "none" : localAddr);
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
        final ClientSocketFactory other = (ClientSocketFactory) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (connectTimeoutMs != other.connectTimeoutMs) {
            return false;
        }
        if (readTimeoutMs != other.readTimeoutMs) {
            return false;
        }
        if (localAddr == null) {
            if (other.localAddr != null) {
                return false;
            }
        } else if (!localAddr.equals(other.localAddr)) {
            return false;
        }
        if (csfGeneration != other.csfGeneration) {
            return false;
        }
        if (csfId != other.csfId) {
            return false;
        }
        return true;
    }

    /**
     * Read the object and override the server supplied default timeout values
     * with any client side timeouts.
     */
    private void readObject(ObjectInputStream in)
       throws IOException, ClassNotFoundException {

        in.defaultReadObject();

        updateAfterDeserialization();
    }

    private void updateAfterDeserialization() {

        /* Reset the generation for the client side */
        csfGeneration = currCsfGeneration.get();

        /* Store the current client ID */
        clientId = getCurrentClientId();

        if ((name == null) && (clientId == null)) {
            /* use the defaults. */
            return;
        }

        /* Override defaults, if necessary, with client side timeout settings.*/
        SocketTimeouts timeouts =
            getFromServiceMap(name, clientId, serviceToTimeoutsMap);
        if (timeouts != null) {
            connectTimeoutMs = timeouts.connectTimeoutMs;
            readTimeoutMs = timeouts.readTimeoutMs;
        }
        localAddr = getFromServiceMap(name, clientId, serviceToLocalAddrMap);
        socketCount = new AtomicInteger();
        socketFactoryCount.incrementAndGet();
    }

    /*
     * Cancel the TimeoutTask and terminate the timer thread
     */
    private static void terminateTimer() {
        synchronized (mutex) {
            if (timeoutTask != null) {
                timeoutTask.cancel();
                timeoutTask = null;
            }
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeString String}) {@link
     *      #getBindingName name}
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@link
     *      #getConnectTimeoutMs connectTimeoutMs}
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@link
     *      #getReadTimeoutMs readTimeoutMs}
     * <li> ({@link SerializationUtil#writePackedLong} packed long}
     *      <i>csfId</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeString(out, serialVersion, name);
        writePackedInt(out, connectTimeoutMs);
        writePackedInt(out, readTimeoutMs);
        writePackedLong(out, csfId);
    }

    /**
     * @see java.rmi.server.RMIClientSocketFactory#createSocket
     */
    @Override
    public Socket createSocket(String host, int port)
         throws java.net.UnknownHostException, IOException {
        return createTimeoutSocket(host, port);
    }

    protected TimeoutSocket createTimeoutSocket(String host, int port)
        throws java.net.UnknownHostException, IOException {

        /*
         * Use a TimeoutSocket rather than a vanilla socket and
         * Socket.setSoTimeout(). The latter is implemented using a poll system
         * call, which is too cpu intensive.
         */
        final TimeoutSocket sock = new TimeoutSocket(readTimeoutMs);
        sock.bind(localAddr);

        sock.connect(new InetSocketAddress(host, port), connectTimeoutMs);

        /* Disable Nagle's algorithm to minimize request latency. */
        sock.setTcpNoDelay(true);
        socketCount.incrementAndGet();

        synchronized (mutex) {
            /*
             * Start the timer thread if it's not started yet
             */
            if (timeoutTask == null) {
                timeoutTask = new TimeoutTask(defaultTimerIntervalMs);
            }
            /*
             * Register the socket regardless of its readTimeoutMS value, because
             * the default server supplied timeouts may be overridden in
             * readObject() with client side timeouts.
             *
             * Register only after the connect has been successful to ensure the
             * socket object is not leaked.
             */
            timeoutTask.register(sock);
        }

        return sock;
    }

    /**
     * Note this configuration for use by any future client socket factories.
     * Existing socket factories cannot be changed, since it would break
     * the hash code and equals methods, preventing RMI from locating and using
     * socket factories it had cached.
     *
     * @param bindingName the binding name associated with this interface
     * in the registry.
     * @param config the store config parameters, a subset of which is relevant
     * to the CSF's client side configuration.
     * @param clientId the client ID of the caller or null if called in a
     * server context
     */
    public static void configure(String bindingName,
                                 KVStoreConfig config,
                                 ClientId clientId) {
        checkNull("bindingName", bindingName);
        final long requestTimeoutMs =
            config.getRequestTimeout(TimeUnit.MILLISECONDS);
        final long readTimeoutMs =
            config.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        if (!config.getUseAsync() && (requestTimeoutMs > readTimeoutMs)) {
            final String format = "Invalid KVStoreConfig. " +
                "Request timeout: %,d ms exceeds " +
                "socket read timeout: %,d ms" ;
            throw new IllegalArgumentException
                (String.format(format, requestTimeoutMs, readTimeoutMs));
        }
        final int openTimeoutMs =
            (int) config.getSocketOpenTimeout(TimeUnit.MILLISECONDS);
        putIntoServiceMap(bindingName, clientId,
                          new SocketTimeouts(openTimeoutMs,
                                             (int) readTimeoutMs),
                          serviceToTimeoutsMap);
        if (config.getLocalAddress() != null) {
            putIntoServiceMap(bindingName, clientId, config.getLocalAddress(),
                              serviceToLocalAddrMap);
        }
    }

    /**
     * Clears out the configuration done by {@link #configure}).
     *
     * This method is for test use only.
     */
    public static void clearConfiguration() {
        serviceToTimeoutsMap.clear();
        serviceToLocalAddrMap.clear();
    }

    /**
     * Returns the size of serviceToTimeoutsMap, for testing.
     */
    public static int getServiceToTimeoutsMapSize() {
        return serviceToTimeoutsMap.size();
    }

    /**
     * Clears out service entries added by {@link #configure} associated with a
     * client ID for a KVStoreImpl that is being shutdown.
     *
     * @clientId the client ID of the KVStoreImpl
     */
    public static void clearConfiguration(ClientId clientId) {
        checkNull("clientId", clientId);
        clearClientIdFromServiceMap(clientId, serviceToTimeoutsMap);
        clearClientIdFromServiceMap(clientId, serviceToLocalAddrMap);
        final RMISocketPolicy policy = getRMIPolicy();
        if (policy != null) {
            policy.clearPreparedClient(clientId);
        }
    }

    /**
     * Just a simple struct to hold timeouts.
     */
    private static class SocketTimeouts {
        private final int connectTimeoutMs;
        private final int readTimeoutMs;

        SocketTimeouts(int connectTimeoutMs, int readTimeoutMs) {
            super();
            this.connectTimeoutMs = connectTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
        }
    }

    /**
     * Set a logger to be used by the static TimeoutTask, to report socket read
     * timeouts.
     */
    public static void setTimeoutLogger(Logger logger) {
        ClientSocketFactory.logger = logger;
    }

    /**
     * Set transport information for KVStore client access where the client
     * does not need to manage connections to multiple stores concurrently.
     *
     * @throws IllegalStateException if the configuration is bad
     * @throws IllegalArgumentException if the transport is not supported
     */
    public static void setRMIPolicy(Properties securityProps) {
        setRMIPolicy(securityProps, null, null);
    }

    /**
     * Set transport information for KVStore client access.
     * @throws IllegalStateException if the configuration is bad
     * @throws IllegalArgumentException if the transport is not supported
     */
    public static synchronized void setRMIPolicy(Properties securityProps,
                                                 String storeName,
                                                 ClientId clientId) {
        final String transportName = (securityProps == null) ? null :
            securityProps.getProperty(KVSecurityConstants.TRANSPORT_PROPERTY);

        if ("internal".equals(transportName)) {
            /*
             * INTERNAL transport is a signal that the currently installed
             * transport configuration should be used.
             */
            return;
        }

        sslConfig = null;
        if ("ssl".equals(transportName)) {
            sslConfig = new SSLConfig(securityProps);
            clientPolicy = sslConfig.makeClientSocketPolicy(localDebugLogger);
        } else if (transportName == null || "clear".equals(transportName)) {
            clientPolicy = new ClearSocketPolicy();
        } else {
            throw new IllegalArgumentException(
                "Transport " + transportName + " is not supported.");
        }

        clientPolicy.prepareClient(storeName, clientId);
    }

    /**
     * Set transport information for non-KVStore access.
     */
    public static synchronized void setRMIPolicy(RMISocketPolicy policy) {

        clientPolicy = policy;
        clientPolicy.prepareClient(null, null);
    }

    private static synchronized RMISocketPolicy getRMIPolicy() {

        return clientPolicy;
    }

    public static synchronized RMISocketPolicy ensureRMISocketPolicy() {

        RMISocketPolicy policy = getRMIPolicy();
        if (policy == null) {
            setRMIPolicy(new ClearSocketPolicy());
        }

        return clientPolicy;
    }

    /**
     * Reset RMI socket policy with current SSL configuration in order to
     * reload the entries of keystore and truststore.
     */
    public static synchronized RMISocketPolicy resetRMISocketPolicy(
        String storeName, ClientId clientId, Logger debugLogger) {

        if (sslConfig != null) {
            clientPolicy = sslConfig.makeClientSocketPolicy(debugLogger);
            clientPolicy.prepareClient(storeName, clientId);
        }
        return clientPolicy;
    }

    /**
     * Return an {@link EndpointConfig} object that represents the
     * configuration of sockets created by this factory.
     *
     * @return the configuration
     * @throws IOException if there is a problem creating the configuration
     */
    public final EndpointConfig getEndpointConfig()
        throws IOException {

        return getEndpointConfigBuilder().build();
    }

    /**
     * Returns a new {@link EndpointConfigBuilder} that can be used to create
     * an {@link EndpointConfig} that represents the configuration of sockets
     * created by this factory.
     *
     * @return the configuration
     * @throws IOException if there is a problem creating the configuration
     * builder
     */
    /*
     * Suppress warning about unused IOException, since we need it for
     * subclasses
     */
    @SuppressWarnings("unused")
    public EndpointConfigBuilder getEndpointConfigBuilder()
        throws IOException {

        return getEndpointConfigBuilder(connectTimeoutMs, readTimeoutMs);
    }

    /**
     * Returns a new {@link EndpointConfigBuilder} that can be used to create
     * an {@link EndpointConfig} that reflects the specified connect and read
     * timeouts. The read timeout value is not currently used.
     *
     * @param connectTimeoutMs the socket connect timeout in milliseconds
     * @param readTimeoutMs the socket read timeout in milliseconds
     * @return the configuration
     */
    public static EndpointConfigBuilder
        getEndpointConfigBuilder(int connectTimeoutMs,
                                 int readTimeoutMs)
    {
        /* Socket factories use 0 to mean an infinite timeout */
        if (connectTimeoutMs <= 0) {
            connectTimeoutMs = Integer.MAX_VALUE;
        }

        /*
         * Use the defaults for the heartbeat timeout, heartbeat interval, and
         * idle timeout. The values had previously depended on the read
         * timeout, which in effect represents the request timeout, but it
         * seems better to make them independent. We want to use heartbeats
         * even if the read timeout is long, and it's not clear why they should
         * be shorter if the read timeout is short.
         */
        return new EndpointConfigBuilder()
            .option(AsyncOption.DLG_CONNECT_TIMEOUT, connectTimeoutMs);
    }

    /**
     * Change the timer interval -- for testing.  If the argument is 0, resets
     * to the default interval.
     */
    public static void changeTimerInterval(long intervalMs) {
        synchronized (mutex) {
            if (timeoutTask != null) {
                timeoutTask.cancel();
            }
            defaultTimerIntervalMs = (intervalMs != 0) ?
                                         intervalMs :
                                         TimeoutTask.DEFAULT_TIMER_INTERVAL_MS;
            timeoutTask = new TimeoutTask(defaultTimerIntervalMs);
        }
    }

    /**
     * The TimeoutTask checks all sockets registered with it to ensure that
     * they are active. The period roughly corresponds to a second, although
     * intervening GC activity may expand this period considerably. Note that
     * elapsedMs used for timeouts is always ticked  up in 1 second
     * increments. Thus multiple seconds of real time may correspond to a
     * single second of "timer time" if the system is particularly busy, or the
     * gc has been particularly active.
     *
     * This property allows the underlying timeout implementation to compensate
     * for GC pauses in which activity on the socket at the java level would
     * have been suspended and thus reduces the number of false timeouts.
     *
     * The task maintains a list of all the sockets which it is monitoring.
     * Access and modification of the list are synchronized, which
     * introduces a possible bottleneck and scalability issue if the list
     * becomes large. In that case, a more concurrent data structure could be
     * used.
     * TODO: TimeoutTask is very similar to
     * com.sleepycat.je.rep.impl.node.ChannelTimeoutTask. In the future,
     * contemplate refactoring for common code.
     */
    private static class TimeoutTask extends TimerTask {

        private static final long DEFAULT_TIMER_INTERVAL_MS = 1000L;
        private final long intervalMs;

        /*
         * Elapsed time as measured by the timer task. It's always incremented
         * by intervalMs, which defaults to one second.
         */
        private long elapsedMs = 0;

        /** Creates and schedules the timer task for the specified interval. */
        TimeoutTask(final long intervalMs) {
            this.intervalMs = intervalMs;
            synchronized (mutex) {
                if (timer == null) {
                    timer = new Timer("KVClientSocketTimeout", true);
                }
                timer.schedule(this, intervalMs, intervalMs);
            }
        }

        /**
         * Runs once each interval to check if a socket is still active. Each
         * socket establishes its own timeout period using elapsedMs to check
         * for timeouts. Inactive sockets are removed from the list of
         * registered sockets.
         */
        @Override
        public void run() {
            elapsedMs += intervalMs;
            try {
                synchronized (mutex) {
                    /* If there is no active socket, terminate the timer thread
                     * The socket will be closed when
                     *   1. read timeout
                     *   2. socket idle for 15 seconds
                     *      This default value is controlled by
                     *      sun.rmi.transport.connectionTimeout system property
                     *   3. Other exceptions
                     */
                    if (sockets.isEmpty()) {
                        /*
                         * Terminate timer thread within the run function
                         * guarantees that the ongoing task execution is the
                         * last task execution that will ever be performed by
                         * this timer.
                         */
                        terminateTimer();
                        return;
                    }

                    sockets.removeIf(
                        socket -> !socket.isActive(elapsedMs, logger));
                }
            } catch (Throwable t) {
                /*
                 * The task is executed by a simple Timer, so this catch
                 * attempts to act as a sort of unexpected exception handler.
                 */
                final String message = "ClientSocketFactory.TimerTask: " +
                    CommonLoggerUtils.getStackTrace(t);
                if (logger != null) {
                    logger.severe(message);
                } else if (!EmbeddedMode.isEmbedded()){
                    System.err.println(message);
                }
            }
        }

        /**
         * Registers a socket so that the timer can make periodic calls to
         * isActive(). Note that closing a socket renders it inactive and
         * causes it to be removed from the list by the run()
         * method. Consequently, there is no corresponding unregister
         * operation.
         *
         * Registration will block when the actual timeout check, from the run()
         * method, are executing. Be aware of this potential bottleneck on
         * socket opens.
         *
         * @param socket the socket being registered.
         */
        public void register(TimeoutSocket socket) {
            if ((logger != null) && logger.isLoggable(Level.FINE)) {
                logger.fine("Registering " + socket  +
                            " onto timeout monitoring list. " +
                            sockets.size() + " sockets currently registered");
            }

            synchronized (mutex) {
                sockets.add(socket);
            }
        }
    }

    /**
     * Returns the current client Id.
     */
    public static ClientId getCurrentClientId() {
        return currentClientId.get();
    }

    /**
     * Sets the current client ID.
     *
     * @param clientId the client ID
     */
    public static void setCurrentClientId(ClientId clientId) {
        currentClientId.set(clientId);
    }

    /**
     * Look up a service entry by binding name and client ID.
     */
    private static <V> V getFromServiceMap(String name,
                                           ClientId clientId,
                                           Map<ServiceKey, V> map) {
        V v = map.get(new ServiceKey(name, clientId));
        if (v != null) {
            return v;
        }

        /*
         * Unless both fields are non-null, there is only one key that has at
         * least one non-null field, so we are done.
         */
        if ((name == null) || (clientId == null)) {
            return null;
        }
        /* Client IDs are unique, so look up by client ID before store name */
        v = map.get(new ServiceKey(null, clientId));
        if (v != null) {
            return v;
        }
        return map.get(new ServiceKey(name, null));
    }

    /**
     * Add a service entry by binding name and client ID.
     */
    private static <V> void putIntoServiceMap(String name,
                                              ClientId clientId,
                                              V value,
                                              Map<ServiceKey, V> map) {
        map.put(new ServiceKey(name, clientId), value);
        if ((name != null) && (clientId != null)) {
            map.put(new ServiceKey(name, null), value);
            map.put(new ServiceKey(null, clientId), value);
        }
    }

    /**
     * Clear service entries associated with the specified client ID.
     */
    private static <V> void clearClientIdFromServiceMap(
        ClientId clientId, Map<ServiceKey, V> map) {

        final Iterator<ServiceKey> keys = map.keySet().iterator();
        while (keys.hasNext()) {
            final ServiceKey key = keys.next();
            if (clientId.equals(key.clientId)) {
                keys.remove();
            }
        }
    }

    private static class ServiceKey {
        final String bindingName;
        final ClientId clientId;
        ServiceKey(String bindingName, ClientId clientId) {
            this.bindingName = bindingName;
            this.clientId = clientId;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ServiceKey)) {
                return false;
            }
            final ServiceKey other = (ServiceKey) obj;
            return Objects.equals(bindingName, other.bindingName) &&
                Objects.equals(clientId, other.clientId);
        }
        @Override
        public int hashCode() {
            return Objects.hash(bindingName, clientId);
        }
        @Override
        public String toString() {
            return "CSFServiceKey[" + bindingName + "," + clientId + "]";
        }
    }
}

