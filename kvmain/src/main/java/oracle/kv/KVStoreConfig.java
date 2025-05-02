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

package oracle.kv;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import oracle.kv.impl.api.lob.ChunkConfig;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.lob.KVLargeObject;
import oracle.kv.stats.KVStats;

import com.sleepycat.je.utilint.PropUtil;

/**
 * Represents the configuration parameters used to create a handle to an
 * existing KV store.
 */
public class KVStoreConfig implements Serializable, Cloneable,
                                      KVSecurityConstants {

    private static final long serialVersionUID = 1L;

    /**
     * The default timeout in ms associated with KVStore requests.
     */
    public static final int DEFAULT_REQUEST_TIMEOUT = 5 * 1000;

    /**
     * The default open timeout in ms associated with the sockets used to make
     * KVStore requests.
     */
    public static final int DEFAULT_OPEN_TIMEOUT = 3 * 1000;

    /**
     * The default read timeout in ms associated with the sockets used to make
     * KVStore requests.
     * <p>
     * If the {@linkplain #getUseAsync async network protocol} is disabled, the
     * default read timeout value must be larger than {@link
     * #DEFAULT_REQUEST_TIMEOUT} to ensure that read requests are not timed out
     * by the socket.
     */
    public static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

    /**
     * The default open timeout in ms associated with the sockets used to make
     * registry requests.
     */
    public static final int DEFAULT_REGISTRY_OPEN_TIMEOUT = 3 * 1000;

    /**
     * The default read timeout associated with the sockets used to make
     * registry requests.
     */
    public static final int DEFAULT_REGISTRY_READ_TIMEOUT = 10 * 1000;

    /**
     * The default value in milliseconds for the amount of time to allow for a
     * single roundtrip network communication with the server.
     *
     * @see #getNetworkRoundtripTimeout
     * @since 19.5
     */
    /*
     * TODO: We should reduce this value to a more modest number -- maybe 10 ms
     * -- when we reduce the initial start up time for async connections.
     */
    public static final int DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT = 25;

    private static final Consistency DEFAULT_CONSISTENCY =
        Consistency.NONE_REQUIRED;

    private static final Durability DEFAULT_DURABILITY =
        Durability.COMMIT_NO_SYNC;

    /**
     * The default LOB suffix ({@value}) used to identify keys associated with
     * Large Objects.
     *
     * @since 2.0
     */
    static final String DEFAULT_LOB_SUFFIX = ".lob";

    /**
     * The default number of trailing bytes ({@value}) of a partial LOB that
     * must be verified against the user supplied LOB stream when resuming a
     * LOB <code>put</code> operation.
     *
     * @see KVLargeObject#putLOB
     *
     * @since 2.0
     */
    static final long DEFAULT_LOB_VERIFICATION_BYTES = 1024;

    /**
     * The default timeout value ({@value} ms) associated with internal
     * LOB access during operations on LOBs.
     *
     * @since 2.0
     */
    public static final int DEFAULT_LOB_TIMEOUT = 10000;


    /**
     * Default value for the timeout of cached sequence generator attributes in
     * milliseconds.
     */
    public static int DEFAULT_SG_CACHED_ATTRS_TIMEOUT = 5 * 60 * 1000;

    /**
     * @hidden
     * The number of times the client will attempt to check status for the
     * execution of an asynchronous data definition or administrative statement
     * execution in the face of network connection problems.
     */
    public static final int DEFAULT_MAX_CHECK_RETRIES = 10;

    /**
     * @hidden
     * The cap of interval in milliseconds with which the client will
     * check for the completion of an asynchronous data definition or
     * administrative statement execution.
     */
    public static final int DEFAULT_MAX_CHECK_INTERVAL_MILLIS = 5000;

    /**
     * @hidden
     * Initial delay in milliseconds with which the client will check for the
     * completion of an asynchronous data definition or administrative
     * statement execution.
     */
    public static final int DEFAULT_CHECK_INIT_DELAY_MILLIS = 10;

    /**
     * The name of the system property that controls whether to use the async
     * network protocol by default.
     *
     * @since 19.5
     */
    public static final String USE_ASYNC = "oracle.kv.async";

    /**
     * Whether to use the async network protocol by default.
     *
     * @since 19.5
     */
    public static final boolean DEFAULT_USE_ASYNC = true;

    /**
     * The name of the system property that controls whether to use RMI by
     * default.
     *
     * @hidden For internal use only
     */
    public static final String USE_RMI = "oracle.kv.rmi";

    /**
     * Whether to use RMI by default.
     *
     * @hidden For internal use only
     */
    public static final boolean DEFAULT_USE_RMI = true;

    /**
     * The name of system property that controls whether to exclude tombstones
     * for methods in kvstore api.
     *
     * @hidden
     */
    public static final String EXCLUDE_TOMBSTONES =
        "oracle.kv.exclude.tombstones";

    /**
     * Whether to exclude tombstones for methods in kvstore api by default.
     *
     * @hidden
     */
    public static final boolean DEFAULT_EXCLUDE_TOMBSTONES = true;

    /**
     * The name of the system property that controls whether to enable
     * client-side statistics monitoring by default. If not set, statistics
     * monitoring is disabled by default. If this property is set, the default
     * will be set based on calling {@link Boolean#getBoolean} on the value.
     *
     * @hidden Until statistics monitoring is made public
     */
    public static final String ENABLE_STATS_MONITOR =
        "oracle.kv.stats.enable.stats.monitor";

    /**
     * The default KVStats monitor log interval in milliseconds.
     *
     * @hidden Until statistics monitoring is made public
     */
    public static final long DEFAULT_STATS_MONITOR_LOG_INTERVAL_MILLIS =
        60 * 1000;

    /**
     * The name of the system property that controls whether to enable the table
     * cache by default.
     *
     * @since 20.3
     * @hidden
     */
    public static final String ENABLE_TABLE_CACHE =
            "oracle.kv.table.cache.enable";

    /**
     * Whether to enable the table cache by default.
     *
     * @since 20.3
     * @hidden
     */
    public static final boolean DEFAULT_ENABLE_TABLE_CACHE = true;

    /**
     * The name of the system property that controls whether to enable using the
     * table MD system table by default.
     *
     * @since 23.3
     * @hidden
     */
    public static final String ENABLE_TABLE_SYS_TABLE =
            "oracle.kv.enable.table.systable";

    /**
     * Whether to enable using table MD system table by default.
     *
     * @since 23.3
     * @hidden
     */
    public static final boolean DEFAULT_ENABLE_TABLE_SYS_TABLE = true;

    /* TODO: add bean properties file. */
    /* TODO: add load/save to Properties object. */

    private String storeName;
    private String[] helperHosts;

    /* The current set of merged security properties */
    private Properties securityProps;

    /*
     * The set of security properties provided through a file specified by the
     * oracle.kv.security system property.  These are read at construction time
     * and applied dynamically as an overlay on top of caller-specified
     * properties.
     */
    private final Properties masterSecurityProps;

    /* Socket related properties. */
    private int openTimeout;
    private int readTimeout;
    private InetSocketAddress localAddr;

    private int registryOpenTimeout;
    private int registryReadTimeout;

    private int requestTimeout;
    private int networkRoundtripTimeout;
    private Consistency consistency;
    private Durability durability;

    /* LOB related properties. */
    private long lobVerificationBytes;
    private String lobSuffix;
    private int lobTimeout;

    private RequestLimitConfig requestLimitConfig;

    private ChunkConfig lobConfig = new ChunkConfig();

    /**
     * The zones in which nodes must be located to be used for read operations,
     * or {@code null} if read operations can be performed on nodes in any
     * zone.
     *
     * @since 3.0
     */
    private String[] readZones = null;

    /* Async ddl related timeouts */
    private int maxCheckRetries;
    private int checkIntervalMillis;

    /* The the timeout of cached sequence generator attributes in ms. */
    private int sgAttrsCacheTimeout;

    /**
     * If true, enables the use of the async network protocol for requests on
     * the client side.
     */
    private boolean useAsync = getDefaultUseAsync();

    /**
     * If true, enables the use of RMI for requests on the client side.
     */
    private boolean useRmi = getDefaultUseRmi();

    /**
     * Whether to exclude tombstones for methods in kvstore api.
     */
    private boolean excludeTombstones;

    /* Whether the KVStats monitor is enabled */
    private boolean enableStatsMonitor =
        Boolean.getBoolean(ENABLE_STATS_MONITOR);
    /* The KVStats monitor log interval in milliseconds */
    private long statsMonitorLogIntervalMillis =
        DEFAULT_STATS_MONITOR_LOG_INTERVAL_MILLIS;
    /* The KVStats monitor log callback */
    private StatsMonitorCallback statsMonitorCallback = (kvstats) -> {};

    private boolean enableTableCache =
            (System.getProperty(ENABLE_TABLE_CACHE) == null) ?
                                        DEFAULT_ENABLE_TABLE_CACHE :
                                        Boolean.getBoolean(ENABLE_TABLE_CACHE);


    private boolean enableTableMDSysTable =
            (System.getProperty(ENABLE_TABLE_SYS_TABLE) == null) ?
                    DEFAULT_ENABLE_TABLE_SYS_TABLE :
                    Boolean.getBoolean(ENABLE_TABLE_SYS_TABLE);

    /**
     * Creates a config object with the minimum required properties.
     *
     * @param storeName the name of the KVStore.  The store name is used to
     * guard against accidental use of the wrong host or port.  The store name
     * must consist entirely of upper or lowercase, letters and digits.
     *
     * @param helperHostPort one or more strings containing the host and port
     * of an active node in the KVStore. Each string has the format:
     * hostname:port. It is good practice to pass multiple hosts so that if
     * one host is down, the system will attempt to open the next one, and
     * so on.
     *
     * @throws IllegalArgumentException if an argument has an illegal value.
     * This may be thrown if the
     * {@value oracle.kv.KVSecurityConstants#SECURITY_FILE_PROPERTY} property
     * is set and an error occurs while attempting to read that file.
     */
    public KVStoreConfig(String storeName, String... helperHostPort)
        throws IllegalArgumentException {
        setStoreName(storeName);
        setHelperHosts(helperHostPort);

        openTimeout = DEFAULT_OPEN_TIMEOUT;
        readTimeout = DEFAULT_READ_TIMEOUT;

        registryOpenTimeout = DEFAULT_REGISTRY_OPEN_TIMEOUT;
        registryReadTimeout = DEFAULT_REGISTRY_READ_TIMEOUT;

        requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        networkRoundtripTimeout = DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;

        consistency = DEFAULT_CONSISTENCY;
        durability = DEFAULT_DURABILITY;

        lobVerificationBytes = DEFAULT_LOB_VERIFICATION_BYTES;
        lobSuffix = DEFAULT_LOB_SUFFIX;
        lobTimeout = DEFAULT_LOB_TIMEOUT;

        requestLimitConfig = RequestLimitConfig.getDefault();

        maxCheckRetries = DEFAULT_MAX_CHECK_RETRIES;
        checkIntervalMillis = DEFAULT_MAX_CHECK_INTERVAL_MILLIS;

        masterSecurityProps = readSecurityProps();
        securityProps = mergeSecurityProps(null, masterSecurityProps);

        sgAttrsCacheTimeout = DEFAULT_SG_CACHED_ATTRS_TIMEOUT;

        excludeTombstones =
            (System.getProperty(EXCLUDE_TOMBSTONES) == null) ?
                DEFAULT_EXCLUDE_TOMBSTONES :
                Boolean.getBoolean(EXCLUDE_TOMBSTONES);
    }

    @Override
    public KVStoreConfig clone() {
        try {
            KVStoreConfig clone = (KVStoreConfig) super.clone();
            clone.lobConfig = clone.lobConfig.clone();
            return clone;
        } catch (CloneNotSupportedException neverHappens) {
            return null;
        }
    }

    /**
     * Configures the store name.
     *
     * @param storeName the name of the KVStore.  The store name is used to
     * guard against accidental use of the wrong host or port.  The store name
     * must consist entirely of upper or lowercase, letters and digits.
     *
     * @return this
     */
    public KVStoreConfig setStoreName(String storeName)
        throws IllegalArgumentException {

        if (storeName == null) {
            throw new IllegalArgumentException("Store name may not be null");
        }
        this.storeName = storeName;
        return this;
    }

    /**
     * Returns the store name.
     *
     * <p>If it is not overridden by calling {@link #setStoreName}, the
     * default value is the one specified to the {@link KVStoreConfig}
     * constructor.</p>
     *
     * @return the store name.
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Configures the helper host/port pairs.
     *
     * @param helperHostPort one or more strings containing the host and port
     * of an active node in the KVStore. Each string has the format:
     * hostname:port. It is good practice to pass multiple hosts so that if
     * one host is down, the system will attempt to open the next one, and
     * so on.
     *
     * @return this
     *
     * @throws IllegalArgumentException if no helperHostPort is specified.
     */
    public KVStoreConfig setHelperHosts(String... helperHostPort)
        throws IllegalArgumentException {

        if (helperHostPort.length == 0) {
            throw new IllegalArgumentException("No helperHostPort specified");
        }
        for (final String s : helperHostPort) {
            if (s == null) {
                throw new IllegalArgumentException("helperHostPort is null");
            }
        }
        helperHosts = helperHostPort;
        return this;
    }

    /**
     * Returns the helper host/port pairs.
     *
     * <p>If it is not overridden by calling {@link #setHelperHosts}, the
     * default value is the one specified to the {@link KVStoreConfig}
     * constructor.</p>
     *
     * @return the helper hosts.
     */
    public String[] getHelperHosts() {
        return helperHosts;
    }

    /**
     * Configures the open timeout used when establishing sockets used to make
     * client requests. Shorter timeouts result in more rapid failure detection
     * and recovery. The default open timeout ({@value
     * oracle.kv.KVStoreConfig#DEFAULT_OPEN_TIMEOUT} milliseconds) should be
     * adequate for most applications.
     * <p>
     * The client does not directly open sockets when making requests. KVStore
     * manages the network connections used to make client requests opening and
     * closing connections as needed.
     * <p>
     * Please note that the socket timeout applies to any duplicate KVStore
     * handles to the same KVStore within this process.
     *
     * @param timeout the socket open timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     *
     * @see #DEFAULT_OPEN_TIMEOUT
     *
     * @since 2.0
     */
    public KVStoreConfig setSocketOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        openTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * @deprecated replaced by {@link #setSocketOpenTimeout}
     */
    @Deprecated
    public KVStoreConfig setOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        openTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * Returns the socket open timeout.
     *
     * <p>If it is not overridden by calling {@link #setOpenTimeout}, the
     * default value is {@value oracle.kv.KVStoreConfig#DEFAULT_OPEN_TIMEOUT}
     * milliseconds.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The socket open timeout.
     *
     * @since 2.0
     */
    public long getSocketOpenTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(openTimeout, unit);
    }

    /**
     * @deprecated replaced by {@link #getSocketOpenTimeout}
     */
    @Deprecated
    public long getOpenTimeout(TimeUnit unit) {
        return getSocketOpenTimeout(unit);
    }

    /**
     * Configures the read timeout associated with the underlying sockets used
     * to make client requests. Shorter timeouts result in more rapid failure
     * detection and recovery. If the {@linkplain #getUseAsync async network
     * protocol} is disabled, then this timeout should be sufficiently long so
     * as to allow for the longest timeout associated with a request.
     * <p>
     * The client does not directly manage sockets when making requests.
     * KVStore manages the network connections used to make client requests
     * opening and closing connections as needed.
     * <p>
     * Please note that the socket timeout applies to any duplicate KVStore
     * handles to the same KVStore within this process.
     *
     * @param timeout the socket read timeout
     * @param unit the {@code TimeUnit} of the timeout value
     * @return this
     *
     * @throws IllegalArgumentException if the timeout is invalid
     *
     * @since 2.0
     */
    public KVStoreConfig setSocketReadTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        readTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * Returns the read timeout associated with the sockets used to
     * make requests.
     *
     * <p>If it is not overridden by calling {@link #setSocketReadTimeout}, the
     * default value is {@value oracle.kv.KVStoreConfig#DEFAULT_READ_TIMEOUT}
     * milliseconds.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The socket read timeout
     *
     * @since 2.0
     */
    public long getSocketReadTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(readTimeout, unit);
    }

    /**
     * Configures the local address used to establish connections to the store.
     * Use this method on machines with multiple NICs to restrict traffic to
     * KVStore to a specific NIC, or to chose a more desirable network path.
     *
     * Setting localAddr can be beneficial in the following circumstances:
     *
     * <ol>
     * <li>When there are multiple network paths through the NICs to the
     * store's nodes and the routing rules are insufficient to disambiguate
     * amongst them. Setting the address explicitly ensures the best choice of
     * network path based on knowledge of the overall application behavior and
     * network configuration.
     *
     * <li>To isolate store traffic to a specific NIC, for latency-sensitive
     * applications, for example, when the client is used in the web tier to
     * isolate client/store request response http request traffic.
     * </ol>
     *
     * @param localAddr the local address identifying the NIC. If null, any
     * local address is chosen. Similarly, if the port is zero any free local
     * port (associated with the address) is used to bind the connection.
     *
     * @since 18.3
     */
    public KVStoreConfig setLocalAddress(InetSocketAddress localAddr) {
        this.localAddr = localAddr;
        return this;
    }

    /**
     * Gets the local address. If user does not specify it, returns null
     *
     * @return the host port of local address or null.
     *
     * @since 18.3
     */
    public InetSocketAddress getLocalAddress() {
        return localAddr;
    }

    /**
     * Configures the default request timeout.
     *
     * @param timeout the default request timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     */
    public KVStoreConfig setRequestTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be zero or negative");
        }
        requestTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * Returns the default request timeout.
     *
     * <p>If it is not overridden by calling {@link #setRequestTimeout}, the
     * default value is {@link #DEFAULT_REQUEST_TIMEOUT}.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The transaction timeout.
     */
    public long getRequestTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(requestTimeout, unit);
    }

    /**
     * Configures the amount of time to allow for a single roundtrip network
     * communication with the server.  This value is added to the request
     * timeout to determine the total amount of time that the client should
     * wait for a request to complete before timing out.
     *
     * <p>Specifying a smaller value, or to zero, will reduce the amount of
     * time it takes for the client to report a request that fails to complete
     * within the requested timeout, but increases the chance that the
     * resulting exception will be a {@link RequestTimeoutException} rather
     * than an exception the reflects more specific information about the
     * failure on the server, for example, a {@link ConsistencyException}.
     * Specifying a longer value will increase the chance that the client will
     * be able to throw an exception based on more specific information from
     * the server in case of longer network communication delays.
     *
     * @param timeout the roundtrip network communication timeout
     * @param unit the {@code TimeUnit} of the timeout value
     * @throws IllegalArgumentException if the timeout value is negative
     * @see #setRequestTimeout
     * @since 19.5
    */
    public KVStoreConfig setNetworkRoundtripTimeout(long timeout,
                                                    TimeUnit unit) {

        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout must not be negative");
        }
        networkRoundtripTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * Returns the amount of time to allow for a single roundtrip network
     * communication with the server.  This value is added to the request
     * timeout to determine the total amount of time that the client should
     * wait for a request to complete before timing out.
     *
     * <p>If it is not overridden by calling {@link
     * #setNetworkRoundtripTimeout}, the default value is {@value
     * #DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT}.
     *
     * @param unit the {@code Timeout} to use for the return value
     * @return the network roundtrip timeout
     * @since 19.5
     */
    public long getNetworkRoundtripTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(networkRoundtripTimeout, unit);
    }

    /**
     * Configures the default read Consistency to be used when a Consistency is
     * not specified for a particular read operation.
     *
     * @param consistency the default read Consistency.
     *
     * @return this
     */
    public KVStoreConfig setConsistency(Consistency consistency)
        throws IllegalArgumentException {

        if (consistency == null) {
            throw new IllegalArgumentException("Consistency may not be null");
        }
        this.consistency = consistency;
        return this;
    }

    /**
     * Returns the default read Consistency.
     *
     * <p>If it is not overridden by calling {@link #setConsistency}, the
     * default value is {@link Consistency#NONE_REQUIRED}.</p>
     *
     * @return the default read Consistency.
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Configures the default write Durability to be used when a Durability is
     * not specified for a particular write operation.
     *
     * @param durability the default write Durability.
     *
     * @return this
     */
    public KVStoreConfig setDurability(Durability durability)
        throws IllegalArgumentException {

        if (durability == null) {
            throw new IllegalArgumentException("Durability may not be null");
        }
        this.durability = durability;
        return this;
    }

    /**
     * Returns the default write Durability.
     *
     * <p>If it is not overridden by calling {@link #setDurability}, the
     * default value is {@link Durability#COMMIT_NO_SYNC}.</p>
     *
     * @return the default write Durability.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * Configures the maximum number of requests that can be active for a node
     * in the KVStore. Limiting requests in this way helps minimize the
     * possibility of thread starvation in situations where one or more nodes
     * in the KVStore exhibits long service times and as a result retains
     * threads, making them unavailable to service requests to other reachable
     * and healthy nodes.
     * <p>
     * The long service times can be due to problems at the node itself, or in
     * the network path to that node. The KVStore request dispatcher will,
     * whenever possible, minimize use of nodes with long service times
     * automatically, by re-routing requests to other nodes that can handle
     * them. So this mechanism provides an additional margin of safety when
     * such re-routing of requests is not possible.
     *
     * @return this
     */
    public KVStoreConfig
        setRequestLimit(RequestLimitConfig requestLimitConfig) {

        if (requestLimitConfig == null) {
            throw new IllegalArgumentException
                ("requestLimitConfig may not be null");
        }
        this.requestLimitConfig = requestLimitConfig;
        return this;
    }

    /**
     * Returns the configuration describing how the number of active requests
     * to a node are limited.
     * <p>
     * It returns the default value, documented in {@link RequestLimitConfig},
     * if it was not overridden by calling {@link #setRequestLimit}.
     *
     * @return the request limit configuration
     */
    public RequestLimitConfig getRequestLimit() {
        return requestLimitConfig;
    }

    /**
     * @hidden
     * Configures the connect/open timeout used when making RMI registry
     * lookup requests.
     * <p>
     * Note that the setting of these registry timeouts is global and is not
     * KVS-specific like the other timeouts. The API itself could be static,
     * but that would make it stand out relative to the other timeout apis.
     * Revisit this if we ever make these registry apis public.
     *
     * @param timeout the open timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     */
    public KVStoreConfig setRegistryOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        registryOpenTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * @hidden
     * Returns the socket open timeout associated with sockets used to access
     * an RMI registry.
     *
     * <p>
     * If it is not overridden by calling {@link #setRegistryOpenTimeout}, the
     * default value is {@link #DEFAULT_REGISTRY_OPEN_TIMEOUT}.
     * </p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The open timeout.
     */
    public long getRegistryOpenTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(registryOpenTimeout, unit);
    }

    /**
     * @hidden
     * Configures the read timeout associated with sockets used to make RMI
     * registry requests. Shorter timeouts result in more rapid failure
     * detection and recovery. However, this timeout should be sufficiently
     * long so as to allow for the longest timeout associated with a request.
     * <p>
     * The default value {@link #DEFAULT_REGISTRY_OPEN_TIMEOUT} should
     * be sufficient for most configurations.
     *
     * <p>
     * Note that the setting of these registry timeouts is global and is not
     * KVS-specific like the other timeouts.
     *
     * @param timeout the read timeout
     * @param unit the {@code TimeUnit} of the timeout value.
     * @return this
     *
     * @throws IllegalArgumentException if the timeout is not greater than 0
     */
    public KVStoreConfig setRegistryReadTimeout(long timeout, TimeUnit unit)
            throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        registryReadTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * @hidden
     * Returns the read timeout associated with the sockets used to
     * make RMI registry requests.
     *
     * <p>If it is not overridden by calling {@link #setSocketReadTimeout}, the
     * default value is {@link #DEFAULT_REGISTRY_OPEN_TIMEOUT}.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The open timeout.
     */
    public long getRegistryReadTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(registryReadTimeout, unit);
    }

    /**
     * Returns the default timeout value (in ms) associated with chunk access
     * during operations on LOBs.
     *
     * @see KVLargeObject#getLOB
     * @see KVLargeObject#putLOB
     *
     * @since 2.0
     */
    public long getLOBTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(lobTimeout, unit);
    }

    /**
     * Configures default timeout value associated with chunk access during
     * operations on LOBs.
     *
     * @param timeout the open timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @see KVLargeObject#getLOB
     * @see KVLargeObject#putLOB
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBTimeout(long timeout, TimeUnit unit) {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        lobTimeout = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * Returns the default suffix associated with LOB keys.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public String getLOBSuffix() {
        return lobSuffix;
    }

    /**
     *
     * Configures the default suffix associated with LOB keys. The application
     * must ensure that the suffix is used consistently across all KVStore
     * handles. The suffix is used by the KVStore APIs to ensure that any
     * value-changing non-LOB APIs are not invoked on LOB objects and vice
     * versa. Failure to use the LOB suffix consistently can result in
     * <code>IllegalArgumentException</code> being thrown if a LOB object is
     * created with one LOB suffix and is subsequently accessed
     * when a different suffix is in effect. Write operations using
     * inconsistent LOB suffixes may result in LOB storage not being reclaimed.
     *
     * <p>
     * This method should only be used in the rare case that the default LOB
     * suffix value ".lob" is unsuitable.
     *
     * @param lobSuffix the LOB suffix string. It must be non-null and
     * have a length &gt; 0.
     * @return this
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBSuffix(String lobSuffix) {

        if ((lobSuffix == null) || (lobSuffix.length() == 0)) {
            throw new IllegalArgumentException
                ("The lobSuffix argument must be non-null " +
                    "and have a length > 0 ");
        }
        this.lobSuffix = lobSuffix;
        return this;
    }

    /**
     * Returns the number of trailing bytes of a partial LOB that must be
     * verified against the user supplied LOB stream when resuming a LOB
     * <code>put</code> operation.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public long getLOBVerificationBytes() {
        return lobVerificationBytes;
    }

    /**
     * Configures the number of trailing bytes of a partial LOB that must be
     * verified against the user supplied LOB stream when resuming a
     * <code>putLOB</code> operation.
     *
     * @param lobVerificationBytes the number of bytes to be verified. A value
     * &lt;=0 disables verification.
     *
     * @return this
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBVerificationBytes(long lobVerificationBytes) {

        if (lobVerificationBytes < 0) {
            throw new IllegalArgumentException("lobVerificationBytes: " +
                                                lobVerificationBytes);
        }
        this.lobVerificationBytes = lobVerificationBytes;
        return this;
    }

    /**
     * @hidden
     * Returns the number of contiguous chunks that can be stored in the same
     * partition for a given LOB.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public int getLOBChunksPerPartition() {
        return lobConfig.getChunksPerPartition();
    }

    /**
     * @hidden
     *
     * Configures the number of contiguous chunks that can be stored in the
     * same partition for a given LOB.
     *
     * @param lobChunksPerPartition the number of partitions
     *
     * @return this
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBChunksPerPartition(int lobChunksPerPartition) {

        if (lobChunksPerPartition <= 0) {
            throw new IllegalArgumentException("lobChunksPerPartition: " +
                                               lobChunksPerPartition);
        }
        lobConfig.setChunksPerPartition(lobChunksPerPartition);
        return this;
    }

    /**
     * @hidden
     * Returns the chunk size associated with the chunks used to store a LOB.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public int getLOBChunkSize() {
        return lobConfig.getChunkSize();
    }

    /**
     * @hidden
     * Configures the chunk size associated with the chunks used to store a LOB.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBChunkSize(int chunkSize) {

        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize: " + chunkSize);
        }
        lobConfig.setChunkSize(chunkSize);
        return this;
    }

    /**
     * Returns the zones in which nodes must be located to be used for read
     * operations, or {@code null} if read operations can be performed on nodes
     * in any zone.
     *
     * @return the zones or {@code null}
     * @since 3.0
     */
    public String[] getReadZones() {
        return readZones == null ?
            null :
            Arrays.copyOf(readZones, readZones.length);
    }

    /**
     * Sets the zones in which nodes must be located to be used for read
     * operations.  If the argument is {@code null}, or this method has not
     * been called, then read operations can be performed on nodes in any zone.
     *
     * <p>The specified zones must exist at the time that this configuration
     * object is used to create a store, or else {@link
     * KVStoreFactory#getStore} will throw an {@link IllegalArgumentException}.
     *
     * <p>Zones specified for read operations can include primary and secondary
     * zones.  If the master is not located in any of the specified zones,
     * either because the zones are all secondary zones or because the master
     * node is not currently in one of the specified primary zones, then read
     * operations with {@link Consistency#ABSOLUTE} will fail.
     *
     * @param zones the zones or {@code null}
     * @return this
     * @throws IllegalArgumentException if the array argument is not {@code
     * null} and is either empty or contains {@code null} or duplicate elements
     * @since 3.0
     */
    public KVStoreConfig setReadZones(final String... zones) {

        if (zones == null) {
            readZones = null;
        } else if (zones.length == 0) {
            throw new IllegalArgumentException(
                "The zones argument must not be empty");
        } else {
            final String[] copy = Arrays.copyOf(zones, zones.length);
            for (int i = 0; i < copy.length; i++) {
                final String zone = copy[i];
                if (zone == null) {
                    throw new IllegalArgumentException(
                        "The zones argument must not contain null elements");
                }
                for (int j = i + 1; j < copy.length; j++) {
                    if (zone.equals(copy[j])) {
                        throw new IllegalArgumentException(
                            "The zones argument must not contain" +
                            " duplicate elements; found multiple copies of '" +
                            zone + "'");
                    }
                }
            }
            readZones = copy;
        }
        return this;
    }

    /**
     * @hidden
     * Configures the default interval for checking on data definition
     * operation progress.
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     */
    public KVStoreConfig setCheckInterval(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be zero or negative");
        }
        checkIntervalMillis = PropUtil.durationToMillis(timeout, unit);
        return this;
    }

    /**
     * @hidden
     * Returns the default interval for checking on the progress of async
     * data definition or administrative operations.
     */
    public long getCheckInterval(TimeUnit unit) {
        return PropUtil.millisToDuration(checkIntervalMillis, unit);
    }

    /**
     * @hidden
     * Get the number of times the client will attempt to check status for the
     * execution of an asynchronous data definition or administrative statement
     * execution in the face of network connection problems.
     */
    public int getMaxCheckRetries() {
        return maxCheckRetries;
    }

    /**
     * @hidden
     * Set the number of times the client will attempt to check status for the
     * execution of an asynchronous data definition or administrative statement
     * execution in the face of network connection problems.
     */
    public KVStoreConfig setMaxCheckRetries(int maxCheckRetries) {

        if (maxCheckRetries <= 0) {
            throw new IllegalArgumentException("maxCheckRetries: " +
                                               maxCheckRetries);
        }
        this.maxCheckRetries = maxCheckRetries;
        return this;
    }

    /**
     * Whether calls to the store should use the async network protocol.
     *
     * <p>If it is not overridden by calling {@link #setUseAsync}, the default
     * value is {@value #DEFAULT_USE_ASYNC}, unless the {@link #USE_ASYNC}
     * system property is set, in which case that value is parsed by calling
     * {@link Boolean#getBoolean}.
     *
     * @since 19.5
     */
    public boolean getUseAsync() {
        return useAsync;
    }

    /**
     * Specifies whether calls to the store should use the async network
     * protocol.
     *
     * @since 19.5
     */
    public KVStoreConfig setUseAsync(boolean useAsync) {
        this.useAsync = useAsync;
        return this;
    }

    /**
     * Returns the default for whether to use the async network protocol.
     *
     * @hidden For internal use only
     */
    public static boolean getDefaultUseAsync() {
        return (System.getProperty(USE_ASYNC) == null) ?
            DEFAULT_USE_ASYNC :
            Boolean.getBoolean(USE_ASYNC);
    }

    /**
     * Whether calls to the store should try using RMI. Calls will favor async
     * if enabled, and will use RMI regardless of this setting if async is
     * disabled.
     *
     * <p>If it is not overridden by calling {@link #setUseRmi}, the default
     * value is {@value #DEFAULT_USE_RMI}, unless the {@link #USE_RMI} system
     * property is set, in which case that value is parsed by calling {@link
     * Boolean#getBoolean}.
     *
     * @hidden For internal use only
     */
    public boolean getUseRmi() {
        return useRmi;
    }

    /**
     * Specifies whether calls to the store should use RMI.
     *
     * @hidden For internal use only
     */
    public KVStoreConfig setUseRmi(boolean useRmi) {
        this.useRmi = useRmi;
        return this;
    }

    /**
     * Returns the default for whether to use RMI.
     *
     * @hidden For internal use only
     */
    public static boolean getDefaultUseRmi() {
        return (System.getProperty(USE_RMI) == null) ?
            DEFAULT_USE_RMI :
            Boolean.getBoolean(USE_RMI);
    }

    /**
     * Whether to exclude tombstones for methods in kvstore api.
     *
     * <p>If it is not overridden by calling
     * {@link #setExcludeTombstones}, the default value is
     * {@value #DEFAULT_EXCLUDE_TOMBSTONES}, unless the
     * {@link #EXCLUDE_TOMBSTONES} system property is set, in which
     * case that value is parsed by calling {@link Boolean#getBoolean}.
     *
     * @hidden
     */
    public boolean getExcludeTombstones() {
        return excludeTombstones;
    }

    /**
     * Specifies whether to return tombstones for methods in kvstore api.
     *
     * @hidden
     */
    public KVStoreConfig setExcludeTombstones(boolean excludeTombstones) {
        this.excludeTombstones = excludeTombstones;
        return this;
    }

    /**
     * Whether the table cache is enabled.
     *
     * <p>If it is not overridden by calling {@link #setEnableTableCache},
     * the default value is {@value #DEFAULT_ENABLE_TABLE_CACHE}, unless the
     * {@link #ENABLE_TABLE_CACHE} system property is set, in which case that
     * value is parsed by calling {@link Boolean#getBoolean}.
     *
     * @since 20.3
     * @hidden
     */
    public boolean getEnableTableCache() {
        return enableTableCache;
    }

    /**
     * Specifies whether the table cache is enabled.
     *
     * @since 20.3
     * @hidden
     */
    public KVStoreConfig setEnableTableCache(boolean enableTableCache) {
        this.enableTableCache = enableTableCache;
        return this;
    }

    public boolean getEnableTableMDSysTable() {
        return enableTableMDSysTable;
    }

    @Override
    public String toString() {
        return "<KVStoreConfig" +
               " storeName=" + storeName +
               " helperHosts=" + Arrays.toString(helperHosts) +
               " requestTimeout=" + requestTimeout +
               " consistency=" + consistency +
               " durability=" + durability +
               " lobSuffix=" + lobSuffix +
               " lobVerificationBytes=" + lobVerificationBytes +
               " lobTimeout=" + lobTimeout +
               ((readZones != null) ?
                " readZones=" + Arrays.toString(readZones) :
                "") +
               " useAsync=" + useAsync +
               ((useRmi != DEFAULT_USE_RMI) ?
                " useRmi=" + useRmi :
                "") +
               " enableStatsMonitor=" + enableStatsMonitor +
               ">";
    }

    /**
     * Configures security properties for the client. The supported properties
     * include both authentication properties and transport properties.
     * See {@link KVSecurityConstants} for the supported properties.
     * @return this
     */
    public KVStoreConfig setSecurityProperties(Properties securityProps) {

        this.securityProps = mergeSecurityProps(securityProps,
                                                masterSecurityProps);
        return this;
    }

    /**
     * Returns a copy of the current security properties. This reflects both the
     * properties explicitly set through setSecurityProperties as well as any
     * properties obtained from a security property file. Changes to the
     * returned object have no effect on configuration settings.
     *
     * @return the current security properties
     */
    public Properties getSecurityProperties() {
        return (Properties) securityProps.clone();
    }

    /**
     * Merge the masterSecurityProperties with the input properties.
     *
     * @return a Properties object, with the settings from masterSecurityProps
     * overriding values from newSecurityProps. The caller must ensure that the
     * returned value is not modified, as it could be the master security
     * properties object that was read at construction time.
     */
    private static Properties mergeSecurityProps(
        Properties newSecurityProps,
        Properties masterSecurityProps) {

        if (newSecurityProps == null)  {
            if (masterSecurityProps == null) {
                /* If both are null, return an empty property set */
                return new Properties();
            }

            /* The (internal) caller is warned to not modify this */
            return masterSecurityProps;
        }

        final Properties result = (Properties) newSecurityProps.clone();

        if (masterSecurityProps != null) {
            for (String propName : masterSecurityProps.stringPropertyNames()) {
                final String propVal =
                    masterSecurityProps.getProperty(propName);
                result.setProperty(propName, propVal);
            }
        }

        return result;
    }

    /**
     * Read security properties from a configured property file.
     */
    private static Properties readSecurityProps()
        throws IllegalArgumentException {

        final String securityFile =
            System.getProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        if (securityFile == null) {
            return null;
        }

        try {
            return KVStoreLogin.createSecurityProperties(securityFile);
        } catch (IllegalStateException ise) {
            throw new IllegalArgumentException(
                "An error was encountered while processing the security " +
                "property file " + securityFile, ise);
        }
    }

    /**
     * Sets the timeout of cached sequence generator attributes in
     * milliseconds.
     */
    public KVStoreConfig setSGAttrsCacheTimeout(int sgAttrsCacheTimeout) {
        this.sgAttrsCacheTimeout = sgAttrsCacheTimeout;
        return this;
    }

    /**
     * Gets the timeout of cached sequence generator attributes in
     * milliseconds.
     */
    public int getSGAttrsCacheTimeout() {
        return sgAttrsCacheTimeout;
    }

    /**
     * Specifies whether to enable statistics monitoring.
     *
     * <p>If enabled, the client-side stats (including request latency, node
     * stats, etc.) will be logged through a logger with the name
     * oracle.kv.KVStats.&lt;clientID&gt;, where the &lt;clientID&gt; is a
     * random client ID. The logger can be controlled under the prefix
     * oracle.kv.KVStats using the java logging configuration facility (see
     * {@link java.util.logging} for more information).
     *
     * @param value whether to enable statistics monitoring
     * @hidden Until statistics monitoring is made public
     */
    public KVStoreConfig setEnableStatsMonitor(boolean value) {
        this.enableStatsMonitor = value;
        return this;
    }

    /**
     * Returns whether to enable statistics monitoring.
     *
     * <p>Monitoring, once enabled, will periodically log the {@link KVStats}.
     *
     * <p>If it is not overridden by calling {@link #setEnableStatsMonitor},
     * statistics monitoring will be disabled by default unless the {@link
     * #ENABLE_STATS_MONITOR} system property is set, in which case that value
     * is parsed by calling {@link Boolean#getBoolean}.
     *
     * @hidden Until statistics monitoring is made public
     */
    public boolean getEnableStatsMonitor() {
        return enableStatsMonitor;
    }

    /**
     * Sets the KVStats monitor log interval in milliseconds. The default value
     * is {@value #DEFAULT_STATS_MONITOR_LOG_INTERVAL_MILLIS}. The KVStats
     * monitor must be {@link #setEnableStatsMonitor enabled} for this
     * configuration to have effect.
     *
     * @hidden Until statistics monitoring is made public
     */
    public KVStoreConfig setStatsMonitorLogIntervalMillis(long value) {
        this.statsMonitorLogIntervalMillis = value;
        return this;
    }

    /**
     * Returns the KVStats monitor log interval in milliseconds
     *
     * @hidden Until statistics monitoring is made public
     */
    public long getStatsMonitorLogIntervalMillis() {
        return statsMonitorLogIntervalMillis;
    }

    /**
     * Sets the KVStats monitor callback which is invoked every time the
     * monitor has obtained and logged the stats. The KVStats monitor
     * must be {@link #setEnableStatsMonitor enabled} for this configuration
     * to have effect.
     *
     * @hidden Until statistics monitoring is made public
     */
    public KVStoreConfig setStatsMonitorCallback(
        StatsMonitorCallback callback) {
        this.statsMonitorCallback = callback;
        return this;
    }

    /**
     * Returns the KVStats monitor callback.
     *
     * @hidden Until statistics monitoring is made public
     */
    public StatsMonitorCallback getStatsMonitorCallback() {
        return statsMonitorCallback;
    }

    /**
     * A stats monitor callback.
     *
     * @hidden Until statistics monitoring is made public
     */
    public interface StatsMonitorCallback
            extends Consumer<KVStats>, Serializable {
    }
}
