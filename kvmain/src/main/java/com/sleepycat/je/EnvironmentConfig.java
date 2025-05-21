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

package com.sleepycat.je;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.config.LongDurationConfigParam;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.util.DbCacheSize;
import com.sleepycat.je.util.DbVerify;
import com.sleepycat.je.util.DbVerifyLog;
import com.sleepycat.je.utilint.TaskCoordinator;

/**
 * Specifies the attributes of an environment.
 *
 * <p>To change the default settings for a database environment, an application
 * creates a configuration object, customizes settings and uses it for
 * environment construction. The set methods of this class validate the
 * configuration values when the method is invoked.  An
 * IllegalArgumentException is thrown if the value is not valid for that
 * attribute.</p>
 *
 * <p>Most parameters are described by the parameter name String constants in
 * this class. These parameters can be specified or individually by calling
 * {@link #setConfigParam}, through a Properties object passed to {@link
 * #EnvironmentConfig(Properties)}, or via properties in the je.properties
 * files located in the environment home directory.</p>
 *
 * <p>For example, an application can change the default btree node size
 * with:</p>
 *
 * <pre>
 *     envConfig.setConfigParam(EnvironmentConfig.LOCK_TIMEOUT, "250 ms");
 * </pre>
 *
 * <p>Some commonly used environment attributes have convenience setter/getter
 * methods defined in this class.  For example, to change the default
 * lock timeout setting for an environment, the application can instead do
 * the following:</p>
 * <pre class=code>
 *     // customize an environment configuration
 *     EnvironmentConfig envConfig = new EnvironmentConfig();
 *     // will throw if timeout value is invalid
 *     envConfig.setLockTimeout(250, TimeUnit.MILLISECONDS);
 *     // Open the environment using this configuration.
 *     Environment myEnvironment = new Environment(home, envConfig);
 * </pre>
 *
 * <p>Parameter values are applied using this order of precedence:</p>
 * <ol>
 *     <li>Configuration parameters specified in je.properties take first
 *      precedence.</li>
 *     <li>Configuration parameters set in the EnvironmentConfig object used at
 *     Environment construction are next.</li>
 *     <li>Any configuration parameters not set by the application are set to
 *     system defaults, described along with the parameter name String
 *     constants in this class.</li>
 * </ol>
 *
 * <p>However, a small number of parameters do not have string constants in
 * this class, and cannot be set using {@link #setConfigParam}, a Properties
 * object, or the je.properties file. These parameters can only be changed
 * via the following setter methods:</p>
 * <ul>
 *     <li>{@link #setAllowCreate}</li>
 *     <li>{@link #setCacheMode}</li>
 *     <li>{@link #setClassLoader}</li>
 *     <li>{@link #setCustomStats}</li>
 *     <li>{@link #setExceptionListener}</li>
 *     <li>{@link #setLoggingHandler}</li>
 *     <li>{@link #setNodeName}</li>
 *     <li>{@link #setRecoveryProgressListener}</li>
 * </ul>
 *
 * <p>An EnvironmentConfig can be used to specify both mutable and immutable
 * environment properties.  Immutable properties may be specified when the
 * first Environment handle (instance) is opened for a given physical
 * environment.  When more handles are opened for the same environment, the
 * following rules apply:</p>
 * <ol>
 *     <li>Immutable properties must equal the original values specified when
 * constructing an Environment handle for an already open environment.  When a
 * mismatch occurs, an exception is thrown.</li>
 *     <li>Mutable properties are ignored when constructing an Environment handle
 * for an already open environment.</li>
 * </ol>
 *
 * <p>After an Environment has been constructed, its mutable properties may be
 * changed using {@link Environment#setMutableConfig}.  See {@link
 * EnvironmentMutableConfig} for a list of mutable properties; all other
 * properties are immutable.  Whether a property is mutable or immutable is
 * also described along with the parameter name String constants in this
 * class.</p>
 *
 * <h2>Getting the Current Environment Properties</h2>
 *
 * To get the current "live" properties of an environment after constructing it
 * or changing its properties, you must call {@link Environment#getConfig} or
 * {@link Environment#getMutableConfig}.  The original EnvironmentConfig or
 * EnvironmentMutableConfig object used to set the properties is not kept up to
 * date as properties are changed, and does not reflect property validation or
 * properties that are computed.
 *
 * <h3><a id="timeDuration">Time Duration Properties</a></h3>
 *
 * <p>Several environment and transaction configuration properties are time
 * durations.  For these properties, a time unit is specified along with an
 * integer duration value.</p>
 *
 * <p>When specific setter and getter methods exist for a time duration
 * property, these methods have a {@link TimeUnit} argument.  Examples are
 * {@link #setLockTimeout(long,TimeUnit)} and {@link
 * #getLockTimeout(TimeUnit)}.  Note that the {@link TimeUnit} argument may
 * be null only when the duration value is zero; there is no default unit that
 * is used when null is specified.</p>
 *
 * <p>When a time duration is specified as a string value, the following format
 * is used.</p>
 *
 * <pre>   {@code <value> [ <whitespace> <unit> ]}</pre>
 *
 * <p>The {@code <value>} is an integer.  The {@code <unit>} name, if present,
 * must be preceded by one or more spaces or tabs.</p>
 *
 * <p>The following {@code <unit>} names are allowed.  Both {@link TimeUnit}
 * names and IEEE standard abbreviations are allowed.  Unit names are case
 * insensitive.</p>
 *
 * <table border="1">
 * <caption>Time unit abbreviations, names, and definitions</caption>
 * <tr><th>IEEE abbreviation</th>
 *     <th>TimeUnit name</th>
 *     <th>Definition</th>
 * </tr>
 * <tr><td>{@code ns}</td>
 *     <td>{@code NANOSECONDS}</td>
 *     <td>one billionth (10<sup>-9</sup>) of a second</td>
 * </tr>
 * <tr><td>{@code us}</td>
 *     <td>{@code MICROSECONDS}</td>
 *     <td>one millionth (10<sup>-6</sup>) of a second</td>
 * </tr>
 * <tr><td>{@code ms}</td>
 *     <td>{@code MILLISECONDS}</td>
 *     <td>one thousandth (10<sup>-3</sup>) of a second</td>
 * </tr>
 * <tr><td>{@code s}</td>
 *     <td>{@code SECONDS}</td>
 *     <td>1 second</td>
 * </tr>
 * <tr><td>{@code min}</td>
 *     <td>{@code MINUTES}</td>
 *     <td>60 seconds</td>
 * </tr>
 * <tr><td>{@code h}</td>
 *     <td>{@code HOURS}</td>
 *     <td>60 minutes</td>
 * </tr>
 * <tr><td>&nbsp;</td>
 *     <td>{@code DAYS}</td>
 *     <td>24 hours</td>
 * </tr>
 * </table>
 *
 * <p>Examples are:</p>
 * <pre>
 * 3 seconds
 * 3 s
 * 500 ms
 * 1000000 (microseconds is implied)
 * </pre>
 *
 * <p> For most config options the maximum duration value is currently
 * Integer.MAX_VALUE milliseconds. This translates to almost 25 days
 * (2147483647999999 ns, 2147483647999 us, 2147483647 ms, 2147483 s, 35791 min,
 * 596 h).</p>
 *
 * <p> For rare cases like erasure time period, where this is not sufficient,
 * maximum supported duration value is Long.MAX_VALUE milliseconds (using
 * {@link LongDurationConfigParam}).
 * </p>
 *
 * <p>Note that when the {@code <unit>} is omitted, microseconds is implied.
 * This default is supported for compatibility with JE 3.3 and earlier.  In JE
 * 3.3 and earlier, explicit time units were not used and durations were always
 * implicitly specified in microseconds.</p>
 */
public class EnvironmentConfig extends EnvironmentMutableConfig {
    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     * For internal use, to allow null as a valid value for the config
     * parameter.
     */
    public static final EnvironmentConfig DEFAULT = new EnvironmentConfig();

    /**
     * Configures the JE cache size in bytes.
     *
     * <p>Either MAX_MEMORY or MAX_MEMORY_PERCENT may be used to configure the
     * cache size. When MAX_MEMORY is zero (its default value),
     * MAX_MEMORY_PERCENT determines the cache size. See
     * {@link #MAX_MEMORY_PERCENT} for more information.</p>
     *
     * <p>When using MAX_MEMORY, take care to ensure that the overhead
     * of the JVM does not leave less free space in the heap than intended.
     * Some JVMs have more overhead than others, and some JVMs allocate their
     * overhead within the specified heap size (the -Xmx value). To be sure
     * that enough free space is available, use MAX_MEMORY_PERCENT rather than
     * MAX_MEMORY.</p>
     *
     * <p>When using the Oracle NoSQL DB product</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>-none-</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #setCacheSize
     * @see #MAX_MEMORY_PERCENT
     *
     * @see <a href="EnvironmentStats.html#cacheSizing">Cache Statistics:
     * Sizing</a>
     */
    public static final String MAX_MEMORY = "je.maxMemory";

    /**
     * Configures the JE cache size as a percentage of the JVM maximum
     * memory.
     *
     * <p>The system will evict database objects when it comes within a
     * prescribed margin of the limit.</p>
     *
     * <p>By default, JE sets the cache size to:</p>
     *
     * <pre>
     *         (MAX_MEMORY_PERCENT *  JVM maximum memory) / 100
     * </pre>
     *
     * <p>where JVM maximum memory is specified by the JVM -Xmx flag. Note that
     * the actual heap size may be somewhat less, depending on JVM overheads.
     * The value used in the calculation above is the actual heap size as
     * returned by {@link Runtime#maxMemory()}.</p>
     *
     * <p>The above calculation applies when {@link #MAX_MEMORY} is zero, which
     * is its default value. Setting MAX_MEMORY to a non-zero value overrides
     * the percentage based calculation and sets the cache size explicitly.</p>
     *
     * <p>The following details apply to setting the cache size to a percentage
     * of the JVM heap size byte size (this parameter) as well as to a byte
     * size ({@link #MAX_MEMORY}</p>
     *
     * <p>If {@link #SHARED_CACHE} is set to true, MAX_MEMORY and
     * MAX_MEMORY_PERCENT specify the total size of the shared cache, and
     * changing these parameters will change the size of the shared cache. New
     * environments that join the cache may alter the cache size if their
     * configuration uses a different cache size parameter.</p>
     *
     * <p>The size of the cache is often directly proportional to operation
     * performance. See <a href="EnvironmentStats.html#cache">Cache
     * Statistics</a> for information on understanding and monitoring the
     * cache. It is strongly recommended that the cache is large enough to
     * hold all INs. See {@link DbCacheSize} for information on sizing the
     * cache.</p>
     *
     * <p>To take full advantage of JE cache memory, it is strongly recommended
     * that
     * <a href="http://download.oracle.com/javase/7/docs/technotes/guides/vm/performance-enhancements-7.html#compressedOop">compressed oops</a>
     * (<code>-XX:+UseCompressedOops</code>) is specified when a 64-bit JVM is
     * used and the maximum heap size is less than 32 GB.  As described in the
     * referenced documentation, compressed oops is sometimes the default JVM
     * mode even when it is not explicitly specified in the Java command.
     * However, if compressed oops is desired then it <em>must</em> be
     * explicitly specified in the Java command when running DbCacheSize or a
     * JE application.  If it is not explicitly specified then JE will not
     * aware of it, even if it is the JVM default setting, and will not take it
     * into account when calculating cache memory sizes.</p>
     *
     * <p>Note that log write buffers may be flushed to disk if the cache size
     * is changed after the environment has been opened.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>60</td>
     * <td>1</td>
     * <td>90</td>
     * </tr>
     * </table>
     *
     * @see #setCachePercent
     * @see #MAX_MEMORY
     *
     * @see <a href="EnvironmentStats.html#cacheSizing">Cache Statistics:
     * Sizing</a>
     */
    public static final String MAX_MEMORY_PERCENT = "je.maxMemoryPercent";

    /**
     * If true, the shared cache is used by this environment.
     *
     * <p>By default this parameter is false and this environment uses a
     * private cache.  If this parameter is set to true, this environment will
     * use a cache that is shared with all other open environments in this
     * process that also set this parameter to true.  There is a single shared
     * cache per process.</p>
     *
     * <p>By using the shared cache, multiple open environments will make
     * better use of memory because the cache LRU algorithm is applied across
     * all information in all environments sharing the cache.  For example, if
     * one environment is open but not recently used, then it will only use a
     * small portion of the cache, leaving the rest of the cache for
     * environments that have been recently used.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @see #setSharedCache
     *
     * @see <a href="EnvironmentStats.html#cacheSizing">Cache Statistics:
     * Sizing</a>
     */
    public static final String SHARED_CACHE = "je.sharedCache";

    /**
     * An upper limit on the number of bytes used for data storage. Works
     * with {@link #FREE_DISK} to define the storage limit. If the limit is
     * exceeded, write operations will be prohibited.
     * <p>
     * If set to zero (the default), no usage limit is enforced, meaning that
     * all space on the storage volume, minus {@link #FREE_DISK}, may be used.
     * If MAX_DISK is non-zero, FREE_DISK is subtracted from MAX_DISK to
     * determine the usage threshold for prohibiting write operations. If
     * multiple JE environments share the same storage volume, setting MAX_DISK
     * to a non-zero value is strongly recommended.
     *
     *   <p style="margin-left: 2em">Note: An exception to the rule above is
     *   when MAX_DISK is less than or equal to 10GB and FREE_DISK is not
     *   explicitly specified. See {@link #FREE_DISK} more information.</p>
     *
     * Both the FREE_DISK and MAX_DISK thresholds (if configured) are checked
     * during a write operation. If either threshold is crossed, the behavior
     * of the JE environment is as follows:
     * <ul>
     *     <li>
     *     Application write operations will throw {@link DiskLimitException}.
     *     DiskLimitException extends {@link OperationFailureException} and
     *     will invalidate the transaction, but will not invalidate the
     *     environment. Read operations may continue even when write operations
     *     are prohibited.
     *     </li>
     *     <li>
     *     {@link Environment#checkpoint} and {@link Environment#sync} will
     *     throw DiskLimitException.
     *     </li>
     *     <li>
     *     {@link Environment#close} may throw DiskLimitException when a final
     *     checkpoint is performed. However, the environment will be properly
     *     closed in other respects.
     *     </li>
     *     <li>
     *     The JE evictor will not log dirty nodes when the cache overflows
     *     and therefore dirty nodes cannot be evicted from cache. So
     *     although read operations are allowed, cache thrashing may occur if
     *     all INs do not fit in cache as {@link DbCacheSize recommended}.
     *     </li>
     *     <li>
     *     In an HA environment a disk limit may be violated on a replica node
     *     but not the master node. In this case, a DiskLimitException will not
     *     be thrown by a write operation on the master node. Instead,
     *     {@link com.sleepycat.je.rep.InsufficientAcksException} or
     *     {@link com.sleepycat.je.rep.InsufficientReplicasException} will be
     *     thrown if the {@link Durability#getReplicaAck() ack requirements}
     *     are not met.
     *     </li>
     * </ul>
     * <p>
     * JE uses a log structured storage system where data files often become
     * gradually obsolete over time (see {@link #CLEANER_MIN_UTILIZATION}). The
     * JE cleaner is responsible for reclaiming obsolete space by cleaning and
     * deleting data files. In a standalone (non-HA) environment, data files
     * are normally deleted quickly after being cleaned, but may be reserved
     * and protected temporarily by a {@link com.sleepycat.je.util.DbBackup},
     * for example. These reserved files will be deleted as soon as they are
     * no longer protected.
     * <p>
     * In an HA environment, JE will retain as many reserved files as possible
     * to support replication to nodes that are out of contact. All cleaned
     * files are reserved (not deleted) until approaching a disk limit, at
     * which time they are deleted, as long as they are not protected.
     * Reserved files are protected when they are needed for
     * replication to active nodes or for feeding an active network restore.
     * <p>
     * For more information on reserved and protected data files, see
     * {@link EnvironmentStats#getActiveLogSize()},
     * {@link EnvironmentStats#getReservedLogSize()},
     * {@link EnvironmentStats#getProtectedLogSize()},
     * {@link EnvironmentStats#getProtectedLogSizeMap()},
     * {@link EnvironmentStats#getAvailableLogSize()} and
     * {@link EnvironmentStats#getTotalLogSize}.
     * <p>
     * When multiple JE environments share the same storage volume, the
     * FREE_DISK amount will be maintained for each environment. The following
     * scenario illustrates use of a single shared volume with capacity 300GB:
     * <ul>
     *     <li>
     *     JE-1 and JE-2 each have MAX_DISK=100GB and FREE_DISK=5GB,
     *     </li>
     *     <li>
     *     100GB is used for fixed miscellaneous storage.
     *     </li>
     * </ul>
     * <p>
     * Each JE environment will use no more than 95GB each, so at least 10GB
     * will remain free overall. In other words, if both JE environments reach
     * their threshold and write operations are prohibited, each JE environment
     * will have 5GB of free space for recovery (10GB total).
     * <p>
     * On the other hand, when an external service is also consuming disk
     * space and its usage of disk space is variable over time, the situation
     * is more complex and JE cannot always guarantee that FREE_DISK is
     * honored. The following scenario includes multiple JE environments as
     * well an external service, all sharing a 300GB volume.
     * <ul>
     *     <li>
     *     JE-1 and JE-2 each have MAX_DISK=100GB and FREE_DISK=5GB,
     *     </li>
     *     <li>
     *     an external service is expected to use up to 50GB, and
     *     </li>
     *     <li>
     *     50GB is used for fixed miscellaneous storage.
     *     </li>
     * </ul>
     * <p>
     * Assuming that the external service stays within its 50GB limit then, as
     * the previous example, each JE environment will normally use no more than
     * 95GB each, and at least 10GB will remain free overall. However, if the
     * external service exceeds its threshold, JE will make a best effort to
     * prohibit write operations in order to honor the FREE_DISK limit, but
     * this is not always possible, as illustrated by the following sequence
     * of events:
     * <ul>
     *     <li>
     *     If the external service uses all its allocated space, 50GB, and JE
     *     environments are each using 75GB, then there will be 50GB free
     *     overall (25GB for each JE environment). Write operations are allowed
     *     in both JE environments.
     *     </li>
     *     <li>
     *     If the external service then exceeds its limit by 25GB and uses
     *     75GB, there will only 25GB free overall. But each JE environment is
     *     still under its 90GB limit and there is still more than 5GB free
     *     overall, so write operations are still allowed.
     *     </li>
     *     <li>
     *     If each JE environment uses an additional 10GB of space, there will
     *     only be 5GB free overall. Each JE environment is using only 85GB,
     *     which is under its 95GB limit. But the 5GB FREE_DISK limit for the
     *     volume overall has been reached and therefore JE write operations
     *     will be prohibited.
     *     </li>
     * </ul>
     * Leaving only 5GB of free space in the prior scenario is not ideal, but
     * it is at least enough for one JE environment at a time to be recovered.
     * The reality is that when an external entity exceeds its expected disk
     * usage, JE cannot always compensate. For example, if the external service
     * continues to use more space in the scenario above, the volume will
     * eventually be filled completely.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #FREE_DISK
     * @see #setMaxDisk(long)
     * @see #getMaxDisk()
     * @since 7.5
     */
    public static final String MAX_DISK = "je.maxDisk";

    /**
     * A lower limit on the number of bytes of free space to maintain on a
     * volume and per JE Environment. Works with {@link #MAX_DISK} to define
     * the storage limit. If the limit is exceeded, write operations will be
     * prohibited.
     * <p>
     * The default FREE_DISK value is 5GB. This value is intended to be large
     * enough to allow manual recovery after exceeding a disk threshold.
     * <p>
     * If FREE_DISK is set to zero, no free space limit is enforced. This is
     * not recommended, since manual recovery may be very difficult or
     * impossible when the volume is completely full.
     * <p>
     * If non-zero, this parameter is used in two ways.
     * <ul>
     *     <li>
     *     FREE_DISK determines the minimum of free space left on the storage
     *     volume. If less than this amount is free, write operations are
     *     prohibited.
     *     </li>
     *     <li>
     *     If MAX_DISK is configured, FREE_DISK is subtracted from MAX_DISK to
     *     determine the usage threshold for prohibiting write operations. See
     *     {@link #MAX_DISK} for more information.
     *
     *       <p style="margin-left: 2em">Note that this subtraction could make
     *       testing inconvenient when a small value is specified for MAX_DISK
     *       and FREE_DISK is not also specified. For example, if MAX_DISK is
     *       1GB and FREE_DISK is 5G (its default value), then no writing
     *       would be allowed (MAX_DISK minus FREE_DISK is negative 4G). To
     *       address this, the subtraction is performed only if one of two
     *       conditions is met:
     *       <ol>
     *           <li>FREE_DISK is explicitly specified, or</li>
     *           <li>MAX_DISK is greater than 10GB.</li>
     *       </ol>
     *
     *     </li>
     * </ul>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>Yes</td>
     * <td>5,368,709,120 (5GB)</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #MAX_DISK
     * @since 7.5
     */
    public static final String FREE_DISK = "je.freeDisk";

    /**
     * The desired upper limit on the number of bytes of reserved space to
     * retain in a replicated JE Environment. This parameter is ignored in
     * a non-replicated JE Environment.
     * <p>
     * Normally this parameter should not be specified, and {@link #MAX_DISK}
     * alone should be used to limit the total amount of disk used. This
     * allows JE to retain as many reserved files as possible to support
     * replication to nodes that are out of contact. However, there are
     * exceptional circumstances that may require limiting the retention of
     * reserved files. For example, the underlying storage system may be
     * shared and dynamically expandable, and therefore it may be desirable
     * to allow the JE active data size to grow without bounds rather than
     * specifying {@code MAX_DISK}. In this case a limit on the space taken by
     * reserved files is needed to conserve the shared storage space.
     * <p>
     * If {@code RESERVED_DISK} is zero, the default value, it is not used.
     * If it is non-zero, {@code RESERVED_DISK} limit is applied in addition
     * to {@link #MAX_DISK} and {@link #FREE_DISK}, when these values are also
     * non-zero. Reserved files that are unprotected will be deleted in order
     * to satisfy all three limits. Reserved files that are
     * {@link EnvironmentStats#getProtectedLogSizeMap() protected}, however,
     * cannot be deleted and are retained without regard for the limits.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>Yes</td>
     * <td>0 (no limit)</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #MAX_DISK
     * @since 7.5.13
     */
    public static final String RESERVED_DISK = "je.reservedDisk";

    /**
     * If true, JE attempts to "fail fast" and behavior is modified (compared
     * to production mode) in the following ways:
     * <ul>
     *   <li>When an operation accesses an LSN that refers to a reserved file,
     *   in test mode this will cause the environment to be invalidated and
     *   the invalidating exception will be logged and thrown. In production
     *   mode, such errors are logged but do not cause a failure.</li>
     *
     *   <li>When the Btree verifier detects a severe error, as above, in test
     *   mode this this will cause the environment to be invalidated and the
     *   invalidating exception will be logged and thrown. In production mode,
     *   such errors are logged using a rate-limited logger but do not cause
     *   failures.</li>
     * </ul>
     *
     * <p>If this param is unspecified and the {@code je.testMode} system
     * property is specified (non-null), the system property is used to set
     * this param. In that case, if the system property is "true" then this
     * param is set to true else it is set to false (as with {@link
     * Boolean#valueOf(String)}).</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String TEST_MODE = "je.testMode";

    /**
     * If true, a checkpoint is forced following recovery, even if the
     * log ends with a checkpoint.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String ENV_RECOVERY_FORCE_CHECKPOINT =
        "je.env.recoveryForceCheckpoint";

    /**
     * Used after performing a restore from backup to force creation of a new
     * log file prior to recovery.
     * <p>
     * As of JE 6.3, the use of this parameter is unnecessary except in special
     * cases. See the "Restoring from a backup" section in the DbBackup javadoc
     * for more information.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @see <a href="util/DbBackup.html#restore">Restoring from a backup</a>
     */
    public static final String ENV_RECOVERY_FORCE_NEW_FILE =
        "je.env.recoveryForceNewFile";

    /**
     * By default, if a checksum exception is found at the end of the log
     * during Environment startup, JE will assume the checksum is due to
     * previously interrupted I/O and will quietly truncate the log and
     * restart. If this property is set to true, when a ChecksumException
     * occurs in the last log file during recovery, instead of truncating the
     * log file, and automatically restarting, attempt to continue reading past
     * the corrupted record with the checksum error to see if there are commit
     * records following the corruption. If there are, throw an
     * EnvironmentFailureException to indicate the presence of committed
     * transactions. The user may then need to run DbTruncateLog to truncate
     * the log for further recovery after doing manual analysis of the log.
     * Setting this property is suitable when the application wants to guard
     * against unusual cases.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION =
        "je.haltOnCommitAfterChecksumException";

    /**
     * If true, starts up the INCompressor thread.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_RUN_IN_COMPRESSOR =
        "je.env.runINCompressor";

    /**
     * If true, starts up the checkpointer thread.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_RUN_CHECKPOINTER = "je.env.runCheckpointer";

    /**
     * If true, starts up the cleaner thread.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_RUN_CLEANER = "je.env.runCleaner";

    /**
     * Allows disabling the thread used for performing
     * {@link ExtinctionFilter record extinction}. By default this thread is
     * enabled, and if it is disabled then
     * {@link Environment#discardExtinctRecords} cannot be called. Disabling
     * this thread is intended primarily for testing and debugging.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_RUN_EXTINCT_RECORD_SCANNER =
        "je.env.runExtinctRecordScanner";

    /**
     * If true, eviction is done by a pool of evictor threads, as well as being
     * done inline by application threads. If false, the evictor pool is not
     * used, regardless of the values of {@link #EVICTOR_CORE_THREADS} and
     * {@link #EVICTOR_MAX_THREADS}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_RUN_EVICTOR = "je.env.runEvictor";

    /**
     * The maximum number of read operations performed by JE background
     * activities (e.g., cleaning) before sleeping to ensure that application
     * threads can perform I/O.  If zero (the default) then no limitation on
     * I/O is enforced.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #ENV_BACKGROUND_SLEEP_INTERVAL
     */
    public static final String ENV_BACKGROUND_READ_LIMIT =
        "je.env.backgroundReadLimit";

    /**
     * The maximum number of write operations performed by JE background
     * activities (e.g., checkpointing and eviction) before sleeping to ensure
     * that application threads can perform I/O.  If zero (the default) then no
     * limitation on I/O is enforced.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #ENV_BACKGROUND_SLEEP_INTERVAL
     */
    public static final String ENV_BACKGROUND_WRITE_LIMIT =
        "je.env.backgroundWriteLimit";

    /**
     * The duration that JE background activities will sleep when the {@link
     * #ENV_BACKGROUND_WRITE_LIMIT} or {@link #ENV_BACKGROUND_READ_LIMIT} is
     * reached.  If {@link #ENV_BACKGROUND_WRITE_LIMIT} and {@link
     * #ENV_BACKGROUND_READ_LIMIT} are zero, this setting is not used.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>1 ms</td>
     * <td>1 ms</td>
     * <td>24 d</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String ENV_BACKGROUND_SLEEP_INTERVAL =
        "je.env.backgroundSleepInterval";

    /**
     * Debugging support: check leaked locks and txns at env close.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_CHECK_LEAKS = "je.env.checkLeaks";

    /**
     * Debugging support: call Thread.yield() at strategic points.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String ENV_FORCED_YIELD = "je.env.forcedYield";

    /**
     * Configures the use of transactions.
     *
     * <p>This should be set to true when transactional guarantees such as
     * atomicity of multiple operations and durability are important.</p>
     *
     * <p>If true, create an environment that is capable of performing
     * transactions.  If true is not passed, transactions may not be used.  For
     * licensing purposes, the use of this method distinguishes the use of the
     * Transactional product.  Note that if transactions are not used,
     * specifying true does not create additional overhead in the
     * environment.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @see #setTransactional
     */
    public static final String ENV_IS_TRANSACTIONAL = "je.env.isTransactional";

    /**
     * Configures the database environment for no locking.
     *
     * <p>If true, create the environment with record locking.  This property
     * should be set to false only in special circumstances when it is safe to
     * run without record locking.</p>
     *
     * <p>This configuration option should be used when locking guarantees such
     * as consistency and isolation are not important.  If locking mode is
     * disabled (it is enabled by default), the cleaner is automatically
     * disabled.  The user is responsible for invoking the cleaner and ensuring
     * that there are no concurrent operations while the cleaner is
     * running.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @see #setLocking
     */
    public static final String ENV_IS_LOCKING = "je.env.isLocking";

    /**
     * Configures the database environment to be read-only, and any attempt to
     * modify a database will fail.
     *
     * <p>A read-only environment has several limitations and is recommended
     * only in special circumstances.  Note that there is no performance
     * advantage to opening an environment read-only.</p>
     *
     * <p>The primary reason for opening an environment read-only is to open a
     * single environment in multiple JVM processes.  Only one JVM process at a
     * time may open the environment read-write.  See {@link
     * EnvironmentLockedException}.</p>
     *
     * <p>When the environment is open read-only, the following limitations
     * apply.</p>
     * <ul>
     * <li>In the read-only environment no writes may be performed, as
     * expected, and databases must be opened read-only using {@link
     * DatabaseConfig#setReadOnly}.</li>
     * <li>The read-only environment receives a snapshot of the data that is
     * effectively frozen at the time the environment is opened. If the
     * application has the environment open read-write in another JVM process
     * and modifies the environment's databases in any way, the read-only
     * version of the data will not be updated until the read-only JVM process
     * closes and reopens the environment (and by extension all databases in
     * that environment).</li>
     * <li>If the read-only environment is opened while the environment is in
     * use by another JVM process in read-write mode, opening the environment
     * read-only (recovery) is likely to take longer than it does after a clean
     * shutdown.  This is due to the fact that the read-write JVM process is
     * writing and checkpoints are occurring that are not coordinated with the
     * read-only JVM process.  The effect is similar to opening an environment
     * after a crash.</li>
     * <li>In a read-only environment, the JE cache will contain information
     * that cannot be evicted because it was reconstructed by recovery and
     * cannot be flushed to disk.  This means that the read-only environment
     * may not be suitable for operations that use large amounts of memory, and
     * poor performance may result if this is attempted.</li>
     * <li>In a read-write environment, the log cleaner will be prohibited from
     * deleting log files for as long as the environment is open read-only in
     * another JVM process.  This may cause disk usage to rise, and for this
     * reason it is not recommended that an environment is kept open read-only
     * in this manner for long periods.</li>
     * </ul>
     *
     * <p>For these reasons, it is recommended that a read-only environment be
     * used only for short periods and for operations that are not performance
     * critical or memory intensive.  With few exceptions, all application
     * functions that require access to a JE environment should be built into a
     * single application so that they can be performed in the JVM process
     * where the environment is open read-write.</p>
     *
     * <p>In most applications, opening an environment read-only can and should
     * be avoided.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @see #setReadOnly
     */
    public static final String ENV_READ_ONLY = "je.env.isReadOnly";

    /**
     * If true, use latches instead of synchronized blocks to implement the
     * lock table and log write mutexes. Latches require that threads queue to
     * obtain the mutex in question and therefore guarantee that there will be
     * no mutex starvation, but do incur a performance penalty. Latches should
     * not be necessary in most cases, so synchronized blocks are the default.
     * An application that puts heavy load on JE with threads with different
     * thread priorities might find it useful to use latches.  In a Java 5 JVM,
     * where java.util.concurrent.locks.ReentrantLock is used for the latch
     * implementation, this parameter will determine whether they are 'fair' or
     * not.  This parameter is 'static' across all environments.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String ENV_FAIR_LATCHES = "je.env.fairLatches";

    /**
     * The timeout for detecting internal latch timeouts, so that deadlocks can
     * be detected. Latches are held internally for very short durations. If
     * due to unforeseen problems a deadlock occurs, a timeout will occur after
     * the duration specified by this parameter. When a latch timeout occurs:
     * <ul>
     *     <li>The Environment is invalidated and must be closed.</li>
     *     <li>An {@link EnvironmentFailureException} is thrown.</li>
     *     <li>A full thread dump is logged at level SEVERE.</li>
     * </ul>
     * If this happens, thread dump in je.info file should be preserved so it
     * can be used to analyze the problem.
     * <p>
     * Most applications should not change this parameter. The default value, 5
     * minutes, should be much longer than a latch is ever held.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>5 min</td>
     * <td>1 ms</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @since 6.2
     */
    public static final String ENV_LATCH_TIMEOUT = "je.env.latchTimeout";

    /**
     * The interval added to the system clock time for determining that a
     * record may have expired. Used when an internal integrity error may be
     * present, but may also be due to a record that expired and the system
     * clock was moved back.
     * <p>
     * For example, say a record expires and then the clock is moved back by
     * one hour to correct a daylight savings time error. Because the LN and
     * BIN slot for an expired record are purged separately (see
     * <a href="WriteOptions#ttl">Time-To_live</a>), in this case the LN was
     * purged but the BIN slot was not purged. When accessing the record's key
     * via the BIN slot, it will appear that it is not expired. But then when
     * accessing the the data, the LN will not be accessible. Normally this
     * would be considered a fatal integrity error, but since the record will
     * expire within the 2 hour limit, it is simply treated as an expired
     * record.
     * <p>
     * Most applications should not change this parameter. The default value,
     * two hours, is enough to account for minor clock adjustments or
     * accidentally setting the clock one hour off.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>2 h</td>
     * <td>1 ms</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @since 7.0
     */
    public static final String ENV_TTL_CLOCK_TOLERANCE =
        "je.env.ttlClockTolerance";

    /**
     * If true (the default), expired data is filtered from queries and purged
     * by the cleaner. This might be set to false to recover data after an
     * extended down time.
     * <p>
     * WARNING: Disabling expiration is intended for special-purpose access
     * for data recovery only. When this parameter is set to false, records
     * that have expired may or may not have been purged, so they may or may
     * not be accessible. In addition, it is possible for the key and data of
     * a record to expire independently, so the key may be accessible (if the
     * data is not requested by the read operation), while the record will
     * appear to be deleted when the data is requested. The same thing is
     * true of primary and secondary records, which are also purged
     * independently. A record may be accessible by primary key but not
     * secondary key, and vice-versa.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String ENV_EXPIRATION_ENABLED =
        "je.env.expirationEnabled";

    /**
     * If true, enable eviction of metadata for closed databases. There is
     * no known benefit to setting this parameter to false.
     *
     * <p>This param is unlikely to be needed for tuning, but is sometimes
     * useful for debugging and testing.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheDebugging">Cache Statistics:
     * Debugging</a>
     */
    public static final String ENV_DB_EVICTION = "je.env.dbEviction";

    /**
     * By default, JE passes an entire log record to the Adler32 class for
     * checksumming.  This can cause problems with the GC in some cases if the
     * records are large and there is concurrency.  Setting this parameter will
     * cause JE to pass chunks of the log record to the checksumming class so
     * that the GC does not block.  0 means do not chunk.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>0</td>
     * <td>1048576 (1M)</td>
     * </tr>
     * </table>
     */
    public static final String ADLER32_CHUNK_SIZE = "je.adler32.chunkSize";

    /**
     * The maximum memory taken by log buffers, in bytes. If 0, use 7% of
     * je.maxMemory. If 0 and je.sharedCache=true, use 7% divided by N where N
     * is the number of environments sharing the global cache. The resulting
     * value is used to restrict the size of each log buffer as described
     * under {@link #LOG_BUFFER_SIZE}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>No</td>
     * <td>0</td>
     * <td>{@value
     * com.sleepycat.je.config.EnvironmentParams#LOG_MEM_SIZE_MIN}</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #LOG_BUFFER_SIZE
     *
     * @see <a href="EnvironmentStats.html#logBuffer">I/O Statistics: Log
     * Buffers</a>
     */
    public static final String LOG_TOTAL_BUFFER_BYTES =
        "je.log.totalBufferBytes";

    /**
     * The number of JE log buffers.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>{@value
     * com.sleepycat.je.config.EnvironmentParams#NUM_LOG_BUFFERS_DEFAULT}</td>
     * <td>2</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #LOG_BUFFER_SIZE
     *
     * @see <a href="EnvironmentStats.html#logBuffer">I/O Statistics: Log
     * Buffers</a>
     */
    public static final String LOG_NUM_BUFFERS = "je.log.numBuffers";

    /**
     * The maximum size of each JE log buffer. The actual buffer size is
     * further restricted and calculated as follows:
     * <ul>
     *     <li>The initial buffer size is calculated by dividing
     *     {@link #LOG_TOTAL_BUFFER_BYTES} by {@link #LOG_NUM_BUFFERS}.</li>
     *
     *     <li>If the initial size is greater than {@code LOG_BUFFER_SIZE}
     *     then {@code LOG_BUFFER_SIZE} is used.</li>
     *
     *     <li>Otherwise, if the initial buffer size is less than two KB
     *     then two KB is used.</li>
     *
     *     <li>Otherwise, the initial buffer size is used.</li>
     * </ul>
     * <p>In addition, if the resulting buffer size is greater than
     * {@link #LOG_FILE_MAX} then {@code #LOG_FILE_MAX} is used.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>1048576 (1M)</td>
     * <td>1024 (1K)</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#logBuffer">I/O Statistics: Log
     * Buffers</a>
     *
     * @see <a href="EnvironmentStats.html#logWriteQueue">I/O Statistics:
     * The Write Queue</a>
     */
    public static final String LOG_BUFFER_SIZE = "je.log.bufferSize";

    /**
     * Whether to use thread-local buffers for serializing non-replicated
     * entries during logging and when fetching log entries from disk.
     *
     * <p>If this param is false or {@link #LOG_FAULT_READ_SIZE} is less than
     * the size of the entry, a transient buffer is allocated for the entry.
     * The use of transient buffers adds to Java GC costs and setting this
     * param to true (the default value) is recommended.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @see #LOG_FAULT_READ_SIZE
     * @see #LOG_ITEM_POOL_SIZE
     * @see <a href="EnvironmentStats.html#logItemBuffer">I/O Statistics: Log
     * Item Buffers</a>
     */
    public static final String LOG_ITEM_THREAD_LOCAL =
        "je.log.itemThreadLocal";

    /**
     * Number of byte buffers in the log item pool for replicated entries.
     *
     * <p>The size of each pooled buffer is {@link #LOG_FAULT_READ_SIZE}.</p>
     *
     * <p>When serializing a replicated entry, if a pooled buffer cannot be
     * used because {@link #LOG_FAULT_READ_SIZE} is less than the size of the
     * entry, or this param is zero, or the pool is empty, then a transient
     * buffer is allocated for the entry. The use of transient buffers adds
     * to Java GC costs.</p>
     *
     * <p>To prevent the use of transient buffers, this param should be
     * roughly the size of je.rep.vlsn.logCacheSize, plus the number of
     * writing threads, plus the expected total feeder backlog. Buffers are
     * used to serialize replicable log entries, and are returned to the
     * pool after they have been sent to all replicas.</p>
     *
     * <p>If this param is zero, no log item buffers are pooled.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>1000</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #LOG_FAULT_READ_SIZE
     * @see #LOG_ITEM_THREAD_LOCAL
     * @see <a href="EnvironmentStats.html#logItemBuffer">I/O Statistics: Log
     * Item Buffers</a>
     */
    public static final String LOG_ITEM_POOL_SIZE = "je.log.itemPoolSize";

    /**
     * Cached buffer size for log items, and the initial size for faulting
     * in items from disk when the item size is unknown.
     *
     * <p>TODO: This param should ideally be renamed LOG_ITEM_BUFFER_SIZE,
     * since it is now used for logging as well as reads. However, this would
     * be an incompatible change that would be difficult to make in all
     * applications. Perhaps a new LOG_ITEM_BUFFER_SIZE should be created that
     * supersedes this param, although the behavior when both params are
     * non-zero will have to be defined.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>2048 (2K)</td>
     * <td>32</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #LOG_ITEM_THREAD_LOCAL
     * @see #LOG_ITEM_POOL_SIZE
     * @see <a href="EnvironmentStats.html#logItemBuffer">I/O Statistics: Log
     * Item Buffers</a>
     * @see <a href="EnvironmentStats.html#logFileAccess">I/O Statistics: File
     * Access</a>
     */
    public static final String LOG_FAULT_READ_SIZE = "je.log.faultReadSize";

    /**
     * The read buffer size for log iterators, which are used when scanning the
     * log during activities like log cleaning and environment open, in bytes.
     * This may grow as the system encounters larger log entries.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>8192 (8K)</td>
     * <td>128</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String LOG_ITERATOR_READ_SIZE =
        "je.log.iteratorReadSize";

    /**
     * The maximum read buffer size for log iterators, which are used when
     * scanning the log during activities like log cleaning and environment
     * open, in bytes.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>16777216 (16M)</td>
     * <td>128</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#logFileAccess">I/O Statistics: File
     * Access</a>
     */
    public static final String LOG_ITERATOR_MAX_SIZE =
        "je.log.iteratorMaxSize";

    /**
     * The maximum size of each individual JE log file, in bytes.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>No</td>
     * <td>10000000 (10M)</td>
     * <td>1000000 (1M)</td>
     * <td>1073741824 (1G)</td>
     * </tr>
     * </table>
     */
    public static final String LOG_FILE_MAX = "je.log.fileMax";

    /**
     * If true, perform a checksum check when reading entries from log.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String LOG_CHECKSUM_READ = "je.log.checksumRead";

    /**
     * If true, perform a checksum verification just before and after writing
     * to the log.  This is primarily used for debugging.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String LOG_VERIFY_CHECKSUMS = "je.log.verifyChecksums";

    /**
     * If true (the default), a checksum verification failure will create a
     * marker file that prevents re-opening the environment as described under
     * {@link EnvironmentFailureException#isCorrupted}.
     *
     * <p>Note that the Environment is always invalidated when a checksum error
     * is detected outside of the verifier. When a checksum error is detected
     * during verification, a marker file is not created and the Environment is
     * not invalidated.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String LOG_CHECKSUM_FATAL = "je.log.checksumFatal";

    /**
     * If true, operates in an in-memory test mode without flushing the log to
     * disk. An environment directory must be specified, but it need not exist
     * and no files are written.  The system operates until it runs out of
     * memory, at which time an OutOfMemoryError is thrown.  Because the entire
     * log is kept in memory, this mode is normally useful only for testing.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String LOG_MEM_ONLY = "je.log.memOnly";

    /**
     * The size of the file handle cache.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>100</td>
     * <td>3</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#logFileAccess">I/O Statistics: File
     * Access</a>
     */
    public static final String LOG_FILE_CACHE_SIZE = "je.log.fileCacheSize";

    /**
     * If true, periodically detect unexpected file deletions. Normally all
     * file deletions should be performed as a result of JE log cleaning.
     * If an external file deletion is detected, JE assumes this was
     * accidental. This will cause the environment to be invalidated and
     * all methods will throw {@link EnvironmentFailureException}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @since 7.2
     */
    public static final String LOG_DETECT_FILE_DELETE =
        "je.log.detectFileDelete";

    /**
     * The interval used to check for unexpected file deletions.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>1000 ms</td>
     * <td>1 ms</td>
     * <td>none</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String LOG_DETECT_FILE_DELETE_INTERVAL =
        "je.log.detectFileDeleteInterval";

    /**
     * The timeout limit for group commit.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>500 ms</td>
     * <td>10 ms</td>
     * <td>24 d</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @see <a href="EnvironmentStats.html#logFsync">I/O Statistics:
     * Fsync and Group Commit</a>
     */
    public static final String LOG_FSYNC_TIMEOUT = "je.log.fsyncTimeout";

    /**
     * If the time taken by an fsync exceeds this limit, a WARNING level
     * message is logged. If this parameter set to zero, a message will not be
     * logged. By default, this parameter is 5 seconds.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>5 s</td>
     * <td>zero</td>
     * <td>30 s</td>
     * </tr>
     * </table>
     *
     * @since 7.0
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @see <a href="EnvironmentStats.html#logFsync">I/O Statistics:
     * Fsync and Group Commit</a>
     *
     * @see EnvironmentStats#getFSyncMaxMs()
     */
    public static final String LOG_FSYNC_TIME_LIMIT = "je.log.fsyncTimeLimit";

    /**
     * The maximum time interval between committing a transaction with
     * {@link Durability.SyncPolicy#NO_SYNC NO_SYNC} or {@link
     * Durability.SyncPolicy#WRITE_NO_SYNC WRITE_NO_SYNC} durability,
     * and making the transaction durable with respect to the storage device.
     * To provide this guarantee, a JE background thread is used to flush any
     * data buffered by JE to the file system, and also perform an fsync to
     * force any data buffered by the file system to the storage device. If
     * this parameter is set to zero, this JE background task is disabled and
     * no such guarantee is provided.
     * <p>
     * Separately, the {@link #LOG_FLUSH_NO_SYNC_INTERVAL} flushing provides a
     * guarantee that data is periodically flushed to the file system. To guard
     * against data loss due to an OS crash (and to improve performance) we
     * recommend that the file system is configured to periodically flush dirty
     * pages to the storage device. This parameter, {@code
     * LOG_FLUSH_SYNC_INTERVAL}, provides a fallback for flushing to the
     * storage device, in case the file system is not adequately configured.
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * <td>Minimum</td>
     * <td>Maximum</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>
     * <a href="../EnvironmentConfig.html#timeDuration">Duration</a>
     * </td>
     * <td>Yes</td>
     * <td>20 s</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="../EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @see <a href="EnvironmentStats.html#logFsync">I/O Statistics:
     * Fsync and Group Commit</a>
     *
     * @since 7.2
     */
    public static final String LOG_FLUSH_SYNC_INTERVAL =
        "je.log.flushSyncInterval";

    /**
     * The maximum time interval between committing a transaction with
     * {@link Durability.SyncPolicy#NO_SYNC NO_SYNC} durability, and
     * making the transaction durable with respect to the file system. To
     * provide this guarantee, a JE background thread is used to flush any data
     * buffered by JE to the file system. If this parameter is set to zero,
     * this JE background task is disabled and no such guarantee is provided.
     * <p>
     * Frequent periodic flushing to the file system provides improved
     * durability for NO_SYNC transactions. Without this flushing, if
     * application write operations stop, then some number of NO_SYNC
     * transactions would be left in JE memory buffers and would be lost in the
     * event of a crash. For HA applications, this flushing reduces the
     * possibility of {@link com.sleepycat.je.rep.RollbackProhibitedException}.
     * Note that periodic flushing reduces the time window where a crash can
     * cause transaction loss and {@code RollbackProhibitedException}, but the
     * window cannot be closed completely when using NO_SYNC durability.
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * <td>Minimum</td>
     * <td>Maximum</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>
     * <a href="../EnvironmentConfig.html#timeDuration">Duration</a>
     * </td>
     * <td>Yes</td>
     * <td>5 s</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="../EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @see <a href="EnvironmentStats.html#logFsync">I/O Statistics:
     * Fsync and Group Commit</a>
     *
     * @since 7.2
     */
    public static final String LOG_FLUSH_NO_SYNC_INTERVAL =
        "je.log.flushNoSyncInterval";

    /**
     * If true (default is false) O_DSYNC is used to open JE log files.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String LOG_USE_ODSYNC = "je.log.useODSYNC";

    /**
     * If true (default is true) the Write Queue is used for file I/O
     * operations which are blocked by concurrent I/O operations.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#logWriteQueue">I/O Statistics:
     * The Write Queue</a>
     */
    public static final String LOG_USE_WRITE_QUEUE = "je.log.useWriteQueue";

    /**
     * The size of the Write Queue.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>1MB</td>
     * <td>4KB</td>
     * <td>32MB-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#logWriteQueue">I/O Statistics:
     * The Write Queue</a>
     */
    public static final String LOG_WRITE_QUEUE_SIZE = "je.log.writeQueueSize";

    /**
     * Whether to run the background verifier.
     * <p>
     * If true (the default), the verifier runs according to the schedule
     * given by {@link #VERIFY_SCHEDULE}. Each time the verifier runs, it
     * performs checksum verification if the {@link #VERIFY_LOG} setting is
     * true and performs Btree verification if the {@link #VERIFY_BTREE}
     * setting is true.
     * <p>
     * When corruption is detected, the Environment will be invalidated and an
     * EnvironmentFailureException will be thrown. Applications catching this
     * exception can call the new {@link
     * EnvironmentFailureException#isCorrupted()} method to determine whether
     * corruption was detected.
     * <p>
     * If isCorrupted returns true, a full restore (an HA {@link
     * com.sleepycat.je.rep.NetworkRestore} or restore from backup)
     * should be performed to avoid further problems. The advantage of
     * performing verification frequently is that a problem may be detected
     * sooner than it would be otherwise. For HA applications, this means that
     * the network restore can be done while the other nodes in the group are
     * up, minimizing exposure to additional failures.
     * <p>
     * When index corruption is detected, the environment is not invalidated.
     * Instead, the corrupt index (secondary database) is marked as corrupt
     * in memory and a warning message is logged. All subsequent access to the
     * index will throw {@link SecondaryIntegrityException}. To correct the
     * problem, the application may perform a full restore or rebuild the
     * corrupt index.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @since 7.3
     */
    public static final String ENV_RUN_VERIFIER = "je.env.runVerifier";

    /**
     * A crontab-format string indicating when to start the background
     * verifier.
     * <p>
     * See https://en.wikipedia.org/wiki/Cron#Configuration_file
     * Note that times and dates are specified in local time, not UTC time.
     * <p>
     * The data verifier will run at most once per scheduled interval. If the
     * complete verification (log verification followed by Btree verification)
     * takes longer than the scheduled interval, then the next verification
     * will start at the next increment of the interval. For example, if the
     * default schedule is used (one per day at midnight), and verification
     * takes 25 hours, then verification will occur once every two
     * days (48 hours), starting at midnight.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>Yes</td>
     * <td>"0 0 * * * (run once a day at midnight, local time)"</td>
     * </tr>
     * </table>
     *
     * @since 7.3
     */
    public static final String VERIFY_SCHEDULE = "je.env.verifySchedule";

    /**
     * Whether the background verifier should verify checksums in the log,
     * as if the {@link DbVerifyLog} utility were run.
     * <p>
     * If true, the entire log is read sequentially and verified. The size
     * of the read buffer is determined by LOG_ITERATOR_READ_SIZE.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @since 7.3
     */
    public static final String VERIFY_LOG = "je.env.verifyLog";

    /**
     * The delay between reads during {@link #VERIFY_LOG log verification}.
     * A delay between reads is needed to allow other JE components, such as
     * HA, to make timely progress.
     * <p>
     * A 100ms delay, the default value, with the read buffer size 131072, i.e.
     * 128K, for a 1GB file, the total delay time is about 13 minutes.
     * <p>
     * This parameter applies only to the {@link #ENV_RUN_VERIFIER background
     * verifier}. It does not apply to use of {@link DbVerifyLog}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>100 ms</td>
     * <td>0 ms</td>
     * <td>10 s</td>
     * </tr>
     * </table>
     *
     * @since 7.5
     */
    public static final String VERIFY_LOG_READ_DELAY =
        "je.env.verifyLogReadDelay";

    /**
     * Whether the background verifier should perform Btree verification,
     * as if the {@link DbVerify} utility were run.
     * <p>
     * If true, the Btree of all databases, external and internal, is
     * verified. The in-memory cache is used for verification and internal
     * data structures are checked. References to data records (log sequence
     * numbers, or LSNs) are checked to ensure they do not refer to deleted
     * files -- this is the most common type of corruption. Additional
     * checks are performed, depending on the settings for {@link
     * #VERIFY_SECONDARIES} and {@link #VERIFY_DATA_RECORDS}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @since 7.5
     */
    public static final String VERIFY_BTREE = "je.env.verifyBtree";

    /**
     * Whether to verify secondary index references during Btree verification.
     * <p>
     * An index record contains a reference to a primary key, and the
     * verification involves checking that a record for the primary key exists.
     * <p>
     * Note that secondary index references are verified only for each
     * {@link SecondaryDatabase} that is currently open. The relationship
     * between a secondary and primary database is not stored persistently,
     * so JE is not aware of the relationship unless the secondary database
     * has been opened by the application.
     * <p>
     * When index corruption is detected, the environment is not invalidated.
     * Instead, the corrupt index (secondary database) is marked as corrupt
     * in memory and a warning message is logged. All subsequent access to the
     * index will throw {@link SecondaryIntegrityException}. To correct the
     * problem, the application may perform a full restore or rebuild the
     * corrupt index.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @since 7.5
     */
    public static final String VERIFY_SECONDARIES = "je.env.verifySecondaries";

    /**
     * Whether to verify data records (leaf nodes, or LNs) during Btree
     * verification.
     * <p>
     * Regardless of this parameter's value, the Btree reference to the data
     * record (the log sequence number, or LSN) is checked to ensure that
     * it doesn't refer to a file that has been deleted by the JE cleaner --
     * this sort of "dangling reference" is the most common type of
     * corruption. If this parameter value is true, the LN is additionally
     * fetched from disk (if not in cache) to verify that the LSN refers to
     * a valid log entry. Because LNs are often not cached, this can cause
     * expensive random IO, and the default value for this parameter is false
     * for this reason. Some applications may choose to set this parameter to
     * true, for example, when using a storage device with fast random
     * IO (an SSD).
     * <p>
     * Note that Btree internal nodes (INs) are always fetched from disk
     * during verification, if they are not in cache, and this can result
     * in random IO. Verification was implemented with the assumption that
     * most INs will be in cache.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @since 7.5
     */
    public static final String VERIFY_DATA_RECORDS =
        "je.env.verifyDataRecords";

    /**
     * Whether to verify references to obsolete records during Btree
     * verification.
     * <p>
     * For performance reasons, the JE cleaner maintains a set of
     * references(log sequence numbers, or LSNs) to obsolete records.
     * If such a reference is incorrect and the record at the LSN is
     * actually active, the cleaner may delete a data file without
     * migrating the active record, and this will result in a dangling
     * reference from the Btree.
     * <p>
     * If this parameter's value is true, all active LSNs in the Btree are
     * checked to ensure they are not in the cleaner's set of obsolete LSNs.
     * To perform this check efficiently, the set of all obsolete LSNs must
     * be fetched from disk and kept in memory during the verification run,
     * and the default value for this parameter is false for this reason.
     * Some applications may choose to set this parameter to true, when the
     * use of more Java heap memory is worth the additional safety measure.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @since 18.1
     */
    public static final String VERIFY_OBSOLETE_RECORDS =
        "je.env.verifyObsoleteRecords";

    /**
     * The number of records verified per batch during {@link #VERIFY_BTREE
     * Btree verification}. In order to give database remove/truncate the
     * opportunity to execute, records are verified in batches and there is
     * a {@link #VERIFY_BTREE_BATCH_DELAY delay} between batches.
     * <p>
     * This parameter applies only to the {@link #ENV_RUN_VERIFIER background
     * verifier}. It does not apply to use of {@link DbVerify}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>1000</td>
     * <td>1</td>
     * <td>10000</td>
     * </tr>
     * </table>
     */
    public static final String VERIFY_BTREE_BATCH_SIZE =
        "je.env.verifyBtreeBatchSize";

    /**
     * The delay between batches during {@link #VERIFY_BTREE Btree
     * verification}. In order to give database remove/truncate the
     * opportunity to execute, records are verified in {@link
     * #VERIFY_BTREE_BATCH_SIZE batches} and there is a delay between batches.
     * <p>
     * A 10ms delay, the default value, should be enough to allow other
     * threads to run. A large value, for example 1s, would result in a total
     * delay of 28 hours when verifying 100m records or 100k batches.
     * <p>
     * This parameter applies only to the {@link #ENV_RUN_VERIFIER background
     * verifier}. It does not apply to use of {@link DbVerify}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>10 ms</td>
     * <td>0 ms</td>
     * <td>10 s</td>
     * </tr>
     * </table>
     */
    public static final String VERIFY_BTREE_BATCH_DELAY =
        "je.env.verifyBtreeBatchDelay";

    /**
     * The maximum number of entries in an internal btree node.  This can be
     * set per-database using the DatabaseConfig object.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>128</td>
     * <td>4</td>
     * <td>32767 (32K)</td>
     * </tr>
     * </table>
     */
    public static final String NODE_MAX_ENTRIES = "je.nodeMaxEntries";

    /**
     * The maximum size (in bytes) of a record's data portion that will cause
     * the record to be embedded in its parent BIN.
     * <p>
     * Normally, records (key-value pairs) are stored on disk as individual
     * byte sequences called LNs (leaf nodes) and they are accessed via a
     * Btree. The nodes of the Btree are called INs (Internal Nodes) and the
     * INs at the bottom layer of the Btree are called BINs (Bottom Internal
     * Nodes). Conceptually, each BIN contains an array of slots. A slot
     * represents an associated data record. Among other things, it stores
     * the key of the record and the most recent disk address of that record.
     * Records and INs share the disk space (are stored in the same kind of
     * files), but LNs are stored separately from BINs, i.e., there is no
     * clustering or co-location of a BIN and its child LNs.
     * <p>
     * With embedded LNs, a whole record may be stored inside a BIN (i.e.,
     * a BIN slot may contain both the key and the data portion of a record).
     * Specifically, a record will be "embedded" if the size (in bytes) of its
     * data portion is less than or equal to the value of the
     * TREE_MAX_EMBEDDED_LN configuration parameter. The decision to embed a
     * record or not is taken on a record-by-record basis. As a result, a BIN
     * may contain both embedded and non-embedded records. The "embeddedness"
     * of a record is a dynamic property: a size-changing update may turn a
     * non-embedded record to an embedded one or vice-versa.
     * <p>
     * Notice that even though a record may be embedded, when the record is
     * inserted, updated, or deleted an LN for that record is still generated
     * and written to disk. This is because LNs also act as log records,
     * which are needed during recovery and/or transaction abort to undo/redo
     * operations that are/are-not currently reflected in the BINs. However,
     * during normal processing, these LNs will never be fetched from disk.
     * <p>
     * Obviously, embedding records has the performance advantage that no
     * extra disk read is needed to fetch the record data (i.e., the LN)
     * during read operations. This is especially true for operations like
     * cursor scans and for random searches within key ranges whose
     * containing BINs can fit in the JE cache (in other words when there
     * is locality of reference). Furthermore, embedded records do not need
     * to be migrated during cleaning; they are considered obsolete by default,
     * because they will never be needed again after their containing log file
     * is deleted. This makes cleaning faster, and more importantly, avoids
     * the dirtying of the parent BINs, which would otherwise cause even more
     * cleaning later.
     * <p>
     * On the other hand, embedded LNs make the BINs larger, which can lead to
     * more cache eviction of BINs and the associated performance problems.
     * When eviction does occur, performance can deteriorate as the size of
     * the data portion of the records grows. This is especially true for
     * insertion-only workloads. Therefore, increasing the value of
     * TREE_MAX_EMBEDDED_LN beyond the default value of 16 bytes should be
     * done "carefully": by considering the kind of workloads that will be run
     * against BDB-JE and their relative importance and expected response
     * times, and by running performance tests with both embedded and
     * non-embedded LNs.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>16</td>
     * <td>0</td>
     * <td>Integer.MAX_VALUE</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheSizeOptimizations">Cache
     * Statistics: Size Optimizations</a>
     *
     * @see <a href="#cleanerEfficiency">Cleaning Efficiency</a>
     */
    public static final String TREE_MAX_EMBEDDED_LN = "je.tree.maxEmbeddedLN";

    /**
     * If more than this percentage of entries are changed on a BIN, log a a
     * full version instead of a delta.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>25</td>
     * <td>0</td>
     * <td>75</td>
     * </tr>
     * </table>
     */
    public static final String TREE_BIN_DELTA = "je.tree.binDelta";

    /**
     * The minimum bytes allocated out of the memory cache to hold Btree data
     * including internal nodes and record keys and data.  If the specified
     * value is larger than the size initially available in the cache, it will
     * be truncated to the amount available.
     *
     * <p>{@code TREE_MIN_MEMORY} is the minimum for a single environment.  By
     * default, 500 KB or the size initially available in the cache is used,
     * whichever is smaller.</p>
     *
     * <p>This param is only likely to be needed for tuning of Environments
     * with extremely small cache sizes. It is sometimes also useful for
     * debugging and testing.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>Yes</td>
     * <td>512000 (500K)</td>
     * <td>51200 (50K)</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheDebugging">Cache Statistics:
     * Debugging</a>
     */
    public static final String TREE_MIN_MEMORY = "je.tree.minMemory";

    /**
     * Specifies the maximum unprefixed key length for use in the compact
     * in-memory key representation.
     *
     * <p>In the Btree, the JE in-memory cache, the default representation for
     * keys uses a byte array object per key.  The per-key object overhead of
     * this approach ranges from 20 to 32 bytes, depending on the JVM
     * platform.</p>
     *
     * <p>To reduce memory overhead, a compact representation can instead be
     * used where keys will be represented inside a single byte array instead
     * of having one byte array per key. Within the single array, all keys are
     * assigned a storage size equal to that taken up by the largest key, plus
     * one byte to hold the actual key length.  The use of the fixed size array
     * reduces Java GC activity as well as memory overhead.</p>
     *
     * <p>In order for the compact representation to reduce memory usage, all
     * keys in a database, or in a Btree internal node, must be roughly the
     * same size.  The more fully populated the internal node, the more the
     * savings with this representation since the single byte array is sized to
     * hold the maximum number of keys in the internal node, regardless of the
     * actual number of keys that are present.</p>
     *
     * <p>It's worth noting that the storage savings of the compact
     * representation are realized in addition to the storage benefits of key
     * prefixing (if it is configured), since the keys stored in the key array
     * are the smaller key values after the prefix has been stripped, reducing
     * the length of the key and making it more likely that it's small enough
     * for this specialized representation.  This configuration parameter
     * ({@code TREE_COMPACT_MAX_KEY_LENGTH}) is the maximum key length, not
     * including the common prefix, for the keys in a Btree internal node
     * stored using the compact representation.  See {@link
     * DatabaseConfig#setKeyPrefixing}.</p>
     *
     * <p>The compact representation is used automatically when both of the
     * following conditions hold.</p>
     * <ul>
     * <li>All keys in a Btree internal node must have an unprefixed length
     * that is less than or equal to the length specified by this parameter
     * ({@code TREE_COMPACT_MAX_KEY_LENGTH}).</li>
     * <li>If key lengths vary by large amounts within an internal node, the
     * wasted space of the fixed length storage may negate the benefits of the
     * compact representation and cause more memory to be used than with the
     * default representation.  In that case, the default representation will
     * be used.</li>
     * </ul>
     *
     * <p>If this configuration parameter is set to zero, the compact
     * representation will not be used.</p>
     *
     * <p>The default value of this configuration parameter is 42 bytes.  The
     * potential drawbacks of specifying a larger length are:</p>
     * <ul>
     * <li>Insertion and deletion for larger keys move bytes proportional to
     * the storage length of the keys.</li>
     * <li>With the compact representation, all operations create temporary
     * byte arrays for each key involved in the operation.  Larger byte arrays
     * mean more work for the Java GC, even though these objects are short
     * lived.</li>
     * </ul>
     *
     * <p>Mutation of the key representation between the default and compact
     * approaches is automatic on a per-Btree internal node basis.  For
     * example, if a key that exceeds the configured length is added to a node
     * that uses the compact representation, the node is automatically
     * mutated to the default representation.  A best effort is made to
     * prevent frequent mutations that could increase Java GC activity.</p>
     *
     * <p>To determine how often the compact representation is used in a
     * running application, see {@link EnvironmentStats#getNINCompactKeyIN}.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>42</td>
     * <td>0</td>
     * <td>256</td>
     * </tr>
     * </table>
     *
     * @see DatabaseConfig#setKeyPrefixing
     * @see EnvironmentStats#getNINCompactKeyIN
     *
     * @see <a href="EnvironmentStats.html#cacheSizeOptimizations">Cache
     * Statistics: Size Optimizations</a>
     *
     * @since 5.0
     */
    public static final String TREE_COMPACT_MAX_KEY_LENGTH =
        "je.tree.compactMaxKeyLength";

    /**
     * If true (the default), a secondary integrity failure will set the
     * {@link SecondaryDatabase} handle to corrupted, preventing it from being
     * used until it is re-opened.
     *
     * <p>Note that when a secondary integrity error is detected by explicit
     * verification ({@link Environment#verify} or {@link Database#verify}),
     * the secondary database state is not set to corrupted.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String TREE_SECONDARY_INTEGRITY_FATAL =
        "je.tree.secondaryIntegrityFatal";

    /*
     * Below are measurements justifying the use of 42 as a default value for
     * TREE_COMPACT_MAX_KEY_LENGTH.
     *
     *  KeySize NonCompactGB CompactGB %Decrease
     *  20      7.04         5.48      22
     *  30      7.85         6.93      12
     *  32      7.85         7.23       8
     *  34      8.66         7.52      13
     *  36      8.66         7.81      10
     *  38      8.66         8.10       6
     *  40      8.66         8.39       3
     *  42      9.47         8.68       8
     *  44      9.47         8.97       5
     *  46      9.47         9.26       2
     *  48      9.47         9.47       0
     *  50      10.3         9.84       4
     *  52      10.3         10.1       2
     */

    /**
     * The compressor thread wakeup interval in microseconds.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>5 s</td>
     * <td>1 s</td>
     * <td>75 min</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String COMPRESSOR_WAKEUP_INTERVAL =
        "je.compressor.wakeupInterval";

    /**
     * The number of times to retry a compression run if a deadlock occurs.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>3</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String COMPRESSOR_DEADLOCK_RETRY =
        "je.compressor.deadlockRetry";

    /**
     * The lock timeout for compressor transactions in microseconds.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>500 ms</td>
     * <td>0</td>
     * <td>75 min</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String COMPRESSOR_LOCK_TIMEOUT =
        "je.compressor.lockTimeout";

    /**
     * When eviction occurs, the evictor will push memory usage to this number
     * of bytes below {@link #MAX_MEMORY}.  No more than 50% of je.maxMemory
     * will be evicted per eviction cycle, regardless of this setting.
     *
     * <p>When using the shared cache feature, the value of this property is
     * applied the first time the cache is set up. New environments that
     * join the cache do not alter the cache setting.</p>
     *
     * <p>This parameter impacts
     * <a href="EnvironmentStats.html#cacheEviction">how often background
     * evictor threads are awoken</a> as well as the size of latency spikes
     * caused by
     * <a href="EnvironmentStats.html#cacheCriticalEviction">critical
     * eviction</a>.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>No</td>
     * <td>524288 (512K)</td>
     * <td>1024 (1K)</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheEviction">Cache Statistics:
     * Eviction</a>
     *
     * @see <a href="EnvironmentStats.html#cacheCriticalEviction">Cache
     * Statistics: Critical Eviction</a>
     */
    public static final String EVICTOR_EVICT_BYTES = "je.evictor.evictBytes";

    /**
     * @deprecated as of JE 6.0. This parameter is ignored by the new, more
     * efficient and more accurate evictor.
     *
     * As we move to JE 24.2, this parameter should be removed, however, this
     * one is used in kv/tests/standalone/ycsb/je.properties
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static final String EVICTOR_NODES_PER_SCAN =
            "je.evictor.nodesPerScan";

    /**
     * At this percentage over the allotted cache, critical eviction will
     * start.  For example, if this parameter is 5, then when the cache size is
     * 5% over its maximum or 105% full, critical eviction will start.
     * <p>
     * Critical eviction is eviction performed in application threads as part
     * of normal database access operations.  Background eviction, on the other
     * hand, is performed in JE evictor threads as well as during log cleaning
     * and checkpointing.  Background eviction is unconditionally started when
     * the cache size exceeds its maximum.  When critical eviction is also
     * performed (concurrently with background eviction), it helps to ensure
     * that the cache size does not continue to grow, but can have a negative
     * impact on operation latency.
     * <p>
     * By default this parameter is zero, which means that critical eviction
     * will start as soon as the cache size exceeds its maximum.  Some
     * applications may wish to set this parameter to a non-zero value to
     * improve operation latency, when eviction is a significant performance
     * factor and latency requirements are not being satisfied.
     * <p>
     * When setting this parameter to a non-zero value, for example 5, be sure
     * to reserve enough heap memory for the cache size to be over its
     * configured maximum, for example 105% full.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>0</td>
     * <td>0</td>
     * <td>1000</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheCriticalEviction">Cache
     * Statistics: Critical Eviction</a>
     */
    public static final String EVICTOR_CRITICAL_PERCENTAGE =
        "je.evictor.criticalPercentage";

    /**
     * @deprecated  as of JE 6.0. This parameter is ignored by the new,
     * more efficient and more accurate evictor.
     * As we move to JE 24.2, this parameter should be removed, however, this
     * one is used in kv/tests/standalone/ycsb/je.properties
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static final String EVICTOR_LRU_ONLY = "je.evictor.lruOnly";

    /**
     * The number of LRU lists in the JE cache.
     *
     * <p>Ideally, all nodes managed by an LRU eviction policy should appear in
     * a single LRU list, ordered by the "hotness" of each node. However,
     * such a list is accessed very frequently by multiple threads, and can
     * become a synchronization bottleneck. To avoid this problem, the
     * evictor can employ multiple LRU lists. The nLRULists parameter
     * specifies the number of LRU lists to be used. Increasing the number
     * of LRU lists alleviates any potential synchronization bottleneck, but
     * it also decreases the quality of the LRU approximation.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>4</td>
     * <td>1</td>
     * <td>32</td>
     * </tr>
     * </table>
     *
     * @see <a href="#cacheLRUListContention">Cache Statistics: LRU List
     * Contention</a>
     */
    public static final String EVICTOR_N_LRU_LISTS = "je.evictor.nLRULists";

    /**
     * Call Thread.yield() at each check for cache overflow. This potentially
     * improves GC performance, but little testing has been done and the actual
     * benefit is unknown.
     *
     * <p>When using the shared cache feature, the value of this property is
     * applied the first time the cache is set up. New environments that
     * join the cache do not alter the cache setting.</p>
     *
     * <p>This param is unlikely to be needed for tuning, but is sometimes
     * useful for debugging and testing.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheDebugging">Cache Statistics:
     * Debugging</a>
     */
    public static final String EVICTOR_FORCED_YIELD = "je.evictor.forcedYield";

    /**
     * The minimum number of threads in the eviction thread pool.
     * <p>
     * These threads help keep memory usage within cache bounds, offloading
     * work from application threads.
     * <p>
     * {@link #EVICTOR_CORE_THREADS}, {@link #EVICTOR_MAX_THREADS} and {@link
     * #EVICTOR_KEEP_ALIVE} are used to configure the core, max and keepalive
     * attributes for the {@link java.util.concurrent.ThreadPoolExecutor} which
     * implements the eviction thread pool.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>yes</td>
     * <td>1</td>
     * <td>0</td>
     * <td>Integer.MAX_VALUE</td>
     * </tr>
     * </table>
     */
    public static final String EVICTOR_CORE_THREADS = "je.evictor.coreThreads";

    /**
     * The maximum number of threads in the eviction thread pool.
     * <p>
     * These threads help keep memory usage within cache bound, offloading work
     * from application threads. If the eviction thread pool receives more
     * work, it will allocate up to this number of threads. These threads will
     * terminate if they are idle for more than the time indicated by {@link
     * #EVICTOR_KEEP_ALIVE}.
     * <p>
     * {@link #EVICTOR_CORE_THREADS}, {@link #EVICTOR_MAX_THREADS} and {@link
     * #EVICTOR_KEEP_ALIVE} are used to configure the core, max and keepalive
     * attributes for the {@link java.util.concurrent.ThreadPoolExecutor} which
     * implements the eviction thread pool.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>yes</td>
     * <td>10</td>
     * <td>1</td>
     * <td>Integer.MAX_VALUE</td>
     * </tr>
     * </table>
     */
    public static final String EVICTOR_MAX_THREADS = "je.evictor.maxThreads";

    /**
     * The duration that excess threads in the eviction thread pool will stay
     * idle; after this period, idle threads will terminate.
     * <p>
     * {@link #EVICTOR_CORE_THREADS}, {@link #EVICTOR_MAX_THREADS} and {@link
     * #EVICTOR_KEEP_ALIVE} are used to configure the core, max and keepalive
     * attributes for the {@link java.util.concurrent.ThreadPoolExecutor} which
     * implements the eviction thread pool.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>10 min</td>
     * <td>1 s</td>
     * <td>1 d</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String EVICTOR_KEEP_ALIVE = "je.evictor.keepAlive";

    /**
     * Allow Bottom Internal Nodes (BINs) to be written in a delta format
     * during eviction. Using a delta format will improve write and log
     * cleaning performance. There is no known performance benefit to setting
     * this parameter to false.
     *
     * <p>This param is unlikely to be needed for tuning, but is sometimes
     * useful for debugging and testing.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cacheDebugging">Cache Statistics:
     * Debugging</a>
     */
    public static final String EVICTOR_ALLOW_BIN_DELTAS =
        "je.evictor.allowBinDeltas";

    /**
     * Ask the checkpointer to run every time we write this many bytes to the
     * log.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>No</td>
     * <td>20000000 (20M)</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="#cleanerEfficiency">Cleaning Efficiency</a>
     */
    public static final String CHECKPOINTER_BYTES_INTERVAL =
        "je.checkpointer.bytesInterval";

    /**
     * The number of times to retry a checkpoint if it runs into a deadlock.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>3</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String CHECKPOINTER_DEADLOCK_RETRY =
        "je.checkpointer.deadlockRetry";

    /**
     * If true, the checkpointer uses more resources in order to complete the
     * checkpoint in a shorter time interval.  Btree latches are held and other
     * threads are blocked for a longer period.  When set to true, application
     * response time may be longer during a checkpoint.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String CHECKPOINTER_HIGH_PRIORITY =
        "je.checkpointer.highPriority";

    /**
     * The cleaner will keep the total disk space utilization percentage above
     * this value.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>50</td>
     * <td>0</td>
     * <td>90</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cleanerUtil">Cleaning Statistics:
     * Utilization</a>
     */
    public static final String CLEANER_MIN_UTILIZATION =
        "je.cleaner.minUtilization";

    /**
     * A log file will be cleaned if its utilization percentage is below this
     * value, irrespective of total utilization. For some workloads this
     * allows maintaining a overall utilization that is higher than
     * {@link #CLEANER_MIN_UTILIZATION}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>5</td>
     * <td>0</td>
     * <td>50</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cleanerUtil">Cleaning Statistics:
     * Utilization</a>
     */
    public static final String CLEANER_MIN_FILE_UTILIZATION =
        "je.cleaner.minFileUtilization";

    /**
     * The cleaner checks disk utilization every time we write this many bytes
     * to the log.  If zero (and by default) it is set to either the {@link
     * #LOG_FILE_MAX} value divided by four, or to 100 MB, whichever is
     * smaller.
     *
     * <p>When overriding the default value, use caution to ensure that the
     * cleaner is woken frequently enough, so that reserved files are deleted
     * quickly enough to avoid violating a disk limit.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Long</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see #CLEANER_WAKEUP_INTERVAL
     */
    public static final String CLEANER_BYTES_INTERVAL =
        "je.cleaner.bytesInterval";

    /**
     * The cleaner checks whether cleaning is needed if this interval elapses
     * without any writing, to handle the case where cleaning or checkpointing
     * is necessary to reclaim disk space, but writing has stopped. This
     * addresses the problem that {@link #CLEANER_BYTES_INTERVAL} may not cause
     * cleaning, and {@link #CHECKPOINTER_BYTES_INTERVAL} may not cause
     * checkpointing, when enough writing has not occurred to exceed these
     * intervals.
     *
     * <p>If this parameter is set to zero, the cleaner wakeup interval is
     * disabled, and cleaning and checkpointing will occur only via {@link
     * #CLEANER_BYTES_INTERVAL}, {@link #CHECKPOINTER_BYTES_INTERVAL}
     *
     * <p>For example, if a database were removed or truncated, or large
     * records were deleted, the amount written to the log may not exceed
     * CLEANER_BYTES_INTERVAL. If writing were to stop at that point, no
     * cleaning would occur, if it were not for the wakeup interval.</p>
     *
     * <p>In addition, even when cleaning is performed, a checkpoint is
     * additionally needed to reclaim disk space. This may not occur if
     * {@link #CHECKPOINTER_BYTES_INTERVAL} does not happen to cause a
     * checkpoint after write operations have stopped. If files have been
     * cleaned and a checkpoint is needed to reclaim space, and write
     * operations have stopped, a checkpoint will be scheduled when the
     * CLEANER_WAKEUP_INTERVAL elapses. The checkpoint will be performed in the
     * JE checkpointer thread if it is not disabled, or when
     * {@link Environment#checkpoint} is called.</p>
     *
     * <p>In test environments it is fairly common for application writing to
     * stop, and then to expect cleaning to occur as a result of the last set
     * of operations. This situation may also arise in production environments,
     * for example, during repair of an out-of-disk situation.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>10 s</td>
     * <td>0</td>
     * <td>10 h</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @see #CLEANER_BYTES_INTERVAL
     *
     * @since 7.1
     */
    public static final String CLEANER_WAKEUP_INTERVAL =
        "je.cleaner.wakeupInterval";

    /**
     * If true, the cleaner will fetch records to determine their size and more
     * accurately calculate log utilization.  Normally when a record is updated
     * or deleted without first being read (sometimes called a blind
     * delete/update), the size of the previous version of the record is
     * unknown and therefore the cleaner's utilization calculations may be
     * incorrect.  Setting this parameter to true will cause a record to be
     * read during a blind delete/update, in order to determine its size.  This
     * will ensure that the cleaner's utilization calculations are correct, but
     * will cause more (potentially random) IO.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_FETCH_OBSOLETE_SIZE =
        "je.cleaner.fetchObsoleteSize";

    /**
     * The number of times to retry cleaning if a deadlock occurs.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>3</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_DEADLOCK_RETRY =
        "je.cleaner.deadlockRetry";

    /**
     * The lock timeout for cleaner transactions in microseconds.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>500 ms</td>
     * <td>0</td>
     * <td>75 min</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String CLEANER_LOCK_TIMEOUT = "je.cleaner.lockTimeout";

    /**
     * If true (the default setting), the cleaner deletes log files after
     * successful cleaning.
     *
     * This parameter may be set to false for diagnosing log cleaning problems.
     * For example, if a bug causes a LOG_FILE_NOT_FOUND exception, when
     * reproducing the problem it is often necessary to avoid deleting files so
     * they can be used for diagnosis. When this parameter is false:
     * <ul>
     *     <li>
     *     Rather than delete files that are successfully cleaned, the cleaner
     *     renames them.
     *     </li>
     *     <li>
     *     When renaming a file, its extension is changed from ".jdb" to ".del"
     *     and its last modification date is set to the current time.
     *     </li>
     *     <li>
     *     Depending on the setting of the {@link #CLEANER_USE_DELETED_DIR}
     *     parameter, the file is either renamed in its current data directory
     *     (the default), or moved into the "deleted" sub-directory.
     *     </li>
     * </ul>
     * <p>
     * When this parameter is set to false, disk usage may grow without bounds
     * and the application is responsible for removing the cleaned files. It
     * may be necessary to write a script for deleting the least recently
     * cleaned files when disk usage is low. The .del extension and the last
     * modification time can be leveraged to write such a script. The "deleted"
     * sub-directory can be used to avoid granting write or delete permissions
     * for the main data directory to the script.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>true</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_EXPUNGE = "je.cleaner.expunge";

    /**
     * When {@link #CLEANER_EXPUNGE} is false, the {@code
     * CLEANER_USE_DELETED_DIR} parameter determines whether successfully
     * cleaned files are moved to the "deleted" sub-directory.
     *
     * {@code CLEANER_USE_DELETED_DIR} applies only when {@link
     * #CLEANER_EXPUNGE} is false. When {@link #CLEANER_EXPUNGE} is true,
     * successfully cleaned files are deleted and the {@code
     * CLEANER_USE_DELETED_DIR} parameter setting is ignored.
     * <p>
     * When {@code CLEANER_USE_DELETED_DIR} is true (and {@code
     * CLEANER_EXPUNGE} is false), the cleaner will move successfully cleaned
     * data files (".jdb" files) to the "deleted" sub-directory of the
     * Environment directory, in addition to changing the file extension to
     * "*.del". In this case, the "deleted" sub-directory must have been
     * created by the application before opening the Environment. This allows
     * the application to control permissions on this sub-directory. Note
     * that {@link java.io.File#renameTo(File)} is used to move the file, and
     * this method may or may not support moving the file to a different volume
     * (when the "deleted" directory is a file system link) on a particular
     * platform.
     * <p>
     * When {@code CLEANER_USE_DELETED_DIR} is false (and {@code
     * CLEANER_EXPUNGE} is false), the cleaner will change the file extension
     * of successfully cleaned data files from ".jdb" to ".del", but will not
     * move the files to a different directory.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_USE_DELETED_DIR =
        "je.cleaner.useDeletedDir";

    /**
     * The minimum age of a file (number of files between it and the active
     * file) to qualify it for cleaning under any conditions.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>2</td>
     * <td>2</td>
     * <td>1000</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_MIN_AGE = "je.cleaner.minAge";

    /**
     * The read buffer size for cleaning.  If zero (the default), then {@link
     * #LOG_ITERATOR_READ_SIZE} value is used.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>0</td>
     * <td>128</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="#cleanerEfficiency">Cleaning Efficiency</a>
     */
    public static final String CLEANER_READ_SIZE = "je.cleaner.readSize";

    /**
     * Tracking of detailed cleaning information will use no more than this
     * percentage of the cache.  The default value is 2% of {@link
     * #MAX_MEMORY}. If 0 and {@link #SHARED_CACHE} is true, use 2% divided by
     * N where N is the number of environments sharing the global cache.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>2</td>
     * <td>1</td>
     * <td>90</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE =
        "je.cleaner.detailMaxMemoryPercentage";

    /**
     * Specifies a list of files or file ranges to be cleaned at a time when no
     * other log cleaning is necessary.  This parameter is intended for use in
     * forcing the cleaning of a large number of log files.  File numbers are
     * in hex and are comma separated or hyphen separated to specify ranges,
     * e.g.: '9,a,b-d' will clean 5 files.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_FORCE_CLEAN_FILES =
        "je.cleaner.forceCleanFiles";

    /**
     * All log files having a log version prior to the specified version will
     * be cleaned at a time when no other log cleaning is necessary.  Intended
     * for use in upgrading old format log files forward to the current log
     * format version, e.g., to take advantage of format improvements; note
     * that log upgrading is optional.  The default value zero (0) specifies
     * that no upgrading will occur.  The value negative one (-1) specifies
     * upgrading to the current log version.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>0</td>
     * <td>-1</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String CLEANER_UPGRADE_TO_LOG_VERSION =
        "je.cleaner.upgradeToLogVersion";

    /**
     * The number of threads allocated by the cleaner for log file processing.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>1</td>
     * <td>1</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cleanerUtil">Cleaning Statistics:
     * Utilization</a>
     */
    public static final String CLEANER_THREADS = "je.cleaner.threads";

    /**
     * The look ahead cache size for cleaning in bytes.  Increasing this value
     * can reduce the number of Btree lookups.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>8192 (8K)</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentStats.html#cleanerProcessing">Cleaning
     * Statistics: Processing Details</a>
     */
    public static final String CLEANER_LOOK_AHEAD_CACHE_SIZE =
        "je.cleaner.lookAheadCacheSize";

    /**
     * Number of Lock Tables.  Set this to a value other than 1 when an
     * application has multiple threads performing concurrent JE operations.
     * It should be set to a prime number, and in general not higher than the
     * number of application threads performing JE operations.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>1</td>
     * <td>1</td>
     * <td>32767 (32K)</td>
     * </tr>
     * </table>
     */
    public static final String LOCK_N_LOCK_TABLES = "je.lock.nLockTables";

    /**
     * Configures the default lock timeout. It may be overridden on a
     * per-transaction basis by calling
     * {@link Transaction#setLockTimeout(long, TimeUnit)}.
     *
     * <p>A value of zero disables lock timeouts. This is not recommended, even
     * when the application expects that deadlocks will not occur or will be
     * easily resolved. A lock timeout is a fall-back that guards against
     * unexpected "live lock", unresponsive threads, or application failure to
     * close a cursor or to commit or abort a transaction.</p>
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>500 ms</td>
     * <td>0</td>
     * <td>75 min</td>
     * </tr>
     * </table>
     *
     * @see #setLockTimeout(long,TimeUnit)
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String LOCK_TIMEOUT = "je.lock.timeout";

    /**
     * Whether to perform deadlock detection when a lock conflict occurs.
     * By default, deadlock detection is enabled (this parameter is true) in
     * order to reduce thread wait times when there are deadlocks.
     * <p>
     * Deadlock detection is performed as follows.
     * <ol>
     *   <li>When a lock is requested by a record read or write operation, JE
     *       checks for lock conflicts with another transaction or another
     *       thread performing a non-transactional operation. If there is no
     *       conflict, the lock is acquired and the operation returns
     *       normally.</li>
     *   <li>When there is a conflict, JE performs deadlock detection. However,
     *       before performing deadlock detection, JE waits for the
     *       {@link #LOCK_DEADLOCK_DETECT_DELAY} interval, if it is non-zero.
     *       This delay is useful for avoiding the overhead of deadlock
     *       detection when normal, short-lived contention (not a deadlock) is
     *       the reason for the conflict. If the lock is acquired during the
     *       delay, the thread wakes up and the operation returns
     *       normally.</li>
     *   <li>If a deadlock is detected, {@link DeadlockException} is thrown in
     *       one of the threads participating in the deadlock, called the
     *       "victim". The victim is chosen at random to prevent a repeated
     *       pattern of deadlocks, called "live lock". A non-victim thread that
     *       detects a deadlock will notify the victim and perform short
     *       delays, waiting for the deadlock to be broken; if the lock is
     *       acquired, the operation returns normally.</li>
     *   <li>It is possible for live lock to occur in spite of using random
     *       victim selection. It is also possible that a deadlock is not
     *       broken because the victim thread is unresponsive or the
     *       application fails to close a cursor or to commit or abort a
     *       transaction. In these cases, if the lock or transaction timeout
     *       expires without acquiring the lock, a {@code DeadlockException} is
     *       thrown for the last deadlock detected, in the thread that detected
     *       the deadlock. In this case, {@code DeadlockException} may be
     *       thrown by more than one thread participating in the deadlock.
     *       </li>
     *   <li>When no deadlock is detected, JE waits for the lock or transaction
     *       timeout to expire. If the lock is acquired during this delay, the
     *       thread wakes up and the operation returns normally.</li>
     *   <li>When the lock or transaction timeout expires without acquiring the
     *       lock, JE checks for deadlocks one final time. If a deadlock is
     *       detected, {@code DeadlockException} is thrown; otherwise,
     *       {@link LockTimeoutException} or
     *       {@link TransactionTimeoutException}is thrown.</li>
     * </ol>
     * <p>
     * Deadlock detection may be disabled (by setting this parameter to false)
     * in applications that are known to be free of deadlocks, and this may
     * provide a slight performance improvement in certain scenarios. However,
     * this is not recommended because deadlock-free operation is difficult to
     * guarantee. If deadlock detection is disabled, JE skips steps 2, 3 and 4
     * above. However, deadlock detection is always performed in the last step,
     * and {@code DeadlockException} may be thrown.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>true</td>
     * </tr>
     * </table>
     *
     * @since 7.1
     */
    public static final String LOCK_DEADLOCK_DETECT = "je.lock.deadlockDetect";

    /**
     * The delay after a lock conflict, before performing deadlock detection.
     *
     * This delay is used to avoid the overhead of deadlock detection when
     * normal contention (not a deadlock) is the reason for the conflict. See
     * {@link #LOCK_DEADLOCK_DETECT} for more information.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>0</td>
     * <td>0</td>
     * <td>75 min</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @since 7.1
     */
    public static final String LOCK_DEADLOCK_DETECT_DELAY =
        "je.lock.deadlockDetectDelay";

    /**
     * Configures the transaction timeout.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>0</td>
     * <td>0</td>
     * <td>75 min</td>
     * </tr>
     * </table>
     *
     * @see #setTxnTimeout
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String TXN_TIMEOUT = "je.txn.timeout";

    /**
     * Configures the default durability associated with transactions.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
      * <td>String</td>
     * <td>Yes</td>
     * <td>null</td>
     * </tr>
     * </table>
     *
     * The format of the durability string is described at
     * {@link Durability#parse(String)}
     *
     * @see Durability
     * @see #setDurability
     */
    public static final String TXN_DURABILITY = "je.txn.durability";

    /**
     * Set this parameter to true to add stacktrace information to deadlock
     * (lock timeout) exception messages.  The stack trace will show where each
     * lock was taken.  The default is false, and true should only be used
     * during debugging because of the added memory/processing cost.  This
     * parameter is 'static' across all environments.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String TXN_DEADLOCK_STACK_TRACE =
        "je.txn.deadlockStackTrace";

    /**
     * Dump the lock table when a lock timeout is encountered, for debugging
     * assistance.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>false</td>
     * </tr>
     * </table>
     */
    public static final String TXN_DUMP_LOCKS = "je.txn.dumpLocks";


    /**
     * The maximum number of transactions that can be grouped to amortize the
     * cost of a fsync when a transaction commits with SyncPolicy#SYNC as a
     * non replicated one. A value of zero effectively turns off the group commit
     * optimization.
     * <p>
     * Specifying larger values can result in more transactions being grouped
     * together decreasing average commit times.
     * <p>
     * An fsync is issued if the size of the transaction group reaches the
     * maximum within the time period specified by
     * {@link #NON_REP_GROUP_COMMIT_INTERVAL}.
     * <p>
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * <td>Minimum</td>
     * <td>Maximum</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>No</td>
     * <td>200</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @since 25.1.4
     * @see #NON_REP_GROUP_COMMIT_INTERVAL
     */
    public static final String NON_REP_MAX_GROUP_COMMIT =
        "je.txn.nonRepMaxGroupCommit";

    /**
     * The time interval during which transactions may be grouped to amortize
     * the cost of fsync when a transaction commits with SyncPolicy#SYNC as a
     * non replicated one. This parameter is only meaningful if the
     * {@link #NON_REP_MAX_GROUP_COMMIT group commit size} is greater than one.
     * <p>
     * The first (as ordered by transaction serialization) transaction in a
     * transaction group may be delayed by at most this amount. Subsequent
     * transactions in the group will have smaller delays since they are later
     * in the serialization order.
     * <p>
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * <td>Minimum</td>
     * <td>Maximum</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>3 ms</td>
     * <td>0</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @since 25.1.4
     * @see #NON_REP_MAX_GROUP_COMMIT
     */
    public static final String NON_REP_GROUP_COMMIT_INTERVAL =
        "je.txn.nonRepGroupCommitInterval";

    /**
     * @deprecated As of JE 4.0, use the standard java.util.logging
     * configuration methodologies. To set the FileHandler output file count,
     * set com.sleepycat.je.util.FileHandler.count = {@literal <NUMBER>}
     * through the java.util.logging configuration file, or through the
     * java.util.logging.LogManager.
     * As we move to JE 24.2, this parameter should be removed, however, this
     * one is used in kv/tests/stress_test/logging.properties
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static final String TRACE_FILE_COUNT =
            "java.util.logging.FileHandler.count";

    /**
     * @deprecated As of JE 4.0, use the standard java.util.logging
     * configuration methodologies. To set the FileHandler output file size,
     * set com.sleepycat.je.util.FileHandler.limit = {@literal <NUMBER>}
     * through the java.util.logging configuration file, or through the
     * java.util.logging.LogManager.
     * As we move to JE 24.2, this parameter should be removed, however, this
     * one is used in kv/tests/stress_test/logging.properties
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static final String TRACE_FILE_LIMIT =
            "java.util.logging.FileHandler.limit";

    /**
     * Trace messages equal and above this level will be logged to the
     * console. Value should be one of the predefined
     * java.util.logging.Level values.
     * <p>
     * Setting this parameter in the je.properties file or through {@link
     * EnvironmentConfig#setConfigParam} is analogous to setting
     * the property in the java.util.logging properties file or MBean.
     * It is preferred to use the standard java.util.logging mechanisms for
     * configuring java.util.logging.Handler, but this JE parameter is provided
     * because the java.util.logging API doesn't provide a method to set
     * handler levels programmatically.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"OFF"</td>
     * </tr>
     * </table>
     * @see <a href="{@docRoot}/../GettingStartedGuide/managelogging.html"
     * target="_top">Chapter 12. Logging</a>
     */
    public static final String CONSOLE_LOGGING_LEVEL =
        "com.sleepycat.je.util.ConsoleHandler.level";

    /**
     * Trace messages equal and above this level will be logged to the je.info
     * file, which is in the Environment home directory.  Value should
     * be one of the predefined java.util.logging.Level values.
     * <p>
     * Setting this parameter in the je.properties file or through {@link
     * EnvironmentConfig#setConfigParam} is analogous to setting
     * the property in the java.util.logging properties file or MBean.
     * It is preferred to use the standard java.util.logging mechanisms for
     * configuring java.util.logging.Handler, but this JE parameter is provided
     * because the java.util.logging APIs doesn't provide a method to set
     * handler levels programmatically.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"INFO"</td>
     * </tr>
     * </table>
     * @see <a href="{@docRoot}/../GettingStartedGuide/managelogging.html"
     * target="_top">Chapter 12. Logging</a>
     */
    public static final String FILE_LOGGING_LEVEL =
        "com.sleepycat.je.util.FileHandler.level";

    /**
     * If environment startup exceeds this duration, startup statistics are
     * logged and can be found in the je.info file.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>5 min</td>
     * <td>0</td>
     * <td>none</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String STARTUP_DUMP_THRESHOLD =
        "je.env.startupThreshold";

    /**
     * If true collect and log statistics. The statistics are logged in CSV
     * format and written to the log file at a user specified interval.
     * The logging occurs per-Environment when the Environment is opened
     * in read/write mode. Statistics are written to a filed named je.stat.csv.
     * Successively older files are named by adding "0", "1", "2", etc into
     * the file name. The file name format is je.stat.[version number].csv.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>True</td>
     * <td>0</td>
     * <td>none</td>
     * </tr>
     * </table>
     */
    public static final String STATS_COLLECT =
        "je.stats.collect";

    /**
     * Maximum number of statistics log files to retain. The rotating set of
     * files, as each file reaches a given size limit, is closed, rotated out,
     * and a new file opened. The name of the log file is je.stat.csv.
     * Successively older files are named by adding "0", "1", "2", etc into
     * the file name. The file name format is je.stat.[version number].csv.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>10</td>
     * <td>1</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String STATS_MAX_FILES =
        "je.stats.max.files";

    /**
     * Log file maximum row count for Stat collection. When the number of
     * rows in the statistics file reaches the maximum row count, the file
     * is closed, rotated out, and a new file opened. The name of the log
     * file is je.stat.csv. Successively older files are named by adding "0",
     * "1", "2", etc into the file name. The file name format is
     * je.stat.[version number].csv.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>1440</td>
     * <td>1</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     */
    public static final String STATS_FILE_ROW_COUNT =
        "je.stats.file.row.count";

    /**
     * The duration of the statistics capture interval. Statistics are captured
     * and written to the log file at this interval.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>Yes</td>
     * <td>1 min</td>
     * <td>1 s</td>
     * <td>24 d</td>
     * </tr>
     * </table>
     *
     * @see <a href="EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String STATS_COLLECT_INTERVAL =
        "je.stats.collect.interval";

    /**
     * The directory to save the statistics log file.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>NULL-&gt; Environment home directory</td>
     * </tr>
     * </table>
     */
    public static final String STATS_FILE_DIRECTORY =
        "je.stats.file.directory";

    /**
     * The file name prefix used for JE trace files (je.info files) and
     * statistics (je.stat.csv and je.config.csv) files.
     * <p>
     * The advantage of using this parameter is that the trace and statistics
     * files for multiple JE environments can be stored in a single directory,
     * as long as the specified prefix makes the file names unique.
     * <p>
     * If this parameter is an empty string or is not specified, then no
     * prefix is used.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table>
     * @see EnvironmentConfig#FILE_LOGGING_DIRECTORY
     *
     * @since 18.1
     */
    public static final String FILE_LOGGING_PREFIX =
        "je.file.logging.prefix";

    /**
     * The directory used for JE trace files (je.info files) and statistics
     * (je.stat.csv and je.config.csv) files.
     * <p>
     * The advantage of using this parameter is that the data files can be
     * stored on a storage device that is separate from the storage device
     * used for trace and statistics files.
     * <p>
     * If this parameter is not specified, the envHome parameter is used for
     * trace files. And for statistics files, if this parameter and
     *  {@link EnvironmentConfig#STATS_FILE_DIRECTORY} is not specified, the
     *  envHome is used for statistics file.
     * <p>
     * If this parameter is specified, the directory must exist. If it does
     * not exist, JE will throw {@link IllegalArgumentException} when the
     * Environment is opened.
     * <p>
     * Note that for statistics file, the config
     *  {@link EnvironmentConfig#STATS_FILE_DIRECTORY} takes precedence.<br>
     *
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>NULL-&gt; Environment home directory</td>
     * </tr>
     * </table>
     *
     * @see EnvironmentConfig#FILE_LOGGING_PREFIX
     * @since 18.1
     */
    public static final String FILE_LOGGING_DIRECTORY =
        "je.file.logging.directory";

    /**
     * @hidden For internal use: automatic backups
     *
     * Whether to perform backups automatically.
     *
     * <p>If true (the default is false), performs backups on the schedule
     * specified by {@link #BACKUP_SCHEDULE}, copying files as specified by
     * {@link #BACKUP_COPY_CLASS} and {@link #BACKUP_LOCATION_CLASS}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>No</td>
     * <td>false</td>
     * </tr>
     * </table>
     *
     * @see #BACKUP_SCHEDULE
     * @see #BACKUP_COPY_CLASS
     * @see #BACKUP_LOCATION_CLASS
     */
    public static final String ENV_RUN_BACKUP = "je.env.runBackup";

    /**
     * @hidden For internal use: automatic backups
     *
     * Information in crontab format that specifies the schedule for when to
     * perform automatic backups.
     *
     * <p>Backups are performed automatically according to the specified
     * schedule if enabled by {@link #ENV_RUN_BACKUP}.
     *
     * <p>For information about crontab format, see
     * <a href="https://en.wikipedia.org/wiki/Cron#Configuration_file">
     * https://en.wikipedia.org/wiki/Cron#Configuration_file</a>.
     *
     * <p>The specified times and dates are interpreted in the UTC time zone.
     *
     * <p>The format of the crontab entry is limited as follows. The minute
     * must be 0, the hour value may be in the range of 0 to 23 inclusive or *,
     * the day of month and month must both be *, and the day of week may be in
     * the range of 0 to 6 inclusive or *, but must be * if the hour value is
     * *. These restrictions mean that backups may be scheduled to be performed
     * either every hour, once a day at a particular hour, or once a week on a
     * particular day and at a particular hour.
     *
     * <p>When a backup is performed, the backup will create a subdirectory
     * under the environment home directory containing hard links to files
     * representing a complete snapshot as determined by calling {@link
     * com.sleepycat.je.util.DbBackup#getLogFilesInSnapshot}. Each snapshot
     * directory represents a full backup that can be used to reconstruct the
     * state of the environment at the time of the backup. The files in the new
     * snapshot directory that have not already been archived will then be
     * copied to an archive location using the mechanism specified by {@link
     * #BACKUP_COPY_CLASS} and the location specified by {@link
     * #BACKUP_LOCATION_CLASS}.
     *
     * <p>Backups will be performed at the specified times. If the environment
     * discovers on start up that backups are enabled and there is a pending
     * backup that has not been completed successfully, it will perform the
     * backup then.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"0 0 * * *"</td>
     * </tr>
     * </table>
     *
     * @see #ENV_RUN_BACKUP
     * @see #BACKUP_COPY_CLASS
     * @see #BACKUP_LOCATION_CLASS
     */
    public static final String BACKUP_SCHEDULE = "je.backup.schedule";

    /**
     * @hidden For internal use: automatic backups
     *
     * Performs an on demand backup at the given date and time, which must be
     * of the format "YYYY-MM-DD HH-mm-ss z", see
     * {@link java.text.SimpleDateFormat}.  The default value is null, meaning
     * that no on-demand backup is performed.
     *
     * <p>Usually backups are performed at a regular schedule, such as once a
     * day at 00:00:00 UTC, that is set using {@link #BACKUP_SCHEDULE}, but
     * this allows a backup to be performed at a specific date and time.
     *
     * <p>Only one backup date can be set at a time.  If a date is set for the
     * past then it will be silently ignored, as such make sure any time set is
     * far enough in the future to avoid this problem.
     *
     * <p>Note if the time given for the on-demand backup is around the same
     * time as a scheduled backup, only one backup will be performed at that
     * time.
     *
     * <p>To cancel an on-demand backup set the backup time to null or the
     * empty string "".
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>Yes</td>
     * <td>null</td>
     * </tr>
     * </table>
     *
     * @see #BACKUP_SCHEDULE
     */
    public static final String BACKUP_DATETIME = "je.backup.datetime";

    /**
     * @hidden For internal use: automatic backups
     *
     * The name of the class to use to copy files during an automatic backup.
     *
     * <p>The class is used to archive files created by automatic backups if
     * enabled by {@link #ENV_RUN_BACKUP}.
     *
     * <p>The default class copies snapshot files to a locally mounted
     * directory based on the contents of the configuration file specified by
     * {@link #BACKUP_COPY_CONFIG}.
     *
     * <p>The value should be the fully qualified name of a non-abstract class
     * that provides a public, no arguments constructor and that implements the
     * {@link BackupFileCopy} interface. The {@link BackupFileCopy#initialize}
     * method will be called on the created instance using the configuration
     * file specified by {@link #BACKUP_COPY_CONFIG}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"com.sleepycat.je.BackupFSArchiveCopy"</td>
     * </tr>
     * </table>
     *
     * @see #ENV_RUN_BACKUP
     * @see #BACKUP_COPY_CONFIG
     */
    public static final String BACKUP_COPY_CLASS = "je.backup.copyClass";

    /**
     * @hidden For internal use: automatic backups
     *
     * The pathname of the configuration file for the backup copy class.
     *
     * <p>The value is passed to the {@link BackupFileCopy#initialize} method
     * used to initialize an instance of the class specified by {@link
     * #BACKUP_COPY_CLASS}, and the required format of its contents depends on
     * the behavior of that class.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>""</td>
     * </tr>
     * </table>
     *
     * @see #BACKUP_COPY_CLASS
     */
    public static final String BACKUP_COPY_CONFIG = "je.backup.copyConfig";

    /**
     * @hidden For internal use: automatic backups
     *
     * The name of the class to use to determine archive locations when copying
     * files during an automatic backup.
     *
     * <p>The class is used to determine the archive URL associated with a file
     * in a backup snapshot created by automatic backups if enabled by {@link
     * #ENV_RUN_BACKUP}.
     *
     * <p>The default class creates {@code file:} URLs under the directory
     * specified by the value of the {@link #BACKUP_LOCATION_CONFIG} parameter,
     * and is suitable for use with the default copy class.
     *
     * <p>The value should be the fully qualified name of a non-abstract class
     * that provides a public constructor and that implements the {@link
     * BackupArchiveLocation} interface. The {@link
     * BackupArchiveLocation#initialize} method will be called on the created
     * instance using the configuration value specified by {@link
     * #BACKUP_LOCATION_CONFIG}.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"com.sleepycat.je.BackupFileLocation"</td>
     * </tr>
     * </table>
     *
     * @see #ENV_RUN_BACKUP
     * @see #BACKUP_LOCATION_CONFIG
     */
    public static final String BACKUP_LOCATION_CLASS =
        "je.backup.locationClass";

    /**
     * @hidden For internal use: automatic backups
     *
     * A pathname used to configuration the backup location class.
     *
     * <p>The value is passed to the {@link BackupArchiveLocation#initialize}
     * method to initialize an instance of the class specified by
     * {#BACKUP_LOCATION_CLASS}, and its meaning depends on the behavior of
     * that class.
     *
     * <p>The default value, suitable for use with the default location class,
     * specifies a location in the /tmp directory which will be the root
     * directory under which archive copies are stored. This value is only
     * suitable for simple testing and should be replaced by a pathname name
     * that represents a location where archive files can be stored safely.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>No</td>
     * <td>"/tmp/snapshots"</td>
     * </tr>
     * </table>
     *
     * @see #BACKUP_LOCATION_CLASS
     */
    public static final String BACKUP_LOCATION_CONFIG =
        "je.backup.locationConfig";

    /**
     * @hidden For internal use: restoremarker switch
     * to enable/disable the restoremarker
     * <p> The default value is false. Restoremarker creates a file indicating
     * a normal recovery is impossible. To make it easier for
     * troubleshooting, disable it to allow restarting the RN for some
     * persistent error.
     */
    public static final String RESTOREMARKER_ENABLE =
        "je.log.enableRestoreMarker";

    /**
     * For unit testing, to prevent using the utilization profile and
     * expiration profile DB.
     */
    private transient boolean createUP = true;
    private transient boolean createEP = true;

    /**
     * For unit testing, to prevent writing utilization data during checkpoint.
     */
    private transient boolean checkpointUP = true;

    private boolean allowCreate = false;

    /**
     * For unit testing, to set readCommitted as the default.
     */
    private transient boolean txnReadCommitted = false;

    private String nodeName = null;

    /**
     * The loggingHandler is an instance and cannot be serialized.
     */
    private transient Handler loggingHandler;

    private transient
       ProgressListener<RecoveryProgress> recoveryProgressListener;

    private transient TaskCoordinator taskCoordinator;

    private transient ClassLoader classLoader;

    private transient ExtinctionFilter extinctionFilter;

    private CustomStats customStats;

    /**
     * Creates an EnvironmentConfig initialized with the system default
     * settings.
     */
    public EnvironmentConfig() {
        super();
    }

    /**
     * Creates an EnvironmentConfig which includes the properties specified in
     * the properties parameter.
     *
     * @param properties Supported properties are described in this class
     *
     * @throws IllegalArgumentException If any properties read from the
     * properties param are invalid.
     */
    public EnvironmentConfig(Properties properties)
        throws IllegalArgumentException {

        super(properties);
    }

    /**
     * If true, creates the database environment if it doesn't already exist.
     *
     * @param allowCreate If true, the database environment is created if it
     * doesn't already exist.
     *
     * @return this
     */
    public EnvironmentConfig setAllowCreate(boolean allowCreate) {

        this.allowCreate = allowCreate;
        return this;
    }

    /**
     * Returns a flag that specifies if we may create this environment.
     *
     * @return true if we may create this environment.
     */
    public boolean getAllowCreate() {

        return allowCreate;
    }

    /**
     * Convenience method for setting {@link EnvironmentConfig#LOCK_TIMEOUT}.
     *
     * @param timeout The lock timeout for all transactional and
     * non-transactional operations, or zero to disable lock timeouts.
     *
     * @param unit the {@code TimeUnit} of the timeout value. May be null only
     * if timeout is zero.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value of timeout is invalid
     *
     * @see EnvironmentConfig#LOCK_TIMEOUT
     * @see Transaction#setLockTimeout(long,TimeUnit)
     */
    public EnvironmentConfig setLockTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        DbConfigManager.setDurationVal(props, EnvironmentParams.LOCK_TIMEOUT,
                                       timeout, unit, validateParams);
        return this;
    }

    /**
     * Returns the lock timeout setting.
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * A value of 0 means no timeout is set.
     */
    public long getLockTimeout(TimeUnit unit) {

        return DbConfigManager.getDurationVal
            (props, EnvironmentParams.LOCK_TIMEOUT, unit);
    }

    /**
     * Convenience method for setting {@link EnvironmentConfig#ENV_READ_ONLY}.
     *
     * @param readOnly If true, configure the database environment to be read
     * only, and any attempt to modify a database will fail.
     *
     * @return this
     */
    public EnvironmentConfig setReadOnly(boolean readOnly) {

        DbConfigManager.setBooleanVal(props, EnvironmentParams.ENV_RDONLY,
                                      readOnly, validateParams);
        return this;
    }

    /**
     * Returns true if the database environment is configured to be read only.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the database environment is configured to be read only.
     */
    public boolean getReadOnly() {

        return DbConfigManager.getBooleanVal(props,
                                             EnvironmentParams.ENV_RDONLY);
    }

    /**
     * Convenience method for setting
     * {@link EnvironmentConfig#ENV_IS_TRANSACTIONAL}.
     *
     * @param transactional If true, configure the database environment for
     * transactions.
     *
     * @return this
     */
    public EnvironmentConfig setTransactional(boolean transactional) {

        DbConfigManager.setBooleanVal(props, EnvironmentParams.ENV_INIT_TXN,
                                      transactional, validateParams);
        return this;
    }

    /**
     * Returns true if the database environment is configured for transactions.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the database environment is configured for transactions.
     */
    public boolean getTransactional() {

        return DbConfigManager.getBooleanVal(props,
                                             EnvironmentParams.ENV_INIT_TXN);
    }

    /**
     * Convenience method for setting
     * {@link EnvironmentConfig#ENV_IS_LOCKING}.
     *
     * @param locking If false, configure the database environment for no
     * locking.  The default is true.
     *
     * @return this
     */
    public EnvironmentConfig setLocking(boolean locking) {

        DbConfigManager.setBooleanVal(props,
                                      EnvironmentParams.ENV_INIT_LOCKING,
                                      locking, validateParams);
        return this;
    }

    /**
     * Returns true if the database environment is configured for locking.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the database environment is configured for locking.
     */
    public boolean getLocking() {

        return DbConfigManager.getBooleanVal
            (props, EnvironmentParams.ENV_INIT_LOCKING);
    }

    /**
     * A convenience method for setting {@link EnvironmentConfig#TXN_TIMEOUT}.
     *
     * @param timeout The transaction timeout. A value of 0 turns off
     * transaction timeouts.
     *
     * @param unit the {@code TimeUnit} of the timeout value. May be null only
     * if timeout is zero.
     *
     * @return this
     *
     * @throws IllegalArgumentException If the value of timeout is negative
     *
     * @see EnvironmentConfig#TXN_TIMEOUT
     * @see Transaction#setTxnTimeout
     */
    public EnvironmentConfig setTxnTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        DbConfigManager.setDurationVal(props, EnvironmentParams.TXN_TIMEOUT,
                                       timeout, unit, validateParams);
        return this;
    }

    /**
     * A convenience method for getting {@link EnvironmentConfig#TXN_TIMEOUT}.
     *
     * <p>A value of 0 means transaction timeouts are not configured.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The transaction timeout.
     */
    public long getTxnTimeout(TimeUnit unit) {
        return DbConfigManager.getDurationVal
            (props, EnvironmentParams.TXN_TIMEOUT, unit);
    }

    /**
     * For unit testing, sets readCommitted as the default.
     */
    void setTxnReadCommitted(boolean txnReadCommitted) {

        this.txnReadCommitted = txnReadCommitted;
    }

    /**
     * For unit testing, to set readCommitted as the default.
     */
    boolean getTxnReadCommitted() {

        return txnReadCommitted;
    }

    /**
     * A convenience method for setting the
     * {@link EnvironmentConfig#SHARED_CACHE} parameter.
     *
     * @param sharedCache If true, the shared cache is used by this
     * environment.
     *
     * @return this
     */
    public EnvironmentConfig setSharedCache(boolean sharedCache) {

        DbConfigManager.setBooleanVal
            (props, EnvironmentParams.ENV_SHARED_CACHE, sharedCache,
             validateParams);
        return this;
    }

    /**
     * A convenience method for getting the
     * {@link EnvironmentConfig#SHARED_CACHE} parameter.
     *
     * @return true if the shared cache is used by this environment. @see
     * #setSharedCache
     */
    public boolean getSharedCache() {
        return DbConfigManager.getBooleanVal
            (props, EnvironmentParams.ENV_SHARED_CACHE);
    }

    /**
     * Sets the user defined nodeName for the Environment.  If set, exception
     * messages, logging messages, and thread names will have this nodeName
     * included in them.  If a user has multiple Environments in a single JVM,
     * setting this to a string unique to each Environment may make it easier
     * to diagnose certain exception conditions as well as thread dumps.
     *
     * @return this
     */
    public EnvironmentConfig setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    /**
     * Returns the user defined nodeName for the Environment.
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Sets the custom statistics object.
     *
     * @return this
     */
    public EnvironmentConfig setCustomStats(CustomStats customStats) {
        this.customStats = customStats;
        return this;
    }

    /**
     * Gets the custom statstics object.
     *
     * @return customStats
     */
    public CustomStats getCustomStats() {
        return customStats;
    }

    /**
     * Set a java.util.logging.Handler which will be used by all
     * java.util.logging.Loggers instantiated by this Environment. This lets
     * the application specify a handler which
     * <ul>
     * <li>requires a constructor with arguments</li>
     * <li>is specific to this environment, which is important if the
     * application is using multiple environments within the same process.
     * </ul>
     * Note that {@link Handler} is not serializable, and the logging
     * handler should be set within the same process.
     */
    public EnvironmentConfig setLoggingHandler(Handler handler) {
        loggingHandler = handler;
        return this;
    }

    /**
     * Returns the custom java.util.logging.Handler specified by the
     * application.
     */
    public Handler getLoggingHandler() {
        return loggingHandler;
    }

    /**
     * @hidden
     * Configures the environment to use this task coordinator to coordinate
     * JE and other application level housekeeping tasks.
     *
     * This parameter cannot be changed after the environment has been opened.
     *
     * @param taskCoordinator the coordinator
     */
    public EnvironmentConfig setTaskCoordinator
        (final TaskCoordinator taskCoordinator) {

        this.taskCoordinator = taskCoordinator;
        return this;
    }

    /**
     * @hidden
     * Returns the task coordinator to be used at environment startup. A null
     * return value indicates that task coordination is disabled.
     */
    public TaskCoordinator getTaskCoordinator() {
        return taskCoordinator;
    }

    /* Documentation inherited from EnvironmentMutableConfig.setConfigParam. */
    @Override
    public EnvironmentConfig setConfigParam(String paramName, String value)
        throws IllegalArgumentException {

        DbConfigManager.setConfigParam(props,
                                       paramName,
                                       value,
                                       false, /* requireMutablity */
                                       validateParams,
                                       false  /* forReplication */,
                                       true   /* verifyForReplication */);
        return this;
    }

    /**
     * Configure the environment to make periodic calls to a ProgressListener to
     * provide feedback on environment startup (recovery). The
     * ProgressListener.progress() method is called at different stages of
     * the recovery process. See {@link RecoveryProgress} for information about
     * those stages.
     * <p>
     * When using progress listeners, review the information at {@link
     * ProgressListener#progress} to avoid any unintended disruption to
     * environment startup.
     * @param progressListener The ProgressListener to callback during
     * environment startup (recovery).
     */
    public EnvironmentConfig setRecoveryProgressListener
        (final ProgressListener<RecoveryProgress> progressListener) {
        this.recoveryProgressListener = progressListener;
        return this;
    }

    /**
     * Return the ProgressListener to be used at this environment startup.
     */
    public ProgressListener<RecoveryProgress> getRecoveryProgressListener() {
        return recoveryProgressListener;
    }

    /**
     * Configure the environment to use a specified ClassLoader for loading
     * user-supplied classes by name.
     */
    public EnvironmentConfig setClassLoader(final ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    /**
     * Returns the ClassLoader for loading user-supplied classes by name, or
     * null if no specified ClassLoader is configured.
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Sets the {@link ExtinctionFilter filter} used for purging extinct
     * records.
     */
    public EnvironmentConfig setExtinctionFilter(
        final ExtinctionFilter filter) {

        this.extinctionFilter = filter;
        return this;
    }

     /**
      * Returns the {@link ExtinctionFilter filter} used for purging extinct
      * records.
      */
    public ExtinctionFilter getExtinctionFilter() {
        return extinctionFilter;
    }

    /**
     * For unit testing, to prevent use of the utilization profile DB.
     */
    void setCreateUP(boolean createUP) {
        this.createUP = createUP;
    }

    /**
     * For unit testing, to prevent use of the utilization profile DB.
     */
    boolean getCreateUP() {
        return createUP;
    }

    /**
     * For unit testing, to prevent use of the expiration profile DB.
     */
    void setCreateEP(boolean createUP) {
        this.createEP = createUP;
    }

    /**
     * For unit testing, to prevent use of the expiration profile DB.
     */
    boolean getCreateEP() {
        return createEP;
    }

    /**
     * For unit testing, to prevent writing utilization data during checkpoint.
     */
    void setCheckpointUP(boolean checkpointUP) {
        this.checkpointUP = checkpointUP;
    }

    /**
     * For unit testing, to prevent writing utilization data during checkpoint.
     */
    boolean getCheckpointUP() {
        return checkpointUP;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public EnvironmentConfig clone() {
        return (EnvironmentConfig) super.clone();
    }

    /**
     * Display configuration values.
     */
    @Override
    public String toString() {
        return " nodeName=" + nodeName +
            " allowCreate=" + allowCreate +
            " recoveryProgressListener=" +
            (recoveryProgressListener != null) +
            " classLoader=" + (classLoader != null) +
            " customStats=" + (customStats != null) +
            super.toString();
    }
}
