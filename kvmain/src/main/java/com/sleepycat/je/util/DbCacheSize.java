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

package com.sleepycat.je.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.DbCacheSizeRepEnv;
import com.sleepycat.je.utilint.Preload;
import com.sleepycat.util.RuntimeExceptionWrapper;

/**
 * Estimates the in-memory cache size needed to hold a specified data set.
 *
 * To get an estimate of the in-memory footprint for a given database,
 * specify the number of records and database characteristics and DbCacheSize
 * will return an estimate of the cache size required for holding the
 * database in memory. Based on this information a JE cache size can be
 * chosen and then configured using {@link EnvironmentConfig#setCacheSize} or
 * using the {@link EnvironmentConfig#MAX_MEMORY} property.
 *
 * <h2>Importance of the JE Cache</h2>
 *
 * The JE cache is not an optional cache. It is used to hold the metadata for
 * accessing JE data.  In fact the JE cache size is probably the most critical
 * factor to JE performance, since Btree nodes will have to be fetched during a
 * database read or write operation if they are not in cache. During a single
 * read or write operation, at each level of the Btree that a fetch is
 * necessary, an IO may be necessary at a different disk location for each
 * fetch.  In addition, if internal nodes (INs) are not in cache, then write
 * operations will cause additional copies of the INs to be written to storage,
 * as modified INs are moved out of the cache to make room for other parts of
 * the Btree during subsequent operations.  This additional fetching and
 * writing means that sizing the cache too small to hold the INs will result in
 * lower operation performance.
 * <p>
 * For best performance, all Btree nodes should fit in the JE cache, including
 * leaf nodes (LNs), which hold the record data, and INs, which hold record
 * keys and other metadata.  However, because system memory is limited, it is
 * sometimes necessary to size the cache to hold all or at least most INs, but
 * not the LNs.  This utility estimates the size necessary to hold only INs,
 * and the size to hold INs and LNs.
 * <p>
 * In addition, a common problem with large caches is that Java GC overhead
 * can become significant. When a Btree node is evicted from the JE
 * cache based on JE's LRU algorithm, typically the node will have been
 * resident in the JVM heap for an extended period of time, and will be
 * expensive to GC. Therefore, when most or all LNs do <em>not</em> fit in
 * the cache, using {@link CacheMode#EVICT_LN} can be beneficial to
 * reduce the Java GC cost of collecting the LNs as they are moved out of the
 * cache. With EVICT_LN, the LNs only reside in the JVM heap for a short
 * period and are cheap to collect. A recommended approach is to size the JE
 * cache to hold only INs, and size the Java heap to hold that amount plus
 * the amount needed for GC working space and application objects, leaving
 * any additional memory for use by the file system cache. Tests show this
 * approach results in lower GC overhead and more predictable latency.
 * <p>
 * Another issue is that 64-bit JVMs store object references using less space
 * when the heap size is slightly less than 32GiB. When the heap size is 32GiB
 * or more, object references are larger and less data can be cached per GiB of
 * memory. This JVM feature is enabled with the
 * <a href="http://download.oracle.com/javase/7/docs/technotes/guides/vm/performance-enhancements-7.html#compressedOop">Compressed Oops</a>
 * (<code>-XX:+UseCompressedOops</code>) option, although in modern JVMs it is
 * on by default. Because of this factor, and because Java GC overhead is
 * usually higher with larger heaps, a maximum heap size slightly less than
 * 32GiB is recommended, along with Compressed Oops option.
 * <p>
 * Of course, the JE cache size must be less than the heap size since the
 * cache is stored in the heap. In fact, around 30% of free space should
 * normally be reserved in the heap for use by Java GC, to avoid high GC
 * overheads. For example, if the application uses roughly 2GiB of the heap,
 * then with a 32GiB heap the JE cache should normally be no more than
 * 20GiB.
 *
 * <h3>Estimating the JE Cache Size</h3>
 *
 * Estimating JE in-memory sizes is not straightforward for several reasons.
 * There is some fixed overhead for each Btree internal node, so fanout
 * (maximum number of child entries per parent node) and degree of node
 * sparseness impacts memory consumption. In addition, JE uses various compact
 * in-memory representations that depend on key sizes, data sizes, key
 * prefixing, how many child nodes are resident, etc. The physical proximity
 * of node children also allows compaction of child physical address values.
 * <p>
 * Therefore, when running this utility it is important to specify all {@link
 * EnvironmentConfig} and {@link DatabaseConfig} settings that will be used in
 * a production system.  The {@link EnvironmentConfig} settings are specified
 * by command line options for each property, using the same names as the
 * {@link EnvironmentConfig} parameter name values.  For example, {@link
 * EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH}, which influences the amount
 * of memory used to store small keys, can be specified on the command line as:
 * <p>
 * {@code -je.tree.compactMaxKeyLength LENGTH}
 * <p>
 * To be sure that this utility takes into account all relevant settings,
 * especially as the utility is enhanced in future versions, it is best to
 * specify all {@link EnvironmentConfig} settings used by the application.
 * <p>
 * The {@link DatabaseConfig} settings are specified using command line options
 * defined by this utility.
 * <ul>
 *   <li>{@code -nodemax ENTRIES} corresponds to {@link
 *   DatabaseConfig#setNodeMaxEntries}.</li>
 *   <li>{@code -duplicates} corresponds to passing true to {@link
 *   DatabaseConfig#setSortedDuplicates}.  Note that duplicates are configured
 *   for DPL MANY_TO_ONE and MANY_TO_MANY secondary indices.</li>
 *   <li>{@code -keyprefix LENGTH} corresponds to passing true {@link
 *   DatabaseConfig#setKeyPrefixing}.  Note that key prefixing is always used
 *   when duplicates are configured.</li>
 * </ul>
 * <p>
 * This utility estimates the JE cache size by creating an in-memory
 * Environment and Database.  In addition to the size of the Database, the
 * minimum overhead for the Environment is output.  The Environment overhead
 * shown is likely to be smaller than actually needed because it doesn't take
 * into account use of memory by JE daemon threads (cleaner, checkpointer, etc)
 * the memory used for locks that are held by application operations and
 * transactions, the memory for HA network connections, etc. An additional
 * amount should be added to account for these factors.
 * <p>
 * This utility estimates the cache size for a single JE Database, or a logical
 * table spread across multiple databases (as in the case of Oracle NoSQL DB,
 * for example).  To estimate the size for multiple databases/tables with
 * different configuration parameters or different key and data sizes, run
 * this utility for each database/table and sum the sizes. If you are summing
 * multiple runs for multiple databases/tables that are opened in a single
 * Environment, the overhead size for the Environment should only be added once.
 * <p>
 * In some applications with databases/tables having variable key and data
 * sizes, it may be difficult to determine the key and data size input
 * parameters for this utility.  If a representative data set can be created,
 * one approach is to use the {@link DbPrintLog} utility with the {@code -S}
 * option to find the average key and data size for all databases/tables, and
 * use these values as input parameters, as if there were only a single
 * database/tables.  With this approach, it is important that the {@code
 * DatabaseConfig} parameters are the same, or at least similar, for all
 * databases/tables.
 *
 * <h3>Key Prefixing and Compaction</h3>
 *
 * Key prefixing deserves special consideration.  It can significantly reduce
 * the size of the cache and is generally recommended; however, the benefit can
 * be difficult to predict.  Key prefixing, in turn, impacts the benefits of
 * key compaction, and the use of the {@link
 * EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH} parameter.
 * <p>
 * For a given data set, the impact of key prefixing is determined by how many
 * leading bytes are in common for the keys in a single bottom internal node
 * (BIN).  For example, if keys are assigned sequentially as long (8 byte)
 * integers, and the {@link DatabaseConfig#setNodeMaxEntries maximum entries
 * per node} is 128 (the default value) then 6 or 7 of the 8 bytes of the key
 * will have a common prefix in each BIN.  Of course, when records are deleted,
 * the number of prefixed bytes may be reduced because the range of key values
 * in a BIN will be larger.  For this example we will assume that, on average,
 * 5 bytes in each BIN are a common prefix leaving 3 bytes per key that are
 * unprefixed.
 * <p>
 * Also note that key compaction on the unprefixed keys is applied when the
 * number of unprefixed bytes is less than a configured value. See
 * {@link EnvironmentConfig#TREE_COMPACT_MAX_KEY_LENGTH}.
 * <p>
 * Because key prefixing depends so much on the application key format and the
 * way keys are assigned, the number of expected prefix bytes must be estimated
 * by the user and specified to DbCacheSize using the {@code -keyprefix}
 * argument.
 *
 * <h3>Key Prefixing and Duplicates</h3>
 *
 * When {@link DatabaseConfig#setSortedDuplicates duplicates} are configured
 * for a Database (including DPL MANY_TO_ONE and MANY_TO_MANY secondary
 * indices), key prefixing is always used.  This is because the internal key in
 * a duplicates database BIN is formed by concatenating the user-specified key
 * and data.  In secondary databases with duplicates configured, the data is
 * the primary key, so the internal key is the concatenation of the secondary
 * key and the primary key.
 * <p>
 * Key prefixing is always used for duplicates databases because prefixing is
 * necessary to store keys efficiently.  When the number of duplicates per
 * unique user-specified key is more than the number of entries per BIN, the
 * entire user-specified key will be the common prefix.
 * <p>
 * For example, a database that stores user information may use email address
 * as the primary key and zip code as a secondary key.  The secondary index
 * database will be a duplicates database, and the internal key stored in the
 * BINs will be a two part key containing zip code followed by email address.
 * If on average there are more users per zip code than the number of entries
 * in a BIN, then the key prefix will normally be at least as long as the zip
 * code key.  If there are less (more than one zip code appears in each BIN),
 * then the prefix will be shorter than the zip code key.
 * <p>
 * It is also possible for the key prefix to be larger than the secondary key.
 * If for one secondary key value (one zip code) there are a large number of
 * primary keys (email addresses), then a single BIN may contain concatenated
 * keys that all have the same secondary key (same zip code) and have primary
 * keys (email addresses) that all have some number of prefix bytes in common.
 * Therefore, when duplicates are specified it is possible to specify a prefix
 * size that is larger than the key size.
 *
 * <h3>Small Data Sizes and Embedded LNs</h3>
 *
 * Another special data representation involves small data sizes. When the
 * data size of a record is less than or equal to {@link
 * EnvironmentConfig#TREE_MAX_EMBEDDED_LN} (16 bytes, by default), the data
 * is stored (embedded) in the BIN, and the LN is not stored in cache at all.
 * This increases the size needed to hold all INs in cache, but it decreases
 * the size needed to hold the complete data set. If the data size specified
 * when running this utility is less than or equal to TREE_MAX_EMBEDDED_LN,
 * the size displayed for holding INs only will be the same as the size
 * displayed for holdings INs and LNs.
 * <p>
 * See {@link EnvironmentConfig#TREE_MAX_EMBEDDED_LN} for information about
 * the trade-offs in using the embedded LNs feature.
 *
 * <h3>Running the DbCacheSize utility</h3>
 *
 * Usage:
 * <pre>
 * java { com.sleepycat.je.util.DbCacheSize |
 *        -jar je-&lt;version&gt;.jar DbCacheSize }
 *  -records COUNT
 *      # Total records (key/data pairs); required
 *  -key BYTES
 *      # Average key bytes per record; required
 *  [-data BYTES]
 *      # Average data bytes per record; if omitted no leaf
 *      # node sizes are included in the output; required with
 *      # -duplicates, and specifies the primary key length
 *  [-keyprefix BYTES]
 *      # Expected size of the prefix for the keys in each
 *      # BIN; default: key prefixing is not configured;
 *      # required with -duplicates
 *  [-nodemax ENTRIES]
 *      # Number of entries per Btree node; default: 128
 *  [-orderedinsertion]
 *      # Assume ordered insertions and no deletions, so BINs
 *      # are 100% full; default: unordered insertions and/or
 *      # deletions, BINs are 70% full
 *  [-duplicates]
 *      # Indicates that sorted duplicates are used, including
 *      # MANY_TO_ONE and MANY_TO_MANY secondary indices;
 *      # default: false
 *  [-ttl]
 *      # Indicates that TTL is used; default: false
 *  [-replicated]
 *      # Use a ReplicatedEnvironment; default: false
 *  [-ENV_PARAM_NAME VALUE]...
 *      # Any number of EnvironmentConfig parameters and
 *      # ReplicationConfig parameters (if -replicated)
 *  [-btreeinfo]
 *      # Outputs additional Btree information
 *  [-outputproperties]
 *      # Writes Java properties file to System.out
 * </pre>
 * <p>
 * You should run DbCacheSize on the same target platform and JVM for which you
 * are sizing the cache, as cache sizes will vary.  You may also need to
 * specify -d32 or -d64 depending on your target, if the default JVM mode is
 * not the same as the mode to be used in production.
 * <p>
 * To take full advantage of JE cache memory, it is strongly recommended that
 * <a href="http://download.oracle.com/javase/7/docs/technotes/guides/vm/performance-enhancements-7.html#compressedOop">compressed oops</a>
 * (<code>-XX:+UseCompressedOops</code>) is specified when a 64-bit JVM is used
 * and the maximum heap size is less than 32 GB.  As described in the
 * referenced documentation, compressed oops is sometimes the default JVM mode
 * even when it is not explicitly specified in the Java command.  However, if
 * compressed oops is desired then it <em>must</em> be explicitly specified in
 * the Java command when running DbCacheSize or a JE application.  If it is not
 * explicitly specified then JE will not aware of it, even if it is the JVM
 * default setting, and will not take it into account when calculating cache
 * memory sizes.
 * <p>
 * For example:
 * <pre>
 * $ java -jar je-X.Y.Z.jar DbCacheSize -records 554719 -key 16 -data 100
 *
 *  === Environment Cache Overhead ===
 *
 *  3,157,213 minimum bytes
 *
 * To account for JE daemon operation, record locks, HA network connections, etc,
 * a larger amount is needed in practice.
 *
 *  === Database Cache Size ===
 *
 *  Number of Bytes  Description
 *  ---------------  -----------
 *       23,933,736  Internal nodes only
 *      107,206,616  Internal nodes and leaf nodes
 * </pre>
 * <p>
 * This indicates that the minimum memory size to hold only the internal nodes
 * of the Database Btree is approximately 24MB. The maximum size to hold the
 * entire database, both internal nodes and data records, is approximately
 * 107MB.  To either of these amounts, at least 3MB (plus more for locks and
 * daemons) should be added to account for the environment overhead.
 *
 * <h3>Output Properties</h3>
 *
 * <p>
 * When {@code -outputproperties} is specified, a list of properties in Java
 * properties file format will be written to System.out, instead of the output
 * shown above. The properties and their meanings are listed below.
 * <ul>
 *     <li>The following properties are always output (except allNodes, see
 *     below). They describe the estimated size of the cache.
 *     <ul>
 *         <li><strong>overhead</strong>: The environment overhead, as shown
 *         under Environment Cache Overhead above.</li>
 *         <li><strong>internalNodes</strong>: The Btree size in the
 *         cache for holding the internal nodes. This is the "Internal nodes
 *         only" line above.</li>
 *         <li><strong>internalNodesAndVersions</strong>: The Btree size needed
 *         to hold the internal nodes and record versions in the cache.</li>
 *         <li><strong>allNodes</strong>: The Btree size in the cache
 *         needed to hold all nodes. This is the "Internal nodes and leaf
 *         nodes" line above. This property is not output unless {@code -data}
 *         is specified.</li>
 *     </ul>
 *     <li>The following properties are deprecated but are output for
 *     compatibility with earlier releases.
 *     <ul>
 *         <li> minInternalNodes, maxInternalNodes, minAllNodes, and (when
 *         {@code -data} is specified) maxAllNodes</li>
 *     </ul>
 * </ul>
 *
 * @see EnvironmentConfig#setCacheSize
 * @see CacheMode
 *
 * @see <a href="../EnvironmentStats.html#cacheSizing">Cache Statistics:
 * Sizing</a>
 */
public class DbCacheSize {

    /*
     * Undocumented command line options, used for comparing calculated to
     * actual cache sizes during testing.
     *
     *  [-measure]
     *      # Causes main program to write a database to find
     *      # the actual cache size; default: do not measure;
     *      # without -data, measures internal nodes only
     *
     * Only use -measure without -orderedinsertion when record count is 100k or
     * less, to avoid endless attempts to find an unused key value via random
     * number generation.  Also note that measured amounts will be slightly
     * less than calculated amounts because the number of prefix bytes is
     * larger for smaller key values, which are sequential integers from zero
     * to max records minus one.
     */

    private static final NumberFormat INT_FORMAT =
        NumberFormat.getIntegerInstance();

    private static final String MAIN_HEADER =
        "   Number of Bytes  Description\n" +
        "   ---------------  -----------";
    //   123456789012345678
    //                     12
    private static final int MIN_COLUMN_WIDTH = 18;
    private static final String COLUMN_SEPARATOR = "  ";

    /* IN density for non-ordered insertion. */
    private static final int DEFAULT_DENSITY = 70;
    /* IN density for ordered insertion. */
    private static final int ORDERED_DENSITY = 100;

    /* Parameters. */
    private final EnvironmentConfig envConfig = new EnvironmentConfig();
    private final Map<String, String> repParams = new HashMap<>();
    private long records = 0;
    private int keySize = 0;
    private int dataSize = -1;
    private boolean assumeEvictLN = false;
    private long mainCacheSize = 0;
    private int nodeMaxEntries = 128;
    private int binMaxEntries = -1;
    private int keyPrefix = 0;
    private boolean orderedInsertion = false;
    private boolean duplicates = false;
    private boolean replicated = false;
    private boolean useTTL = false;
    private boolean outputProperties = false;
    private boolean doMeasure = false;
    private boolean btreeInfo = false;

    /* Calculated values. */
    private long envOverhead;
    private long uinWithTargets;
    private long binNoLNsOrVLSNs;
    private long binNoLNsWithVLSNs;
    private long binWithLNsAndVLSNs;
    private long mainNoLNsOrVLSNs;
    private long mainNoLNsWithVLSNs;
    private long mainWithLNsAndVLSNs;
    private long measuredMainNoLNsOrVLSNs;
    private long measuredMainNoLNsWithVLSNs;
    private long measuredMainWithLNsAndVLSNs;
    private long preloadMainNoLNsOrVLSNs;
    private long preloadMainNoLNsWithVLSNs;
    private long preloadMainWithLNsAndVLSNs;
    private int nodeAvg;
    private int binAvg;
    private int btreeLevels;
    private long nBinNodes;
    private long nUinNodes;
    private long nLevel2Nodes;

    private File tempDir;

    DbCacheSize() {
    }

    void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i += 1) {
            String name = args[i];
            String val = null;
            if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                i += 1;
                val = args[i];
            }
            if (name.equals("-records")) {
                if (val == null) {
                    usage("No value after -records");
                }
                try {
                    records = Long.parseLong(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (records <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-key")) {
                if (val == null) {
                    usage("No value after -key");
                }
                try {
                    keySize = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (keySize <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-data")) {
                if (val == null) {
                    usage("No value after -data");
                }
                try {
                    dataSize = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (dataSize < 0) {
                    usage(val + " is not a non-negative integer");
                }
            } else if (name.equals("-maincache")) {
                if (val == null) {
                    usage("No value after -maincache");
                }
                try {
                    mainCacheSize = Long.parseLong(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (mainCacheSize <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-keyprefix")) {
                if (val == null) {
                    usage("No value after -keyprefix");
                }
                try {
                    keyPrefix = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (keyPrefix < 0) {
                    usage(val + " is not a non-negative integer");
                }
            } else if (name.equals("-orderedinsertion")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                orderedInsertion = true;
            } else if (name.equals("-duplicates")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                duplicates = true;
            } else if (name.equals("-ttl")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                useTTL = true;
            } else if (name.equals("-replicated")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                replicated = true;
            } else if (name.equals("-nodemax")) {
                if (val == null) {
                    usage("No value after -nodemax");
                }
                try {
                    nodeMaxEntries = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (nodeMaxEntries <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-binmax")) {
                if (val == null) {
                    usage("No value after -binmax");
                }
                try {
                    binMaxEntries = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    usage(val + " is not a number");
                }
                if (binMaxEntries <= 0) {
                    usage(val + " is not a positive integer");
                }
            } else if (name.equals("-density")) {
                usage
                    ("-density is no longer supported, see -orderedinsertion");
            } else if (name.equals("-overhead")) {
                usage("-overhead is no longer supported");
            } else if (name.startsWith("-je.")) {
                if (val == null) {
                    usage("No value after " + name);
                }
                if (name.startsWith("-je.rep.")) {
                    repParams.put(name.substring(1), val);
                } else {
                    envConfig.setConfigParam(name.substring(1), val);
                }
            } else if (name.equals("-measure")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                doMeasure = true;
            } else if (name.equals("-outputproperties")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                outputProperties = true;
            } else if (name.equals("-btreeinfo")) {
                if (val != null) {
                    usage("No value allowed after " + name);
                }
                btreeInfo = true;
            } else {
                usage("Unknown arg: " + name);
            }
        }

        if (records == 0) {
            usage("-records not specified");
        }
        if (keySize == 0) {
            usage("-key not specified");
        }
    }

    void cleanup() {
        if (tempDir != null) {
            emptyTempDir();
            tempDir.delete();
        }
    }

    long getMainNoLNsOrVLSNs() {
        return mainNoLNsOrVLSNs;
    }

    long getMainNoLNsWithVLSNs() {
        return mainNoLNsWithVLSNs;
    }

    long getMainWithLNsAndVLSNs() {
        return mainWithLNsAndVLSNs;
    }

    long getMeasuredMainNoLNsOrVLSNs() {
        return measuredMainNoLNsOrVLSNs;
    }

    long getMeasuredMainNoLNsWithVLSNs() {
        return measuredMainNoLNsWithVLSNs;
    }

    long getMeasuredMainWithLNsAndVLSNs() {
        return measuredMainWithLNsAndVLSNs;
    }

    long getPreloadMainNoLNsOrVLSNs() {
        return preloadMainNoLNsOrVLSNs;
    }

    long getPreloadMainNoLNsWithVLSNs() {
        return preloadMainNoLNsWithVLSNs;
    }

    long getPreloadMainWithLNsAndVLSNs() {
        return preloadMainWithLNsAndVLSNs;
    }

    /**
     * Runs DbCacheSize as a command line utility.
     * For command usage, see {@link DbCacheSize class description}.
     */
    public static void main(final String[] args)
        throws Throwable {

        final DbCacheSize dbCacheSize = new DbCacheSize();
        try {
            dbCacheSize.parseArgs(args);
            dbCacheSize.calculateCacheSizes();
            if (dbCacheSize.outputProperties) {
                dbCacheSize.printProperties(System.out);
            } else {
                dbCacheSize.printCacheSizes(System.out);
            }
            if (dbCacheSize.doMeasure) {
                dbCacheSize.measure(System.out);
            }
        } finally {
            dbCacheSize.cleanup();
        }
    }

    /**
     * Prints usage and calls System.exit.
     */
    private static void usage(final String msg) {

        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println
            ("usage:" +
             "\njava "  + CmdUtil.getJavaCommand(DbCacheSize.class) +
             "\n   -records <count>" +
             "\n      # Total records (key/data pairs); required" +
             "\n   -key <bytes> " +
             "\n      # Average key bytes per record; required" +
             "\n  [-data <bytes>]" +
             "\n      # Average data bytes per record; if omitted no leaf" +
             "\n      # node sizes are included in the output; required with" +
             "\n      # -duplicates, and specifies the primary key length" +
             "\n  [-keyprefix <bytes>]" +
             "\n      # Expected size of the prefix for the keys in each" +
             "\n      # BIN; default: zero, key prefixing is not configured;" +
             "\n      # required with -duplicates" +
             "\n  [-nodemax <entries>]" +
             "\n      # Number of entries per Btree node; default: 128" +
             "\n  [-orderedinsertion]" +
             "\n      # Assume ordered insertions and no deletions, so BINs" +
             "\n      # are 100% full; default: unordered insertions and/or" +
             "\n      # deletions, BINs are 70% full" +
             "\n  [-duplicates]" +
             "\n      # Indicates that sorted duplicates are used, including" +
             "\n      # MANY_TO_ONE and MANY_TO_MANY secondary indices;" +
             "\n      # default: false" +
             "\n  [-ttl]" +
             "\n      # Indicates that TTL is used; default: false" +
             "\n  [-replicated]" +
             "\n      # Use a ReplicatedEnvironment; default: false" +
             "\n  [-ENV_PARAM_NAME VALUE]..." +
             "\n      # Any number of EnvironmentConfig parameters and" +
             "\n      # ReplicationConfig parameters (if -replicated)" +
             "\n  [-btreeinfo]" +
             "\n      # Outputs additional Btree information" +
             "\n  [-outputproperties]" +
             "\n      # Writes Java properties to System.out");

        System.exit(2);
    }

    /**
     * Calculates estimated cache sizes.
     */
    void calculateCacheSizes() {

        if (binMaxEntries <= 0) {
            binMaxEntries = nodeMaxEntries;
        }

        final Environment env = openCalcEnvironment(true);
        boolean success = false;
        try {
            IN.ACCUMULATED_LIMIT = 0;

            envOverhead = env.getStats(null).getCacheTotalBytes();

            final int density =
                orderedInsertion ? ORDERED_DENSITY : DEFAULT_DENSITY;

            nodeAvg = (nodeMaxEntries * density) / 100;
            binAvg = (binMaxEntries * density) / 100;

            calcTreeSizes(env);
            calcNNodes();
            calcCacheSizes();

            success = true;
        } finally {

            IN.ACCUMULATED_LIMIT = IN.ACCUMULATED_LIMIT_DEFAULT;

            /*
             * Do not propagate exception thrown by Environment.close if
             * another exception is currently in flight.
             */
            try {
                env.close();
            } catch (RuntimeException e) {
                if (success) {
                    throw e;
                }
            }
        }
    }

    private void calcNNodes() {

        nBinNodes = (records + binAvg - 1) / binAvg;
        btreeLevels = 1;
        nUinNodes = 0;
        nLevel2Nodes = 0;

        for (long nodes = nBinNodes / nodeAvg;; nodes /= nodeAvg) {

            if (nodes == 0) {
                nodes = 1; // root
            }

            if (btreeLevels == 2) {
                assert nLevel2Nodes == 0;
                nLevel2Nodes = nodes;
            }

            nUinNodes += nodes;
            btreeLevels += 1;

            if (nodes == 1) {
                break;
            }
        }
    }

    /**
     * Calculates cache sizes.
     */
    private void calcCacheSizes() {

        final long mainUINs = nUinNodes * uinWithTargets;

        mainNoLNsOrVLSNs =
            (nBinNodes * binNoLNsOrVLSNs) + mainUINs;

        mainNoLNsWithVLSNs =
            (nBinNodes * binNoLNsWithVLSNs) + mainUINs;

        mainWithLNsAndVLSNs =
            (nBinNodes * binWithLNsAndVLSNs) + mainUINs;
    }

    private void calcTreeSizes(final Environment env) {

        if (nodeMaxEntries != binMaxEntries) {
            throw new IllegalArgumentException(
                "-binmax not currently supported because a per-BIN max is" +
                " not implemented in the Btree, so we can't measure" +
                " an actual BIN node with the given -binmax value");
        }
        assert nodeAvg == binAvg;

        if (nodeAvg > 0xFFFF) {
            throw new IllegalArgumentException(
                "Entries per node (" + nodeAvg + ") is greater than 0xFFFF");
        }

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        /*
         * Either a one or two byte key is used, depending on whether a single
         * byte can hold the key for nodeAvg entries.
         */
        final byte[] keyBytes = new byte[(nodeAvg <= 0xFF) ? 1 : 2];
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        final WriteOptions options = new WriteOptions();
        if (useTTL) {
            options.setTTL(30, TimeUnit.DAYS);
        }

        /* Insert nodeAvg records into a single BIN. */
        final Database db = openDatabase(env, true);
        for (int i = 0; i < nodeAvg; i += 1) {

            if (keyBytes.length == 1) {
                keyBytes[0] = (byte) i;
            } else {
                assert keyBytes.length == 2;
                keyBytes[0] = (byte) (i >> 8);
                keyBytes[1] = (byte) i;
            }

            setKeyData(keyBytes, keyPrefix, keyEntry, dataEntry);

            final OperationResult result = db.put(
                null, keyEntry, dataEntry,
                duplicates ? Put.NO_DUP_DATA : Put.NO_OVERWRITE,
                options);

            if (result == null) {
                throw new IllegalStateException();
            }
        }

        /* Position a cursor at the first record to get the BIN. */
        final Cursor cursor = db.openCursor(null, null);
        OperationStatus status = cursor.getFirst(keyEntry, dataEntry, null);
        assert status == OperationStatus.SUCCESS;
        final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        cursor.close();
        bin.latchNoUpdateLRU();

        /*
         * Calculate BIN size including LNs. The recalcKeyPrefix and
         * compactMemory methods are called to simulate normal operation.
         * Normally prefixes are recalculated when a IN is split, and
         * compactMemory is called after fetching a IN or evicting an LN.
         */
        bin.recalcKeyPrefix();
        bin.compactMemory();
        binWithLNsAndVLSNs = bin.getInMemorySize();

        /*
         * Evict all LNs so we can calculate BIN size without LNs.  This is
         * simulated by calling partialEviction directly.
         */
        bin.partialEviction();

        assert !bin.hasCachedChildren();

        binNoLNsWithVLSNs = bin.getInMemorySize();

        /*
         * Another variant is when VLSNs are cached, since they are evicted
         * after the LNs in a separate step.  This is simulated by calling
         * partialEviction a second time.
         */
        if (duplicates || !envImpl.getCacheVLSN()) {
            assert bin.getVLSNCache().getMemorySize() == 0;

        } else {
            assert bin.getVLSNCache().getMemorySize() > 0;

            bin.partialEviction();

            if (dataSize <= bin.getEnv().getMaxEmbeddedLN()) {
                assert bin.getVLSNCache().getMemorySize() > 0;
            } else {
                assert bin.getVLSNCache().getMemorySize() == 0;
            }
        }

        /* There are no LNs or VLSNs remaining. */
        binNoLNsOrVLSNs = bin.getInMemorySize();

        /*
         * To calculate IN size, get parent/root IN and artificially fill the
         * slots with nodeAvg entries.
         */
        final IN in = DbInternal.getDbImpl(db).
                                 getTree().
                                 getRootINLatchedExclusive(CacheMode.DEFAULT);
        assert bin == in.getTarget(0);

        for (int i = 1; i < nodeAvg; i += 1) {

            final int result = in.insertEntry1(
                bin, bin.getKey(i), null, bin.getLsn(i),
                false/*blindInsertion*/);

            assert (result & IN.INSERT_SUCCESS) != 0;
            assert i == (result & ~IN.INSERT_SUCCESS);
        }

        in.recalcKeyPrefix();
        in.compactMemory();
        uinWithTargets = in.getInMemorySize();

        bin.releaseLatch();
        in.releaseLatch();

        db.close();
    }

    private long getMainDataSize(final Environment env) {
        return DbInternal.getNonNullEnvImpl(env).
            getMemoryBudget().getTreeMemoryUsage();
    }

    private void setKeyData(final byte[] keyBytes,
                            final int keyOffset,
                            final DatabaseEntry keyEntry,
                            final DatabaseEntry dataEntry) {
        final byte[] fullKey;
        if (duplicates) {
            fullKey = new byte[keySize + dataSize];
        } else {
            fullKey = new byte[keySize];
        }

        if (keyPrefix + keyBytes.length > fullKey.length) {
            throw new IllegalArgumentException(
                "Key doesn't fit, allowedLen=" + fullKey.length +
                " keyLen=" + keyBytes.length + " prefixLen=" + keyPrefix);
        }

        System.arraycopy(keyBytes, 0, fullKey, keyOffset, keyBytes.length);

        final byte[] finalKey;
        final byte[] finalData;
        if (duplicates) {
            finalKey = new byte[keySize];
            finalData = new byte[dataSize];
            System.arraycopy(fullKey, 0, finalKey, 0, keySize);
            System.arraycopy(fullKey, keySize, finalData, 0, dataSize);
        } else {
            finalKey = fullKey;
            finalData = new byte[Math.max(0, dataSize)];
        }

        keyEntry.setData(finalKey);
        dataEntry.setData(finalData);
    }

    /**
     * Prints Java properties for information collected by calculateCacheSizes.
     * Min/max sizes are output for compatibility with earlier versions; in the
     * past, min and max were different values.
     */
    private void printProperties(final PrintStream out) {
        out.println("overhead=" + envOverhead);
        out.println("internalNodes=" + mainNoLNsOrVLSNs);
        out.println("internalNodesAndVersions=" + mainNoLNsWithVLSNs);
        if (dataSize >= 0) {
            out.println("allNodes=" + mainWithLNsAndVLSNs);
        }
        out.println("# Following are deprecated");
        out.println("minInternalNodes=" + mainNoLNsOrVLSNs);
        out.println("maxInternalNodes=" + mainNoLNsOrVLSNs);
        if (dataSize >= 0) {
            out.println("minAllNodes=" + mainWithLNsAndVLSNs);
            out.println("maxAllNodes=" + mainWithLNsAndVLSNs);
        }
    }

    /**
     * Prints information collected by calculateCacheSizes.
     */
    void printCacheSizes(final PrintStream out) {

        out.println();
        out.println("=== Environment Cache Overhead ===");
        out.println();
        out.print(INT_FORMAT.format(envOverhead));
        out.println(" minimum bytes");
        out.println();
        out.println(
            "To account for JE daemon operation, record locks, HA network " +
            "connections, etc,");
        out.println("a larger amount is needed in practice.");
        out.println();
        out.println("=== Database Cache Size ===");
        out.println();
        out.println(MAIN_HEADER);

        out.println(line(mainNoLNsOrVLSNs, "Internal nodes only"));

        if (dataSize >= 0) {
            if (mainNoLNsWithVLSNs != mainNoLNsOrVLSNs) {
                out.println(line(
                    mainNoLNsWithVLSNs,
                    "Internal nodes and record versions"));
            }

            out.println(line(
                mainWithLNsAndVLSNs,
                "Internal nodes and leaf nodes"));

            if (mainNoLNsOrVLSNs == mainWithLNsAndVLSNs){

                if (duplicates) {
                    out.println(
                        "\nNote that leaf nodes do not use additional memory" +
                        " because the database is" +
                        "\nconfigured for duplicates. In addition, record" +
                        " versions are not applicable.");
                } else {
                    out.println(
                        "\nNote that leaf nodes do not use additional memory" +
                        " because with a small" +
                        "\ndata size, the LNs are embedded in the BINs." +
                        " In addition, record versions" +
                        "\n(if configured) are always cached in this mode.");
                }

            }
        } else {
            if (!duplicates) {
                out.println("\nTo get leaf node sizing specify -data");
            }
        }

        if (btreeInfo) {
            out.println();
            out.println("=== Calculated Btree Information ===");
            out.println();
            out.println(line(btreeLevels, "Btree levels"));
            out.println(line(nUinNodes, "Upper internal nodes"));
            out.println(line(nBinNodes, "Bottom internal nodes"));
        }

        out.println();
        out.println("For further information see the DbCacheSize javadoc.");
    }

    private String line(final long num, final String comment) {

        final StringBuilder buf = new StringBuilder(100);

        column(buf, INT_FORMAT.format(num));
        buf.append(COLUMN_SEPARATOR);
        buf.append(comment);

        return buf.toString();
    }

    private void column(final StringBuilder buf, final String str) {

        int start = buf.length();

        while (buf.length() - start + str.length() < MIN_COLUMN_WIDTH) {
            buf.append(' ');
        }

        buf.append(str);
    }

    /**
     * For testing, insert the specified data set and initialize
     * measuredMainNoLNsWithVLSNs and measuredMainWithLNsAndVLSNs.
     */
    void measure(final PrintStream out) {

        Environment env = openMeasureEnvironment(true /*createNew*/);
        try {
            IN.ACCUMULATED_LIMIT = 0;

            Database db = openDatabase(env, true);

            if (out != null) {
                out.println(
                    "Measuring with maximum cache size: " +
                    INT_FORMAT.format(env.getConfig().getCacheSize()));
            }

            insertRecords(out, env, db);

            measuredMainWithLNsAndVLSNs = getStats(
                out, env, "After insert");

            trimLNs(db);

            measuredMainNoLNsWithVLSNs = getStats(
                out, env, "After trimLNs");

            trimVLSNs(db);

            measuredMainNoLNsOrVLSNs = getStats(
                out, env, "After trimVLSNs");

            db.close();
            env.close();
            env = null;

            env = openMeasureEnvironment(false /*createNew*/);
            db = openDatabase(env, false);

            boolean cacheFilled = preloadRecords(out, db, false /*loadLNs*/);

            preloadMainNoLNsOrVLSNs = getStats(
                out, env,
                "Internal nodes only after preload (cacheFilled=" +
                cacheFilled + ")");

            if (assumeEvictLN) {
                preloadMainWithLNsAndVLSNs = preloadMainNoLNsOrVLSNs;
            } else {
                cacheFilled = preloadRecords(out, db, true /*loadLNs*/);

                preloadMainWithLNsAndVLSNs = getStats(
                    out, env,
                    "All nodes after preload (cacheFilled=" +
                        cacheFilled + ")");
            }

            trimLNs(db);

            preloadMainNoLNsWithVLSNs = getStats(
                out, env,
                "Internal nodes plus VLSNs after preload (cacheFilled=" +
                    cacheFilled + ")");

            db.close();
            env.close();
            env = null;

        } finally {

            IN.ACCUMULATED_LIMIT = IN.ACCUMULATED_LIMIT_DEFAULT;

            /*
             * Do not propagate exception thrown by Environment.close if
             * another exception is currently in flight.
             */
            if (env != null) {
                try {
                    env.close();
                } catch (RuntimeException ignore) {
                }
            }
        }
    }

    private Environment openMeasureEnvironment(final boolean createNew) {

        final EnvironmentConfig config = envConfig.clone();
        config.setCachePercent(90);

        return openEnvironment(config, createNew);
    }

    private Environment openCalcEnvironment(final boolean createNew) {

        final EnvironmentConfig config = envConfig.clone();

        /* The amount of disk space needed is quite small. */
        config.setConfigParam(
            EnvironmentConfig.FREE_DISK, String.valueOf(1L << 20));

        return openEnvironment(config, createNew);
    }

    private Environment openEnvironment(final EnvironmentConfig config,
                                        final boolean createNew) {
        mkTempDir();

        if (createNew) {
            emptyTempDir();
        }

        config.setTransactional(true);
        config.setDurability(Durability.COMMIT_NO_SYNC);
        config.setAllowCreate(createNew);

        /* Daemons interfere with cache size measurements. */
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_CLEANER, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_EVICTOR, "false");
        config.setConfigParam(
            EnvironmentConfig.ENV_RUN_VERIFIER, "false");
        config.setConfigParam(
            EnvironmentParams.ENV_RUN_EXTINCT_RECORD_SCANNER.getName(), "false");

        /* Evict in small chunks. */
        config.setConfigParam(
            EnvironmentConfig.EVICTOR_EVICT_BYTES, "1024");

        final Environment newEnv;

        if (replicated) {
            try {
                final Class<?> repEnvClass = Class.forName
                    ("com.sleepycat.je.rep.utilint.DbCacheSizeRepEnv");
                final DbCacheSizeRepEnv repEnv = (DbCacheSizeRepEnv)
                    repEnvClass.getDeclaredConstructor().newInstance();
                newEnv = repEnv.open(tempDir, config, repParams);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            if (!repParams.isEmpty()) {
                throw new IllegalArgumentException(
                    "Cannot set replication params in a standalone " +
                    "environment.  May add -replicated.");
            }
            newEnv = new Environment(tempDir, config);
        }

        return newEnv;
    }

    private void mkTempDir() {
        if (tempDir == null) {
            try {
                tempDir = File.createTempFile("DbCacheSize", null);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            /* createTempFile creates a file, but we want a directory. */
            tempDir.delete();
            tempDir.mkdir();
        }
    }

    private void emptyTempDir() {
        if (tempDir == null) {
            return;
        }
        final File[] children = tempDir.listFiles();
        if (children != null) {
            for (File child : children) {
                child.delete();
            }
        }
    }

    private Database openDatabase(final Environment env,
                                  final boolean createNew) {
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(createNew);
        dbConfig.setExclusiveCreate(createNew);
        dbConfig.setNodeMaxEntries(nodeMaxEntries);
        dbConfig.setKeyPrefixing(keyPrefix > 0);
        dbConfig.setSortedDuplicates(duplicates);
        return env.openDatabase(null, "foo", dbConfig);
    }

    /**
     * Inserts records and ensures that no eviction occurs.  LNs (and VLSNs)
     * are left intact.
     */
    private void insertRecords(final PrintStream out,
                               final Environment env,
                               final Database db) {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        final int lastKey = (int) (records - 1);
        final byte[] lastKeyBytes = BigInteger.valueOf(lastKey).toByteArray();
        final int maxKeyBytes = lastKeyBytes.length;

        final int keyOffset;
        if (keyPrefix == 0) {
            keyOffset = 0;
        } else {

            /*
             * Calculate prefix length for generated keys and adjust key offset
             * to produce the desired prefix length.
             */
            final int nodeAvg = orderedInsertion ?
                nodeMaxEntries :
                ((nodeMaxEntries * DEFAULT_DENSITY) / 100);
            final int prevKey = lastKey - (nodeAvg * 2);
            final byte[] prevKeyBytes =
                padLeft(BigInteger.valueOf(prevKey).toByteArray(),
                        maxKeyBytes);
            int calcPrefix = 0;
            while (calcPrefix < lastKeyBytes.length &&
                   calcPrefix < prevKeyBytes.length &&
                   lastKeyBytes[calcPrefix] == prevKeyBytes[calcPrefix]) {
                calcPrefix += 1;
            }
            keyOffset = keyPrefix - calcPrefix;
        }

        /* Generate random keys. */
        List<Integer> rndKeys = null;
        if (!orderedInsertion) {
            rndKeys = new ArrayList<Integer>(lastKey + 1);
            for (int i = 0; i <= lastKey; i += 1) {
                rndKeys.add(i);
            }
            Collections.shuffle(rndKeys, new Random(123));
        }

        final WriteOptions options = new WriteOptions();
        if (useTTL) {
            options.setTTL(30, TimeUnit.DAYS);
        }

        final Transaction txn = env.beginTransaction(null, null);
        final Cursor cursor = db.openCursor(txn, null);
        boolean success = false;
        try {
            for (int i = 0; i <= lastKey; i += 1) {
                final int keyVal = orderedInsertion ? i : rndKeys.get(i);
                final byte[] keyBytes = padLeft(
                    BigInteger.valueOf(keyVal).toByteArray(), maxKeyBytes);
                setKeyData(keyBytes, keyOffset, keyEntry, dataEntry);

                final OperationResult result = cursor.put(
                    keyEntry, dataEntry,
                    duplicates ? Put.NO_DUP_DATA : Put.NO_OVERWRITE,
                    options);

                if (result == null && !orderedInsertion) {
                    i -= 1;
                    continue;
                }
                if (result == null) {
                    throw new IllegalStateException("Could not insert");
                }

                if (i % 10000 == 0) {
                    checkForEviction(env, i);
                    if (out != null) {
                        out.print(".");
                        out.flush();
                    }
                }
            }
            success = true;
        } finally {
            cursor.close();
            if (success) {
                txn.commit();
            } else {
                txn.abort();
            }
        }

        checkForEviction(env, lastKey);

        /* Checkpoint to speed recovery and reset the memory budget. */
        env.checkpoint(new CheckpointConfig().setForce(true));

        /* Let's be sure the memory budget is updated. */
        iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.updateMemoryBudget();
                return true;
            }
        });
    }

    private void checkForEviction(Environment env, int recNum) {
        final EnvironmentStats stats = env.getStats(null);
        if (stats.getNNodesTargeted() > 0) {
            getStats(System.out, env, "Out of cache");
            throw new IllegalStateException(
                "*** Ran out of cache at record " + recNum +
                " -- try increasing Java heap size ***");
        }
    }

    private void trimLNs(final Database db) {
        iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.evictLNs();
                bin.updateMemoryBudget();
                return true;
            }
        });
    }

    private void trimVLSNs(final Database db) {
        iterateBINs(db, new BINVisitor() {
            @Override
            public boolean visitBIN(final BIN bin) {
                bin.discardVLSNCache();
                bin.updateMemoryBudget();
                return true;
            }
        });
    }

    private interface BINVisitor {
        boolean visitBIN(BIN bin);
    }

    private boolean iterateBINs(final Database db, final BINVisitor visitor) {

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(0, 0, true);

        final Cursor c = db.openCursor(null, null);
        BIN prevBin = null;
        boolean keepGoing = true;

        while (keepGoing &&
               c.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
               OperationStatus.SUCCESS) {

            final BIN bin = DbInternal.getCursorImpl(c).getBIN();

            if (bin == prevBin) {
                continue;
            }

            if (prevBin != null) {
                prevBin.latch();
                keepGoing = visitor.visitBIN(prevBin);
                prevBin.releaseLatchIfOwner();
            }

            prevBin = bin;
        }

        c.close();

        if (keepGoing && prevBin != null) {
            prevBin.latch();
            visitor.visitBIN(prevBin);
            prevBin.releaseLatch();
        }

        return keepGoing;
    }

    /**
     * Pads the given array with zeros on the left, and returns an array of
     * the given size.
     */
    private byte[] padLeft(byte[] data, int size) {
        assert data.length <= size;
        if (data.length == size) {
            return data;
        }
        final byte[] b = new byte[size];
        System.arraycopy(data, 0, b, size - data.length, data.length);
        return b;
    }

    /**
     * Preloads the database.
     */
    private boolean preloadRecords(final PrintStream out,
                                   final Database db,
                                   final boolean loadLNs) {
        Thread thread = null;
        if (out != null) {
            thread = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            out.print(".");
                            out.flush();
                            Thread.sleep(5 * 1000);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            };
            thread.start();
        }
        final boolean cacheFilled;
        try {
            cacheFilled = Preload.preloadDb(db, loadLNs, 100);
        } finally {
            if (thread != null) {
                thread.interrupt();
            }
        }
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeExceptionWrapper(e);
            }
        }

        return cacheFilled;
    }

    /**
     * Returns the Btree size, and prints a few other stats for testing.
     */
    private long getStats(final PrintStream out,
                          final Environment env,
                          final String msg) {
        if (out != null) {
            out.println();
            out.println(msg + ':');
        }

        final EnvironmentStats stats = env.getStats(null);

        final long dataSize = getMainDataSize(env);

        if (out != null) {
            out.println(
                "MainCache= " + INT_FORMAT.format(stats.getCacheTotalBytes()) +
                " Data= " + INT_FORMAT.format(dataSize)  +
                " BINs= " + INT_FORMAT.format(stats.getNCachedBINs()) +
                " UINs= " + INT_FORMAT.format(stats.getNCachedUpperINs()) +
                " CacheMiss= " + INT_FORMAT.format(stats.getNCacheMiss()));
        }

        if (stats.getNNodesTargeted() > 0) {
            throw new IllegalStateException(
                "*** All records did not fit in the cache ***");
        }
        return dataSize;
    }
}
