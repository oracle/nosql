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

package oracle.kv.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KVStoreException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ParamConstant;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.split.SplitBuilder;
import oracle.kv.impl.topo.split.TopoSplit;
import oracle.kv.impl.util.ExternalDataSourceUtils;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * This is the base class for Oracle NoSQL Database InputFormat classes.
 * Keys and Value types are determined by the specific subclass.
 * <p>
 *
 * Parameters may be passed using either the static setters on this class or
 * through the Hadoop JobContext configuration parameters. The following
 * parameters are recognized:
 * <ul>
 *
 * <li><code>oracle.kv.kvstore</code> - the KV Store name for this InputFormat
 * to operate on. This is equivalent to the {@link #setKVStoreName} method.
 *
 * <li><code>oracle.kv.hosts</code> - one or more <code>hostname:port</code>
 * pairs separated by commas naming hosts in the KV Store. This is equivalent
 * to the {@link #setKVHelperHosts} method.
 *
 * <li><code>oracle.kv.batchSize</code> - Specifies the suggested number of
 * keys to fetch during each network round trip by the InputFormat.  If 0, an
 * internally determined default is used. This is equivalent to the {@link
 * #setBatchSize} method.
 *
 * <li><code>oracle.kv.parentKey</code> - Specifies the parent key whose
 * "child" KV pairs are to be returned by the InputFormat.  null will result
 * in fetching all keys in the store. If non-null, the major key path must be a
 * partial path and the minor key path must be empty. This is equivalent to the
 * {@link #setParentKey} method.
 *
 * <li><code>oracle.kv.subRange</code> - Specifies a sub range to further
 * restrict the range under the parentKey to the major path components in this
 * sub range.  It may be null. This is equivalent to the {@link #setSubRange}
 * method.
 *
 * <li><code>oracle.kv.depth</code> - Specifies whether the parent and only
 * children or all descendents are returned.  If null,
 * Depth.PARENT_AND_DESCENDENTS is implied. This is equivalent to the
 * {@link #setDepth} method.
 *
 * <li><code>oracle.kv.consistency</code> - Specifies the read consistency
 * associated with the lookup of the child KV pairs.  Version- and Time-based
 * consistency may not be used.  If null, the default consistency is used.
 * This is equivalent to the {@link #setConsistency} method.
 *
 * <li><code>oracle.kv.timeout</code> - Specifies an upper bound on the time
 * interval for processing a particular KV retrieval.  A best effort is made to
 * not exceed the specified limit. If zero, the default request timeout is
 * used. This value is always in milliseconds. This is equivalent to the
 * {@link #setTimeout} and {@link #setTimeoutUnit} methods.
 *
 * </ul>
 *
 * <p>
 * Internally, the KVInputFormatBase class utilizes {@link
 * oracle.kv.KVStore#storeIterator(Direction, int, Key, KeyRange,
 * Depth, Consistency, long, TimeUnit) KVStore.storeIterator} to
 * retrieve records. You should refer to the javadoc for that method
 * for information about the various parameters.
 * <p>
 *
 * <code>KVInputFormatBase</code> creates one split per Oracle NoSQL
 * DB partition.  The <code>location</code> value for each split is an
 * array of hosts holding the partition.  If the consistency passed to
 * <code>KVInputFormatBase</code> is {@link Consistency#NONE_REQUIRED
 * NONE_REQUIRED} (the default), then {@link InputSplit#getLocations
 * InputSplit.getLocations()} will return an array of the names of the
 * master and the replica(s) which contain the partition.
 * Alternatively, if the consistency is {@link
 * Consistency#NONE_REQUIRED_NO_MASTER NONE_REQUIRED_NO_MASTER}, then
 * the array returned will contain only the names of the replica(s);
 * not the master.  Finally, if the consistency is {@link
 * Consistency#ABSOLUTE ABSOLUTE}, then the array returned will
 * contain only the name of the master.  This means that if Hadoop job
 * trackers are running on the nodes named in the returned
 * <code>location</code> array, Hadoop will generally attempt to run
 * the subtasks for a particular partition on those nodes where the
 * data is stored and replicated.  Hadoop and Oracle NoSQL DB
 * administrators should be careful about co-location of Oracle NoSQL
 * DB and Hadoop processes since they may compete for resources.
 *
 * <p>
 * Partitions in Oracle NoSQL DB are considered to be roughly equal in size;
 * therefore {@link InputSplit#getLength InputSplit.getLength()} always returns
 * 1.
 * <p>
 *
 * A simple example demonstrating the Oracle NoSQL DB Hadoop
 * oracle.kv.hadoop.InputFormat class reading data from Hadoop in a Map/Reduce
 * job and counting the number of records for each major key in the store can
 * be found in the <code>KVHOME/examples/hadoop</code> directory.  The javadoc
 * for that program describes the simple Map/Reduce processing as well as how
 * to invoke the program in Hadoop.
 *
 * @since 2.0
 */
@SuppressWarnings("javadoc")
public abstract class KVInputFormatBase<K, V> extends InputFormat<K, V> {

    private static String kvStoreName = null;
    private static String[] kvHelperHosts = null;
    private static Direction direction = Direction.UNORDERED;
    private static int batchSize = 0;
    private static Key parentKey = null;
    private static KeyRange subRange = null;
    private static Depth depth = Depth.PARENT_AND_DESCENDANTS;
    private static Consistency consistency = null;
    private static long timeout = 0;
    private static TimeUnit timeoutUnit = null;
    private static String formatterClassName = null;
    private static String kvStoreSecurityFile = null;

    /**
     * @hidden
     */
    protected KVInputFormatBase() { }

    /**
     * @hidden
     * Logically split the set of input data for the job.
     *
     * @param context job configuration.
     *
     * @return an array of {@link InputSplit}s for the job.
     */
    @Override
    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {

        if (context != null) {
            final Configuration conf = context.getConfiguration();
            initializeParameters(conf);
        }

        if (kvStoreName == null) {
            throw new IllegalArgumentException
                ("No KV Store Name provided. Use either the " +
                 ParamConstant.KVSTORE_NAME.getName() +
                 " parameter or call " + KVInputFormatBase.class.getName() +
                 ".setKVStoreName().");
        }

        if (kvHelperHosts == null) {
            throw new IllegalArgumentException
                ("No KV Helper Hosts were provided. Use either the " +
                 ParamConstant.KVSTORE_NODES.getName() +
                 " parameter or call " + KVInputFormatBase.class.getName() +
                 ".setKVHelperHosts().");
        }

        final KVStoreLogin storeLogin =
            new KVStoreLogin(null, kvStoreSecurityFile);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        LoginManager loginMgr = null;
        if (storeLogin.foundTransportSettings()) {
            loginMgr = KVStoreLogin.getRepNodeLoginMgr(
                kvHelperHosts, storeLogin.getLoginCredentials(), kvStoreName);
        }
        Topology topology = null;
        try {
            topology = TopologyLocator.get(
                kvHelperHosts, 0, loginMgr, kvStoreName,
                /*
                 * This class shouldn't need to support multiple stores, and
                 * doesn't have a KVStoreImpl to get a client ID from, so don't
                 * supply a client ID
                 */
                null /* clientId */);
        } catch (KVStoreException KVSE) {
            KVSE.printStackTrace();
            return null;
        }

        /* Create a set of splits based on shards and consistency */
        final SplitBuilder sb = new SplitBuilder(topology);

        final List<TopoSplit> splits = sb.createShardSplits(consistency);
        final List<InputSplit> ret = new ArrayList<InputSplit>(splits.size());
        final RegistryUtils regUtils =
            new RegistryUtils(topology, loginMgr,
                              ClientLoggerUtils.getLogger(
                                  getClass(), kvStoreName));

        for (TopoSplit ts : splits) {
            if (ts.isEmpty()) {
                /* Split is empty, skip */
                continue;
            }

            final List<String> repNodeNames = new ArrayList<String>();
            final List<String> repNodeNamesAndPorts = new ArrayList<String>();

            for (StorageNode sn : ts.getSns(consistency, topology, regUtils)) {
                repNodeNames.add(sn.getHostname());
                repNodeNamesAndPorts.add(sn.getHostname() + ":" +
                                         sn.getRegistryPort());
            }

            ret.add(new KVInputSplit().
                    setKVHelperHosts
                    (repNodeNamesAndPorts.toArray(new String[0])).
                    setKVStoreName(kvStoreName).
                    setKVStoreSecurityFile(storeLogin.getSecurityFilePath()).
                    setLocations(repNodeNames.toArray(new String[0])).
                    setDirection(direction).
                    setBatchSize(batchSize).
                    setParentKey(parentKey).
                    setSubRange(subRange).
                    setDepth(depth).
                    setConsistency(consistency).
                    setTimeout(timeout).
                    setTimeoutUnit(timeoutUnit).
                    setFormatterClassName(formatterClassName).
                    setPartitionSets(ts.getPartitionSets()));
        }

        return ret;
    }

    /**
     * Set the KV Store name for this InputFormat to operate on. This is
     * equivalent to passing the <code>oracle.kv.kvstore</code> Hadoop
     * property.
     *
     * @param kvStoreName the KV Store name
     */
    public static void setKVStoreName(String kvStoreName) {
        KVInputFormatBase.kvStoreName = kvStoreName;
    }

    /**
     * Set the KV Helper host:port pair(s) for this InputFormat to operate on.
     * This is equivalent to passing the <code>oracle.kv.hosts</code> Hadoop
     * property.
     *
     * @param kvHelperHosts array of hostname:port strings of any hosts in the
     * KV Store.
     */
    public static void setKVHelperHosts(String[] kvHelperHosts) {
        KVInputFormatBase.kvHelperHosts = kvHelperHosts;
    }

    /**
     * Specifies the order in which records are returned by the InputFormat.
     * Only Direction.UNORDERED is allowed.
     *
     * @param direction the direction to retrieve data
     * @hidden
     */
    @Deprecated
    public static void setDirection(Direction direction) {
        if (!direction.equals(Direction.UNORDERED)) {
            throw new IllegalArgumentException("Direction " + direction +
                                               " is not supported");
        }
        KVInputFormatBase.direction = direction;
    }

    /**
     * Specifies the suggested number of keys to fetch during each network
     * round trip by the InputFormat.  If 0, an internally determined default
     * is used.  This is equivalent to passing the
     * <code>oracle.kv.batchSize</code> Hadoop property.
     *
     * @param batchSize the suggested number of keys to fetch during each
     * network round trip.
     */
    public static void setBatchSize(int batchSize) {
        KVInputFormatBase.batchSize = batchSize;
    }

    /**
     * Specifies the parent key whose "child" KV pairs are to be returned by
     * the InputFormat.  null will result in fetching all keys in the store.
     * If non-null, the major key path must be a partial path and the minor key
     * path must be empty.  This is equivalent to passing the
     * <code>oracle.kv.parentKey</code> Hadoop property.
     *
     * @param parentKey the parentKey
     */
    public static void setParentKey(Key parentKey) {
        KVInputFormatBase.parentKey = parentKey;
    }

    /**
     * Specifies a sub range to further restrict the range under the parentKey
     * to the major path components in this sub range.  It may be null.  This
     * is equivalent to passing the <code>oracle.kv.subRange</code> Hadoop
     * property.
     *
     * @param subRange the sub range.
     */
    public static void setSubRange(KeyRange subRange) {
        KVInputFormatBase.subRange = subRange;
    }

    /**
     * Specifies whether the parent and only children or all descendents are
     * returned.  If null, Depth.PARENT_AND_DESCENDENTS is implied.
     * This is equivalent to passing the <code>oracle.kv.depth</code> Hadoop
     * property.
     *
     * @param depth the depth.
     */
    public static void setDepth(Depth depth) {
        KVInputFormatBase.depth = depth;
    }

    /**
     * Specifies the read consistency associated with the lookup of the child
     * KV pairs.  Version- and Time-based consistency may not be used.  If
     * null, the default consistency is used.  This is equivalent to passing
     * the <code>oracle.kv.consistency</code> Hadoop property.
     *
     * @param consistency the consistency
     */
    @SuppressWarnings("deprecation")
    public static void setConsistency(Consistency consistency) {
        if (consistency == Consistency.ABSOLUTE ||
            consistency == Consistency.NONE_REQUIRED_NO_MASTER ||
            consistency == Consistency.NONE_REQUIRED ||
            consistency == null) {
            KVInputFormatBase.consistency = consistency;
        } else {
            throw new IllegalArgumentException
                ("Consistency may only be ABSOLUTE, " +
                 "NONE_REQUIRED_NO_MASTER, or NONE_REQUIRED");
        }
    }

    /**
     * Specifies an upper bound on the time interval for processing a
     * particular KV retrieval.  A best effort is made to not exceed the
     * specified limit.  If zero, the default request timeout is used.  This is
     * equivalent to passing the <code>oracle.kv.timeout</code> Hadoop
     * property.
     *
     * @param timeout the timeout
     */
    public static void setTimeout(long timeout) {
        KVInputFormatBase.timeout = timeout;
    }

    /**
     * Specifies the unit of the timeout parameter.  It may be null only if
     * timeout is zero.  This is equivalent to passing the
     * <code>oracle.kv.timeout</code> Hadoop property.
     *
     * @param timeoutUnit the timeout unit
     */
    public static void setTimeoutUnit(TimeUnit timeoutUnit) {
        KVInputFormatBase.timeoutUnit = timeoutUnit;
    }

    /**
     * Allows KVStore security to be set. The kvStoreSecurity file is a property
     * file utilizing the format supported by the CLI tools. This security file
     * and any wallet or password store needed to support it must be
     * distributed on the hadoop cluster.
     *
     * @since 3.0
     */
    public static void setKVSecurity(String kvStoreSecurity) {
        KVInputFormatBase.kvStoreSecurityFile = kvStoreSecurity;
    }

    private void initializeParameters(Configuration conf) {
        if (conf != null) {
            if (kvStoreName == null) {
                kvStoreName = conf.get(ParamConstant.KVSTORE_NAME.getName());
            }

            if (kvHelperHosts == null) {
                final String helperHosts =
                    conf.get(ParamConstant.KVSTORE_NODES.getName());
                if (helperHosts != null) {
                    kvHelperHosts = helperHosts.trim().split(",");
                }
            }

            final String batchSizeStr =
                conf.get(ParamConstant.BATCH_SIZE.getName());
            if (batchSizeStr != null) {
                try {
                    batchSize = Integer.parseInt(batchSizeStr);
                } catch (NumberFormatException NFE) {
                    throw new IllegalArgumentException
                        ("Invalid value for " +
                         ParamConstant.BATCH_SIZE.getName() + ": " +
                         batchSizeStr);
                }
            }

            final String parentKeyStr =
                conf.get(ParamConstant.PARENT_KEY.getName());
            if (parentKeyStr != null) {
                parentKey = Key.fromString(parentKeyStr);
            }

            final String subRangeStr =
                conf.get(ParamConstant.SUB_RANGE.getName());
            if (subRangeStr != null) {
                subRange = KeyRange.fromString(subRangeStr);
            }

            final String depthStr = conf.get(ParamConstant.DEPTH.getName());
            if (depthStr != null) {
                depth = Depth.valueOf(depthStr);
            }

            final String consistencyStr =
                conf.get(ParamConstant.CONSISTENCY.getName());
            if (consistencyStr != null) {
                consistency = ExternalDataSourceUtils.
                    parseConsistency(consistencyStr);
            }

            final String timeoutParamName = ParamConstant.TIMEOUT.getName();
            final String timeoutStr = conf.get(timeoutParamName);
            if (timeoutStr != null) {
                timeout = ExternalDataSourceUtils.parseTimeout(timeoutStr);
                timeoutUnit = TimeUnit.MILLISECONDS;
            }

            final String formatterClassNameStr =
                conf.get(ParamConstant.FORMATTER_CLASS.getName());
            if (formatterClassNameStr != null) {
                formatterClassName = formatterClassNameStr;
            }

            final String kvStoreSecurityStr =
                conf.get(ParamConstant.KVSTORE_SECURITY.getName());
            if (kvStoreSecurityStr != null && kvStoreSecurityFile == null) {
                kvStoreSecurityFile = kvStoreSecurityStr;
            }
        }
    }
}
