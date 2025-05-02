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

package oracle.kv.hadoop.table;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStoreException;
import oracle.kv.ParamConstant;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.split.SplitBuilder;
import oracle.kv.impl.topo.split.TopoSplit;
import oracle.kv.impl.util.ExternalDataSourceUtils;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This is the base class for Oracle NoSQL Database InputFormat classes that
 * can be used to run MapReduce against data stored via the Table API.
 * Keys are of type PrimaryKey. Values are always of type Row.
 * <p>
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
 * <li><code>oracle.kv.hadoop.hosts</code> - one or more <code>hostname</code>
 * strings separated by commas naming the Hadoop data node hosts in the
 * Hadoop cluster that will support MapReduce jobs.
 * This is equivalent to the {@link #setKVHadoopHosts} method.
 * The value(s) specified by this property will be returned by the
 * <code>getLocations</code> method of <code>TableInputSplit</code>. If this
 * property is not specified, or if the {@link #setKVHadoopHosts} method is
 * not called, then the values specified via the <code>oracle.kv.hosts</code>
 * property (or the {@link #setKVHelperHosts} method) will be used instead.
 *
 * <li><code>oracle.kv.tableName</code> - the name of the table in the
 * store from which data will be retrieved. This is equivalent
 * to the {@link #setTableName} method.
 *
 * <li><code>oracle.kv.primaryKey</code> - Property whose value consists of
 * the components to use when constructing the key to employ when iterating
 * the table. The format of this property's value must be a list of name:value
 * pairs in JSON FORMAT like the following:
 * <code>
 *   -Doracle.kv.primaryKey="{\"name\":\"stringVal\",\"name\":floatVal}"
 * </code>
 * where the list itself is enclosed in un-escaped double quotes and
 * corresponding curly brace; and each field name component -- as well
 * as each STRING type field value component -- is enclosed in ESCAPED
 * double quotes.
 * <p>
 * In addition to the JSON format requirement above, the values referenced by
 * the various fieldValue components of this Property must satisfy the
 * semantics of PrimaryKey for the given table; that is, they must represent
 * a first-to-last subset of the table's primary key fields, and they must be
 * specified in the same order as those primary key fields. If the components
 * of this property do not satisfy these requirements, a full primary key
 * wildcard will be used when iterating the table.
 * <p>
 * This is equivalent to the {@link #setPrimaryKeyProperty} method.
 *
 * <li><code>oracle.kv.fieldRange</code> - Property whose value consists of
 * the components to use when constructing the field range to employ when
 * iterating the table. The format of this property's value must be a list
 * of name:value pairs in JSON FORMAT like the following:
 * <code>
 *   -Doracle.kv.fieldRange="{\"name\":\"fieldName\",
 *      \"start\":\"startVal\",[\"startInclusive\":true|false],
 *      \"end\"\"endVal\",[\"endInclusive\":true|false]}"
 * </code>
 * where for the given field over which to range, the 'start', and 'end'
 * components are required, and the 'startInclusive' and 'endInclusive'
 * components are optional; defaulting to 'true' if not included. Note
 * that the list itself is enclosed in un-escaped double quotes and
 * corresponding curly brace; and each name component and string type
 * value component is enclosed in ESCAPED double quotes.
 * <p>
 * In addition to the JSON format requirement above, the values referenced
 * by the components of this Property's value must also satisfy the semantics
 * of FieldRange; that is,
 * <ul>
 *   <li>the values associated with the target key must correspond to a
 *       valid primary key in the table
 *   <li>the value associated with the fieldName must be the name of a valid
 *       field of the primary key over which iteration will be performed
 *   <li>the values associated with the start and end of the range must
 *       correspond to valid values of the given fieldName
 *   <li>the value associated with either of the inclusive components
 *       must be either 'true' or 'false'
 * </ul>
 * If the components of this property do not satisfy these requirements, then
 * table iteration will be performed over the full range of values of the
 * PrimaryKey iteration rather than a sub-range.
 * <p>
 * This is equivalent to the {@link #setFieldRangeProperty} method.
 *
 * <li><code>oracle.kv.consistency</code> - Specifies the read consistency
 * associated with the lookup of the child KV pairs.  Version- and Time-based
 * consistency may not be used.  If null, the default consistency is used.
 * <p>
 * This is equivalent to the {@link #setConsistency} method.
 *
 * <li><code>oracle.kv.timeout</code> - Specifies an upper bound on the time
 * interval for processing a particular KV retrieval.  A best effort is made to
 * not exceed the specified limit. If zero, the default request timeout is
 * used. This value is always in milliseconds.
 * <p>
 * This is equivalent to the {@link #setTimeout} and {@link #setTimeoutUnit}
 * methods.
 *
 * <li><code>oracle.kv.maxRequests</code> - Specifies the maximum number of
 * client side threads to use when running an iteration; where a value of 1
 * causes the iteration to be performed using only the current thread, and a
 * value of 0 causes the client to base the number of threads to employ on
 * the current store topology.
 * <p>
 * This is equivalent to the {@link #setMaxRequests} method.
 *
 * <li><code>oracle.kv.batchSize</code> - Specifies the suggested number of
 * keys to fetch during each network round trip by the InputFormat.  If 0, an
 * internally determined default is used. This is equivalent to the {@link
 * #setBatchSize} method.
 *
 * <li><code>oracle.kv.maxBatches</code> - Specifies the maximum number of
 * result batches that can be held in memory on the client side before
 * processing on the server side pauses. This parameter can be used to prevent
 * the client side memory from being exceeded if the client cannot consume
 * results as fast as they are generated by the server side.
 * <p>
 * This is equivalent to the {@link #setMaxBatches} method.
 *
 * </ul>
 *
 * <p>
 * Internally, the TableInputFormatBase class utilizes the method
 * <code>
 *  oracle.kv.table.TableIterator&lt;oracle.kv.table.Row&gt;
 *  TableAPI.tableIterator
 * </code>
 * to retrieve records. You should refer to the javadoc for that method
 * for information about the various parameters.
 * <p>
 *
 * <code>TableInputFormatBase</code> dynamically generates a number of
 * splits, each encapsulating a list of sets in which the elements of each
 * set are partition ids over which can be retrieved in parallel; to
 * optimize retrieval performance. The "size" of each split that is
 * generated -- which will be the value returned by the <code>getLength</code>
 * method of <code>TableInputSplit</code> -- is the number of that
 * encapsulated list of partition id sets. If the consistency passed to
 * <code>TableInputFormatBase</code> is {@link Consistency#NONE_REQUIRED
 * NONE_REQUIRED} (the default), then {@link InputSplit#getLocations
 * InputSplit.getLocations()} will return an array of the names of the
 * master and the replica(s) which contain the partition. Alternatively,
 * if the consistency is {@link Consistency#ABSOLUTE ABSOLUTE}, then the
 * array returned will contain only the name of the master. This means
 * that if Hadoop job trackers are running on the nodes named in the
 * returned <code>location</code> array, Hadoop will generally attempt
 * to run the subtasks for a particular partition on those nodes where
 * the data is stored and replicated.  Hadoop and Oracle NoSQL DB
 * administrators should be careful about co-location of Oracle NoSQL
 * DB and Hadoop processes since they may compete for resources.
 *
 * <p>
 * {@link InputSplit#getLength InputSplit.getLength()} always returns 1.
 * <p>
 *
 * A simple example demonstrating the Oracle NoSQL DB Hadoop
 * <code>oracle.kv.hadoop.table.TableInputFormat</code> class reading
 * data from Hadoop in a MapReduce job and counting the number of rows
 * in a given table in the store can be found in the
 * <code>KVHOME/examples/hadoop/table</code> directory.  The javadoc
 * for that program describes the simple MapReduce processing as well as
 * how to invoke the program in Hadoop.
 *
 * @since 3.1
 */
public abstract class TableInputFormatBase<K, V> extends InputFormat<K, V> {

    private static final String THIS_CLASSNAME =
                                    TableInputFormatBase.class.getName();
    private static final Logger LOG2 = LogManager.getLogger(THIS_CLASSNAME);

    private static final String FILE_SEP =
        System.getProperty("file.separator");
    /* Note: has protected scope to support testing. */
    protected static final String USER_SECURITY_DIR =
        System.getProperty("user.dir") + FILE_SEP +
                                         "TABLE_INPUT_FORMAT_SECURITY_DIR";

    /*
     * Static fields are used to support the MapReduce programming model;
     * where the MapReduce job is initalized with this class, and the
     * static setter methods of this class are used to initialize the
     * fields below for use in the job.
     */
    private static String kvStoreName = null;
    private static String[] kvHelperHosts = null;
    private static String[] kvHadoopHosts = null;
    private static String tableName = null;

    private static String primaryKeyProperty = null;

    /* For MultiRowOptions */
    private static String fieldRangeProperty = null;

    /* For TableIteratorOptions */
    private static Direction direction = Direction.UNORDERED;
    private static Consistency consistency = null;
    private static long timeout = 0;
    private static TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
    private static int maxRequests = 0;
    private static int batchSize = 0;
    private static int maxBatches = 0;

    /* Used by getSplits to initialize the splits that are created. */
    private static String loginFlnm = null;
    private static PasswordCredentials passwordCredentials = null;
    private static String trustFlnm = null;
    /* Used by getSplits to contact secure store locally. */
    private static String localLoginFile = null;

    private static int queryBy =
                           TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS;
    private static String whereClause = null;
    private static Integer shardKeyPartitionId = null;

    protected TopologyLocatorWrapper topologyLocator; /* For unit tests */

    /**
     * @hidden
     */
    protected TableInputFormatBase() { }

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
                 " parameter or call " + TableInputFormatBase.class.getName() +
                 ".setKVStoreName().");
        }

        if (kvHelperHosts == null) {
            throw new IllegalArgumentException
                ("No KV Helper Hosts were provided. Use either the " +
                 ParamConstant.KVSTORE_NODES.getName() +
                 " parameter or call " + TableInputFormatBase.class.getName() +
                 ".setKVHelperHosts().");
        }

        if (kvHadoopHosts == null) {
            kvHadoopHosts = new String[kvHelperHosts.length];
            for (int i = 0; i < kvHelperHosts.length; i++) {
                /* Strip off the ':port' suffix */
                final String[] hostPort = (kvHelperHosts[i]).trim().split(":");
                kvHadoopHosts[i] = hostPort[0];
            }
        }

        if (tableName == null) {
            throw new IllegalArgumentException
                ("No Table Name provided. Use either the " +
                 ParamConstant.TABLE_NAME.getName() +
                 " parameter or call " + TableInputFormatBase.class.getName() +
                 ".setTableName().");
        }

        final String userName = (passwordCredentials == null ? null :
                                 passwordCredentials.getUsername());
        final KVStoreLogin storeLogin = new KVStoreLogin(
                                                userName, localLoginFile);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        LoginManager loginMgr = null;

        if (storeLogin.foundTransportSettings()) {
            loginMgr = KVStoreLogin.getRepNodeLoginMgr(
                kvHelperHosts, passwordCredentials, kvStoreName);
        }

        /*
         * Retrieve the topology of the store.
         *
         * Note that if the there are queries that
         * must connect to different KVStores where one store is non-secure
         * and the other is secure, then if the most recent call to this method
         * invoked the code below to retrieve the topology from the secure
         * store, then the security information is stored in the system
         * properties and the state of the splits, and the client socket
         * factory used when communicating with the RMI registry while
         * retrieving the topology is configured for SSL communication. As
         * a result, if the current call to this method invokes the code below
         * to retrieve the topology of the non-secure store, and if the client
         * socket factory is not reconfigured for non-SSL communication, then
         * a KVServerException (wrapping a java.rmi.ConnectIOException) will
         * be encountered. To address this, KVStoreException is caught, the
         * client socket factory is reconfigured for non-SSL communication,
         * and the attempt to retrieve the topology is retried with no
         * security information.
         *
         * If both secure and non-secure attempts fail, then the stack trace
         * is sent to the DataNode's stderr log file.
         */
        Topology topology;
        try {
            topology = topologyLocator.get(
                kvHelperHosts, 0, loginMgr, kvStoreName,
                /*
                 * This class shouldn't need to support multiple stores, and
                 * doesn't have a KVStoreImpl to get a client ID from, so don't
                 * supply a client ID
                 */
                null /* clientId */);
        } catch (KVStoreException e) {

            if (passwordCredentials != null) {

                /* Retry with no security */

                if (LOG2.isDebugEnabled()) {
                    LOG2.debug(
                        "Failure on topology retrieval: attempt to " +
                        "communicate with RMI registry over SSL " +
                        "unsuccessful. Changing from SSLClientSocketFactory " +
                        "to ClientSocketFactory and retrying ...");
                }

                ClientSocketFactory.setRMIPolicy(null, kvStoreName,
                                                 null /* clientId */);
                RegistryUtils.initRegistryCSF();
                try {
                    topology = topologyLocator.get(kvHelperHosts, 0, null,
                                                   kvStoreName,
                                                   null /* clientId */);
                } catch (KVStoreException e1) {
                    if (LOG2.isDebugEnabled()) {
                        LOG2.debug(e1);
                    }
                    return Collections.emptyList();
                }

            } else {
                if (LOG2.isDebugEnabled()) {
                    LOG2.debug(e);
                }
                return Collections.emptyList();
            }
        }

        /* Create splits based on the store's partitions or its shards. */
        final List<TopoSplitWrapper> splits =
            getSplitInfo(topology, consistency, queryBy, shardKeyPartitionId);

        final List<InputSplit> ret = new ArrayList<InputSplit>(splits.size());
        for (TopoSplitWrapper ts : splits) {

            final TableInputSplit split = new TableInputSplit();

            split.setKVStoreName(kvStoreName);
            split.setKVHelperHosts(kvHelperHosts);
            split.setLocations(kvHadoopHosts);
            split.setTableName(tableName);
            split.setKVStoreSecurity(
                      loginFlnm, passwordCredentials, trustFlnm);
            split.setPrimaryKeyProperty(primaryKeyProperty);

            /* For MultiRowOptions */
            split.setFieldRangeProperty(fieldRangeProperty);

            /* For TableIteratorOptions */
            split.setDirection(direction);
            split.setConsistency(consistency);
            split.setTimeout(timeout);
            split.setTimeoutUnit(timeoutUnit);
            split.setMaxRequests(maxRequests);
            split.setBatchSize(batchSize);
            split.setMaxBatches(maxBatches);

            split.setPartitionSets(ts.getPartitionSets());
            split.setQueryInfo(queryBy, whereClause);
            split.setShardSet(ts.getShardSet());

            ret.add(split);
        }
        return ret;
    }

    /**
     * Set the KV Store name for this InputFormat to operate on. This is
     * equivalent to passing the <code>oracle.kv.kvstore</code> Hadoop
     * property.
     *
     * @param newStoreName the new KV Store name to set
     */
    public static void setKVStoreName(String newStoreName) {
        TableInputFormatBase.kvStoreName = newStoreName;
    }

    /**
     * Set the KV Helper host:port pair(s) for this InputFormat to operate on.
     * This is equivalent to passing the <code>oracle.kv.hosts</code> Hadoop
     * property.
     *
     * @param newHelperHosts array of hostname:port strings of any hosts
     * in the KV Store.
     */
    public static void setKVHelperHosts(String[] newHelperHosts) {
        TableInputFormatBase.kvHelperHosts = newHelperHosts;
    }

    /**
     * Set the KV Hadoop data node host name(s) for this InputFormat
     * to operate on. This is equivalent to passing the
     * <code>oracle.kv.hadoop.hosts</code> property.
     *
     * @param newHadoopHosts array of hostname strings corresponding to the
     * names of the Hadoop data node hosts in the Hadoop cluster that this
     * InputFormat will use to support MapReduce jobs.
     */
    public static void setKVHadoopHosts(String[] newHadoopHosts) {
        TableInputFormatBase.kvHadoopHosts = newHadoopHosts;
    }

    /**
     * Set the name of the table in the KV store that this InputFormat
     * will operate on. This is equivalent to passing the
     * <code>oracle.kv.tableName</code> property.
     *
     * @param newTableName the new table name to set.
     */
    public static void setTableName(String newTableName) {
        TableInputFormatBase.tableName = newTableName;
    }

    /**
     * Sets the String to use for the property value whose contents are used
     * to construct the primary key to employ when iterating the table. The
     * format of the String input to this method must be a comma-separated
     * String of the form:
     * <code>
     *   fieldName,fieldValue,fieldType,fieldName,fieldValue,fieldType,..
     * </code>
     * where the number of elements separated by commas must be a multiple
     * of 3, and each fieldType must be 'STRING', 'INTEGER', 'LONG', 'FLOAT',
     * 'DOUBLE', or 'BOOLEAN'. Additionally, the values referenced by the
     * various fieldType and fieldValue components of this String must
     * satisfy the semantics of PrimaryKey for the given table; that is,
     * they must represent a first-to-last subset of the table's primary
     * key fields, and they must be specified in the same order as those
     * primary key fields. If the String referenced by this property
     * does not satisfy these requirements, a full primary key wildcard
     * will be used when iterating the table.
     * <p>
     * This is equivalent to passing the <code>oracle.kv.primaryKey</code>
     * Hadoop property.
     *
     * @param newProperty the new shard key property to set
     */
    public static void setPrimaryKeyProperty(String newProperty) {
        TableInputFormatBase.primaryKeyProperty = newProperty;
    }

    /* Methods related to MultiRowOptions */

    /**
     * Sets the String to use for the property value whose contents are used
     * to construct the field range to employ when iterating the table. The
     * format of this property's value must be a list of name:value pairs in
     * JSON FORMAT like the following:
     * <code>
     *   -Doracle.kv.fieldRange="{\"name\":\"fieldName\",
     *      \"start\":\"startVal\",[\"startInclusive\":true|false],
     *      \"end\"\"endVal\",[\"endInclusive\":true|false]}"
     * </code>
     * where for the given field over which to range, the 'start', and 'end'
     * components are required, and the 'startInclusive' and 'endInclusive'
     * components are optional; defaulting to 'true' if not included. Note
     * that the list itself is enclosed in un-escaped double quotes and
     * corresponding curly brace; and each name component and string type
     * value component is enclosed in ESCAPED double quotes.
     * <p>
     * In addition to the JSON format requirement above, the values referenced
     * by the components of this Property's value must also satisfy the
     * semantics of FieldRange; that is,
     * <ul>
     *   <li>the values associated with the target key must correspond to a
     *       valid primary key in the table
     *   <li>the value associated with the fieldName must be the name of a
     *       valid field of the primary key over which iteration will be
     *       performed
     *   <li>the values associated with the start and end of the range must
     *       correspond to valid values of the given fieldName
     *   <li>the value associated with either of the inclusive components
     *       must be either 'true' or 'false'
     * </ul>
     * If the components of this property do not satisfy these requirements,
     * then table iteration will be performed over the full range of values
     * of the PrimaryKey iteration rather than a sub-range.
     * <p>
     * This is equivalent to passing the <code>oracle.kv.fieldRange</code>
     * Hadoop property.
     *
     * @param newProperty the new field range property to set
     */
    public static void setFieldRangeProperty(String newProperty) {
        TableInputFormatBase.fieldRangeProperty = newProperty;
    }

    /* Methods related to TableIteratorOptions */

    /**
     * Specifies the order in which records are returned by the InputFormat.
     * Note that when doing PrimaryKey iteration, only Direction.UNORDERED
     * is allowed.
     *
     * @param newDirection the direction to retrieve data
     */
    public static void setDirection(Direction newDirection) {
        TableInputFormatBase.direction = newDirection;
    }

    /**
     * Specifies the read consistency associated with the lookup of the child
     * KV pairs.  Version- and Time-based consistency may not be used.  If
     * null, the default consistency is used.  This is equivalent to passing
     * the <code>oracle.kv.consistency</code> Hadoop property.
     *
     * @param consistency the consistency
     */
    public static void setConsistency(Consistency consistency) {
        if (consistency == Consistency.ABSOLUTE ||
            consistency == Consistency.NONE_REQUIRED ||
            consistency == null) {
            TableInputFormatBase.consistency = consistency;
        } else {
            /* Note: Consistency.NONE_REQUIRED_NO_MASTER was deprecated
             * as of release 4.3, and users are told to use
             * Consistency.NONE_REQUIRED instead.
             */
            throw new IllegalArgumentException
                ("Consistency may only be ABSOLUTE, or NONE_REQUIRED");
        }
    }

    /**
     * Specifies an upper bound on the time interval for processing a
     * particular KV retrieval.  A best effort is made to not exceed the
     * specified limit.  If zero, the default request timeout is used.
     * This is equivalent to passing the <code>oracle.kv.timeout</code>
     * Hadoop property.
     *
     * @param timeout the timeout
     */
    public static void setTimeout(long timeout) {
        TableInputFormatBase.timeout = timeout;
    }

    /**
     * Specifies the unit of the timeout parameter. The value input may
     * be null, only if timeout is currently zero; in which case an
     * IllegalArgumentException is thrown. This is equivalent to passing
     * the <code>oracle.kv.timeout.unit</code> Hadoop property.
     *
     * @param timeoutUnit the timeout unit
     */
    public static void setTimeoutUnit(TimeUnit timeoutUnit) {
        if (timeoutUnit == null && timeout != 0) {
            throw new IllegalArgumentException("null can be input for the " +
                          "TimeUnit only if the current timeout value " +
                          "is 0 [timeout=" + timeout + "]");
        }
        TableInputFormatBase.timeoutUnit = timeoutUnit;
    }

    /**
     * Specifies the maximum number of client side threads to use when running
     * an iteration; where a value of 1 causes the iteration to be performed
     * using only the current thread, and a value of 0 causes the client to
     * base the number of threads to employ on the current store topology.
     * <p>
     * This is equivalent to passing the <code>oracle.kv.maxRequests</code>
     * Hadoop property.
     *
     * @param newMaxRequests the suggested number of threads to employ when
     * an iteration.
     */
    public static void setMaxRequests(int newMaxRequests) {
        TableInputFormatBase.maxRequests = newMaxRequests;
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
        TableInputFormatBase.batchSize = batchSize;
    }

    /**
     * Specifies the maximum number of result batches that can be held in
     * memory on the client side before processing on the server side
     * pauses. This parameter can be used to prevent the client side memory
     * from being exceeded if the client cannot consume results as fast as
     * they are generated by the server side.
     * <p>
     * This is equivalent to passing the <code>oracle.kv.maxBatches</code>
     * Hadoop property.
     *
     * @param newMaxBatches the suggested number of threads to employ when
     * an iteration.
     */
    public static void setMaxBatches(int newMaxBatches) {
        TableInputFormatBase.maxBatches = newMaxBatches;
    }

    /**
     * Sets the login properties file and the public trust file (keys
     * and/or certificates), as well as the <code>PasswordCredentials</code>
     * for authentication. The value of the <code>loginFile</code> and
     * <code>trustFile</code> parameters must be either a fully qualified
     * path referencing a file located on the local file system, or the
     * name of a file (no path) whose contents can be retrieved as a
     * resource from the current VM's classpath.
     * <p>
     * Note that this class provides the <code>getSplits</code> method;
     * which must be able to contact a secure store, and so will need
     * access to local copies of the login properties and trust files.
     * As a result, if the values input for the <code>loginFile</code> and
     * <code>trustFile</code> parameters are simple file names rather
     * than fully qualified paths, this method will retrieve the contents
     * of each from the classpath and generate private, local copies of
     * the associated file for availability to the <code>getSplits</code>
     * method.
     */
    public static void setKVSecurity(
                           final String loginFile,
                           final PasswordCredentials userPasswordCredentials,
                           final String trustFile)
                               throws IOException {

        setLocalKVSecurity(loginFile, userPasswordCredentials, trustFile);
    }

    private void initializeParameters(Configuration conf) throws IOException {
        /*
         * Must always reinitialize all fields of this class; because there
         * are use cases in which the field values of this class change over
         * time. For example, if jobs can/may specify a
         * different tableName and/or kvStoreName and/or kvHelperHosts.
         * If these values are not reinitialized on each call to this
         * method, then because the fields are static, the values from the
         * previous query will be incorrectly used for the current query;
         * which can result in errors or incorrect results.
         *
         * On the other hand, for a basic MapReduce job, the values of the
         * static fields are set once (via either command line arguments or
         * a system property), when the job is initiated . In that case, the
         * system property will not survive the serialization/deserialization
         * process and thus will return null below. For the purposes of this
         * method then, a null system property is taken to be an indication
         * that this class is being employed in a MapReduce job initiated
         * from the command line.
         * And it is then assumed that the static fields processed by this
         * method must have been set during job initiation and that each
         * field's value survived its journey from the job's client side to
         * the MapReduce server side.
         */
        if (conf != null) {

            final String kvStoreNameProp =
                             conf.get(ParamConstant.KVSTORE_NAME.getName());
            if (kvStoreNameProp != null) {
                kvStoreName = kvStoreNameProp;
            }

            final String helperHosts =
                conf.get(ParamConstant.KVSTORE_NODES.getName());
            if (helperHosts != null) {
                kvHelperHosts = helperHosts.trim().split(",");
            }

            final String hadoopHosts =
                conf.get(ParamConstant.KVHADOOP_NODES.getName());
            if (hadoopHosts != null) {
                kvHadoopHosts = hadoopHosts.trim().split(",");
            } else {
                if (kvHelperHosts != null) {
                    kvHadoopHosts = new String[kvHelperHosts.length];
                    for (int i = 0; i < kvHelperHosts.length; i++) {
                        /* Strip off the ':port' suffix */
                        final String[] hostPort =
                            (kvHelperHosts[i]).trim().split(":");
                        kvHadoopHosts[i] = hostPort[0];
                    }
                }
            }

            final String tableNameProp =
                             conf.get(ParamConstant.TABLE_NAME.getName());
            if (tableNameProp != null) {
                tableName = tableNameProp;
            }

            final String primaryKeyProp =
                             conf.get(ParamConstant.PRIMARY_KEY.getName());
            if (primaryKeyProp != null) {
                primaryKeyProperty = primaryKeyProp;
            }

            /* For MultiRowOptions. */
            final String fieldRangeProp =
                             conf.get(ParamConstant.FIELD_RANGE.getName());
            if (fieldRangeProp != null) {
                fieldRangeProperty = fieldRangeProp;
            }

            /*
             * For TableIteratorOptions. Note that when doing PrimaryKey
             * iteration, Direction must be UNORDERED.
             */

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
            }

            final String timeoutUnitParamName =
                ParamConstant.TIMEOUT_UNIT.getName();
            final String timeoutUnitStr = conf.get(timeoutUnitParamName);
            if (timeoutUnitStr != null) {
                timeoutUnit = TimeUnit.valueOf(timeoutUnitStr);
            }

            final String maxRequestsStr =
                conf.get(ParamConstant.MAX_REQUESTS.getName());
            if (maxRequestsStr != null) {
                try {
                    maxRequests = Integer.parseInt(maxRequestsStr);
                } catch (NumberFormatException NFE) {
                    throw new IllegalArgumentException
                        ("Invalid value for " +
                         ParamConstant.MAX_REQUESTS.getName() + ": " +
                         maxRequestsStr);
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

            final String maxBatchesStr =
                conf.get(ParamConstant.MAX_BATCHES.getName());
            if (maxBatchesStr != null) {
                try {
                    maxBatches = Integer.parseInt(maxBatchesStr);
                } catch (NumberFormatException NFE) {
                    throw new IllegalArgumentException
                        ("Invalid value for " +
                         ParamConstant.MAX_BATCHES.getName() + ": " +
                         maxBatchesStr);
                }
            }

            /* Handle the properties related to security. */
            final String loginFile =
                conf.get(KVSecurityConstants.SECURITY_FILE_PROPERTY);
            final String trustFile =
                conf.get(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
            final String username =
                conf.get(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
            final String passwordStr =
                conf.get(ParamConstant.AUTH_USER_PWD_PROPERTY.getName());

            /* Create the PasswordCredentials needed to contact the store. */
            PasswordCredentials passwordCreds = null;
            if (username != null && passwordStr != null) {
                final char[] userPassword = passwordStr.toCharArray();
                passwordCreds =
                    new PasswordCredentials(username, userPassword);
            }

            if (passwordCreds == null) {
                String passwordLoc = conf.get(
                                     KVSecurityConstants.AUTH_WALLET_PROPERTY);
                PasswordManager storeMgr = null;
                if (passwordLoc != null) {

                    /* Retrieve the password from the given wallet. */
                    final File walletDirFd = new File(passwordLoc);
                    try {
                        storeMgr = PasswordManager.load(
                            PasswordManager.WALLET_MANAGER_CLASS);
                    } catch (Exception e) {
                        if (LOG2.isDebugEnabled()) {
                            e.printStackTrace(); /* Send to DataNode stderr */
                        }
                        throw new IOException(e);
                    }
                    final PasswordStore fileStore =
                                        storeMgr.getStoreHandle(walletDirFd);
                    fileStore.open(null);
                    final Collection<String> secretAliases =
                                                 fileStore.getSecretAliases();
                    final Iterator<String> aliasItr = secretAliases.iterator();
                    final char[] userPassword = (aliasItr.hasNext() ?
                                 fileStore.getSecret(aliasItr.next()) : null);
                    if (username != null) {
                        passwordCreds =
                            new PasswordCredentials(username, userPassword);
                    }
                    fileStore.discard();
                } else {
                    passwordLoc = conf.get(
                                  KVSecurityConstants.AUTH_PWDFILE_PROPERTY);
                    if (passwordLoc != null) {

                        /* Retrieve password from the given password file. */
                        final File passwordFileFd = new File(passwordLoc);
                        try {
                            storeMgr = PasswordManager.load(
                                PasswordManager.FILE_STORE_MANAGER_CLASS);
                        } catch (Exception e) {
                            if (LOG2.isDebugEnabled()) {
                                e.printStackTrace(); /* To DataNode stderr */
                            }
                            throw new IOException(e);
                        }
                        final PasswordStore fileStore =
                                  storeMgr.getStoreHandle(passwordFileFd);
                        fileStore.open(null);
                        final Collection<String> secretAliases =
                                                 fileStore.getSecretAliases();
                        final Iterator<String> aliasItr =
                                                   secretAliases.iterator();
                        final char[] userPassword = (aliasItr.hasNext() ?
                                  fileStore.getSecret(aliasItr.next()) : null);
                        if (username != null) {
                            passwordCreds = new PasswordCredentials(
                                                    username, userPassword);
                        }
                        fileStore.discard();
                    }
                }
            }
            setLocalKVSecurity(loginFile, passwordCreds, trustFile);
        }
    }

    /**
     * Set/create the artifacts required to connect to and interact
     * with a secure store; specifically, a login properties file,
     * a trust file containing public keys and/or certificates, and
     * <code>PasswordCredentials</code>. If the value input for the
     * <code>loginFile</code> and <code>trustFile</code> parameter
     * is a fully-qualified path, then this method initializes the
     * corresponding static variables to those values so that the
     * <code>getSplits</code> method can contact a secure store, extracts
     * the filenames from those paths, uses those values to initialize
     * the corresponding static filename variables used to initialize
     * the splits that are created, and returns.
     * <p>
     * If the value input for the <code>loginFile</code> and
     * <code>trustFile</code> parameter is not a fully-qualified path,
     * then this method uses the given file names to retrieve the contents
     * of the associated login file and trust file as resources from
     * the classpath, and writes that information to corresponding files
     * on the local file system (in a directory owned by the user under
     * which the application is executed). After generating the local
     * files, the fully-qualified paths to those files are used to
     * initialize the corresponding static variables so that the
     * <code>getSplits</code> method can contact a secure store.
     */
    private static void setLocalKVSecurity(
                            final String loginFile,
                            final PasswordCredentials userPasswordCredentials,
                            final String trustFile)
                                throws IOException {

        if (loginFile == null) {
            return;
        }

        if (userPasswordCredentials == null) {
            return;
        }

        if (trustFile == null) {
            return;
        }

        final File loginFd = new File(loginFile);
        boolean loginIsAbsolute = false;
        if (loginFd.isAbsolute()) {
            loginIsAbsolute = true;
            TableInputFormatBase.localLoginFile = loginFile;
            TableInputFormatBase.loginFlnm = loginFd.getName();
        } else {
            TableInputFormatBase.loginFlnm = loginFile;
        }

        TableInputFormatBase.passwordCredentials = userPasswordCredentials;

        final File trustFd = new File(trustFile);
        boolean trustIsAbsolute = false;
        if (trustFd.isAbsolute()) {
            trustIsAbsolute = true;
            TableInputFormatBase.trustFlnm = trustFd.getName();
        } else {
            TableInputFormatBase.trustFlnm = trustFile;
        }

        if (loginIsAbsolute && trustIsAbsolute) {
            return;
        }

        /*
         * If loginFile and/or trustFile is a filename and not an absolute
         * path, then generate local versions of the file.
         */
        final File userSecurityDirFd = new File(USER_SECURITY_DIR);
        if (!userSecurityDirFd.exists()) {
            if (!userSecurityDirFd.mkdirs()) {
                throw new IOException("failed to create " + userSecurityDirFd);
            }
        }

        final ClassLoader cl = TableInputFormatBase.class.getClassLoader();

        if (!loginIsAbsolute) {

            InputStream loginStream = null;
            if (cl != null) {
                loginStream = cl.getResourceAsStream(loginFlnm);
            } else {
                loginStream = ClassLoader.getSystemResourceAsStream(loginFlnm);
            }

            /*
             * Retrieve the login configuration as a resource from the
             * classpath, and write that information to the user's local
             * file system. But exclude any properties related to user
             * authorization; that is, exclude all properties of the form
             * 'oracle.kv.auth.*", to prevent those property values from
             * being sent and cached on the DataNodes.
             */
            final Properties loginProps = new Properties();
            if (loginStream != null) {
                loginProps.load(loginStream);
            }

            /* Exclude 'oracle.kv.auth.*" properties. */
            loginProps.remove(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
            loginProps.remove(KVSecurityConstants.AUTH_WALLET_PROPERTY);
            loginProps.remove(KVSecurityConstants.AUTH_PWDFILE_PROPERTY);

            /* Strip off the path of the trust file. */
            final String trustProp =
                loginProps.getProperty(
                    KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
            if (trustProp != null) {
                final File trustPropFd = new File(trustProp);
                if (!trustPropFd.exists()) {
                    loginProps.setProperty(
                        KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                        trustPropFd.getName());
                }
            }

            final File absoluteLoginFd =
                new File(USER_SECURITY_DIR + FILE_SEP + loginFlnm);
            final FileOutputStream loginFos =
                                       new FileOutputStream(absoluteLoginFd);
            try {
                loginProps.store(loginFos, null);
            } finally {
                loginFos.close();
            }

            TableInputFormatBase.localLoginFile = absoluteLoginFd.toString();
        }

        if (!trustIsAbsolute) {

            InputStream trustStream = null;
            if (cl != null) {
                trustStream = cl.getResourceAsStream(trustFlnm);
            } else {
                trustStream = ClassLoader.getSystemResourceAsStream(trustFlnm);
            }

            /*
             * To prevent NullPointerException, test for null for the case
             * where the classpath does not contain the trust file name as
             * a resource.
             */
            if (trustStream != null) { /* To prevent NullPointerException */
                /*
                 * Retrieve the trust credentials as a resource from the
                 * classpath, and write that information to the user's
                 * local file system.
                 */
                final File absoluteTrustFd =
                    new File(USER_SECURITY_DIR + FILE_SEP + trustFlnm);
                final FileOutputStream trustFlnmFos =
                    new FileOutputStream(absoluteTrustFd);
                try {
                    int nextByte = trustStream.read();
                    while (nextByte != -1) {
                        trustFlnmFos.write(nextByte);
                        nextByte = trustStream.read();
                    }
                } finally {
                    trustFlnmFos.close();
                }
            }
        }
    }

    public void setQueryInfo(final int newQueryBy,
                             final String newWhereClause,
                             final Integer newPartitionId) {

        queryBy = newQueryBy;
        whereClause = newWhereClause;
        shardKeyPartitionId = newPartitionId;
    }

    /**
     * Convenience method that returns a list whose elements encapsulate
     * the information needed to create the necessary splits; based on
     * whether a TableScan (partition based) or an IndexScan (shard based)
     * will be used to satisfy the read request (query).
     */
    private List<TopoSplitWrapper> getSplitInfo(
                                       final Topology topology,
                                       final Consistency readConsistency,
                                       final int whereQueryBy,
                                       final Integer singlePartitionId) {

        final List<TopoSplitWrapper> retList =
            new ArrayList<TopoSplitWrapper>();

        if (topology == null) {
            return retList;
        }

        /* Determine how the splits should be generated. */
        boolean buildSplits = false;
        boolean singleSplit = false;
        int singleId = 1;

        switch (whereQueryBy) {

            case TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS:
            case TableInputSplit.QUERY_BY_ONQL_ALL_PARTITIONS:

                buildSplits = true;
                break;

            case TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION:
            case TableInputSplit.QUERY_BY_ONQL_SINGLE_PARTITION:

                if (singlePartitionId == null) {
                    buildSplits = true;
                } else {
                    singleSplit = true;
                    singleId = singlePartitionId.intValue();
                }
                break;

            default:

                /* Skip partition based splits for shard based splits. */
                break;
        }

        /*
         * If a table scan (ALL_PARTITIONS) will be used when pushing the
         * predicate, then the SplitBuilder is first used to compute disjoint
         * subsets of the store's partitions, wrapped in a TopoSplit class.
         * Then a TopoSplitWrapper is used to map each of those partition
         * subsets to the shards corresponding to the partitions of the
         * subset; so that one split per partition subset is created.
         */
        if (buildSplits) {

            final SplitBuilder sb = new SplitBuilder(topology);
            final List<TopoSplit> topoSplits =
                sb.createShardSplits(readConsistency);

            for (TopoSplit topoSplit : topoSplits) {
                final Set<RepGroupId> shardSet = new HashSet<RepGroupId>();
                final List<Set<Integer>> partitionSets =
                    topoSplit.getPartitionSets();
                for (Set<Integer> partitionIds : partitionSets) {
                    for (Integer pId : partitionIds) {
                        final PartitionId partitionId = new PartitionId(pId);
                        final RepGroupId repGroupId =
                            topology.getRepGroupId(partitionId);
                        shardSet.add(repGroupId);
                    }
                }
                retList.add(new TopoSplitWrapper(topoSplit, shardSet));
            }
            return retList;
        }

        /* For executing the query against a SINGLE_PARTITION. */
        if (singleSplit) {

            final Set<Integer> partitionSet = new HashSet<Integer>();
            partitionSet.add(singlePartitionId);

            final Set<RepGroupId> shardSet = new HashSet<RepGroupId>();
            shardSet.add(topology.getRepGroupId(new PartitionId(singleId)));

            retList.add(
                new TopoSplitWrapper(
                        new TopoSplit(0, partitionSet), shardSet));

            return retList;
        }

        /*
         * Either QUERY_BY_PRIMARY_ALL_SHARDS or QUERY_BY_ONQL_ALL_SHARDS
         * remains. For either case, since an index is involved, simply
         * return the store's shards; so that one split per shard is
         * created.
         */
        final Set<RepGroupId> shardIds = topology.getRepGroupIds();
        for (RepGroupId shardId : shardIds) {
            final Set<RepGroupId> shardSet = new HashSet<RepGroupId>();
            shardSet.add(shardId);
            retList.add(new TopoSplitWrapper(null, shardSet));
        }
        return retList;
    }

    /**
     * Convenience class that wraps a TopoSplit containing a subset of the
     * store's partitions and a set containing one of the store's shards.
     */
    private static class TopoSplitWrapper {

        private final TopoSplit topoSplit;
        private final Set<RepGroupId> shardSet;

        TopoSplitWrapper(TopoSplit topoSplit, Set<RepGroupId> shardSet) {
            this.topoSplit = topoSplit;
            this.shardSet = shardSet;
        }

        List<Set<Integer>> getPartitionSets() {
            if (topoSplit != null) {
                return topoSplit.getPartitionSets();
            }
            /* Avoid NPE in write method during split serialization. */
            return Collections.emptyList();
        }

        Set<RepGroupId> getShardSet() {
            if (shardSet != null) {
                return shardSet;
            }
            /* Avoid NPE in write method during split serialization. */
            return Collections.emptySet();
        }
    }

    /**
     * Convenience method for testing. A test calls this method when a test
     * run prior to the current test has set the security related fields
     * to values that may interfere with the current test.
     */
    public static void clearLocalKVSecurity() {
        TableInputFormatBase.localLoginFile = null;
        TableInputFormatBase.passwordCredentials = null;
        TableInputFormatBase.trustFlnm = null;
    }

    /**
     * Special class that wraps the static get method of the TopologyLocator
     * utility. Since static methods are difficult to mock, this class is
     * provided as a convenience; to allow tests to mock the behavior of the
     * static methods of the TopologyLocator class.
     */
    public static class TopologyLocatorWrapper {

        public TopologyLocatorWrapper() { }

        public Topology get(final String[] registryHostPorts,
                            final int maxRNs,
                            final LoginManager loginMgr,
                            final String expectedStoreName,
                            final oracle.kv.impl.api.ClientId clientId)
                                throws KVStoreException {

            return TopologyLocator.get(registryHostPorts, maxRNs, loginMgr,
                                       expectedStoreName, clientId);
        }
    }
}
