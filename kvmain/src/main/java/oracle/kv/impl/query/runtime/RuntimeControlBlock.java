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

package oracle.kv.impl.query.runtime;

import static oracle.kv.impl.util.ThreadUtils.threadId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import java.time.temporal.ChronoUnit;
import java.time.Clock;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.ParallelScanIterator;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.ops.TableQueryHandler;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.ResumeInfo.VirtualScan;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.query.ExecuteOptions;

/**
 *
 */
public class RuntimeControlBlock {

    /*
     * KVStoreImpl is set on the client side and is used by the dispatch
     * code that sends queries to the server side. Not applicable to the
     * server RCBs.
     */
    private final KVStoreImpl theStore;

    private final Logger theLogger;

    /*
     * TableMetadataHelper is required by operations to resolve table and
     * index names.
     */
    private final TableMetadataHelper theMetadataHelper;

    /*
     * To achieve improved client side parallelization when satisfying a
     * query, some clients (for example, the Hive/BigDataSQL mechanism)
     * will distribute requests among separate processes and then scan
     * either a set of partitions or shards; depending on the distribution
     * kind (ALL_PARTITIONS, SINGLE_PARTITION, ALL_SHARDS) computed for
     * the given query. Not applicable to the server RCBs.
     */
    private Set<Integer> thePartitions;

    private Set<RepGroupId> theShards;

    /*
     * ExecuteOptions are options set by the application and used to control
     * some aspects of database access, such as Consistency, timeouts, batch
     * sizes, etc. Not applicable to the server RCBs (for server RCBs, the
     * options are caried in theQueryOp).
     */
    private final ExecuteOptions theExecuteOptions;

    private final byte theTraceLevel;

    private final StringBuilder theTraceBuilder = new StringBuilder();

    /* The TableQuery operation. Not applicable to the client RCB. */
    private final TableQuery theQueryOp;

    /* The TableQueryHandler. Not applicable to the client RCB. */
    private final TableQueryHandler theQueryHandler;

    private int theNumJoinBranches = 1;

    private int theJoinPid = -1;

    private final PlanIter theRootIter;

    /*
     * See javadoc for ServerIterFactory. Not applicable to the client RCB.
     */
    private final ServerIterFactory theServerIterFactory;

    /*
     * The state array contains as many elements as there are PlanIter instances
     * in the query plan to be executed.
     */
    private final PlanIterState[] theIteratorStates;

    /*
     * The register array contains as many elements as required by the
     * instances in the query plan to be executed.
     */
    private final FieldValueImpl[] theRegisters;

    /*
     * An array storing the values of the extenrnal variables set for the
     * operation. These come from the map in the BoundStatement.
     */
    private FieldValueImpl[] theExternalVars;

    /* Not applicable to the server RCBs. */
    private byte[] theContinuationKey;

    /*
     * The following 2 fields store the deserialized values from the
     * continuation key given back to us by the app. Not applicable
     * to the server RCBs.
     */
    private int thePidOrShardIdx;

    private ResumeInfo theResumeInfo;

    /*
     * At the proxy, these are the total readKB/writeKB consumed during
     * execution of the current query batch. Not applicable to the server RCBs.
     * (at the servers, theRead/WriteKB are in theQueryOp).
     */
    private int theReadKB;
    private int theWriteKB;

    /* The total number of records returned */
    private int theResultSize;

    /* Applies to server RCBs only. It is set to true when the query reaches its
     * batch limit (either the readKB limit, or the numResults limit, or the
     * timeout). */
    private boolean theNeedToSuspend;

    /* Applies to server RCBs only. Set to true if the query is in a state that
     * canoot suspend. Currently this is the case with an SFW iterator that is
     * in the middle of unnesting. */
    private boolean theCannotSuspend;

    private int theCannotSuspendDueToDelayedResult;

    /* This flag applies to proxy-based queries only. A true value means that
     * the query must suspend everywhere (at the RNs, the proxy, and the SDK
     * driver) and return control to the application, At the RNs, the flag is
     * set to true when the query reaches the readKB limit. At the proxy, it
     * is set to true when the query reaches either the readKB limit or the
     * numResults limit. This flag is then propagated by the proxy to the SDKs. */
    private boolean theReachedLimit;

    /*
     * The RCB holds the TableIterator for the current remote call from the
     * ReceiveIter if there is one. This is here so that the query results
     * objects can return partition and shard metrics for the distributed
     * query operation. Not applicable to the server RCBs.
     */
    private ParallelScanIterator<FieldValueImpl> theTableIterator;

    /*
     * The number of memory bytes consumed by the query at the client for
     * blocking operations (duplicate elimination, sorting). Not applicable
     * to the server RCBs.
     */
    private long theMemoryConsumption;

    private volatile long theEndTime;

    private long theTimeout;

    /*
     * True if the query uses a secondary index. The value of this field is
     * valid only after the query plan has been opened. It is used to determine
     * whether a single-partition query uses the primary or a secondary index.
     * Not applicable to the client RCBs.
     */
    private boolean theUsesIndex;

    private Topology theBaseTopo;

    /*
     * The partitions that migrate out of this shard during the index scan
     * Not applicable to the client RCBs.
     */
    private final ArrayList<PartitionId> theMigratedPartitions;

    /*
     * The partitions that return to this shard during the index scan. (A
     * partition that migrates out of a shard may return to it due to a
     * failure at the target shard).
     * Not applicable to the client RCBs.
     */
    private final ArrayList<PartitionId> theRestoredPartitions;

    List<VirtualScan> theNewVirtualScans;

    /**
     * Constructor used at the client only.
     */
    public RuntimeControlBlock(
        KVStoreImpl store,
        Logger logger,
        TableMetadataHelper mdHelper,
        Set<Integer> partitions,
        Set<RepGroupId> shards,
        ExecuteOptions executeOptions,
        PlanIter rootIter,
        int numIters,
        int numRegs,
        FieldValueImpl[] externalVars) {

        theStore = store;
        theLogger = logger;
        theMetadataHelper = mdHelper;

        thePartitions = partitions;
        theShards = shards;

        theExecuteOptions = executeOptions;
        theTraceLevel = (executeOptions != null ?
                         executeOptions.getTraceLevel() :
                         0);

        theRootIter = rootIter;

        theIteratorStates = new PlanIterState[numIters];
        theRegisters = new FieldValueImpl[numRegs];
        theExternalVars = (externalVars != null ?
                           externalVars :
                           new FieldValueImpl[8]);

        theContinuationKey = (theExecuteOptions != null ?
                              theExecuteOptions.getContinuationKey() :
                              null);
        parseContinuationKey();

        theMigratedPartitions = null;
        theRestoredPartitions = null;
        theQueryOp = null;
        theQueryHandler = null;
        theServerIterFactory = null;

        theTimeout = (theStore != null ?
                      theStore.getTimeoutMs(theExecuteOptions.getTimeout(),
                                            theExecuteOptions.getTimeoutUnit()) :
                      0);
        theEndTime = System.currentTimeMillis() + theTimeout;
    }

    /**
     * Constructor used at the RNs only.
     */
    public RuntimeControlBlock(
        Logger logger,
        TableMetadataHelper mdHelper,
        ExecuteOptions options,
        TableQuery queryOp,
        TableQueryHandler queryHandler,
        ServerIterFactory serverIterFactory) {

        theStore = null;
        thePartitions = null;
        theShards = null;

        theExecuteOptions = options;

        theLogger = logger;
        theMetadataHelper = mdHelper;

        theQueryOp = queryOp;
        theQueryHandler = queryHandler;
        theServerIterFactory = serverIterFactory;

        theTraceLevel = queryOp.getTraceLevel();

        theExternalVars = (queryOp.getExternalVars() != null ?
                           queryOp.getExternalVars() :
                           new FieldValueImpl[8]);

        theRootIter = queryOp.getQueryPlan();

        theIteratorStates = new PlanIterState[queryOp.getNumIterators()];
        theRegisters = new FieldValueImpl[queryOp.getNumRegisters()];

        theResumeInfo = theQueryOp.getResumeInfo();
        theMigratedPartitions = new ArrayList<PartitionId>();
        theRestoredPartitions = new ArrayList<PartitionId>();

        theTimeout = theQueryOp.getTimeout();
        theEndTime = System.currentTimeMillis() + theTimeout;
    }

    boolean isServerRCB() {
        return theStore == null;
    }

    public KVStoreImpl getStore() {
        return theStore;
    }

    public Logger getLogger() {
        return theLogger;
    }

    public TableMetadataHelper getMetadataHelper() {
        return theMetadataHelper;
    }

    public Set<Integer> getPartitionSet() {
        return thePartitions;
    }

    void setPartitionSet(Set<Integer> parts) {
        if (thePartitions != null) {
            thePartitions.retainAll(parts);
        } else {
            thePartitions = parts;
        }
    }

    void setShardSet(Set<RepGroupId> shards) {
       if (theShards != null) {
            theShards.retainAll(shards);
        } else {
            theShards = shards;
        }
    }

    public Set<RepGroupId> getShardSet() {
        return theShards;
    }

    public ExecuteOptions getExecuteOptions() {
        return theExecuteOptions;
    }

    public byte getTraceLevel() {
        return theTraceLevel;
    }

    public boolean doLogFileTracing() {
        return (isServerRCB() ?
                theQueryOp.doLogFileTracing() :
                theExecuteOptions.doLogFileTracing());
    }

    public void trace(String msg) {
        trace(false, msg);
    }

    public void trace(boolean forceLogFileTracing, String msg) {

        if (UserDataControl.hideUserData()) {
            return;
        }

        if (forceLogFileTracing || doLogFileTracing()) {
            if (isServerRCB() && theLogger != null) {
                theLogger.info(
                     "QUERY " + getQueryName() +
                     "(" + threadId(Thread.currentThread()) + ")" +
                     " : " + msg);
            } else {
                System.out.println(
                    Clock.systemUTC().instant().
                    truncatedTo(ChronoUnit.MILLIS) +
                    " QUERY " + getQueryName() +
                    "(" + threadId(Thread.currentThread()) +
                    ") : " + msg);
            }
        } else {
            synchronized (theTraceBuilder) {
                String time = Clock.systemUTC().instant().
                              truncatedTo(ChronoUnit.MILLIS).toString();
                time = time.substring(11);
                theTraceBuilder.append(time);
                if (!isServerRCB()) {
                    theTraceBuilder.append(" (").
                        append(threadId(Thread.currentThread())).
                        append(") :\n");
                } else {
                    theTraceBuilder.append(" :\n");
                }
                theTraceBuilder.append(msg).append("\n");
            }
        }
    }

    public String getTrace() {
        synchronized (theTraceBuilder) {
            return theTraceBuilder.toString();
        }
    }

    public void setTrace(String trace) {

        if (trace != null) {
            theTraceBuilder.append(trace);
        }
    }

    int getBatchCounter() {
        assert(!isServerRCB());
        return theExecuteOptions.getBatchCounter();
    }

    Consistency getConsistency() {
        return theExecuteOptions.getConsistency();
    }

    Durability getDurability() {
        return theExecuteOptions.getDurability();
    }

    short getCompilerVersion() {
        return (isServerRCB() ?
                theQueryOp.getPlanVersion() :
                SerialVersion.CURRENT);
    }

    public MathContext getMathContext() {
        return (isServerRCB() ?
                theQueryOp.getMathContext() :
                theExecuteOptions.getMathContext());
    }

    public long getTimeoutMs() {
        return theTimeout;
    }

    public void refreshEndTime() {

        assert(!isServerRCB());
        if(!isProxyQuery()) {
            theEndTime = System.currentTimeMillis() + theTimeout;
        }
    }

    long getRemainingTime() {

        assert(!isServerRCB());
        long tm =  theEndTime -  System.currentTimeMillis();

        if (tm <= 0) {
            throw new RequestTimeoutException((int)getTimeoutMs(),
                                              "Query request timed out",
                                              null,
                                              false);
        }

        return tm;
    }

    /**
     * Returns the remaining time or {@code 0} if the value is non-positive.
     */
    public long getRemainingTimeOrZero() {
        final long val =  theEndTime -  System.currentTimeMillis();
        return (val < 0) ? 0 : val;
    }

    public boolean checkTimeout() {

        assert(isServerRCB());
        long currentTime = System.currentTimeMillis();

        if (currentTime >= theEndTime) {
            setReachedLimit();
            return true;
        }

        return false;
    }

    int getBatchSize() {
        return (isServerRCB() ?
                theQueryOp.getBatchSize() :
                theExecuteOptions.getResultsBatchSize());
    }

    public boolean getUseBatchSizeAsLimit() {
        return theExecuteOptions.getUseBatchSizeAsLimit();
    }

    public boolean getUseBytesLimit() {
        return getMaxReadKB() > 0;
    }

    public int getMaxReadKB() {
        return (isServerRCB() ?
                theQueryOp.getMaxReadKB() :
                theExecuteOptions.getMaxReadKB());
    }

    public int getMaxWriteKB() {
        return theExecuteOptions.getMaxWriteKB();
    }

    public long getMaxMemoryConsumption() {
        assert(!isServerRCB());
        return theExecuteOptions.getMaxMemoryConsumption();
    }

    public long getMaxServerMemoryConsumption() {
        if (isServerRCB()) {
            return theQueryOp.getMaxServerMemoryConsumption();
        }

        return theExecuteOptions.getMaxServerMemoryConsumption();
    }

    public int getDeleteLimit() {
        return (isServerRCB() ?
                theQueryOp.getDeleteLimit() :
                theExecuteOptions.getDeleteLimit());
    }

    public int getUpdateLimit() {
        return (isServerRCB() ?
                theQueryOp.getUpdateLimit() :
                theExecuteOptions.getUpdateLimit());
    }

    public String getRowMetadata() {
        return (isServerRCB() ?
            theQueryOp.getRowMetadata() :
            theExecuteOptions.getRowMetadata());
    }

    public String getQueryName() {
        return (isServerRCB() ?
                theQueryOp.getQueryName() :
                theExecuteOptions.getQueryName());
    }

    public boolean isProxyQuery() {
        return theExecuteOptions.isProxyQuery();
    }

    void incMemoryConsumption(long v) {
        theMemoryConsumption += v;
        assert(theMemoryConsumption >= 0);

        if (isServerRCB()) {
            if (theMemoryConsumption > getMaxServerMemoryConsumption()) {

                if (getTraceLevel() > 1) {
                    trace("Query needs to suspend due to memory consumption. " +
                          "Max allowed = " + getMaxServerMemoryConsumption() +
                          " Current = " + theMemoryConsumption);
                }
                setNeedToSuspend(true);
            }
        } else if (theMemoryConsumption > getMaxMemoryConsumption()) {
            throw new QueryStateException(
                "Memory consumption at the client exceeded maximum " +
                "allowed value " + getMaxMemoryConsumption());
        }
    }

    void decMemoryConsumption(long v) {
        theMemoryConsumption -= v;
        assert(theMemoryConsumption >= 0);
    }

    public TableQuery getQueryOp() {
        return theQueryOp;
    }

    public TableQueryHandler getQueryHandler() {
        return theQueryHandler;
    }

    public int getShardId() {
        return theQueryHandler.getRepNode().getRepNodeId().getGroupId();
    }

    public ResumeInfo getResumeInfo() {
        return theResumeInfo;
    }

    void setResumeInfo(ResumeInfo ri) {
        theResumeInfo = ri;
    }

    public ServerIterFactory getServerIterFactory() {
        return theServerIterFactory;
    }

    PlanIter getRootIter() {
        return theRootIter;
    }

    public void setState(int pos, PlanIterState state) {
        theIteratorStates[pos] = state;
    }

    public PlanIterState getState(int pos) {
        return theIteratorStates[pos];
    }

    public FieldValueImpl[] getRegisters() {
        return theRegisters;
    }

    public FieldValueImpl getRegVal(int regId) {
        return theRegisters[regId];
    }

    public void setRegVal(int regId, FieldValueImpl value) {
        theRegisters[regId] = value;
    }

    FieldValueImpl[] getExternalVars() {
        return theExternalVars;
    }

    FieldValueImpl getExternalVar(int id) {

        if (theExternalVars == null) {
            return null;
        }
        return theExternalVars[id];
    }

    void setExternalVar(int id, FieldValueImpl val) {

        if (id >= theExternalVars.length) {
            FieldValueImpl[] newVars = new FieldValueImpl[2*id];
            for (int i = 0; i < theExternalVars.length; ++i) {
                newVars[i] = theExternalVars[i];
                theExternalVars = newVars;
            }
        }

        theExternalVars[id] = val;
    }

    public int getCurrentMaxReadKB() {
        return theQueryOp.getCurrentMaxReadKB();
    }

    public int getCurrentMaxWriteKB() {
        return theQueryOp.getCurrentMaxWriteKB();
    }

    public void tallyReadKB(int nkb) {
        assert(!isServerRCB());
        theReadKB += nkb;
    }

    public void tallyWriteKB(int nkb) {
        assert(!isServerRCB());
        theWriteKB += nkb;
    }

    public int getReadKB() {
        assert(!isServerRCB());
        return theReadKB;
    }

    public int getWriteKB() {
        assert(!isServerRCB());
        return theWriteKB;
    }

    public void tallyResultSize(int size) {
        theResultSize += size;
    }

    public int getResultSize() {
        return theResultSize;
    }

    public void setReachedLimit() {
        theReachedLimit = true;
        if (isServerRCB()) {
            setNeedToSuspend(true);
        }
    }

    public boolean getReachedLimit() {
        /*
         * Even if theReachedLimit is true, the method should return always
         * false at the direct driver. theReachedLimit may be set to
         * true in this case because in ReceiveIter, the code that handles
         * single-partition queries is shared for the proxy-based and
         * direct-driver queries.
         */
        return (isServerRCB() ||
                getUseBytesLimit() ||
                getUseBatchSizeAsLimit() ?
                theReachedLimit :
                false);
    }

    public boolean cannotSuspend() {
        return theCannotSuspend;
    }

    public void setCannotSuspend() {
        theCannotSuspend = true;
    }

    public void resetCannotSuspend() {
        theCannotSuspend = false;
    }

    public boolean cannotSuspend2() {
        return theCannotSuspendDueToDelayedResult > 0;
    }

    public void setCannotSuspend2() {
        ++theCannotSuspendDueToDelayedResult;
    }

    public void resetCannotSuspend2() {
        --theCannotSuspendDueToDelayedResult;
        assert(theCannotSuspendDueToDelayedResult >= 0);
    }

    public boolean needToSuspend() {
        return theNeedToSuspend;
    }

    public void setNeedToSuspend(boolean v) {
        theNeedToSuspend = v;
    }

    public boolean usesIndex() {
        return theUsesIndex;
    }

    void setUsesIndex() {
        theUsesIndex = true;
    }

    int getPidIdx() {
        return thePidOrShardIdx;
    }

    int incPidIdx() {
        return (++thePidOrShardIdx);
    }

    int getShardIdx() {
        return thePidOrShardIdx;
    }

    int incShardIdx() {
        return (++thePidOrShardIdx);
    }

    public byte[] getContinuationKey() {
        return theContinuationKey;
    }

    public void setContinuationKey(byte[] key) {
        theContinuationKey = key;
    }

    private void parseContinuationKey() {

        if (theContinuationKey == null) {
            theResumeInfo = new ResumeInfo(this);
            return;
        }

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(theContinuationKey);
        final DataInput in = new DataInputStream(bais);

        short v = SerialVersion.CURRENT;

        try {
            thePidOrShardIdx = in.readInt();

            theResumeInfo = new ResumeInfo(in, v);
            theResumeInfo.setRCB(this);

        } catch (IOException e) {
            throw new QueryStateException(
                "Failed to parse continuation key");
        }
    }

    void createContinuationKey() {
        createContinuationKey(true);
    }

    void createContinuationKey(boolean setReachedLimit) {

        theContinuationKey = createContinuationKey(thePidOrShardIdx,
                                                   theResumeInfo);

        if (setReachedLimit) {
            setReachedLimit();
        }
    }

    byte[] createContinuationKey(int pid, ResumeInfo ri) {

        final ByteArrayOutputStream baos =
            new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);

        short v = SerialVersion.CURRENT;

        try {
            out.writeInt(pid);
            ri.writeFastExternal(out, v);
        } catch (IOException e) {
            throw new QueryStateException(
                "Failed to create continuation key. Reason:\n" +
                e.getMessage());
        }

        return baos.toByteArray();
    }

    int getDriverBaseTopoNum() {
        return theExecuteOptions.getDriverTopoSeqNum();
    }

    VirtualScan getDriverVirtualScan() {
        return theExecuteOptions.getVirtualScan();
    }

    public void addMigratedPartition(PartitionId pid) {

        if (getTraceLevel() >= 1) {
            trace("Detected migrated partition during scan: " + pid);
        }
        theMigratedPartitions.add(pid);
    }

    public ArrayList<PartitionId> getMigratedPartitions() {
        /* No need for synchtonizetion with addMigratedPartition() because
         * when this method is called, this RCB has been removed from
         * MigrationManager.queries, and as a result addMigratedPartition()
         * canot be called on this RCB anymore */
        return theMigratedPartitions;
    }

    public void addRestoredPartition(PartitionId pid) {
        theRestoredPartitions.add(pid);
    }

    public ArrayList<PartitionId> getRestoredPartitions() {
        return theRestoredPartitions;
    }

    void saveNewVirtualScans(List<VirtualScan> vss) {
        theNewVirtualScans = vss;
    }

    public List<VirtualScan> getNewVirtualScans() {
        return theNewVirtualScans;
    }

    public Topology getBaseTopo() {
        return theBaseTopo;
    }

    public void setBaseTopo(Topology topo) {
        theBaseTopo = topo;
    }

    void setTableIterator(ParallelScanIterator<FieldValueImpl> iter) {
        theTableIterator = iter;
    }

    public ParallelScanIterator<FieldValueImpl> getTableIterator() {
        return theTableIterator;
    }

    public int getRegionId() {
        return theExecuteOptions.getRegionId();
    }

    public boolean doTombstone() {
        return theExecuteOptions.doTombstone();
    }

    public boolean inTestMode() {
        return theExecuteOptions.inTestMode();
    }

    public int getNumJoinBranches() {
        return theNumJoinBranches;
    }

    void setNumJoinBranches(int v) {
        theNumJoinBranches = v;
    }

    public int getJoinPid() {
        return theJoinPid;
    }

    public void setJoinPid(int pid) {
        theJoinPid = pid;
    }
}
