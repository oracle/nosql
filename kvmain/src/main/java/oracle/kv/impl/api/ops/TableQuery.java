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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_14;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;
import static oracle.kv.impl.util.SerialVersion.CLOUD_MR_TABLE;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_18;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.MathContext;
import java.math.RoundingMode;

import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FieldValueSerialization;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ReceiveIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.util.SerializationUtil;


/**
 * TableQuery represents and drives the execution of a query subplan at a
 * server site, over one partition or one shard.
 *
 * Instances of TableQuery are created via the parallel-scan infrastructure,
 * when invoked from the open() method of the ReceiveIter in the query plan.
 *
 * This class contains the result schema (resultDef) so it can be passed
 * to the result class (Result.QueryResult) and serialized only once for each
 * batch of results. If this were not done, each RecordValue result would have
 * its associated RecordDef serialized (redundantly) along with it.
 */
public class TableQuery extends InternalOperation {

    /*
     * The TIMEOUT_ADJUSTMENT is subtracted from the timeout received from the
     * client/proxy to compute the timeout that will be used by the RN.
     *
     * In general, a timeout at the RN is not treated as an error. Instead, if
     * the timeout expires, the RN sends whatever results it has so far back to
     * the client/proxy. If the response reaches the client/proxy before the
     * client/proxy timeout expires, the query may be able to do forward
     * progress, thus avoiding a user-visible timeout. For this reason we want
     * the RN timeout to be somewhat less than the client/proxy timeout.
     *
     * We also need to account for the time needed to serialize the query
     * results, send them back to the client/proxy and deserialize them there.
     * Also for thread contention at the RN (where a thread cannot get the cpu
     * for a while).
     */
    private static final int TIMEOUT_ADJUSTMENT = 200;

    private static final int MIN_TIMEOUT = 100;

    private final FieldDefImpl resultDef;

    /*
     * added in QUERY_VERSION_2
     */
    private final boolean mayReturnNULL;

    /*
     * The serial version used to serialize the server-side query plan.
     * It is set in ReceiveIter.ensureSerializedIter(), which is called by
     * TableQuery.writeFastExternal(). When this TableQuery is deserialized
     * at an RN, the query plan must be deserialized using planVersion.
     *
     * added in QUERY_VERSION_10 (20.2)
     */
    private short planVersion;

    private final PlanIter queryPlan;

    /*
     * Optional Bind Variables. If none exist or are not set this is null.
     * If it would be easier for callers this could be made an empty Map.
     */
    private final FieldValueImpl[] externalVars;

    private final int numIterators;

    private final int numRegisters;

    /* used for collecting read/write units (which is done on a
     * per-table-hierarchy basis) . */
    private final long rootTableId;

    /*
     * Added in QUERY_VERSION_4
     */
    private final MathContext mathContext;

    private byte traceLevel;

    private final boolean doLogFileTracing;

    private final int batchSize;

    /*
     * The maximum number of KB that the query is allowed to read during
     * the execution of this TableQuery operation. This will be <= to the
     * maxReadKB field below, because the query may have already consumed
     * some bytes in a previous incarnation.
     */
    private final int currentMaxReadKB;

    /*
     * The maximum number of KB that the query is allowed to read during
     * the execution of any TableQuery op created by the query.
     */
    private final int maxReadKB;

    private final int currentMaxWriteKB;

    private ResumeInfo resumeInfo;

    private int emptyReadFactor;

    private final int deleteLimit;

    private final int updateLimit;

    private final String queryName;

    private final int localRegionId;

    private final boolean doTombstone;

    private final long maxServerMemoryConsumption;

    private final boolean performsWrite;

    private final String rowMetadata;

    public TableQuery(
        String queryName,
        DistributionKind distKind,
        FieldDefImpl resultDef,
        boolean mayReturnNULL,
        PlanIter queryPlan,
        FieldValueImpl[] externalVars,
        int numIterators,
        int numRegisters,
        long rootTableId,
        MathContext mathContext,
        byte traceLevel,
        boolean doLogFileTracing,
        int batchSize,
        int maxReadKB,
        int currentMaxReadKB,
        int currentMaxWriteKB,
        ResumeInfo resumeInfo,
        int emptyReadFactor,
        int deleteLimit,
        int updateLimit,
        int localRegionId,
        boolean doTombstone,
        long maxServerMemoryConsumption,
        boolean performsWrite,
        String rowMetadata) {

        /*
         * The distinct OpCodes are primarily for a finer granularity of
         * statistics, allowing the different types of queries to be tallied
         * independently.
         */
        super(distKind == DistributionKind.ALL_PARTITIONS ?
              OpCode.QUERY_MULTI_PARTITION :
              (distKind == DistributionKind.SINGLE_PARTITION ?
               OpCode.QUERY_SINGLE_PARTITION :
               OpCode.QUERY_MULTI_SHARD));
        this.queryName = queryName;
        this.resultDef = resultDef;
        this.mayReturnNULL = mayReturnNULL;
        this.queryPlan = queryPlan;
        this.externalVars = externalVars;
        this.numIterators = numIterators;
        this.numRegisters = numRegisters;
        this.rootTableId = rootTableId;
        this.mathContext = mathContext;
        this.traceLevel = traceLevel;
        this.doLogFileTracing = doLogFileTracing;
        this.batchSize = batchSize;
        this.currentMaxReadKB = currentMaxReadKB;
        this.maxReadKB = maxReadKB;
        this.currentMaxWriteKB = currentMaxWriteKB;
        this.resumeInfo = resumeInfo;
        /* emptyReadFactor is serialized as a byte */
        assert emptyReadFactor <= Byte.MAX_VALUE;
        this.emptyReadFactor = emptyReadFactor;
        this.deleteLimit = deleteLimit;
        this.updateLimit = updateLimit;
        this.localRegionId = localRegionId;
        this.doTombstone = doTombstone;
        this.maxServerMemoryConsumption = maxServerMemoryConsumption;
        this.performsWrite = performsWrite;
        this.rowMetadata = rowMetadata;
    }

    FieldDefImpl getResultDef() {
        return resultDef;
    }

    boolean mayReturnNULL() {
        return mayReturnNULL;
    }

    public short getPlanVersion() {
        return planVersion;
    }

    public void setPlanVersion(short v) {
        planVersion = v;
    }

    public PlanIter getQueryPlan() {
        return queryPlan;
    }

    public FieldValueImpl[] getExternalVars() {
        return externalVars;
    }

    public int getNumIterators() {
        return numIterators;
    }

    public int getNumRegisters() {
        return numRegisters;
    }

    @Override
    public long getTableId() {
        return rootTableId;
    }

    /**
     * Return operation local region id.
     */
    public int getLocalRegionId() {
        return localRegionId;
    }

    /**
     * Return whether put tombstone when delete.
     */
    public boolean doTombstone() {
        return doTombstone;
    }

    public MathContext getMathContext() {
        return mathContext;
    }

    public byte getTraceLevel() {
        return traceLevel;
    }

    public void setTraceLevel(int v) {
        traceLevel = (byte)v;
    }

    public boolean doLogFileTracing() {
        return doLogFileTracing;
    }

    @Override
    public int getTimeout() {
        int tm = timeoutMs - TIMEOUT_ADJUSTMENT;
        if (tm <= MIN_TIMEOUT) {
            return MIN_TIMEOUT;
        }
        return tm;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getCurrentMaxReadKB() {
        return currentMaxReadKB;
    }

    public int getMaxReadKB() {
        return maxReadKB;
    }

    public int getCurrentMaxWriteKB() {
        return currentMaxWriteKB;
    }

    public ResumeInfo getResumeInfo() {
        return resumeInfo;
    }

    public void setResumeInfo(ResumeInfo ri) {
        resumeInfo = ri;
    }

    @Override
    public void addEmptyReadCharge() {
        /* Override to factor in the emptyReadFactor */
        if (getReadKB() == 0) {
            addReadBytes(MIN_READ * emptyReadFactor);
        }
    }

    public int getEmptyReadFactor() {
        return emptyReadFactor;
    }

    public void setEmptyReadFactor(int v) {
        emptyReadFactor = v;
    }

    public int getDeleteLimit() {
        return deleteLimit;
    }

    public int getUpdateLimit() {
        return updateLimit;
    }

    public String getQueryName() {
        return queryName;
    }

    public long getMaxServerMemoryConsumption() {
        return maxServerMemoryConsumption;
    }

    public String getRowMetadata() {
        return rowMetadata;
    }

    @Override
    public boolean performsWrite() {
        return performsWrite;
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        byte[] serializedQueryPlan;

        /*
         * A TableQuery instance is always created at the client initially,
         * by the receive iterator, which passes itself as the queryPlan arg
         * of the TableQuery constructor. So, initially, this.queryPlan is a
         * ReceiveIter. However, when the TableQuery is serialized and sent
         * to the server for execution, it is not the ReceiveIter that is
         * serialized, but it's child (which is an SFWIter normally). So, when
         * the TableQuery is deserialized and instantiated at the server,
         * this.queryPlan is an SFWIter.
         *
         * If something goes "wrong" at the server, for example the partition
         * has migrated, the server may try to forward the TableQuery to another
         * server and calls TableQuery.writeFastExternal() again. In this case,
         * this.queryPlan is not a ReceiveIter and the plan must be serialized
         * "from scratch" again.
         */
        if (queryPlan instanceof ReceiveIter) {
            serializedQueryPlan = ((ReceiveIter)queryPlan).
                                  ensureSerializedIter(this, serialVersion);
        } else {
            final ByteArrayOutputStream baos =
                new ByteArrayOutputStream();
            final DataOutput dataOut = new DataOutputStream(baos);

            PlanIter.serializeIter(queryPlan, dataOut, serialVersion);
            serializedQueryPlan = baos.toByteArray();
        }

        out.writeShort(planVersion);

        out.write(serializedQueryPlan);

        FieldDefSerialization.writeFieldDef(resultDef, out, serialVersion);

        out.writeBoolean(mayReturnNULL);

        writeExternalVars(externalVars, out, serialVersion);

        out.writeInt(numIterators);
        out.writeInt(numRegisters);
        out.writeInt(batchSize);
        out.writeByte(traceLevel);
        if (serialVersion >= QUERY_VERSION_14) {
            out.writeBoolean(doLogFileTracing);
        }
        resumeInfo.writeFastExternal(out, serialVersion);
        writeMathContext(mathContext, out);
        out.writeLong(rootTableId);
        out.writeInt(currentMaxReadKB);
        out.writeInt(maxReadKB);

        out.writeInt(currentMaxWriteKB);

        out.writeByte(emptyReadFactor);

        out.writeInt(deleteLimit);

        if (serialVersion >= QUERY_VERSION_14) {
            SerializationUtil.writeString(out, serialVersion, queryName);
        }

        if (includeCloudMRTable(serialVersion)) {
            out.writeInt(localRegionId);
            out.writeBoolean(doTombstone);
        } else {
            checkCloudMRTableInfo(localRegionId, doTombstone, serialVersion);
        }

        if (serialVersion >= QUERY_VERSION_14) {
            out.writeLong(maxServerMemoryConsumption);
        }

        if (serialVersion >= QUERY_VERSION_16) {
            out.writeBoolean(performsWrite);
            out.writeInt(updateLimit);
        }

        if (serialVersion >= QUERY_VERSION_18) {
            SerializationUtil.writeString(out, serialVersion, rowMetadata);
        }
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected TableQuery(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        try {
            planVersion = in.readShort();
            queryPlan = PlanIter.deserializeIter(in, planVersion);

            resultDef = FieldDefSerialization.readFieldDef(in, serialVersion);

            mayReturnNULL = in.readBoolean();
            externalVars = readExternalVars(in, serialVersion);

            numIterators = in.readInt();
            numRegisters = in.readInt();
            batchSize = in.readInt();
            traceLevel = in.readByte();
            if (serialVersion >= QUERY_VERSION_14) {
                doLogFileTracing = in.readBoolean();
            } else {
                doLogFileTracing = true;
            }

            resumeInfo = new ResumeInfo(in, serialVersion);

            mathContext = readMathContext(in);

            rootTableId = in.readLong();

            currentMaxReadKB = in.readInt();
            maxReadKB = in.readInt();

            currentMaxWriteKB = in.readInt();

            emptyReadFactor = in.readByte();

            deleteLimit = in.readInt();

            if (serialVersion >= QUERY_VERSION_14) {
                queryName = SerializationUtil.readString(in, serialVersion);
            } else {
                queryName = null;
            }

            if (includeCloudMRTable(serialVersion)) {
                localRegionId = in.readInt();
                doTombstone = in.readBoolean();
            } else {
                localRegionId = Region.NULL_REGION_ID;
                doTombstone = false;
            }

            if (serialVersion >= QUERY_VERSION_14) {
                maxServerMemoryConsumption = in.readLong();
            } else {
                maxServerMemoryConsumption = 0;
            }

            if (serialVersion >= QUERY_VERSION_16) {
                performsWrite = in.readBoolean();
                updateLimit = in.readInt();
            } else {
                performsWrite = false;
                updateLimit = 0;
            }

            if (serialVersion >= QUERY_VERSION_18) {
                rowMetadata = SerializationUtil.readString(in, serialVersion);
            } else {
                rowMetadata = null;
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (RuntimeException re) {
            re.printStackTrace();
            throw new QueryStateException("Read TableQuery failed: " + re);
        }
    }

    static void writeExternalVars(
        FieldValueImpl[] vars,
        DataOutput out,
        short serialVersion)
        throws IOException {

        if (vars != null && vars.length > 0) {
            int numVars = vars.length;
            out.writeInt(numVars);

            for (int i = 0; i < numVars; ++i) {
                FieldValueSerialization.writeFieldValue(vars[i],
                                                        true, // writeValDef
                                                        out, serialVersion);
            }
        } else {
            out.writeInt(0);
        }
    }

    static FieldValueImpl[] readExternalVars(DataInput in, short serialVersion)
        throws IOException {

        int numVars = in.readInt();
        if (numVars == 0) {
            return null;
        }

        FieldValueImpl[] vars = new FieldValueImpl[numVars];

        for (int i = 0; i < numVars; i++) {
            FieldValueImpl val = (FieldValueImpl)
                FieldValueSerialization.readFieldValue(null, // def
                                                       in, serialVersion);

            vars[i] = val;
        }
        return vars;
    }

    static void writeMathContext(
        MathContext mathContext,
        DataOutput out)
        throws IOException {

        if (mathContext == null) {
            out.writeByte(0);
        } else if (MathContext.DECIMAL32.equals(mathContext)) {
            out.writeByte(1);
        } else if (MathContext.DECIMAL64.equals(mathContext)) {
            out.writeByte(2);
        } else if (MathContext.DECIMAL128.equals(mathContext)) {
            out.writeByte(3);
        } else if (MathContext.UNLIMITED.equals(mathContext)) {
            out.writeByte(4);
        } else {
            out.writeByte(5);
            out.writeInt(mathContext.getPrecision());
            out.writeInt(mathContext.getRoundingMode().ordinal());
        }
    }

    static MathContext readMathContext(DataInput in)
        throws IOException {

        int code = in.readByte();

        switch (code) {
        case 0:
            return null;
        case 1:
            return MathContext.DECIMAL32;
        case 2:
            return MathContext.DECIMAL64;
        case 3:
            return MathContext.DECIMAL128;
        case 4:
            return MathContext.UNLIMITED;
        case 5:
            int precision = in.readInt();
            int roundingMode = in.readInt();
            return
                new MathContext(precision, RoundingMode.valueOf(roundingMode));
        default:
            throw new QueryStateException("Unknown MathContext code.");
        }
    }

    private static boolean includeCloudMRTable(short serialVersion) {
        return serialVersion >= CLOUD_MR_TABLE;
    }

    private static void checkCloudMRTableInfo(int localRegionId,
                                              boolean doTombstone,
                                              short serialVersion) {
        if (Region.isMultiRegionId(localRegionId)) {
            throw new IllegalStateException("Serial version " +
                serialVersion + " does not support providing operation local " +
                "region Id, must be " + CLOUD_MR_TABLE + " or greater");
        }

        if (doTombstone) {
            throw new IllegalStateException("Serial version " +
                serialVersion + " does not support doTombstone, must be " +
                CLOUD_MR_TABLE + " or greater");
        }
    }
}
