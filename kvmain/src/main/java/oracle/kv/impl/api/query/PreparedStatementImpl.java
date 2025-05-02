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

package oracle.kv.impl.api.query;

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_17;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.compiler.StaticContext.VarInfo;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.PlanIter.PlanIterKind;
import oracle.kv.impl.query.runtime.CloudSerializer;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.impl.query.runtime.ReceiveIter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

import oracle.kv.StatementResult;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;

/**
 * Implementation of a PreparedStatement. This class contains the query plan,
 * along with enough information to construct a runtime context in which the
 * query can be executed (RuntimeControlBlock).
 *
 * An instance of PreparedStatementImpl is created by CompilerAPI.prepare(),
 * after the query has been compiled.
 */
public class PreparedStatementImpl
    implements PreparedStatement,
               InternalStatement {

    /*
     * A serial version used in testing. It is set by the proxy
     * QueryTest.testOldPreparedPlan() test, which mimicks the case where a app
     * caches prepared queries, then the user upgrades the proxy and the
     * servers, and then the app reruns the cached quries after the upgrade.
     * testSerialVersion is used in ReceiveIter.writeFastExternal() to serialize
     * the server-side plan of a proxy-based query using a serial version that
     * is older than the current version of the proxy and the servers. When the
     * driver sends this plan back to the proxy, the proxy will forward it to
     * the RNs, where it will be deseriazed using testSerialVersion, instead of
     * the serial version of the client/server. See the deserializing
     * constructor of ReceiveIter and the ReceiveIter.ensureSerializedIter()
     * method.
     */
    public static short testSerialVersion = -1;

    public static void setTestSerialVersion(short v) {
        testSerialVersion = v;
    }

    /**
     * The type of distribution for the query.
     */
    public enum DistributionKind {
        /** the query goes to a single partition */
        SINGLE_PARTITION,
        /** the query goes to all partitions */
        ALL_PARTITIONS,
        /** the query goes to all shards (it uses an index) */
        ALL_SHARDS;

        private static final DistributionKind[] VALUES = values();
        public static final int VALUES_COUNT = VALUES.length;
        public static DistributionKind valueOf(int ordinal) {
            return VALUES[ordinal];
        }
    }

    /*
     * The query plan
     */
    private final PlanIter queryPlan;

    /*
     * The type of the result
     */
    private final FieldDefImpl resultDef;

    /*
     * The number of registers required to run the plan
     */
    private final int numRegisters;

    /*
     * The number of iterators in the plan
     */
    private final int numIterators;

    /*
     * externalVars maps the name of each external var declared in the query
     * to the numeric id and data type for that var.
     */
    private final Map<String, VarInfo> externalVars;

    /*
     * The DistributionKind
     */
    private final DistributionKind distributionKind;

    /*
     * The PartitionId for SINGLE_PARTITION distribution, null otherwise.
     */
    private final PartitionId partitionId;

    private final long theJsonTableId;

    private final int theTableVersion;

    private QueryControlBlock theQCB;

    public PreparedStatementImpl(
        PlanIter queryPlan,
        FieldDefImpl resultDef,
        int numRegisters,
        int numIterators,
        Map<String, VarInfo> externalVars,
        QueryControlBlock qcb) {

        this.queryPlan = queryPlan;
        this.resultDef = resultDef;
        this.numRegisters = numRegisters;
        this.numIterators = numIterators;
        this.externalVars = externalVars;
        theQCB = qcb;
        this.distributionKind = qcb.getDistributionKind();
        theJsonTableId = qcb.getJsonTableId();
        theTableVersion = qcb.getFirstTableVersion();
        partitionId = qcb.getPartitionId();
    }

    @Override
    public BoundStatement createBoundStatement() {
        return new BoundStatementImpl(this);
    }

    @Override
    public RecordDef getResultDef() {

        if (resultDef instanceof RecordDef) {
            return (RecordDefImpl)resultDef;
        }

        if (resultDef == null) {
            /* this can happen if, for example, the statement has been
             * deserialized from a prepared statement from the proxy, so
             * do not throw or assert, just return null */
            return null;
        }

        /* If this is a select-star query on a jsonCollection table the
         * resultDef will be a map. This will be turned into a
         * JsonCollectionRowImpl on return so return the Row def for the
         * table rather than the map as it's more accurate */
        return theQCB.getJsonTable().getRowDef();
    }

    public char[] getQueryString() {
        return theQCB.getQueryString();
    }

    public boolean isJsonCollectionSelectStar() {
        return (resultDef instanceof MapDef);
    }

    /**
     * Returns the DistributionKind for the prepared query
     */
    public DistributionKind getDistributionKind() {
        return distributionKind;
    }

    /**
     * Returns the PartitionId for queries with DistributionKind of
     * SINGLE_PARTITION. NOTE: for queries with variables that are part of the
     * shard key this method will return an incorrect PartitionId. This is
     * because the query compiler puts a placeholder value in the PrimaryKey
     * that is a valid value for the type. TBD: detect this and throw an
     * exception if this method is called on such a query.
     *
     * Note: this method is used for Hive integration only.
     */
    public PartitionId getPartitionId() {
        return partitionId;
    }

    TableImpl getJsonTable(KVStoreImpl store) {

        TableImpl jsonTable;

        /* When a PreparedStatementImpl is serialized and then
         * deserialized as it is via the Proxy, the QCB is null.*/
        if (theQCB != null) {
            jsonTable = theQCB.getJsonTable();
        } else {
            assert(theJsonTableId > 0);
            jsonTable = (TableImpl)store.getTableAPIImpl().
                        getTableById(theJsonTableId);
        }
        assert(jsonTable.isJsonCollection());
        return jsonTable;
    }

    public int getTableVersion() {
        return theTableVersion;
    }

    public PlanIter getQueryPlan() {
    	return queryPlan;
    }

    public int getNumRegisters() {
        return numRegisters;
    }

    public int getNumIterators() {
        return numIterators;
    }

    @Override
    public Map<String, FieldDef> getVariableTypes() {
        return getExternalVarsTypes();
    }

    @Override
    public FieldDef getVariableType(String variableName) {
        VarInfo vi = getExternalVarInfo(variableName);
        if (vi != null) {
            return vi.getType().getDef();
        }

        return null;
    }

    /*
     * Used by proxy
     */
    public FieldDef getExternalVarType(String variableName) {
        return getVariableType(variableName);
    }

    public Map<String, FieldDef> getExternalVarsTypes() {

        Map<String, FieldDef> varsMap = new HashMap<String, FieldDef>();

        if (externalVars == null) {
            return varsMap;
        }

        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            String varName = entry.getKey();
            VarInfo vi = entry.getValue();
            varsMap.put(varName, vi.getType().getDef());
        }

        return varsMap;
    }

    boolean hasExternalVars() {
        return (externalVars != null && !externalVars.isEmpty());
    }

    public VarInfo getExternalVarInfo(String name) {

        if (externalVars == null) {
            return null;
        }

        return externalVars.get(name);
    }

    /**
     * Convert the map of external vars (maping names to values) to an array
     * with the values only. The array is indexed by an internally assigned id
     * to each external variable. This method also checks that all the external
     * vars declared in the query have been bound.
     */
    public FieldValueImpl[] getExternalVarsArray(Map<String, FieldValue> vars) {

        if (externalVars == null) {
            assert(vars.isEmpty());
            return null;
        }

        int count = 0;
        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            String name = entry.getKey();
            ++count;

            if (vars.get(name) == null) {
                throw new IllegalArgumentException(
                    "Variable " + name + " has not been bound");
            }
        }

        FieldValueImpl[] array = new FieldValueImpl[count];

        count = 0;
        for (Map.Entry<String, FieldValue> entry : vars.entrySet()) {
            String name = entry.getKey();
            FieldValueImpl value = (FieldValueImpl)entry.getValue();

            VarInfo vi = externalVars.get(name);
            if (vi == null) {
                throw new IllegalStateException(
                    "Variable " + name + " does not appear in query");
            }

            array[vi.getId()] = value;
            ++count;
        }

        assert(count == array.length);
        return array;
    }

    public String varPosToName(int pos) {

        int searchId = pos - 1;

        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            int id = entry.getValue().getId();
            if (id == searchId) {
                return entry.getKey();
            }
        }

        throw new IllegalArgumentException(
            "There is no external variable at position " +  pos);
    }

    @Override
    public String toString() {
        return queryPlan.display();
    }

    @Override
    public StatementResult executeSync(
        KVStoreImpl store,
        ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        return new QueryStatementResultImpl(store.getTableAPIImpl(),
                                            options,
                                            this);
    }

    /**
     * Not part of the public PreparedStatement interface available to
     * external clients. This method is employed when the Oracle NoSQL DB
     * Hive/BigDataSQL integration mechanism is used to process a query,
     * and disjoint partition sets are specified for each split.
     */
    public StatementResult executeSyncPartitions(
        KVStoreImpl store,
        ExecuteOptions options,
        final Set<Integer> partitions) {

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this, null /* QueryPublisher */,
            partitions, null);
    }

    /**
     * Not part of the public PreparedStatement interface available to
     * external clients. This method is employed when the Oracle NoSQL DB
     * Hive/BigDataSQL integration mechanism is used to process a query,
     * and disjoint shard sets are specified for each split.
     */
    @Override
    public StatementResult executeSyncShards(
        KVStoreImpl store,
        ExecuteOptions options,
        Set<RepGroupId> shards) {

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this, null /* QueryPublisher */,
            null, shards);
    }

    /**
     * This method is used by the proxy.
     *
     * A "simple" query is one that does not need any state to be
     * maintained at the driver across query batches. We implement this
     * definition by checking that either the query plan does not have a
     * ReceiveIter at all (e.g. INSERT queries) or that the ReceiveIter
     * is the root of the query plan and it does not do sorting or duplicate
     * elimination. This implementation is a little conservative; for
     * example, queries with aggregate functions but no group-by don't need
     * driver state, but are not flagged as "simple" because they contain a
     * re-grouping SFWIter on top of the ReceiveIter.
     *
     * For non-simple queries, their query plan is broken into 2 parts: the
     * part above the ReceiveIter and the part below the ReceiveIter. The
     * following 2 methods (serializeForDriver and serializeForProxy)
     * are used by the proxy to serialize these 2 parts.
     */
    public boolean isSimpleQuery() {

        if (queryPlan.getKind() == PlanIterKind.RECEIVE) {
            ReceiveIter rcv = (ReceiveIter)queryPlan;
            if (!rcv.doesSort() && !rcv.eliminatesDuplicates()) {
                return true;
            }
        }

        return !theQCB.hasReceiveIter();
    }

    /**
     * Serialize the part of the query plan that must be executed at the driver.
     * It is sent to the driver where it is deserialized and not sent back to
     * the proxy again.
     */
    public void serializeForDriver(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        if (isSimpleQuery()) {
            /* Return null as the driver query plan */
            CloudSerializer.writeIter(null, out, driverVersion, null);
            return;
        }

        queryPlan.writeForCloud(out, driverVersion, valWriter);
        out.writeInt(numIterators);
        out.writeInt(numRegisters);

        if (externalVars == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(externalVars.size());
            for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
                CloudSerializer.writeString(entry.getKey(), out);
                VarInfo info = entry.getValue();
                out.writeInt(info.getId());
            }
        }
    }

    /**
     * Serialize a prepared statement for use by the proxy. The serialized
     * statement is sent to the driver, which stores it as an opaque byte
     * array and sends it to the proxy every time a new batch of results
     * is needed.
     *
     * The information is:
     *   serialVersion
     *   distributionKind
     *   table name
     *   table id
     *   table version
     *   externalVars
     *   query plan
     *   numIterators and numRegisters
     */
    public void serializeForProxy(DataOutput out)
        throws IOException {

        /*
         * The current version of the kvstore client serves as the serial
         * version for the entire prepared query
         */
        final short version = SerialVersion.CURRENT;
        out.writeShort(version);
        out.writeByte((distributionKind != null) ?
                      distributionKind.ordinal() : -1);
        SerializationUtil.writePackedLong(out, theJsonTableId);
        SerializationUtil.writePackedInt(out, theTableVersion);

        /* external variables, may be null */
        writeExternalVars(out, version);

        /* Query Plan */
        queryPlan.writeFastExternal(out, version);

        /* numIterators and numRegisters */
        out.writeInt(numIterators);
        out.writeInt(numRegisters);
    }

    public void toByteArray(DataOutput out)
        throws IOException {
        serializeForProxy(out);
    }

    /**
     * Deserialize a prepared statement received from the driver.
     */
    public PreparedStatementImpl(DataInput in)
        throws IOException {

        try {
            final short version = in.readShort();
            if (version > SerialVersion.CURRENT) {
                raiseDeserializeError("unexpected version value: " + version);
            }

            byte ordinal = in.readByte();
            if (ordinal != (byte)(-1)) {
                if (ordinal < 0 || ordinal > DistributionKind.VALUES_COUNT) {
                    raiseDeserializeError("unexpected value for DistributionKind");
                }
                distributionKind = DistributionKind.valueOf(ordinal);
            } else {
                distributionKind = null;
            }


            if (version < QUERY_VERSION_17) {
                String tableName = SerializationUtil.readString(in, version);
                if (tableName == null) {
                    raiseDeserializeError("tableName should not be null");
                }
            }

            theJsonTableId = SerializationUtil.readPackedLong(in);
            if (theJsonTableId < 0) {
                raiseDeserializeError("tableId should not be negative value");
            }

            theTableVersion = SerializationUtil.readPackedInt(in);
            if (theTableVersion < 0) {
                raiseDeserializeError("tableVersion should not be negative value");
            }

            /* external variables, if any */
            externalVars = readExternalVars(in, version);

            /* Query plan */
            queryPlan = PlanIter.deserializeIter(in, version);
            if (queryPlan == null) {
                raiseDeserializeError("query plan is null");
            }

            /* numIterators and numRegisters */
            numIterators = in.readInt();
            if (numIterators < 1) {
                raiseDeserializeError
                    ("numIterators should not be 0 or negative value");
            }
            numRegisters = in.readInt();
            if (numRegisters < 0) {
                raiseDeserializeError
                    ("numRegisters should not be 0 or negative value");
            }

            resultDef = null;

            /* make the compiler happy about final members that aren't used */
            partitionId = null;
        } catch (QueryException qe) {
            throw qe.getIllegalArgument();
        } catch (RuntimeException re) {
            re.printStackTrace();
            throw new IllegalArgumentException(
                "Read PreparedStatement failed: " + re);
        }
    }

    private void writeExternalVars(DataOutput out, short version)
        throws IOException {

        if (externalVars == null) {
            out.writeInt(0);
        }
        out.writeInt(externalVars.size());
        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            SerializationUtil.writeNonNullString(out, version, entry.getKey());
            VarInfo info = entry.getValue();
            out.writeInt(info.getId());
            PlanIter.serializeExprType(info.getType(), out, version);
            out.writeBoolean(info.allowJsonNull());
        }
    }

    private Map<String, VarInfo> readExternalVars(DataInput in, short version)
        throws IOException {

        int numVars = in.readInt();
        if (numVars == 0) {
            return null;
        }

        if (numVars < 0) {
            raiseDeserializeError("Unexpected negtive value: " + numVars);
        }
        try {
            Map<String, VarInfo> vars = new HashMap<String, VarInfo>(numVars);
            for (int i = 0; i < numVars; i++) {
                String name = SerializationUtil.readString(in, version);
                VarInfo info = VarInfo.createVarInfo(
                    in.readInt(), // id
                    PlanIter.deserializeExprType(in, version),
                    in.readBoolean());
                vars.put(name, info);
            }
            return vars;
        } catch (RuntimeException re) {
            raiseDeserializeError(re.getMessage());
        }
        return null;
    }

    private void raiseDeserializeError(String msg) {
        throw new QueryException(
            "Deserializing PreparedStatement failed: " + msg);
    }
}
