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

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeArrayLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeSequenceLength;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_14;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_17;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;

import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/*
 * A query is allowed to run for a limited time only at each server and it will
 * self-terminate even if there more results to be computed. This happens when
 * either a max number of results have been produced or a max number of KBs have
 * been read. In these cases, we say that the query "suspends itself", even
 * though this may not be good terminology because "suspends" may imply that
 * some state remains at the server, which is not true. In fact, all query
 * state at the server is thrown away; the server just forgets all about the
 * query.
 *
 * If the client wants to get more results from a partition/shard, it resends
 * the query to an appropriate server, which must "resume" the query so that
 * it does not produce any results that have been produced in previous
 * incarnations. In other words, the query must restart where it got suspended.
 *
 * To achieve this, when the query suspends, it collects the info that it will
 * need to resume correctly and sends this info back to the client inside the
 * QueryResult. To resume, the client sends the resume info back to the server,
 * inside the TableQuery op.
 *
 * This class represents the resume info. To summarize, a ResumeInfo is carried
 * from a server to the client inside a QueryResult, and from the client back
 * to the server inside a TableQuery.
 */
public class ResumeInfo implements FastExternalizable {

    public static class TableResumeInfo implements FastExternalizable {

        /* It specifies the index range that was being scanned when the query
         * got suspended. This is needed because multiple ranges may be scanned
         * inside the index that is used by a query to access the table. */
        private int theCurrentIndexRange;

        private byte[] thePrimResumeKey;

        private byte[] theSecResumeKey;

        /* The moveAfterResumeKey flag is needed to handle SizeLimitExceptions
         * during query processing, and also for NESTED TABLES queries (See
         * ServerTableIter). */
        private boolean theMoveAfterResumeKey = true;

        /* A transient field (used at RNs only) to specify how to resume this
         * join branch. A false value implies that when the previous batch
         * finished, the join branch was on a row (or index key in case of
         * covering index) and in the current batch, the branch must resume on
         * that row/key. Furthermore, we should not charge any read units for
         * reading that row/key during resume. The value may be set to false
         * by the NestedLoopJoinIter.open() (see further comments there). */
        private boolean theMoveJoinAfterResumeKey = true;

        /* The following 5 fields are used only for NESTED TABLES clauses */

        /* This is used when the query has a NESTED TABLES clause with
         * descendants and the target table is accessed via a seconadry index.
         * In this case, theDescResumeKey the primary key on which to resume
         * the scan on the primary index (and thePrimResumeKey is the primary
         * key on which to resume the scan on the secondary index). */
        private byte[] theDescResumeKey;

        /* Next 3 fields store resume info needed when a query contains
         * a NESTED TABLES clause (see ServerTableIter). */
        private int[] theJoinPathTables;

        /* The prim key of the bottom-most row in the join path, or null if the
         * join path is empty. It is used to re-establish the join path during
         * resume. */
        private byte[] theJoinPathKey;

        /* The secondary key of the target-table row inside the join path. It is
         * used to re-establish the join path during resume. Notice that if the
         * sec index is covering, the target-table row in the join path will be
         * an index entry rather than an actual table row. */
        private byte[] theJoinPathSecKey;

        /* theJoinPathMatched is needed when a query contains a NESTED TABLES
         * clause or the query is suspended due to a SizeLimitException (see
         * ServerTableIter). */
        private boolean theJoinPathMatched;

        TableResumeInfo() {
        }

        /*
         * Constructor used by proxy.
         */
        public TableResumeInfo(
            int currentIndexRange,
            byte[] primResumeKey,
            byte[] secResumeKey,
            boolean moveAfterResumeKey,
            byte[] descResumeKey,
            int[] joinPathTables,
            byte[] joinPathKey,
            byte[] joinPathSecKey,
            boolean joinPathMatched) {

            theCurrentIndexRange = currentIndexRange;
            thePrimResumeKey = primResumeKey;
            theSecResumeKey = secResumeKey;
            theMoveAfterResumeKey = moveAfterResumeKey;
            theDescResumeKey = descResumeKey;
            theJoinPathTables = joinPathTables;
            theJoinPathKey = joinPathKey;
            theJoinPathSecKey = joinPathSecKey;
            theJoinPathMatched = joinPathMatched;
        }

        TableResumeInfo(TableResumeInfo ri) {

            theCurrentIndexRange = ri.theCurrentIndexRange;

            if (ri.thePrimResumeKey != null) {
                thePrimResumeKey = Arrays.copyOf(ri.thePrimResumeKey,
                                                 ri.thePrimResumeKey.length);
            }
            if (ri.theSecResumeKey != null) {
                theSecResumeKey = Arrays.copyOf(ri.theSecResumeKey,
                                                ri.theSecResumeKey.length);
            }
            if (ri.theDescResumeKey != null) {
                theDescResumeKey = Arrays.copyOf(ri.theDescResumeKey,
                                                 ri.theDescResumeKey.length);
            }

            theMoveAfterResumeKey = ri.theMoveAfterResumeKey;
            theMoveJoinAfterResumeKey = ri.theMoveJoinAfterResumeKey;

            if (ri.theJoinPathTables != null) {
                theJoinPathTables = Arrays.copyOf(ri.theJoinPathTables,
                                                  ri.theJoinPathTables.length);
            }
            if (ri.theJoinPathKey != null) {
                theJoinPathKey = Arrays.copyOf(ri.theJoinPathKey,
                                               ri.theJoinPathKey.length);
            }
            if (ri.theJoinPathSecKey != null) {
                theJoinPathSecKey = Arrays.copyOf(ri.theJoinPathSecKey,
                                                  ri.theJoinPathSecKey.length);
            }

            theJoinPathMatched = ri.theJoinPathMatched;
        }

        TableResumeInfo(DataInput in, short serialVersion) throws IOException {

            theCurrentIndexRange = in.readInt();
            thePrimResumeKey = readByteArray(in);

            if (thePrimResumeKey != null) {
                theSecResumeKey = readByteArray(in);
                theDescResumeKey = readByteArray(in);
                theMoveAfterResumeKey = in.readBoolean();

                theJoinPathTables =
                    PlanIter.deserializeIntArray(in, serialVersion);
                theJoinPathKey =
                    PlanIter.deserializeByteArray(in, serialVersion);
                theJoinPathSecKey =
                    PlanIter.deserializeByteArray(in, serialVersion);
                theJoinPathMatched = in.readBoolean();
            }
        }

        @Override
        public void writeFastExternal(
            DataOutput out,
            short serialVersion) throws IOException {

            out.writeInt(theCurrentIndexRange);
            writeByteArray(out, thePrimResumeKey);

            if (thePrimResumeKey != null) {
                writeByteArray(out, theSecResumeKey);
                writeByteArray(out, theDescResumeKey);
                out.writeBoolean(theMoveAfterResumeKey);

                PlanIter.serializeIntArray(theJoinPathTables, out, serialVersion);
                PlanIter.serializeByteArray(theJoinPathKey, out, serialVersion);
                PlanIter.serializeByteArray(theJoinPathSecKey, out, serialVersion);
                out.writeBoolean(theJoinPathMatched);
            }
        }

        void refresh(TableResumeInfo src) {

            theCurrentIndexRange = src.theCurrentIndexRange;
            thePrimResumeKey = src.thePrimResumeKey;
            theSecResumeKey = src.theSecResumeKey;
            theDescResumeKey = src.theDescResumeKey;
            theMoveAfterResumeKey = src.theMoveAfterResumeKey;

            theJoinPathTables = src.theJoinPathTables;
            theJoinPathKey = src.theJoinPathKey;
            theJoinPathSecKey = src.theJoinPathSecKey;
            theJoinPathMatched = src.theJoinPathMatched;
        }

        void reset() {
            theCurrentIndexRange = 0;
            thePrimResumeKey = null;
            theSecResumeKey = null;
            theDescResumeKey = null;
            theMoveAfterResumeKey = true;
            theMoveJoinAfterResumeKey = true;
            theJoinPathKey = null;
            theJoinPathSecKey = null;
            theJoinPathTables = null;
            theJoinPathMatched = true;
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder();

            sb.append("theCurrentIndexRange = ").append(theCurrentIndexRange);
            sb.append("\n");

            if (thePrimResumeKey != null) {
                sb.append("thePrimResumeKey = ");
                sb.append(PlanIter.printByteArray(thePrimResumeKey));
                sb.append("\n");
            }

            if (theSecResumeKey != null) {
                sb.append("theSecResumeKey = ");
                sb.append(PlanIter.printByteArray(theSecResumeKey));
                sb.append("\n");
            }

            if (theDescResumeKey != null) {
                sb.append("theDescResumeKey = ");
                sb.append(PlanIter.printByteArray(theDescResumeKey));
                sb.append("\n");
            }

            sb.append("theMoveAfterResumeKey = ").append(theMoveAfterResumeKey);
            sb.append("\n");

            sb.append("theMooinveJAfterResumeKey = ").append(theMoveJoinAfterResumeKey);
            sb.append("\n");

            if (theJoinPathTables != null) {
                sb.append("theJoinPathTables = [");
                for (int v : theJoinPathTables) {
                    sb.append(v).append(", ");
                }
                sb.append("]\n");
            }

            if (theJoinPathKey != null) {
                sb.append("theJoinPathKey = ");
                sb.append(PlanIter.printByteArray(theJoinPathKey));
                sb.append("\n");

                sb.append("theJoinPathMatched = ").append(theJoinPathMatched);
                sb.append("\n");
            }

            if (theJoinPathSecKey != null) {
                sb.append("theJoinPathSecKey = ");
                sb.append(PlanIter.printByteArray(theJoinPathSecKey));
                sb.append("\n");
            }

            sb.append("\n");
            return sb.toString();
        }

    }

    public static class VirtualScan implements FastExternalizable {

        final private int theSID;
        final private int thePID;

        final private TableResumeInfo[] theTableRIs;

        public VirtualScan(
            int pid,
            int sid,
            TableResumeInfo[] tableRIs) {
            theSID = sid;
            thePID = pid;
            theTableRIs = tableRIs;
        }

        VirtualScan(DataInput in, short serialVersion) throws IOException {

            theSID = SerializationUtil.readPackedInt(in);
            thePID = SerializationUtil.readPackedInt(in);

            if (serialVersion >= QUERY_VERSION_17) {
                int num = readSequenceLength(in);
                theTableRIs = new TableResumeInfo[num];
                for (int i = 0; i < num; ++i) {
                    theTableRIs[i] = new TableResumeInfo(in, serialVersion);
                }
            } else {
                theTableRIs = new TableResumeInfo[1];
                TableResumeInfo tri = new TableResumeInfo();
                theTableRIs[0] = tri;

                tri.thePrimResumeKey = readByteArray(in);

                if (tri.thePrimResumeKey != null) {
                    tri.theSecResumeKey = readByteArray(in);
                    tri.theMoveAfterResumeKey = in.readBoolean();

                    tri.theDescResumeKey = readByteArray(in);
                    tri.theJoinPathTables =
                        PlanIter.deserializeIntArray(in, serialVersion);
                    tri.theJoinPathKey =
                        PlanIter.deserializeByteArray(in, serialVersion);
                    tri.theJoinPathSecKey =
                        PlanIter.deserializeByteArray(in, serialVersion);
                    tri.theJoinPathMatched = in.readBoolean();
                }

            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short sv) throws IOException {

            SerializationUtil.writePackedInt(out, theSID);
            SerializationUtil.writePackedInt(out, thePID);

            if (sv >= QUERY_VERSION_17) {
                writeArrayLength(out, theTableRIs);
                for (int i = 0; i < theTableRIs.length; ++i) {
                    theTableRIs[i].writeFastExternal(out, sv);
                }
            } else {
                TableResumeInfo tri = theTableRIs[0];
                writeByteArray(out, tri.thePrimResumeKey);

                if (tri.thePrimResumeKey != null) {
                    writeByteArray(out, tri.theSecResumeKey);
                    out.writeBoolean(tri.theMoveAfterResumeKey);

                    writeByteArray(out, tri.theDescResumeKey);
                    PlanIter.serializeIntArray(tri.theJoinPathTables, out, sv);
                    PlanIter.serializeByteArray(tri.theJoinPathKey, out, sv);
                    PlanIter.serializeByteArray(tri.theJoinPathSecKey, out, sv);
                    out.writeBoolean(tri.theJoinPathMatched);
                }
            }
        }

        public int sid() {
            return theSID;
        }

        public int pid() {
            return thePID;
        }

        public int numTables() {
            return theTableRIs.length;
        }

        public int currentIndexRange(int i) {
            return theTableRIs[i].theCurrentIndexRange;
        }

        public byte[] secKey(int i) {
            return theTableRIs[i].theSecResumeKey;
        }

        public byte[] primKey(int i) {
            return theTableRIs[i].thePrimResumeKey;
        }

        public boolean moveAfterResumeKey(int i) {
            return theTableRIs[i].theMoveAfterResumeKey;
        }

        public byte[] descResumeKey(int i) {
            return theTableRIs[i].theDescResumeKey;
        }

        public int[] joinPathTables(int i) {
            return theTableRIs[i].theJoinPathTables;
        }

        public byte[] joinPathKey(int i) {
            return theTableRIs[i].theJoinPathKey;
        }

        public byte[] joinPathSecKey(int i) {
            return theTableRIs[i].theJoinPathSecKey;
        }

        public boolean joinPathMatched(int i) {
            return theTableRIs[i].theJoinPathMatched;
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder();

            sb.append("sid/pid = ").append(theSID).append("/").append(thePID);
            sb.append("\n");

            for (int i = 0; i < theTableRIs.length; ++i) {
                sb.append("Table RI ").append(i).append(":\n");
                sb.append(theTableRIs[i]);
            }

            return sb.toString();
        }
    }

    /* Used for tracing only */
    RuntimeControlBlock theRCB;

    /*
     * The number of results received from the server so far. This is needed
     * when a LIMIT clause is pushed to the server. When a server is asked
     * to produce the next result batch, it needs to know how many results
     * it has produced already (in previous batches), so it does not exceed
     * the specified limit. This is necessary when the query is executed
     * at a single partition, because in that case the whole OFFSET/LIMIT
     * clauses are executed at the server. When the query is distributed to
     * multiple partitions/shards, the OFFSET/LIMIT clauses are executed at
     * the client, but a server never needs to send more than OFFSET + LIMIT
     * results. So, a LIMIT equal to the user-specified OFFSET+LIMIT is pushed
     * to the server as an optimization, and numResultsComputed is used as a
     * further optimization. For example, if the batch size is 100 and the
     * server-side limit is 110, this optimization saves the computation and
     * transmission of at least 90 results (110 results with the optimization
     * vs 200 results without). (The savings may be more than 90 because after
     * the client receives one batch, it may immediately ask for the next
     * batch, if its results queue is not full. But with a queue size of 3
     * batches, the maximum savings is 3 batch sizes).
     */
    private int theNumResultsComputed;

    /*
     * It is used by SINGLE_PARTITION and ALL_PARTITIONS queries. In both
     * cases, the theCurrentPid specifies the partition to be scanned by a
     * PrimaryTableScanner. For SINGLE_PARTITION queries, or on-prem
     * ALL_APRTITIONS queries, theCurrentPid is initialized by the
     * TableQueryHandler to the pid specified by the TableQuery op and does
     * not need to be sent back to the client (so it's not really resume info).
     * For details about how this field is used in cloud ALL_PARTITIONS queries
     * see the javadoc of PartitionUnionIter.
     *
     * It is also used as the pid of the target partition in a SINGLE_PARTITION
     * query that uses an index.
     */
    private int theCurrentPid = -1;

    /*
     * It is used used by cloud ALL_PARTITIONS. It is a bitmap with one bit
     * per partition. For details, see the javadoc of PartitionUnionIter.
     */
    private BitSet thePartitionsBitmap;

    private boolean theIsInSortPhase1;

    /*
     * The total reak KB consumed by the whole query so far.
     */
    private int theTotalReadKB;

    /*
     * This is used for a single-partition query with offset, when the query is
     * run in the cloud. In this case, the offset is enforced at the server, but
     * since the query may be suspended before OFFSET results are skipped, the
     * continuation key must include the remaining offset in order for the query
     * to resume correctly.
     */
    private long theOffset = -1;
    /*
     * NEW: This field was used before as described below. Now, a grouping query
     * that suspends in the middle of a group will send the partial group tuple
     * as a normal result (instead of putting it in the resume info) to the
     * client. The field is still used, but only as a boolean field essentially:
     * to check if there is a cached group tuple in the SFWIter. But to maintain
     * compatibility with old clients, the field is not changed to a boolean;
     * instead it is set to a dummy (empty) array.
     *
     * OLD: Resume info used for grouping queries. In this case, to know whether
     * a group is finished, we have to evaluate the 1st tuple of the next
     * group. So, if the batch size is N, at the end of a batch, we have
     * at each RN N results plus the 1st tuple of the next group. This extra
     * result must be sent to the client and then back to the server so that
     * the next group is initialized properly.
     */
    private FieldValueImpl[] theGBTuple;

    /* One TableResumeInfo per join operand (i.e. BaseTableIter) */
    private ArrayList<TableResumeInfo> theTableRIs = new ArrayList<>(8);

    /*
     * The following 3 fields are used to support query execution during
     * elasticity operations. for details, see:
     * kvstore/src/oracle/kv/impl/query/ElasticityAndQueries.txt
     */

    /*
     * The sequence number of the topology that is cached at the proxy when
     * the 1st query request (for the 1st batch) is sent there. This is
     * called the "baseTopology" of the query. It is used by the query to detect
     * partition migrations. The shards in this topology will be called the
     * "base" shards of the query.
    */
    private int theBaseTopoNum = -1;

    /*
     * The MigratedPartitionsMap (VSM) : It contains entries for partitions that
     * are found to have migrated from their source shard. An entry maps a
     * partition id to the id of the target shard and the info needed to
     * determine the starting index entry for the scan of this partition at
     * the target shard.
     *
     * For each VirtualScan VS in theVSM, a scan will be performed in the
     * shard specified by VS, but only for index entries that point to the
     * partition specified by the VS. Such a virtual scan will start at
     * the index entry specified by the resume info contained in the VS.
     */
    private LinkedHashMap<Integer, VirtualScan> theVSM;

    /*
     * If the shard to scan is a virtual one, this is the pid of the associated
     * partition.
     * Note: We cannot overload theCurrentPid for this purpose because
     * of SINGLE_PARTITION queries that use an index.
     */
    private int theVirtualScanPid = -1;


    public ResumeInfo() {
        this((RuntimeControlBlock)null);
    }

    public ResumeInfo(RuntimeControlBlock rcb) {
        theRCB = rcb;
        ensureTableRI(0);
    }

    public ResumeInfo(ResumeInfo ri) {

        theRCB = ri.theRCB;
        theNumResultsComputed = ri.theNumResultsComputed;
        theCurrentPid = ri.theCurrentPid;
        if (ri.getPartitionsBitmap() != null) {
            thePartitionsBitmap = (BitSet)ri.getPartitionsBitmap().clone();
        }
        theIsInSortPhase1 = ri.theIsInSortPhase1;
        theTotalReadKB = ri.theTotalReadKB;
        theOffset = ri.theOffset;

        if (ri.theGBTuple != null) {
            theGBTuple = Arrays.copyOf(ri.theGBTuple, ri.theGBTuple.length);
        }

        theTableRIs = new ArrayList<>(ri.theTableRIs.size());

        for (TableResumeInfo tri : ri.theTableRIs) {
            theTableRIs.add(new TableResumeInfo(tri));
        }

        theVSM = ri.theVSM;
        theBaseTopoNum = ri.theBaseTopoNum;
        theVirtualScanPid = ri.theVirtualScanPid;
    }

    void reset() {
        /* don't reset theNumResultsComputed */
        if (theVirtualScanPid < 0) {
            for (TableResumeInfo tri : theTableRIs) {
                tri.reset();
            }
        }
        theGBTuple = null;
    }

    /*
     * This method is used by a ResumeInfo that lives at the client. It updates
     * the values of "this" with new values coming from the server.
     */
    void refresh(ResumeInfo src) {

        theNumResultsComputed += src.theNumResultsComputed;
        theCurrentPid = src.theCurrentPid;
        thePartitionsBitmap = src.thePartitionsBitmap;
        theIsInSortPhase1 = src.theIsInSortPhase1;
        theOffset = src.theOffset;

        for (int i = 0; i < src.theTableRIs.size(); ++i) {
            if (i >= theTableRIs.size()) {
                theTableRIs.add(new TableResumeInfo());
            }
            theTableRIs.get(i).refresh(src.theTableRIs.get(i));
        }

        theGBTuple = src.theGBTuple;

        theVSM = src.theVSM;
    }

    public void setRCB(RuntimeControlBlock rcb) {
        theRCB = rcb;
    }

    public int getNumResultsComputed() {
        return theNumResultsComputed;
    }

    public void setNumResultsComputed(int v) {

        if (theNumResultsComputed < 0) {
            theNumResultsComputed = v + theNumResultsComputed;
        } else {
            theNumResultsComputed = v;
        }
    }

    void incNumResultsComputed() {
        ++theNumResultsComputed;
    }

    public int getCurrentPid() {
        return theCurrentPid;
    }

    public void setCurrentPid(int pid) {
        theCurrentPid = pid;
    }

    public BitSet getPartitionsBitmap() {
        return thePartitionsBitmap;
    }

    public void setPartitionsBitmap(BitSet bs) {
        thePartitionsBitmap = bs;
    }

    public void setIsInSortPhase1(boolean v) {
        theIsInSortPhase1 = v;
    }

    public boolean isInSortPhase1() {
        return theIsInSortPhase1;
    }

    int getTotalReadKB() {
        return theTotalReadKB;
    }

    public void addReadKB(int kb) {
        theTotalReadKB += kb;
    }

    public long getOffset() {
        return theOffset;
    }

    public void setOffset(long v) {
        theOffset = v;
    }

    public void ensureTableRI(int i) {
        if (i >= theTableRIs.size()) {
            assert(i == theTableRIs.size());
            theTableRIs.add(new TableResumeInfo());
        }
    }

    public int numTables() {
        return theTableRIs.size();
    }

    public void reset(int i) {
        theTableRIs.get(i).reset();
    }

    public int getCurrentIndexRange(int i) {
        return theTableRIs.get(i).theCurrentIndexRange;
    }

    public void setCurrentIndexRange(int i, int v) {
        ensureTableRI(i);
        theTableRIs.get(i).theCurrentIndexRange = v;
    }

    public byte[] getPrimResumeKey(int i) {
        return theTableRIs.get(i).thePrimResumeKey;
    }

    public void setPrimResumeKey(int i, byte[] resumeKey) {

        if (theRCB != null && theRCB.getTraceLevel() >= 4) {
            theRCB.trace("Setting resume key " + i + " to\n" +
                         PlanIter.printByteArray(resumeKey));
        }

        ensureTableRI(i);
        theTableRIs.get(i).thePrimResumeKey = resumeKey;
    }

    public byte[] getSecResumeKey(int i) {
        return theTableRIs.get(i).theSecResumeKey;
    }

    public void setSecResumeKey(int i, byte[] resumeKey) {

        if (theRCB != null && theRCB.getTraceLevel() >= 4) {
            theRCB.trace("Setting secondary resume key " + i + " to\n" +
                         PlanIter.printByteArray(resumeKey));
        }

        ensureTableRI(i);
        theTableRIs.get(i).theSecResumeKey = resumeKey;
    }

    public byte[] getDescResumeKey(int i) {
        return theTableRIs.get(i).theDescResumeKey;
    }

    public void setDescResumeKey(int i, byte[] key) {

        if (theRCB != null && theRCB.getTraceLevel() >= 3) {
            theRCB.trace("Setting descendant resume key " + i + " to\n" +
                         PlanIter.printByteArray(key));
        }

        ensureTableRI(i);
        theTableRIs.get(i).theDescResumeKey = key;
    }

    public boolean getMoveAfterResumeKey(int i) {
        return theTableRIs.get(i).theMoveAfterResumeKey;
    }

    public void setMoveAfterResumeKey(int i, boolean v) {
        ensureTableRI(i);
        theTableRIs.get(i).theMoveAfterResumeKey = v;
    }

    public boolean getMoveJoinAfterResumeKey(int i) {
        return theTableRIs.get(i).theMoveJoinAfterResumeKey;
    }

    public void setMoveJoinAfterResumeKey(int i, boolean v) {
        ensureTableRI(i);
        theTableRIs.get(i).theMoveJoinAfterResumeKey = v;
    }

    public int[] getJoinPathTables(int i) {
        return theTableRIs.get(i).theJoinPathTables;
    }

    public byte[] getJoinPathKey(int i) {
        return theTableRIs.get(i).theJoinPathKey;
    }

    public byte[] getJoinPathSecKey(int i) {
        return theTableRIs.get(i).theJoinPathSecKey;
    }

    public boolean getJoinPathMatched(int i) {
        return theTableRIs.get(i).theJoinPathMatched;
    }

    public void setJoinPathMatched(int i, boolean v) {
        ensureTableRI(i);
        theTableRIs.get(i).theJoinPathMatched = v;
    }

    public void setJoinPath(
        int i,
        int[] tables,
        byte[] primKey,
        byte[] idxKey,
        boolean matched) {

        ensureTableRI(i);
        TableResumeInfo tri = theTableRIs.get(i);
        tri.theJoinPathTables = tables;
        tri.theJoinPathKey = primKey;
        tri.theJoinPathSecKey = idxKey;
        tri.theJoinPathMatched = matched;
    }

    public FieldValueImpl[] getGBTuple() {
        return theGBTuple;
    }

    public void setGBTuple(FieldValueImpl[] gbTuple) {
        theGBTuple = gbTuple;
    }

    public int getBaseTopoNum() {
        return theBaseTopoNum;
    }

    public void setBaseTopoNum(int n) {
        assert(theBaseTopoNum == -1 || theBaseTopoNum == n);
        assert(n > 0);
        theBaseTopoNum = n;
    }

    public int getVirtualScanPid() {
        return theVirtualScanPid;
    }

    public void setVirtualScanPid(int pid) {
        theVirtualScanPid = pid;
    }

    public Map<Integer, VirtualScan> getVirtualScansMap() {
        return theVSM;
    }

    int numVirtualScans() {
        return (theVSM == null ? 0 : theVSM.size());
    }

    public List<VirtualScan> getVirtualScans() {

        if (theVSM == null) {
            return null;
        }

        List<VirtualScan> vscans = new ArrayList<>(theVSM.size());

        for (VirtualScan vs : theVSM.values()) {
            vscans.add(vs);
        }

        return vscans;
    }

    public VirtualScan getVirtualScan(int pid) {
        return theVSM.get(pid);
    }

    public Set<Integer> getMigratedPartitions() {
        return (theVSM == null ? null : theVSM.keySet());
    }

    public boolean isMigratedPartition(PartitionId pid) {
        return (theVSM == null ?
                false :
                theVSM.containsKey(pid.getPartitionId()));
    }

    /*
     * Add an entry to the MPM for the given pid. The resumePid param is the pid
     * for the partition corresponding to this.thePrimResumeKey.
     */
    public void addVirtualScan(int resumePid, int pid, int sid) {

        TableResumeInfo[] tris = new TableResumeInfo[theTableRIs.size()];

        for (int i = 0; i < tris.length; ++i) {

            TableResumeInfo tri1 = theTableRIs.get(i);

            if (resumePid == pid) {

                /* We are trying to resume on a partition that has migrated. */
                tris[i] = new TableResumeInfo(tri1);

                if (i > 0) {
                    tri1.reset();
                }

            } else {
                tris[i] = new TableResumeInfo();
                if (i == 0) {
                    tris[i].theCurrentIndexRange = tri1.theCurrentIndexRange;
                    tris[i].thePrimResumeKey = tri1.thePrimResumeKey;
                    tris[i].theSecResumeKey = tri1.theSecResumeKey;
                }
            }

            if (theRCB.getTraceLevel() >= 1) {
                theRCB.trace(
                    "Added virtual scan at " + i + "for partition " + pid +
                    ", resumePid = " + resumePid + " resume info : \n" +
                    tris[i]);
            }
        }

        if (theVSM == null) {
            theVSM = new LinkedHashMap<Integer, VirtualScan>();
        }

        if (theVSM.containsKey(pid)) {
            throw new QueryStateException(
               "Detected migration for partition " + pid +
               ", which is already known as a migrated partition");
        }

        VirtualScan vs = new VirtualScan(pid, sid, tris);
        theVSM.put(pid, vs);
    }

    public void initForVirtualScan(VirtualScan vs) {

        setVirtualScanPid(vs.pid());

        int numTables = vs.theTableRIs.length;
        theTableRIs = new ArrayList<>(numTables);

        for (int i = 0; i < numTables; ++i) {
            TableResumeInfo tri = new TableResumeInfo(vs.theTableRIs[i]);
            theTableRIs.add(tri);
        }
    }

    public void initForVirtualScan(
        PartitionId[] resumePids,
        PartitionId pid,
        ResumeInfo src) {

        setVirtualScanPid(pid.getPartitionId());

        int numTables = src.theTableRIs.size();
        theTableRIs = new ArrayList<>(numTables);

        for (int i = 0; i < numTables; ++i) {

            TableResumeInfo tri = new TableResumeInfo();
            theTableRIs.add(tri);
            tri.theCurrentIndexRange = src.getCurrentIndexRange(i);
            tri.thePrimResumeKey = src.getPrimResumeKey(i);
            tri.theSecResumeKey = src.getSecResumeKey(i);

            if (pid.equals(resumePids[i])) {
                tri.theMoveAfterResumeKey = src.getMoveAfterResumeKey(i);
                tri.theDescResumeKey = src.getDescResumeKey(i);
                tri.theJoinPathTables =src.getJoinPathTables(i);
                tri.theJoinPathKey = src.getJoinPathKey(i);
                tri.theJoinPathSecKey = src.getJoinPathSecKey(i);
                tri.theJoinPathMatched = src.getJoinPathMatched(i);
            }
        }
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("theNumResultsComputed = ").append(theNumResultsComputed);
        sb.append("\n");

        sb.append("theCurrentPid = ").append(theCurrentPid);
        sb.append("\n");

        sb.append("thePartitionsBitmap = ").append(thePartitionsBitmap);
        sb.append("\n");

        sb.append("theIsInSortPhase1 = ").append(theIsInSortPhase1);
        sb.append("\n");

        sb.append("theTotalReadKB = ").append(theTotalReadKB);
        sb.append("\n");

        sb.append("theOffset = ").append(theOffset);
        sb.append("\n");

        for (int i = 0; i < theTableRIs.size(); ++i) {
            sb.append("Table RI ").append(i).append(":\n");
            sb.append(theTableRIs.get(i));
        }
        sb.append("\n");

        if (theGBTuple != null) {
            sb.append("GB tuple = [ ");
            for (int i = 0; i < theGBTuple.length; ++i) {
                sb.append(theGBTuple[i]).append(" ");
            }
            sb.append("]\n");
        }

        if (theBaseTopoNum >= 0) {
            sb.append("baseTopoNum = ").
            append(theBaseTopoNum).append("\n");
        }

        if (theVSM != null) {
            sb.append("VSM = [\n");
            for (VirtualScan vs : theVSM.values()) {
                sb.append(vs).append("\n");
            }
            sb.append("]\n");
        }

        if (theVirtualScanPid >= 0) {
            sb.append("virtualScanPid = ").
            append(theVirtualScanPid).append("\n");
        }

        return sb.toString();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeInt(theNumResultsComputed);

        out.writeInt(theCurrentPid);
        if (thePartitionsBitmap != null) {
            writeByteArray(out, thePartitionsBitmap.toByteArray());
        } else {
            writeByteArray(out, null);
        }
        out.writeBoolean(theIsInSortPhase1);

        out.writeInt(theTotalReadKB);

        out.writeLong(theOffset);

        if (serialVersion >= QUERY_VERSION_17) {
            writeSequenceLength(out, theTableRIs.size());
        }
        for (int i = 0; i < theTableRIs.size(); ++i) {
            theTableRIs.get(i).writeFastExternal(out, serialVersion);
        }

        PlanIter.serializeFieldValues(theGBTuple, out, serialVersion);

        if (serialVersion >= QUERY_VERSION_14) {
            SerializationUtil.writeMapLength(out, theVSM);
            if (theVSM != null) {
                for (Map.Entry<Integer, VirtualScan> entry : theVSM.entrySet()) {
                    SerializationUtil.writePackedInt(out, entry.getKey());
                    entry.getValue().writeFastExternal(out, serialVersion);
                }
            }

            SerializationUtil.writePackedInt(out, theBaseTopoNum);
            SerializationUtil.writePackedInt(out, theVirtualScanPid);
        }
    }

    public ResumeInfo(DataInput in, short serialVersion) throws IOException {

        theRCB = null;

        try {
            theNumResultsComputed = in.readInt();

            theCurrentPid = in.readInt();
            byte[] array = readByteArray(in);
            if (array == null) {
                thePartitionsBitmap = null;
            } else {
                thePartitionsBitmap = BitSet.valueOf(array);
            }
            theIsInSortPhase1 = in.readBoolean();

            theTotalReadKB = in.readInt();

            theOffset = in.readLong();

            if (serialVersion >= QUERY_VERSION_17) {
                int num = readSequenceLength(in);
                theTableRIs = new ArrayList<>(num);
                for (int i = 0; i < num; ++i) {
                    theTableRIs.add(new TableResumeInfo(in, serialVersion));
                }
            } else {
                theTableRIs = new ArrayList<>(1);
                theTableRIs.add(new TableResumeInfo(in, serialVersion));
            }

            theGBTuple = PlanIter.deserializeFieldValues(in, serialVersion);

            if (serialVersion >= QUERY_VERSION_14) {
                int size = SerializationUtil.readSequenceLength(in);
                if (size < 0) {
                    theVSM = null;
                } else {
                    theVSM = new LinkedHashMap<Integer, VirtualScan>(size);
                    for (int i = 0; i < size; ++i) {
                        int pid = SerializationUtil.readPackedInt(in);
                        VirtualScan value = new VirtualScan(in, serialVersion);
                        theVSM.put(pid, value);
                    }
                }

                theBaseTopoNum = SerializationUtil.readPackedInt(in);
                theVirtualScanPid = SerializationUtil.readPackedInt(in);
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (RuntimeException re) {
            re.printStackTrace();
            throw new QueryStateException(
                "Failed to deserialize ResumeInfo.", re);
        }
    }
}
