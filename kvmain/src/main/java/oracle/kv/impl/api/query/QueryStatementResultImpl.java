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

import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.FastExternalizableException;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ReceiveIter;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableIterator;

/**
 * Implementation of StatementResult when statement is a query.
 *
 * It represents the query result set, and provides the entry-point methods
 * for the computation of this result set.
 *
 * Queries can be executed in 2 modes: synchronous and asynchronous. For async
 * mode, the following diagram shows the high-level data structs participating
 * in query execution, and their relationships (who has references to what)
 *
 * <pre>
 *    QueryStatementResultImpl
 *      /|\             /|\
 *       |               |
 *       |              \|/
 *       |             QueryPublisher ---> Subscriber
 *       |             |    /|\   /|\        |
 *       |             |     |     |         |
 *      \|/           \|/    |     |        \|/
 *    QueryResultIterator    |  QuerySubscription
 *            |              |
 *            |              |
 *           \|/             |
 *       Query PlanIters -----
 * </pre>
 */
public class QueryStatementResultImpl implements StatementResult {

    private final PreparedStatementImpl theStatement;

    private final QueryPublisher thePublisher;

    private RuntimeControlBlock theRCB;

    private final QueryResultIterator theIterator;

    private boolean theClosed;

    public QueryStatementResultImpl(TableAPIImpl tableAPI,
                                    ExecuteOptions options,
                                    InternalStatement stmt) {
        this(tableAPI, options, stmt, null, null, null);
    }

    public QueryStatementResultImpl(TableAPIImpl tableAPI,
                                    ExecuteOptions options,
                                    InternalStatement stmt,
                                    QueryPublisher qpub,
                                    Set<Integer> partitions,
                                    Set<RepGroupId> shards) {

        PreparedStatementImpl ps;
        FieldValueImpl[] externalVars = null;

        if (stmt instanceof BoundStatementImpl) {
            BoundStatementImpl bs = (BoundStatementImpl)stmt;
            ps = bs.getPreparedStmt();
            externalVars = ps.getExternalVarsArray(bs.getVariables());
        } else {
            ps = (PreparedStatementImpl)stmt;
        }

        if (ps.hasExternalVars() && externalVars == null) {
            throw new IllegalArgumentException(
                "The query contains external variables, none of which " +
                "has been bound.");
        }

        thePublisher = qpub;
        theStatement = ps;
        PlanIter iter = ps.getQueryPlan();
        RecordDef resultDef = ps.getResultDef();

        theRCB = new RuntimeControlBlock(
            tableAPI.getStore(),
            tableAPI.getStore().getLogger(),
            tableAPI.getTableMetadataHelper(),
            partitions,
            shards,
            options, /* ExecuteOptions */
            iter,
            ps.getNumIterators(),
            ps.getNumRegisters(),
            externalVars);

        theIterator = new QueryResultIterator(iter, resultDef);
        theClosed = false;
    }

    @Override
    public void close() {
        theIterator.close();
        theClosed = true;
    }

    PreparedStatementImpl getStatement() {
        return theStatement;
    }

    @Override
    public RecordDef getResultDef() {
        return theStatement.getResultDef();
    }

    public RuntimeControlBlock getRCB() {
        return theRCB;
    }

    @Override
    public TableIterator<RecordValue> iterator() {

        if (thePublisher != null) {
            throw new IllegalStateException(
                "Application-driven iteration is not allowed for queries " +
                "executed in asynchronous mode");
        }

        if (theClosed) {
            throw new IllegalStateException("Statement result already closed.");
        }

        return theIterator;
    }

    QueryResultIterator getIterator() {
        return theIterator;
    }

    public QueryPublisher getPublisher() {
        return thePublisher;
    }

    @Override
    public int getPlanId() {
        return 0;
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public String getInfoAsJson() {
        return null;
    }

    @Override
    public String getErrorMessage() {
        return null;
    }

    @Override
    public boolean isSuccessful() {
        return true;
    }

    @Override
    public boolean isDone() {
        return !theIterator.hasNext();
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public String getResult() {
        return null;
    }

    @Override
    public Kind getKind() {
        return Kind.QUERY;
    }

    /**
     * Returns the KB read during the execution of operation.
     */
    public int getReadKB() {
        return theIterator.getReadKB();
    }

    /**
     * Returns the KB written during the execution of operation.
     */
    public int getWriteKB() {
        return theIterator.getWriteKB();
    }

    /**
     * Returns the continuation key for the next execution.
     */
    public byte[] getContinuationKey() {
        return theIterator.getContinuationKey();
    }

    public boolean reachedLimit() {
        return theIterator.reachedLimit();
    }

    public boolean hasSortPhase1Result() {
        return theIterator.hasSortPhase1Result();
    }

    public int writeSortPhase1Results(DataOutput out) throws IOException {
        return theIterator.writeSortPhase1ResultInfo(out);
    }

    @Override
    public void printTrace(PrintStream out) {

        Map<String, String> traces = theIterator.theRootIter.getRNTraces(theRCB);
        StringBuilder sb = new StringBuilder();

        sb.append("\n\n---------------------------------\n");
        sb.append("CLIENT : " + theRCB.getQueryName());
        sb.append("\n---------------------------------\n\n");
        sb.append(theRCB.getTrace());
        sb.append("\n");

        for (Map.Entry<String, String> entry : traces.entrySet()) {
            sb.append("\n\n-------------------------------------------\n");
            sb.append(theRCB.getQueryName());
            sb.append(": ");
            sb.append(entry.getKey());
            sb.append("\n-------------------------------------------\n\n");
            sb.append(entry.getValue());
            sb.append("\n");
        }

        out.println(sb.toString());
    }

    /*
     * QueryResultIterator is public because it is accessed by proxy code.
     */
    public class QueryResultIterator
        implements AsyncTableIterator<RecordValue> {

        private final PlanIter theRootIter;

        private final RecordDef theResultDef;

        private boolean theHasNext;

        private boolean theIsClosed;

        private TableImpl theJsonTable;

        QueryResultIterator(PlanIter iter, RecordDef resultDef) {

            theRootIter = iter;
            theResultDef = resultDef;

            try {
                theRootIter.open(theRCB);

                if (thePublisher != null) {
                    /*
                     * Store the notifier in the iterator, which will supply it
                     * to its children, if any. That way, children can notify
                     * the execution handle directly. Requests the handle makes
                     * to obtain more iteration results will still need to
                     * filter down to the children.
                     */
                    theRootIter.setPublisher(theRCB, thePublisher);
                } else {
                    updateHasNext();
                }
            } catch (QueryStateException qse) {
                /*
                 * Log the exception if a logger is available.
                 */
                Logger logger = theRCB.getStore().getLogger();
                if (logger != null) {
                    logger.warning(qse.toString());
                }
                throw new IllegalStateException(qse.toString());
            } catch (QueryException qe) {
                /* A QueryException thrown at the client; rethrow as IAE */
                throw qe.getIllegalArgument();
            } catch (IllegalArgumentException iae) {
                throw iae;
            } catch (FastExternalizableException fee) {
                throw fee;
            } catch (RuntimeException re) {
                /* why log this as WARNING? */
                String msg = "Query execution failed: " + re;
                Logger logger = theRCB.getStore().getLogger();
                if (logger != null) {
                    logger.warning(msg);
                }
                //re.printStackTrace();
                throw re;
            }
        }

        RecordDef getResultDef() {
            return theResultDef;
        }

        public QueryStatementResultImpl getQueryStatementResult() {
            return QueryStatementResultImpl.this;
        }

        public RuntimeControlBlock getRCB() {
            return theRCB;
        }

        /**
         * Returns the KB read during the execution of operation.
         */
        public int getReadKB() {
            return theRCB.getReadKB();
        }

        /**
         * Returns the KB written during the execution of operation.
         */
        public int getWriteKB() {
            return theRCB.getWriteKB();
        }

        /**
         * Returns the continuation key for the next execution.
         */
        public byte[] getContinuationKey() {
            return theRCB.getContinuationKey();
        }

        public boolean reachedLimit() {
            return theRCB.getReachedLimit();
        }

        public void refreshEndTime() {
            theRCB.refreshEndTime();
        }

        public boolean hasSortPhase1Result() {
            return (theRootIter instanceof ReceiveIter &&
                    ((ReceiveIter)theRootIter).hasSortPhase1Result(theRCB));
        }

        public int writeSortPhase1ResultInfo(DataOutput out)
            throws IOException {
            return ((ReceiveIter)theRootIter).
                   writeSortPhase1ResultInfo(theRCB, out);
        }

        private void updateHasNext() {
            theRCB.refreshEndTime();
            theHasNext = theRootIter.next(theRCB);
        }

        @Override
        public boolean hasNext() {

            if (thePublisher != null) {
                throw new IllegalStateException(
                    "Application-driven iteration is not allowed for queries " +
                    "executed in asynchronous mode");
            }
            return theHasNext;
        }

        @Override
        public RecordValue next() {

            if (!theHasNext) {
                throw new NoSuchElementException();
            }

            return nextInternal(false /* localOnly */);
        }

        @Override
        public RecordValue nextLocal() {

            if (!theRootIter.nextLocal(theRCB)) {
                return null;
            }

            return nextInternal(true /* localOnly */);
        }

        /* Suppress Eclipse warning in assert -- see below */
        @SuppressWarnings("unlikely-arg-type")
        private RecordValue nextInternal(boolean localOnly) {

            final RecordValue record;

            try {
                FieldValueImpl resVal = theRCB.getRegVal(theRootIter.getResultReg());

                if (resVal.isTuple()) {
                    assert(theResultDef == null ||
                           theResultDef.equals(resVal.getDefinition()));
                    record = ((TupleValue)resVal).toRecord();

                } else if (resVal.isMap()) {
                    /* The query is a "select *" over a single JSON collection
                     * table. Create a record for the table and copy the map.
                      */
                    if (theJsonTable == null) {
                        theJsonTable = theStatement.getJsonTable(theRCB.getStore());
                    }
                    record = theJsonTable.createRow();
                    for (Map.Entry<String, FieldValue> entry :
                             ((MapValueImpl)resVal.asMap()).
                             getMap().entrySet()) {
                        record.put(entry.getKey(), entry.getValue());
                    }

                } else {
                    assert(resVal.isRecord());
                    record = (RecordValue)resVal;
                }

                if (!localOnly) {
                    updateHasNext();
                }

            } catch (QueryStateException qse) {
                /*
                 * Log the exception if a logger is available.
                 */
                Logger logger = theRCB.getStore().getLogger();
                if (logger != null) {
                    logger.warning(qse.toString());
                }
                throw new IllegalStateException(qse.toString());
            } catch (QueryException qe) {
                /* A QueryException thrown at the client; rethrow as IAE */
                throw qe.getIllegalArgument();
            }

            return record;
        }

        @Override
        public Throwable getCloseException() {
            Throwable t = theRootIter.getCloseException(theRCB);
            if (t instanceof StoreIteratorException) {
                Throwable cause = t.getCause();
                if (cause instanceof RuntimeException) {
                    return cause;
                }
                if (cause instanceof Error) {
                    return cause;
                }
                return t;
            }
            return t;
        }

        @Override
        public void close() {
            if (!theIsClosed) {
                theRootIter.close(theRCB);
                theHasNext = false;
                theIsClosed = true;
            }
        }


        @Override
        public boolean isClosed() {

            /* Should be used in async mode only */
            assert(thePublisher != null);
            return theIsClosed || theRootIter.isDone(theRCB);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            if (theRCB.getTableIterator() != null) {
                return theRCB.getTableIterator().getShardMetrics();
            }
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            if (theRCB.getTableIterator() != null) {
                return theRCB.getTableIterator().getShardMetrics();
            }
            return Collections.emptyList();
        }
    }
}
