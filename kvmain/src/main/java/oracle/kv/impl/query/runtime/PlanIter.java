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
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeArrayLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import oracle.kv.Key;
import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FieldValueSerialization;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;

/**
 * Base class for all query-plan iterators.
 *
 * The query compiler produces the "query execution plan" as a tree of plan
 * iterators. Each plan iterator has an open()-next()-close() interface. To
 * execute a query, the application must first call open() on the root iterator
 * and then call next() a number of times to retrieve the results. Finally,
 * after the application has retrieved all the results, or when it is not
 * interested in retrieving any more results, it must call close() to release
 * any resources held by the iterators.
 *
 * In general, these calls are propagated top-down within the execution plan,
 * and results flow bottom-up. More specifically, each next() call on the root
 * iterator produces one item in the result set of the query. Actually, the
 * same is true for all iterators: each next() call on a plan iterator produces
 * one item in the result set of that iterator. So, in the most general case,
 * the result set of each iterator is a sequence of zero or more items.
 *
 * Iterator state and registers:
 * -----------------------------
 *
 * As mentioned already, each next() call on an iterator produces one result
 * item. However, the result items are not returned by the next() call directly.
 * Instead, each iterator places its current result (the one produced by the
 * current invocation of the next() call) in a "register", where a consumer
 * iterator can pick it up from. A "register" is just an entry in an array of
 * FiledValueImpl instances. This array is created during the creation of the
 * RuntimeControlBlock (RCB). The size of the regs array is determined during
 * code generation (see CodeGenerator class): a number of zero or more registers
 * are reserved for each generated iterator, and the sum of these registers is
 * the size of the regs array. Each iterator knows the positions (reg ids) of
 * the registers that have been reserved for it, and will, in general, provide
 * this info to its parent (consuming) iterator. Of particular interest is the
 * "result reg" of each iterator: the id of the register where the iterator
 * places its result items. All iterators have a result reg. Notice, however,
 * that some iterators may share the same result reg. For example, if an
 * iterator A is just filtering the result of another iterator B, A can use
 * B's result reg as its own result reg as well.
 *
 * Records vs tuples.
 * ------------------
 * Iterators that produce or propagate records have a choice about how the
 * records are returned:
 *
 * (a) As self-described record items, ie, instances of RecordValueImpl.
 *
 * (b) As "tuple" items, i.e., instances of TupleValue. Tuples can be thought
 * of as un-nested records. Their field values are stored in N registers within
 * the RCB array of registers, where N is the number of fields. A TupleValue
 * instance itself stores only the ids of those registers and a pointer to the
 * RCB array of registers. The field names are not stored in the  TupleValue
 * at all; they are stored only in the associated type def.
 *
 * Whether an iterator returns records or tuples is a decision taken at
 * compilation time, during code generation.
 *
 * Currently, TupleValue instances are created only by iterators that are
 * original producers of tuples, specifically, the BaseTableIter and the
 * SFWIter. These iterators reserve N + 1 registers in the RCB reg array:
 * 1 reg as the result reg, which will hold a TupleValue, and N regs to hold
 * the field values of each tuple they will generate (these are called the
 * "tuple regs"). Then, during the open() call they create one TupleValue
 * and place a ref to it in the result reg. Finally, during each subsequent
 * next() invocation, they compute the N field values and place them in the
 * N tuple regs.
 *
 * Notice that on each next() call, producers of tuples overwrite the field
 * values of the previous tuple. As a result, consumers of tuples who need to
 * retain a tuple value across next() calls must clone the tuple. Currently,
 * we don't have any iterators that cache any kind of values across next()
 * calls.
 *
 * Data members:
 * -------------
 *
 * theKind:
 * The kind of this iterator (there is one PlanIter subclass for each kind).
 * Roughly speaking, each kind of iterator evaluates a different kind of
 * expression that may appear in a query.
 *
 * theResultReg:
 * The id of the register where this iterator will store each item generated
 * by a next() call.
 *
 * theStatePos:
 * The index within the state array where the state of this iterator is stored.
 *
 * theForCloudDriver:
 * If true, this iterator will be executed at the cloud driver.
 */
public abstract class PlanIter implements FastExternalizable {

    private static final int NULL_VALUE = -1;

    private static final PlanIter[] NO_PLAN_ITERS = { };

    /*
     * Add new iterator types to the end of this list. The ordinal is used for
     * serialization of query plans.
     */
    public static enum PlanIterKind {

        CONST(0),
        VAR_REF(1),
        EXTERNAL_VAR_REF(2),
        ARRAY_CONSTRUCTOR(3),
        TABLE(4),
        VALUE_COMPARE(5),
        ANY_COMPARE(6),
        AND_OR(7),
        ARITHMETIC(8),
        ARITHMETIC_NEGATION(9),
        PROMOTE(10),
        FIELD_STEP(11),
        ARRAY_SLICE(12),
        ARRAY_FILTER(13),
        SELECT(14),
        FUNC_SIZE(15),
        FUNC_KEYS(16),   // not supported any more (since R4.3)
        RECEIVE(17),
        SEQ_CONCAT(18),
        CASE(19),
        // Added in R4.3
        MAP_CONSTRUCTOR(20),
        EXISTS(21),
        NOT(22),
        MAP_FILTER(23),
        IS_OF_TYPE(24),
        CAST(25),
        // Added in R4.4
        IS_NULL(26),
        //Added in R4.5
        FUNC_EXTRACT_FROM_TIMESTAMP(27),
        UPDATE_FIELD(28),
        UPDATE_ROW(29),
        FUNC_EXPIRATION_TIME(30),
        FUNC_EXPIRATION_TIME_MILLIS(31),
        FUNC_CURRENT_TIME(32),
        FUNC_CURRENT_TIME_MILLIS(33),
        FUNC_REMAINING_HOURS(34),
        FUNC_REMAINING_DAYS(35),
        FUNC_VERSION(36),

        // Added in R18.1
        FUNC_COUNT_STAR(37),
        FUNC_COUNT(38),
        FUNC_SUM(39),
        FUNC_AVG(40),
        FUNC_MIN_MAX(41),

        SEQ_MAP(42),

        RECORD_CONSTRUCTOR(43),

        // Added in R18.3
        GEO_SEARCH(44),
        GEO_DISTANCE(45),
        GEO_IS_GEOMETRY(46),

        SORT(47),

        SEQ_AGGR(48),

        INSERT_ROW(49),
        DELETE_ROW(50),
        FUNC_PARSE_JSON(51),

        PARTITION_UNION(52),

        FUNC_REGEX_LIKE(53),

        STRING_CONCAT(54),
        FUNC_SUBSTRING(55),
        FUNC_UPPER_LOWER(56),
        FUNC_TRIM(57),
        FUNC_LRTRIM(58),
        FUNC_LENGTH(59),
        FUNC_CONTAINS_STARTS_ENDS_WITH(60),
        FUNC_INDEX_OF(61),
        FUNC_REPLACE(62),
        FUNC_REVERSE(63),

        IN(64),

        GROUP(65),

        /* A second enum for the SortIter. It is used to handle a compatibility
         * problem between the drivers and kvstore */
        SORT2(66),

        FUNC_MOD_TIME(67),
        FUNC_PARTITION(68),
        FUNC_SHARD(69),
        FUNC_ROW_STORAGE_SIZE(70),
        FUNC_INDEX_STORAGE_SIZE(71),
        FUNC_MKINDEX_STORAGE_SIZE(72),

        FUNC_UUID(73),

        FUNC_SEQ_DISTINCT(74),

        FUNC_TIMESTAMP_ADD(75),
        FUNC_TIMESTAMP_DIFF(76),
        FUNC_GET_DURATION(77),

        FUNC_COLLECT(78),
        FUNC_SEQ_SORT(79),

        FUNC_MATH(80),

        FUNC_TIMESTAMP_ROUND(81),
        FUNC_FORMAT_TIMESTAMP(82),
        FUNC_PARSE_TO_TIMESTAMP(83),
        FUNC_LAST_DAY_OF_MONTH(84),
        FUNC_TIMESTAMP_BUCKET(85),

        NESTED_LOOP_JOIN(86);

        private static final PlanIterKind[] VALUES = values();

        PlanIterKind(int ord) {
            if (ord != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        public static PlanIterKind valueOf(int ordinal) {

            if (ordinal >= VALUES.length) {
                throw new IllegalArgumentException(
                    "Unknown query iterator ordinal: " + ordinal +
                    ". This may be an attempt to use a query feature not " +
                    "yet supported in this server");
            }

            return VALUES[ordinal];
        }
    }

    /*
     * See javadoc for the getParentItemContext() method.
     */
    static class ParentItemContext {

        FieldValueImpl theParentItem = null;

        int theTargetPos = -1;

        String theTargetKey = null;

        void reset() {
            theParentItem = null;
            theTargetPos = -1;
            theTargetKey = null;
        }
    }

    protected final int theResultReg;

    protected final int theStatePos;

    protected final Location theLocation;

    protected final boolean theIsCloudDriverIter;

    /**
     * Constructor used by the compiler.
     *
     * @param forCloud Will be true for iterators that must be executed at the
     * cloud driver.
     */
    PlanIter(Expr e, int resultReg, boolean forCloud) {
        theResultReg = resultReg;
        theStatePos = e.getQCB().incNumPlanIters();
        theLocation = e.getLocation();
        theIsCloudDriverIter = forCloud;
    }

    PlanIter(Expr e, int resultReg) {
        this(e, resultReg, false);
    }

    /*
     * Constructor used when creating a server-only iterator.
     */
    protected PlanIter(int statePos, int resultReg, Location location) {
        theResultReg = resultReg;
        theStatePos = statePos;
        theLocation = location;
        theIsCloudDriverIter = false;
    }

    /**
     * FastExternalizable constructor.
     */
    @SuppressWarnings("unused")
    protected PlanIter(DataInput in, short serialVersion) throws IOException {

        theResultReg = readPositiveInt(in, true);
        theStatePos = readPositiveInt(in);
        theLocation = new Location(readPositiveInt(in),
                                   readPositiveInt(in),
                                   readPositiveInt(in),
                                   readPositiveInt(in));
        theIsCloudDriverIter = false;
    }

    /**
     * Read an int value and check if the value is {@literal >=} 0.
     */
    static int readPositiveInt(DataInput in) throws IOException {
        return readPositiveInt(in, false);
    }

    /**
     * Read an int value and check if the value is {@literal >=} 0, the
     * {@code allowNegOne} indicates if -1 is valid value or not.
     */
    static int readPositiveInt(DataInput in, boolean allowNegOne)
        throws IOException {

        int value = in.readInt();
        checkPositiveInt(value, allowNegOne);
        return value;
    }

    /**
     * Read an ordinal number value and validate the value in the range
     * 0 ~ (numValues - 1).
     */
    static short readOrdinal(DataInput in, int numValues) throws IOException {
        short index = in.readShort();
        if (index < 0 || index >= numValues) {
            throw new IllegalArgumentException(index + " is invalid, it must " +
                "be in a range 0 ~ " + numValues);
        }
        return index;
    }

    /**
     * Check if the given int value is a non-negative value, the given
     * {@code allowNegOne} is used to indicate if -1 is valid value.
     */
    private static void checkPositiveInt(int value, boolean allowNegOne) {
        if (allowNegOne) {
            if (value < -1) {
                throw new IllegalArgumentException(value + " is invalid, " +
                    "it must be a positive value or -1");
            }
        } else {
            if (value < 0) {
                throw new IllegalArgumentException(value + " is invalid, " +
                    "it must be a positive value");
            }
        }
    }

    /**
     * This method is used by on-prem kvstore to serialize the part of
     * the query plan that is below the ReceiveIter (and must be sent to
     * the RNs for execution). It is also used by the cloud proxy to
     * serialize the query subplan rooted at the ReceiveIter.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        out.writeByte(getKind().ordinal());
        out.writeInt(theResultReg);
        out.writeInt(theStatePos);
        out.writeInt(theLocation.getStartLine());
        out.writeInt(theLocation.getStartColumn());
        out.writeInt(theLocation.getEndLine());
        out.writeInt(theLocation.getEndColumn());
    }

    /**
     * This method is used to serialize the part of the query plan that must be
     * executed at the cloud driver. Only the iterators that may be executed
     * at the cloud driver override this method to provide an actual
     * implementation.
     */
    @SuppressWarnings("unused")
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        throw new QueryException(
            "Cannot execute this kind of query in a proxy-based environment (" +
            "because it uses a " + getName() + " operation)",
            theLocation);
    }

    void writeForCloudCommon(
        DataOutput out,
        short driverVersion) throws IOException {

        if (getKind() == PlanIterKind.SORT &&
            driverVersion > ExecuteOptions.DRIVER_QUERY_V2) {
            out.writeByte(PlanIterKind.SORT2.ordinal());
        } else {
            out.writeByte(getKind().ordinal());
        }
        out.writeInt(theResultReg);
        out.writeInt(theStatePos);
        out.writeInt(theLocation.getStartLine());
        out.writeInt(theLocation.getStartColumn());
        out.writeInt(theLocation.getEndLine());
        out.writeInt(theLocation.getEndColumn());
    }

    /*
     * This method must be overriden by iterators that implement a family of
     * builtin functions (and as a result have a FuncCode data member).
     * Currently, this method is used only in the display method below.
     */
    FuncCode getFuncCode() {
        return null;
    }

    public final int getResultReg() {
        return theResultReg;
    }

    public Location getLocation() {
        return theLocation;
    }

    PlanIterState getState(RuntimeControlBlock rcb) {
        return rcb.getState(theStatePos);
    }

    public abstract PlanIterKind getKind();

    /*
     * Iterators that always return tuples redefine the getTupleRegs() method to
     * return a non-null array of register ids, specifying the registers storing
     * the tuple values.
     *
     * This method is actually needed only by the CodeGenerator to save the
     * allocation of a result reg for some iterators (e.g. ColumnIter,
     * FieldStepIter, PromoteIter).
     */
    public int[] getTupleRegs() {
        return null;
    }

    public boolean producesTuples() {
        return getTupleRegs() != null;
    }

    public PlanIter getInputIter() {
        throw new QueryStateException(
            "Method not implemented for iterator " + getKind());
    }

    /*
     * This method returns the "parent context" of the item that is returned as
     * the result of each next() call on this plan iter. If the result item is
     * an array element, the parent context is the containing array and the
     * position of the element within the array. If the result item is a field
     * value of a map, the parent context is the containing map and the key
     * corresponding to the result item. If the result item is a field value
     * of a record, the parent context is the containing record and the position
     * of the field inside the record.
     *
     * The parent context is needed for updates (see UpdateFieldIter).
     * Currently, only the path-step iterators implement this method.
     */
    @SuppressWarnings("unused")
    void getParentItemContext(RuntimeControlBlock rcb, ParentItemContext ctx) {
        throw new QueryStateException(
            "Method not implemented for iterator " + getKind());
    }

    @SuppressWarnings("unused")
    void aggregate(RuntimeControlBlock rcb, FieldValueImpl val) {
        throw new QueryStateException(
            "Method not implemented for iterator " + getKind());
    }

    /*
     * Initialize the value of an aggregate function. This is done when a
     * query is started or resumed at an RN.
     *
     * Note: The val parameter is normally null. The only exception is the
     * case of a single-partition query that was compiled with an old kvclient.
     * In this case, the val is sent as resume info from the client.
     *
     */
    @SuppressWarnings("unused")
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {
        throw new QueryStateException(
            "Method not implemented for iterator " + getKind());
    }

    /*
     * Get the current value of an aggragate function. If the reset para is
     * true, the value is the final one and this method will also reset the
     * state of the associated aggregate-function iterator. In this case the
     * method is called when a group is completed. If reset is false, it is
     * actually the value of the aggr function on the 1st tuple of a new
     * group.
     */
    @SuppressWarnings("unused")
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {
        throw new QueryStateException(
            "Method not implemented for iterator " + getKind());
    }

    public abstract void open(RuntimeControlBlock rcb);

    public abstract boolean next(RuntimeControlBlock rcb);

    public abstract void reset(RuntimeControlBlock rcb);

    public abstract void close(RuntimeControlBlock rcb);

    public boolean hasNext(RuntimeControlBlock rcb) {
        return !rcb.getState(theStatePos).isDone();
    }

    /**
     * Updates the RuntimeControlBlock to reflect the next locally available
     * iteration result.  Returns true if a next result was available locally,
     * otherwise false.  Note that a false return value does not mean there
     * will never be more elements available, just that none are available
     * locally.  The implementation must not block.
     *
     * <p>The default implementation of this method just calls the next method,
     * and is appropriate for iterators that perform all computations locally
     * without making remote calls.  Classes that make remote calls should
     * provide an implementation that returns local results immediately and
     * arranges for additional remote results to be delivered asynchronously.
     *
     * @param rcb the RuntimeControlBlock
     * @return whether the next result in the iteration was available locally
     */
    public boolean nextLocal(RuntimeControlBlock rcb) {
        return next(rcb);
    }

    /**
     * Specifies the object that should be notified when new iteration elements
     * become available or the iteration is closed.
     *
     * <p>The default implementation of this method does nothing, and is
     * appropriate for iterators that are perform all computations locally
     * without making remote calls.  Classes that make remote calls should
     * provide an implementation that arranges to notify the notifier as
     * needed.
     */
    @SuppressWarnings("unused")
    public void setPublisher(
        RuntimeControlBlock rcb,
        QueryPublisher sub) {
    }

    /**
     * Returns the exception that caused the iteration to be closed, or null if
     * not known or the close was not due to failure.
     *
     * @param rcb the RuntimeControlBlock
     * @return the cause of the close of the iteration or null
     */
    public Throwable getCloseException(RuntimeControlBlock rcb) {
        return null;
    }

    boolean isOpen(RuntimeControlBlock rcb) {
        return rcb.getState(theStatePos).isOpen();
    }

    public boolean isClosed(RuntimeControlBlock rcb) {
        return rcb.getState(theStatePos).isClosed();
    }

    /**
     * Returns whether the iterator is in the DONE or CLOSED state.  CLOSED is
     * included because, in the order of states, a CLOSED iterator is also
     * DONE.
     */
    public boolean isDone(RuntimeControlBlock rcb) {
        final PlanIterState state = rcb.getState(theStatePos);
        return state.isDone() || state.isClosed();
    }

    @SuppressWarnings("unused")
    public Map<String, String> getRNTraces(RuntimeControlBlock rcb) {
        throw new UnsupportedOperationException();
    }

    public final String display() {
        return display(false);
    }

    public final String display(boolean verbose) {
        StringBuilder sb = new StringBuilder();
        display(sb, new DisplayFormatter(true), verbose);
        return sb.toString();
    }

    void display(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("{\n");

        formatter.incIndent();

        formatter.indent(sb);
        sb.append("\"iterator kind\" : \"");
        displayName(sb);
        sb.append("\"");

        if (verbose) {
            sb.append(",\n");
            displayRegs(sb, formatter);
        }

        switch (getKind()) {
        case FUNC_COUNT_STAR:
        case FUNC_CURRENT_TIME:
        case FUNC_CURRENT_TIME_MILLIS:
            break;
        default:
            sb.append(",\n");
        }

        displayContent(sb, formatter, verbose);

        sb.append("\n");
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("}");
    }

    abstract void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose);

    void displayName(StringBuilder sb) {

        if (getFuncCode() != null) {
            sb.append(getFuncCode());
        } else {
            sb.append(getKind());
        }
    }

    String getName() {
        StringBuilder sb = new StringBuilder();
        displayName(sb);
        return sb.toString();
    }

    void displayInputIter(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose,
        PlanIter iter) {

        formatter.indent(sb);
        sb.append("\"input iterator\" :\n");
        iter.display(sb, formatter, verbose);
    }

    void displayInputIters(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose,
        PlanIter[] iters) {

        formatter.indent(sb);
        sb.append("\"input iterators\" : [\n");
        formatter.incIndent();
        for (int i = 0; i < iters.length; ++i) {
            iters[i].display(sb, formatter, verbose);
            if (i < iters.length - 1) {
                sb.append(",\n");
            }
        }
        sb.append("\n");
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("]");
    }

    void displayRegs(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("\"result registers\" : ");
        sb.append("[ ").append(theResultReg);
        int[] tupleRegs = getTupleRegs();
        if (tupleRegs != null) {
            sb.append(", [ ");
            for (int i = 0; i < tupleRegs.length; ++i) {
                sb.append(tupleRegs[i]);
                if (i < tupleRegs.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" ]");
        }
        sb.append(" ]");
    }

    /*
     * Utility methods
     */


    /*
     * A null array writes a 0 length.
     */
    static void serializeIters(
        PlanIter[] iters,
        DataOutput out,
        short serialVersion) throws IOException {

        if (iters == null) {
            iters = NO_PLAN_ITERS;
        }
        writeNonNullSequenceLength(out, iters.length);
        for (final PlanIter iter : iters) {
            serializeIter(iter, out, serialVersion);
        }
    }

    /*
     * A zero length returns an empty array
     */
    static PlanIter[] deserializeIters(DataInput in, short serialVersion)
            throws IOException {

        int numArgs = readNonNullSequenceLength(in);
        PlanIter[] iters = new PlanIter[numArgs];
        for (int i = 0; i < numArgs; i++) {
            iters[i] = deserializeIter(in, serialVersion);
        }
        return iters;
    }

    /**
     * PlanIter always has a leading short indicating the type of the iterator.
     * This is used by the deserialization code to determine which constructor
     * to call on deserialization. A value of NULL_VALUE indicates that there is
     * no iterator at all in this space in the stream.
     */
    public static void serializeIter(
        PlanIter iter,
        DataOutput out,
        short serialVersion) throws IOException {

        if (iter == null) {
            out.writeByte(NULL_VALUE);
            return;
        }

        iter.writeFastExternal(out, serialVersion);
    }

    /**
     * See comments on serializeIter about layout.
     */
    public static PlanIter deserializeIter(DataInput in, short serialVersion)
            throws IOException {

        int iterType = in.readByte();
        if (iterType == NULL_VALUE) {
            return null;
        }
        PlanIter iter = null;

        PlanIterKind kind = PlanIterKind.valueOf(iterType);

        switch (kind) {
        case CONST:
            iter = new ConstIter(in, serialVersion);
            break;
        case VAR_REF:
            iter = new VarRefIter(in, serialVersion);
            break;
        case EXTERNAL_VAR_REF:
            iter = new ExternalVarRefIter(in, serialVersion);
            break;
        case ARRAY_CONSTRUCTOR:
            iter = new ArrayConstrIter(in, serialVersion);
            break;
        case MAP_CONSTRUCTOR:
            iter = new MapConstrIter(in, serialVersion);
            break;
        case RECORD_CONSTRUCTOR:
            iter = new RecConstrIter(in, serialVersion);
            break;
        case TABLE:
            iter = new BaseTableIter(in, serialVersion);
            break;
        case VALUE_COMPARE:
            iter = new CompOpIter(in, serialVersion);
            break;
        case ANY_COMPARE:
            iter = new AnyOpIter(in, serialVersion);
            break;
        case AND_OR:
            iter = new AndOrIter(in, serialVersion);
            break;
        case PROMOTE:
            iter = new PromoteIter(in, serialVersion);
            break;
        case IS_OF_TYPE:
            iter = new IsOfTypeIter(in, serialVersion);
            break;
        case CAST:
            iter = new CastIter(in, serialVersion);
            break;
        case FIELD_STEP:
            iter = new FieldStepIter(in, serialVersion);
            break;
        case MAP_FILTER:
            iter = new MapFilterIter(in, serialVersion);
            break;
        case ARRAY_SLICE:
            iter = new ArraySliceIter(in, serialVersion);
            break;
        case ARRAY_FILTER:
            iter = new ArrayFilterIter(in, serialVersion);
            break;
        case SELECT:
            iter = new SFWIter(in, serialVersion);
            break;
        case FUNC_SIZE:
            iter = new FuncSizeIter(in, serialVersion);
            break;
        case ARITHMETIC:
            iter = new ArithOpIter(in, serialVersion);
            break;
        case ARITHMETIC_NEGATION:
            iter = new ArithUnaryOpIter(in, serialVersion);
            break;
        case SEQ_CONCAT:
            iter = new ConcatIter(in, serialVersion);
            break;
        case RECEIVE:
            iter = new ReceiveIter(in, serialVersion);
            break;
        case CASE:
            iter = new CaseIter(in, serialVersion);
            break;
        case EXISTS:
            iter = new ExistsIter(in, serialVersion);
            break;
        case NOT:
            iter = new NotIter(in, serialVersion);
            break;
        case IS_NULL:
            iter = new IsNullIter(in, serialVersion);
            break;
        case FUNC_EXTRACT_FROM_TIMESTAMP:
            iter = new FuncExtractFromTimestampIter(in, serialVersion);
            break;
        case UPDATE_FIELD:
            iter = new UpdateFieldIter(in, serialVersion);
            break;
        case UPDATE_ROW:
            iter = new UpdateRowIter(in, serialVersion);
            break;
        case INSERT_ROW:
            iter = new InsertRowIter(in, serialVersion);
            break;
        case DELETE_ROW:
            iter = new DeleteRowIter(in, serialVersion);
            break;
        case FUNC_PARSE_JSON:
            iter = new FuncParseJsonIter(in, serialVersion);
            break;
        case FUNC_EXPIRATION_TIME:
            iter = new FuncExpirationTimeIter(in, serialVersion);
            break;
        case FUNC_EXPIRATION_TIME_MILLIS:
            iter = new FuncExpirationTimeMillisIter(in, serialVersion);
            break;
        case FUNC_CURRENT_TIME_MILLIS:
            iter = new FuncCurrentTimeMillisIter(in, serialVersion);
            break;
        case FUNC_CURRENT_TIME:
            iter = new FuncCurrentTimeIter(in, serialVersion);
            break;
        case FUNC_REMAINING_HOURS:
            iter = new FuncRemainingHoursIter(in, serialVersion);
            break;
        case FUNC_REMAINING_DAYS:
            iter = new FuncRemainingDaysIter(in, serialVersion);
            break;
        case FUNC_VERSION:
            iter = new FuncVersionIter(in, serialVersion);
            break;
        case FUNC_COUNT_STAR:
            iter = new FuncCountStarIter(in, serialVersion);
            break;
        case FUNC_COUNT:
            iter = new FuncCountIter(in, serialVersion);
            break;
        case FUNC_SUM:
            iter = new FuncSumIter(in, serialVersion);
            break;
        case FUNC_MIN_MAX:
            iter = new FuncMinMaxIter(in, serialVersion);
            break;
        case SEQ_MAP:
            iter = new SeqMapIter(in, serialVersion);
            break;
        case GEO_SEARCH:
            iter = new FuncGeoSearchIter(in, serialVersion);
            break;
        case GEO_DISTANCE:
            iter = new FuncGeoDistanceIter(in, serialVersion);
            break;
        case GEO_IS_GEOMETRY:
            iter = new FuncGeoIsGeometryIter(in, serialVersion);
            break;
        case SEQ_AGGR:
            iter = new FuncSeqAggrIter(in, serialVersion);
            break;
        case PARTITION_UNION:
            iter = new PartitionUnionIter(in, serialVersion);
            break;
        case FUNC_REGEX_LIKE:
            iter = new FuncRegexLikeIter(in, serialVersion);
            break;
        case STRING_CONCAT:
            iter = new ConcatenateStringsOpIter(in, serialVersion);
            break;
        case FUNC_SUBSTRING:
            iter = new FuncSubstringIter(in, serialVersion);
            break;
        case FUNC_UPPER_LOWER:
            iter = new FuncUpperLowerIter(in, serialVersion);
            break;
        case FUNC_TRIM:
            iter = new FuncTrimIter(in, serialVersion);
            break;
        case FUNC_LRTRIM:
            iter = new FuncLRTrimIter(in, serialVersion);
            break;
        case FUNC_LENGTH:
            iter = new FuncLengthIter(in, serialVersion);
            break;
        case FUNC_CONTAINS_STARTS_ENDS_WITH:
            iter = new FuncContainsStartsEndsWithIter(in, serialVersion);
            break;
        case FUNC_INDEX_OF:
            iter = new FuncIndexOfIter(in, serialVersion);
            break;
        case FUNC_REPLACE:
            iter = new FuncReplaceIter(in, serialVersion);
            break;
        case FUNC_REVERSE:
            iter = new FuncReverseIter(in, serialVersion);
            break;
        case IN:
            iter = new InOpIter(in, serialVersion);
            break;
        case GROUP:
            iter = new GroupIter(in, serialVersion);
            break;
        case FUNC_MOD_TIME:
            iter = new FuncModTimeIter(in, serialVersion);
            break;
        case FUNC_PARTITION:
            iter = new FuncPartitionIter(in, serialVersion);
            break;
        case FUNC_SHARD:
            iter = new FuncShardIter(in, serialVersion);
            break;
        case FUNC_ROW_STORAGE_SIZE:
            iter = new FuncRowStorageSizeIter(in, serialVersion);
            break;
        case FUNC_INDEX_STORAGE_SIZE:
            iter = new FuncIndexStorageSizeIter(in, serialVersion);
            break;
        case FUNC_MKINDEX_STORAGE_SIZE:
            iter = new FuncMKIndexStorageSizeIter(in, serialVersion);
            break;
        case FUNC_UUID:
            iter = new FuncUUIDIter(in, serialVersion);
            break;
        case FUNC_SEQ_DISTINCT:
            iter = new FuncSeqDistinctIter(in, serialVersion);
            break;
        case FUNC_TIMESTAMP_ADD:
            iter = new FuncTimestampAddIter(in, serialVersion);
            break;
        case FUNC_TIMESTAMP_DIFF:
            iter = new FuncTimestampDiffIter(in, serialVersion);
            break;
        case FUNC_GET_DURATION:
            iter = new FuncGetDurationIter(in, serialVersion);
            break;
        case FUNC_COLLECT:
            iter = new FuncCollectIter(in, serialVersion);
            break;
        case FUNC_SEQ_SORT:
            iter = new FuncSeqSortIter(in, serialVersion);
            break;
        case FUNC_MATH:
            iter = new MathIter(in, serialVersion);
            break;
        case FUNC_TIMESTAMP_ROUND:
            iter = new FuncTimestampRoundIter(in, serialVersion);
            break;
        case FUNC_FORMAT_TIMESTAMP:
            iter = new FuncFormatTimestampIter(in, serialVersion);
            break;
        case FUNC_PARSE_TO_TIMESTAMP:
            iter = new FuncParseToTimestampIter(in, serialVersion);
            break;
        case FUNC_LAST_DAY_OF_MONTH:
            iter = new FuncToLastDayOfMonthIter(in, serialVersion);
            break;
        case FUNC_TIMESTAMP_BUCKET:
            iter = new FuncTimestampBucketIter(in, serialVersion);
            break;
        case NESTED_LOOP_JOIN:
            iter = new NestedLoopJoinIter(in, serialVersion);
            break;
        default:
            throw new IllegalArgumentException(
                "Unknown query iterator kind: " + kind +
                ". This may be an attempt to use a query feature not " +
                "yet supported in this server");
        }

        return iter;
    }

    @SuppressWarnings("unused")
    public static void serializeByteArray(
        byte[] array,
        DataOutput out,
        short serialVersion) throws IOException {

        if (array != null && array.length == 0) {
            array = null;
        }
        writeByteArray(out, array);
    }

    @SuppressWarnings("unused")
    public static byte[] deserializeByteArray(
       DataInput in,
       short serialVersion) throws IOException {

        return readByteArray(in);
    }

    public static void serializeBooleanArray(
        boolean[] array,
        DataOutput out) throws IOException {

        if (array != null && array.length == 0) {
            array = null;
        }
        writeArrayLength(out, array);

        if (array != null) {
            for (boolean b : array) {
                out.writeBoolean(b);
            }
        }
    }

    public static void serializeBooleanArray2(
        boolean[] array,
        DataOutput out) throws IOException {

        writeArrayLength(out, array);
        if (array != null) {
            for (boolean b : array) {
                out.writeBoolean(b);
            }
        }
    }

    public static boolean[] deserializeBooleanArray(DataInput in)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len == -1) {
            return null;
        }

        final boolean[] array = new boolean[len];
        for (int i = 0; i < len; i++) {
            array[i] = in.readBoolean();
        }
        return array;
    }

    @SuppressWarnings("unused")
    public static void serializeIntArray(
        int[] array,
        DataOutput out,
        short serialVersion) throws IOException {

        if (array != null && array.length == 0) {
            array = null;
        }
        writeArrayLength(out, array);

        if (array != null) {
            for (int element : array) {
                out.writeInt(element);
            }
        }
    }

    @SuppressWarnings("unused")
    public static int[] deserializeIntArray(
        DataInput in,
        short serialVersion) throws IOException {

        int len = readSequenceLength(in);
        if (len == -1) {
            return null;
        }

        int[] intArray = new int[len];
        for (int i = 0; i < len; i++) {
            intArray[i] = in.readInt();
        }
        return intArray;
    }

    @SuppressWarnings("unused")
    public static void serializeLongArray(
        long[] array,
        DataOutput out,
        short serialVersion) throws IOException {

        if (array != null && array.length == 0) {
            array = null;
        }
        writeArrayLength(out, array);

        if (array != null) {
            for (long element : array) {
                writePackedLong(out, element);
            }
        }
    }

    @SuppressWarnings("unused")
    public static long[] deserializeLongArray(
        DataInput in,
        short serialVersion) throws IOException {

        int len = readSequenceLength(in);
        if (len == -1) {
            return null;
        }

        long[] longArray = new long[len];
        for (int i = 0; i < len; i++) {
            longArray[i] = readPackedLong(in);
        }
        return longArray;
    }

    static void serializeStringArray(
        String[] array,
        DataOutput out,
        short serialVersion) throws IOException {

        if (array != null && array.length == 0) {
            array = null;
        }
        writeArrayLength(out, array);
        if (array != null) {
            for (final String s : array) {
                writeNonNullString(out, serialVersion, s);
            }
        }
    }

    static void serializeStringArray2(
        String[] array,
        DataOutput out,
        short serialVersion) throws IOException {

        writeArrayLength(out, array);
        if (array != null) {
            for (final String s : array) {
                writeNonNullString(out, serialVersion, s);
            }
        }
    }

    static String[] deserializeStringArray(DataInput in, short serialVersion)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len == -1) {
            return null;
        }
        final String[] array = new String[len];
        for (int i = 0; i < len; i++) {
            array[i] = readNonNullString(in, serialVersion);
        }
        return array;
    }

    static void serializeFieldRange(
        FieldRange range,
        DataOutput out,
        short serialVersion) throws IOException {

        if (range != null) {
            out.writeBoolean(true);
            range.writeFastExternal(out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    static FieldRange deserializeFieldRange(
        DataInput in,
        short serialVersion) throws IOException {

        boolean hasRange = in.readBoolean();
        if (hasRange) {
            return new FieldRange(in, serialVersion);
        }
        return null;
    }

    public static void serializeFieldDef(
        FieldDef def,
        DataOutput out,
        short serialVersion) throws IOException {

        FieldDefSerialization.writeFieldDef(def, out, serialVersion);
    }

    public static FieldDef deserializeFieldDef(
        DataInput in,
        short serialVersion) throws IOException {

        return FieldDefSerialization.readFieldDef(in, serialVersion);
    }

    @SuppressWarnings("unused")
    public static void serializeQuantifier(
        ExprType.Quantifier quantifier,
        DataOutput out,
        short serialVersion) throws IOException {

        short code;
        switch (quantifier) {
        case ONE:
            code = 1;
            break;
        case QSTN:
            code = 2;
            break;
        case STAR:
            code = 3;
            break;
        case PLUS:
            code = 4;
            break;
        default:
            throw new IOException("Unknown quantifier: " + quantifier.name());
        }
        out.writeShort(code);
    }

    @SuppressWarnings("unused")
    public static ExprType.Quantifier deserializeQuantifier(
        DataInput in,
        short serialVersion) throws IOException {

        short code = in.readShort();
        switch (code) {
        case 1:
            return ExprType.Quantifier.ONE;
        case 2:
            return ExprType.Quantifier.QSTN;
        case 3:
            return ExprType.Quantifier.STAR;
        case 4:
            return ExprType.Quantifier.PLUS;
        default:
            throw new IOException("Unknown quantifier code: " + code);
        }
    }

    public static void serializeFieldValues(
        FieldValueImpl[] values,
        DataOutput out,
        short serialVersion) throws IOException {

        if (values != null && values.length == 0) {
            values = null;
        }

        writeArrayLength(out, values);

        if (values != null) {
            for (final FieldValueImpl v : values) {
                serializeFieldValue(v, out, serialVersion);
            }
        }
    }

    public static FieldValueImpl[] deserializeFieldValues(
        DataInput in,
        short serialVersion)
        throws IOException {

        final int len = readSequenceLength(in);
        if (len == -1) {
            return null;
        }

        final FieldValueImpl[] values = new FieldValueImpl[len];
        for (int i = 0; i < len; i++) {
            values[i] = deserializeFieldValue(in, serialVersion);
        }

        return values;
    }

    static void serializeFieldValue(
        FieldValue value,
        DataOutput out,
        short serialVersion) throws IOException {

        FieldValueSerialization.writeFieldValue(value,
                                                true, // writeValDef
                                                out,
                                                serialVersion);
    }

    static FieldValueImpl deserializeFieldValue(
        DataInput in,
        short serialVersion) throws IOException {

        return (FieldValueImpl)
            FieldValueSerialization.readFieldValue(null, // def
                                                   in, serialVersion);
    }

    static void serializeKey(
        RecordValueImpl value,
        DataOutput out,
        short serialVersion) throws IOException {

        FieldValueSerialization.writeRecord(value,
                                            true, // writeValDef
                                            true, // partial
                                            out,
                                            serialVersion);
    }

    static RecordValueImpl deserializeKey(
        DataInput in,
        short serialVersion) throws IOException {

        return FieldValueSerialization.readRecord(null, // def
                                                  true, // partial
                                                  in,
                                                  serialVersion);
    }

    /* public for use by PreparedStatementImpl */
    public static void serializeExprType(
        ExprType type,
        DataOutput out,
        short serialVersion) throws IOException {

        if (type != null) {
            out.writeBoolean(true);
            TypeManager.serializeExprType(type, out, serialVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Read the type and quantifier and builtin boolean. If this is a builtin
     * type, get the (static) type directly from TypeManager.
     *
     * public for use by PreparedStatementImpl
     */
    public static ExprType deserializeExprType(DataInput in,
                                               short serialVersion)
            throws IOException {

        boolean hasType = in.readBoolean();
        if (hasType) {
            return TypeManager.deserializeExprType(in, serialVersion);
        }
        return null;
    }

    static void serializeSortSpecs(
        SortSpec[] specs,
        DataOutput out,
        short serialVersion) throws IOException {

        if ((specs != null) && (specs.length == 0)) {
            specs = null;
        }
        writeArrayLength(out, specs);
        if (specs == null) {
            return;
        }

        for (final SortSpec spec : specs) {
            spec.writeFastExternal(out, serialVersion);
        }
    }

    static SortSpec[] deserializeSortSpecs(DataInput in, short serialVersion)
            throws IOException {

        final int num = readSequenceLength(in);
        if (num == -1) {
            return null;
        }

        final SortSpec[] specs = new SortSpec[num];

        for (int i = 0; i < num; ++i) {
            specs[i] = new SortSpec(in, serialVersion);
        }

        return specs;
    }

    public static String printKey(byte[] keyBytes) {

        if (keyBytes == null) {
            return "null";
        }

        Key key = Key.fromByteArray(keyBytes);

        return key.toString();
    }

    public static String printByteArray(byte[] bytes) {

        if (bytes == null) {
            return "null";
        }

        StringBuffer sb = new StringBuffer();

        sb.append("[ ");

        for (byte b : bytes) {
            sb.append(b).append(" ");
        }

        sb.append("]");

        return sb.toString();
    }

    public static String printIntArray(int[] ints) {

        if (ints == null) {
            return "null";
        }

        StringBuffer sb = new StringBuffer();

        sb.append("[ ");

        for (int i =0; i < ints.length; ++i) {
            int v = ints[i];
            sb.append(v);
            if (i < ints.length - 1) {
                sb.append(", ");
            }
        }

        sb.append(" ]");

        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PlanIter)) {
            return false;
        }
        final PlanIter other = (PlanIter) obj;
        return (theResultReg == other.theResultReg) &&
            (theStatePos == other.theStatePos) &&
            Objects.equals(theLocation, other.theLocation) &&
            (theIsCloudDriverIter == other.theIsCloudDriverIter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(theResultReg,
                            theStatePos,
                            theLocation,
                            theIsCloudDriverIter);
    }
}
