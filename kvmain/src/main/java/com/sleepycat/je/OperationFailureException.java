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

import com.sleepycat.je.txn.Locker;
import com.sleepycat.utilint.FormatUtil;

/**
 * Indicates that a failure has occurred that impacts the current operation
 * and/or transaction.  For failures that impact the environment as a whole,
 * see {@link EnvironmentFailureException}.  For an overview of all exceptions
 * thrown by JE, see {@link DatabaseException}.
 *
 * <p>If an explicit transaction applies to a method which threw this
 * exception, the exception may indicate that {@link Transaction#abort} must be
 * called, depending on the nature of the failure.  A transaction is applicable
 * to a method call in two cases.</p>
 * <ol>
 * <li>When an explicit (non-null) {@code Transaction} instance is specified.
 * This applies when the {@code Transaction} is passed as a parameter to the
 * method that throws the exception, or when the {@code Transaction} is passed
 * to {@link Database#openCursor} and a {@code Cursor} method throws the
 * exception.
 * </li>
 * <li>When a per-thread {@code Transaction} applies to the method that throws
 * the exception.  Per-thread transactions apply when using {@link
 * com.sleepycat.collections persistent collections} with {@link
 * com.sleepycat.collections.CurrentTransaction}
 * </li>
 * </ol>
 *
 * <p>When a transaction is applicable to a method call, the application should
 * catch {@code OperationFailureException} and then call {@link
 * Transaction#isValid}.  If {@code false} is returned, all {@code Cursor}
 * instances that were created with the transaction must be closed and then
 * {@link Transaction#abort} must be called.  Also note that {@link
 * Transaction#isValid} may be called at any time, not just during exception
 * handling.</p>
 *
 * <p>The use of the {@link Transaction#isValid} method allows JE to determine
 * dynamically whether the failure requires an abort or not, and allows for
 * this determination to change in future releases. Over time, internal
 * improvements to error handling may allow more error conditions to be handled
 * without invalidating the {@code Transaction}.</p>
 *
 * <p>The specific handling that is necessary for an {@code
 * OperationFailureException} depends on the specific subclass thrown.  See the
 * javadoc for each method for information on which methods throw which {@code
 * OperationFailureException}s and why.</p>
 *
 * <p>If {@link Transaction#abort} is not called after an {@code
 * OperationFailureException} invalidates the {@code Transaction}, all
 * subsequent method calls using the {@code Transaction} will throw the same
 * exception.  This provides more than one opportunity to catch and handle the
 * specific exception subclass that caused the failure.</p>
 *
 * <p>{@code OperationFailureException} is also thrown by methods where no
 * transaction applies. In most cases the action required to handle the
 * exception is the same as with a transaction, although of course no abort is
 * necessary.</p>
 *
 * <p>However, please be aware that for some operations on a non-transactional
 * {@code Database} or {@code EntityStore}, an {@code
 * OperationFailureException} may cause data corruption.  For example, see
 * {@link SecondaryReferenceException}.</p>
 *
 * <p>There are two groups of operation failure subclasses worth noting since
 * they apply to many methods: read operation failures and write operation
 * failures.  These are described below.</p>
 *
 * <h2><a id="readFailures">Read Operation Failures</a></h2>
 *
 * <p>Read operations are all those performed by the {@code get} family of
 * methods, for example, {@link Database#get Database.get}, {@link
 * Cursor#getNext Cursor.getNext}, {@link
 * com.sleepycat.collections.StoredMap#get StoredMap.get}, and {@link
 * ForwardCursor#getNext ForwardCursor.getNext}. These methods may cause the
 * following operation failures.</p>
 *
 * <ul>
 * <li>{@link OperationFailureException} is the superclass of all read
 * operation failures.
 *   <ul>
 *   <li>{@link LockConflictException} is thrown if a lock conflict prevents
 *   the operation from completing.  A read operation may be blocked by another
 *   locker (transaction or non-transactional cursor) that holds a write lock
 *   on the record.
 *
 *     <ul>
 *     <li>{@link com.sleepycat.je.rep.LockPreemptedException} is a subclass
 *     of {@code LockConflictException} that is thrown in a replicated
 *     environment on the Replica node, when the Master node has changed a
 *     record that was previously locked by the reading transaction or
 *     cursor.</li>
 *     </ul>
 *   </li>
 *
 *   <li>{@link SecondaryIntegrityException} is thrown if a primary-secondary
 *   relationship integrity problem is detected while reading a primary
 *   database record via a secondary index.</li>
 *
 *   <li>{@link com.sleepycat.je.rep.DatabasePreemptedException} is thrown in a
 *   replicated environment on the Replica node, when the Master node has
 *   truncated, removed or renamed the database.</li>
 *
 *   <li>Other {@link OperationFailureException} subclasses may be thrown if
 *   such an exception was thrown earlier and caused the transaction to be
 *   invalidated.</li>
 *   </ul>
 * </li>
 * </ul>
 *
 * <h2><a id="writeFailures">Write Operation Failures</a></h2>
 *
 * <p>Write operations are all those performed by the {@code put} and {@code
 * delete} families of methods, for example, {@link Database#put Database.put},
 * {@link Cursor#delete Cursor.delete}, {@link
 * com.sleepycat.collections.StoredMap#put StoredMap.put}.  These methods may
 * cause the following operation failures, although certain failures are only
 * caused by {@code put} methods and others only by {@code delete} methods, as
 * noted below.</p>
 *
 * <ul>
 * <li>{@link OperationFailureException} is the superclass of all write
 * operation failures.
 *
 *   <ul>
 *   <li>{@link LockConflictException} is thrown if a lock conflict prevents
 *   the operation from completing.  A write operation may be blocked by
 *   another locker (transaction or non-transactional cursor) that holds a read
 *   or write lock on the record.</li>
 *
 *   <li>{@link DiskLimitException} is thrown if a disk limit has been
 *   violated and this prevents the operation from completing.
 *
 *   <li>{@link SecondaryConstraintException} is the superclass of all
 *   exceptions thrown when a write operation fails because of a secondary
 *   constraint.
 *
 *     <ul>
 *     <li>{@link ForeignConstraintException} is thrown when an attempt to
 *     write a primary database record would insert a secondary record with a
 *     key that does not exist in a foreign key database, when the secondary
 *     key is configured as a foreign key.  This exception is only thrown by
 *     {@code put} methods.</li>
 *
 *     <li>{@link UniqueConstraintException} is thrown when an attempt to write
 *     a primary database record would insert a secondary record with a
 *     duplicate key, for secondaries that represent one-to-one and one-to-many
 *     relationships.  This exception is only thrown by {@code put}
 *     methods.</li>
 *
 *     <li>{@link DeleteConstraintException} is thrown when an attempt is made
 *     to delete a key from a foreign key database, when that key is referenced
 *     by a secondary database, and the secondary is configured to cause an
 *     abort in this situation.  This exception is only thrown by {@code
 *     delete} methods.</li>
 *     </ul>
 *   </li>
 *
 *   <li>{@link SecondaryIntegrityException} is thrown if a primary-secondary
 *   relationship integrity problem is detected while writing a record in a
 *   primary database that has one or more secondary indices.
 *
 *   <li>{@link com.sleepycat.je.rep.DatabasePreemptedException} is thrown in a
 *   replicated environment on a Replica node, when the Master node has
 *   truncated, removed or renamed the database.</li>
 *
 *   <li>{@link com.sleepycat.je.rep.ReplicaWriteException} is always thrown in
 *   a replicated environment on a Replica node, since write operations are not
 *   allowed on a Replica.</li>
 *
 *   <li>Other {@link OperationFailureException} subclasses may be thrown if
 *   such an exception was thrown earlier and caused the transaction to be
 *   invalidated.</li>
 *   </ul>
 * </li>
 * </ul>
 *
 * @since 4.0
 */
public abstract class OperationFailureException extends DatabaseException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public OperationFailureException(Locker locker,
                                     boolean abortOnly,
                                     String message,
                                     Throwable cause) {
        super(message, cause);
        if (abortOnly) {
            assert locker != null;
            locker.setOnlyAbortable(this);
        }
    }

    /**
     * For internal use only.
     * @hidden
     * Only for use by bind/collection/persist exception subclasses.
     */
    public OperationFailureException(String message) {
        this(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     * Only for use by wrapSelf methods.
     */
    protected OperationFailureException(String message,
                                        OperationFailureException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     * Use to re-throw an exception that originally caused a Transaction or
     * Database to be invalidated, wrapped in a new exception with the same
     * class and properties.
     *
     * <p>Providing both stack traces is important because the new exception
     * may thrown in a different context than the original exception. The same
     * class and properties are used so that when the new exception is caught,
     * it can be handled in the same way as the original exception.</p>
     *
     * <p>Returns a new exception that has the same class and properties as
     * this exception, other than the message (the given param should be
     * used), the stack trace (should be the current stack trace), and the
     * cause exception (should be null).</p>*
     *
     * <p>The caller must clone the original exception and pass it as the
     * clonedCause exception. Cloning is necessary to avoid a CIRCULAR
     * REFERENCE when a saved cause exception appears in the cause
     * chain for multiple thrown exceptions (see SavedExceptionTest).
     * {@link FormatUtil#cloneBySerialization} is used as a practical way to
     * clone all cause and suppressed exceptions.</p>
     *
     * <p>This method must be overridden by every concrete subclass to return
     * a new instance of the same class.</p>
     * <ul>
     *     <li>The given msg must be used and all other properties must
     *     be copied.</li>
     *
     *     <li>The Transaction or Database must not be invalidated by the
     *     ctor, since this will have already been done (if appropriate) by
     *     the original exception's ctor.</li>
     * </ul>
     *
     * <p>A simple implementation can use a ctor with message and cause params
     * that invokes super(msg, cause) and eventually invokes the (msg, cause)
     * ctor in this class. The ctors should to copy the non-msg properties.
     * All such ctors should only be used when invalidation of the Transaction
     * or Database is unnecessary and should have the minimum possible
     * visibility, preferably private.</p>
     */
    public abstract OperationFailureException wrapSelf(
        String msg,
        OperationFailureException clonedCause);

    /**
     * For internal use only.
     * @hidden
     * Allows standalone JE code to check for ReplicaWriteException.
     */
    public boolean isReplicaWrite() {
        return false;
    }
}
