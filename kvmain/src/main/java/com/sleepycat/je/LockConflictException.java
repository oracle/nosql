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

/**
 * The common base class for all exceptions that result from record lock
 * conflicts during read and write operations.
 *
 * <p>This exception normally indicates that a transaction may be retried.
 * Catching this exception, rather than its subclasses, is convenient and
 * recommended for handling lock conflicts and performing transaction retries
 * in a general purpose manner.  See below for information on performing
 * transaction retries.</p>
 *
 * <p>The exception carries two arrays of transaction ids, one of the owners and
 * the other of the waiters, at the time of the lock conflict.  This
 * information may be used along with the {@link Transaction#getId Transaction
 * ID} for diagnosing locking problems. See {@link #getOwnerTxnIds} and {@link
 * #getWaiterTxnIds}.</p>
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * <h2><a id="retries">Performing Transaction Retries</a></h2>
 *
 * <p>If a lock conflict occurs during a transaction, the transaction may be
 * retried by performing the following steps.  Some applications may also wish
 * to sleep for a short interval before retrying, to give other concurrent
 * transactions a chance to finish and release their locks.</p>
 * <ol>
 * <li>Close all cursors opened under the transaction.</li>
 * <li>Abort the transaction.</li>
 * <li>Begin a new transaction and repeat the operations.</li>
 * </ol>
 *
 * <p>To handle {@link LockConflictException} reliably for all types of JE
 * applications including JE-HA applications, it is important to handle it when
 * it is thrown by all {@link Database} and {@link Cursor} read and write
 * operations.</p>
 *
 * <p>The following example code illustrates the recommended approach.  Note
 * that the {@code Environment.beginTransaction} and {@code Transaction.commit}
 * calls are intentially inside the {@code try} block.  When using JE-HA, this
 * will make it easy to add a {@code catch} for other exceptions that can be
 * resolved by retrying the transaction, such as consistency exceptions.</p>
 *
 * <pre class="code">
 *  void doTransaction(final Environment env,
 *                     final Database db1,
 *                     final Database db2,
 *                     final int maxTries)
 *      throws DatabaseException {
 *
 *      boolean success = false;
 *      long sleepMillis = 0;
 *      for (int i = 0; i &lt; maxTries; i++) {
 *          // Sleep before retrying.
 *          if (sleepMillis != 0) {
 *              Thread.sleep(sleepMillis);
 *              sleepMillis = 0;
 *          }
 *          Transaction txn = null;
 *          try {
 *              txn = env.beginTransaction(null, null);
 *              final Cursor cursor1 = db1.openCursor(txn, null);
 *              try {
 *                  final Cursor cursor2 = db2.openCursor(txn, null);
 *                  try {
 *                      // INSERT APP-SPECIFIC CODE HERE:
 *                      // Perform read and write operations.
 *                  } finally {
 *                      cursor2.close();
 *                  }
 *              } finally {
 *                  cursor1.close();
 *              }
 *              txn.commit();
 *              success = true;
 *              return;
 *          } catch (LockConflictException e) {
 *              sleepMillis = LOCK_CONFLICT_RETRY_SEC * 1000;
 *              continue;
 *          } finally {
 *              if (!success) {
 *                  if (txn != null) {
 *                      txn.abort();
 *                  }
 *              }
 *          }
 *      }
 *      // INSERT APP-SPECIFIC CODE HERE:
 *      // Transaction failed, despite retries.
 *      // Take some app-specific course of action.
 *  }</pre>
 *
 * <p>For more information on transactions and lock conflicts, see <a
 * href="{@docRoot}/../TransactionGettingStarted/index.html"
 * target="_top">Writing Transactional Applications</a>.</p>
 *
 * @since 4.0
 */
public abstract class LockConflictException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    private long[] ownerTxnIds;
    private long[] waiterTxnIds;
    private long timeoutMillis;

    /** 
     * For internal use only.
     * @hidden 
     */
    LockConflictException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    protected LockConflictException(Locker locker, String message) {
        super(locker, true /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    protected LockConflictException(Locker locker,
                                    String message,
                                    Throwable cause) {
        super(locker, true /*abortOnly*/, message, cause);
    }

    /** 
     * For internal use only.
     * @hidden
     * Only for use by wrapSelf methods.
     */
    protected LockConflictException(String message,
                                    LockConflictException cause) {
        super(message, cause);
        ownerTxnIds = cause.ownerTxnIds;
        waiterTxnIds = cause.waiterTxnIds;
        timeoutMillis = cause.timeoutMillis;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setOwnerTxnIds(long[] ownerTxnIds) {
        this.ownerTxnIds = ownerTxnIds;
    }

    /**
     * Returns an array of longs containing transaction ids of owners at the
     * the time of the timeout.
     *
     * @return an array of longs containing transaction ids of owners at the
     * the time of the timeout.
     */
    public long[] getOwnerTxnIds() {
        return ownerTxnIds;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setWaiterTxnIds(long[] waiterTxnIds) {
        this.waiterTxnIds = waiterTxnIds;
    }

    /**
     * Returns an array of longs containing transaction ids of waiters at the
     * the time of the timeout.
     *
     * @return an array of longs containing transaction ids of waiters at the
     * the time of the timeout.
     */
    public long[] getWaiterTxnIds() {
        return waiterTxnIds;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * @hidden
     * Internal use only.
     */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }
}
