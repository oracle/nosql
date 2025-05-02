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

package oracle.kv.impl.client.admin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVVersion;
import oracle.kv.StatementResult;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.SerialVersion;

/**
 * DdlFutures are returned by TableAPI.execute(), to provide a handle to a
 * asynchronous DDL operation.
 *
 * A DdlFuture is thread safe.  Multiple threads may safely call get, cancel,
 * etc on a single instance of DdlFuture. Alternatively, the application may
 * call TableAPI.execute() multiple times on the same statement, obtaining
 * multiple DdlFuture instances. For example:
 *
 * {@literal
 *    thread 1 --+
 *                +---> DdlFutureX --------> polling task (DdlCheckTask)
 *    thread 2 --+
 *
 * or
 *
 *    thread 1 --> DdlFutureX--+
 *                             +-----------> polling task (DdlCheckTask)
 *    thread 2 --> DdlFutureY--+
 * }
 *
 * Checking for operation completion:
 * ---------------------------------
 * The DdlFuture can instigate polling for operation completion.  Polling is
 * started lazily; it's initiated by a call to get() or get(time, unit). At
 * that point the DdlFuture calls DdlStatementExecutor to instigate polling,
 * and to register itself as an interested party.
 *
 * DdlFutures may also ask for current status. In that case, each
 * DdlFuture would make its own direct call to the AdminService
 *
 * Synchronization hierarchy:
 * --------------------------
 * The order of locking is DdlStatementExecutor, followed by DdlFuture.
 * Any DdlFuture synchronization must be sure not to call a synchronized
 * DdlStatementExecutor method.
 *
 * Serialization
 * -------------
 * The DdlFuture can be serialized by the application, and saved for future
 * use. This is particularly useful for stateless clients which wish to
 * persist their ExecutionFuture handles.
 */
public class DdlFuture implements oracle.kv.ExecutionFuture {

    /*
     * A version value for serialization. See toByteArray() for a description
     * of the format.
     */
    private static final short CURRENT_VERSION = 1;

    /* The charset used for en/decoding strings and char arrays */
    static Charset UTF8 = Charset.forName("UTF-8");

    /* The statement issued by the user */
    private final char[] statement;

    /* true if Cancel() was called */
    private volatile boolean cancelRequested;

    /*
     * isCancelled applies to the semantics of Future.isCancelled. It is
     * supposed to be true if the app explicitly requested a cancel, and the
     * cancel has completed. This helps to distinguish between these semantics,
     * and the fact that plan cancellation can happen on the server side in
     * order to terminate a plan that has incurred an error.
     *
     * If the plan was in error, and has been canceled on the server side to
     * terminate it, isCancelled will be false.
     */
    private volatile boolean isCancelled;

    /* true if the operation has ended */
    private volatile boolean isDone;

    private final DdlStatementExecutor statementExec;
    private final CountDownLatch countdown;
    private final int planId;

    /*
     * Most recent plan status. Initialized at first with the planInfo returned
     * by the initial request to AdminStatementService.execute(), and updated
     * every time a new status and planInfo is obtained by the polling task or
     * by a call to getStatus(). May be null in several cases:
     *   - there has not yet been any contact with the kvstore, which happens
     * if the future was created by TableAPI.getFuture(), and get() and
     * getStatus haven't yet been called.
     *   - the DDLFuture was created from a serialized version of the future,
     * as a byte array, and the plan id is 0.
     */
    private volatile ExecutionInfo lastExecInfo;

    /*
     * Hang onto the first exception seen by a status call. We need the
     * exception type so we can know whether to wrap the problem as a
     * ExecutionException, or to directly throw a KVSecurityException.
     */
    private volatile Throwable firstProblem;

    /*
     * This field contains kvstore authentication context supplied by user.
     * By default the authentication context is obtained from kvstore
     * handle automatically, specifying this field would mean that user want to
     * supply its own authentication context to make the call other than the
     * default authentication context from kvstore handle.
     */
    private final AuthContext authContext;

    /**
     * Create a DdlFuture when there's only a planId available, from
     * TableAPIImpl.getFuture();
     * This sort of DdlFuture is only used to check plan status
     */
    public DdlFuture(int planId,
                     DdlStatementExecutor statementExecutor) {
        this(planId, statementExecutor, null);
    }

    /*
     * Internal used by proxy
     * This sort of DdlFuture is only used to check plan status
     */
    public DdlFuture(int planId,
                     DdlStatementExecutor statementExecutor,
                     AuthContext authContext) {

        assert planId > 0;
        /* statement is assumed non-null; it's only used in exceptions */
        this.statement = new char[0];
        this.planId = planId;
        this.statementExec = statementExecutor;
        countdown = new CountDownLatch(1);
        this.authContext = authContext;
    }

    /**
     * Create a DdlFuture from a serialized ExecutionFuture.
     *
     * @throws IllegalArgumentException if the serialized future has a version
     * that we can't handle.
     */
    public DdlFuture(byte[] futureBytes,
                     DdlStatementExecutor statementExecutor) {

        this.statementExec = statementExecutor;

        ByteBuffer buf = ByteBuffer.wrap(futureBytes);
        short version = buf.getShort();

        /*
         * In the future, if we support more versions, do more intelligent
         * checking.
         */
        if (version != 1) {
            throw new IllegalArgumentException
                ("Version " + KVVersion.CURRENT_VERSION +
                 "of the NoSQL DB client cannot accept a serialized " +
                 "ExecutionFuture that is version "+ version);
        }

        /* Note that the planId may be 0 if the statement didn't execute */
        planId = buf.getInt();
        short statementLen = buf.getShort();
        if (statementLen > 0) {
            this.statement = readCharArrayAndReposition(buf, statementLen);
        } else {
            this.statement = new char[0];
        }

        /* PlanId != 0: there's no more information in the serialized result.*/
        if (planId > 0) {
            countdown = new CountDownLatch(1);
            authContext = readAuthContext(buf);
            return;
        }

        /* PlanId == 0: Nothing to wait for, no plan is executing. */
        isDone = true;
        countdown = new CountDownLatch(0);

        int statusLen = buf.getInt();
        String status = readStringAndReposition(buf, statusLen);

        int statusAsJsonLen = buf.getInt();
        String statusAsJson = readStringAndReposition(buf, statusAsJsonLen);

        int resultLen = buf.getInt();
        String result = readStringAndReposition(buf, resultLen);

        this.lastExecInfo = new ExecutionInfoImpl(planId,
                                                  true,    /* isDone */
                                                  status,
                                                  statusAsJson,
                                                  true,   /* isSuccess */
                                                  false,  /* isCancelled */
                                                  null,   /* errMsg */
                                                  false,  /* needsCancel*/
                                                  result);

        authContext = readAuthContext(buf);
    }

    /*
     * Read authentication context from byte buffer
     */
    private AuthContext readAuthContext(ByteBuffer buf) {
        if (buf.hasRemaining()) {
            final int authLen = buf.getInt();
            if (authLen > 0) {
                final byte[] authBytes = Arrays.copyOfRange(
                    buf.array(), buf.position(), buf.position() + authLen);
                try (ByteArrayInputStream bais =
                         new ByteArrayInputStream(authBytes);
                     DataInputStream dis = new DataInputStream(bais)) {
                    buf.position(buf.position() + authLen);
                    return new AuthContext(dis,
                                           SerialVersion.CURRENT);
                } catch (IOException ioe) {
                    throw new IllegalArgumentException(
                        "Fail to convert authContext", ioe);
                }
            }
        }
        return null;
    }

    /**
     * Read a string that is at this ByteBuffer's position, and then reposition
     * the ByteBuffer to leave it ready to read the next field.
     */
    private String readStringAndReposition(ByteBuffer buf, int stringLen) {
        String result = null;
        if (stringLen > 0) {
            result = new String(buf.array(), buf.position(), stringLen, UTF8);
            buf.position(buf.position() + stringLen);
        }
        return result;
    }

    /**
     * Read a char[] that is at this ByteBuffer's position, and then reposition
     * the ByteBuffer to leave it ready to read the next field.
     */
    private char[] readCharArrayAndReposition(ByteBuffer buf, int stringLen) {
        char[] res = null;
        if (stringLen > 0) {
            ByteBuffer buf2 = ByteBuffer.wrap(buf.array(), buf.position(),
                stringLen);
            res = UTF8.decode(buf2).array();
            buf.position(buf.position() + stringLen);
        }
        return res;
    }

    public DdlFuture(char[] statement, ExecutionInfo executionInfo,
                     DdlStatementExecutor statementExecutor) {
        this(statement, executionInfo, statementExecutor, null);
    }

    /*
     * Internal used by proxy
     */
    public DdlFuture(char[] statement, ExecutionInfo executionInfo,
                     DdlStatementExecutor statementExecutor,
                     AuthContext authContext) {

        /*
         * ensure that the statement is non-null if passed as null. The
         * statement is only used in exception messages
         */
        if (statement == null) {
            this.statement = new char[0];
        } else {
            this.statement = statement;
        }
        this.planId = executionInfo.getPlanId();
        this.lastExecInfo = executionInfo;
        this.statementExec = statementExecutor;

        if (planId > 0) {
            countdown = new CountDownLatch(1);
        } else {
            /* Nothing to wait for, no plan is executing. */
            isDone = true;
            countdown = new CountDownLatch(0);
        }

        this.authContext = authContext;

        /*
         * We got some information already about this command. Make note of
         * it -- the command may even have already finished.
         */
        applyNewInfo(executionInfo, null);
    }

    /**
     * Returns false if the operation couldn't be cancelled, true otherwise;
     * The contract for Future.cancel() says that cancel can only execute
     * successfully once. From Future.java:
     *
     * This attempt will fail if the task has already completed, has already
     * been cancelled, or could not be cancelled for some other reason.
     */
    @Override
    synchronized public boolean cancel(boolean mayInterruptIfRunning)
        throws FaultException {

        /*
         * Already completed. This covers all cases where the planId == 0:
         * the futures for those kinds of operations are all marked as done.
         */
        if (isDone) {
            return false;
        }

        /* Already cancelled. */
        if (isCancelled) {
            return false;
        }

        if (!mayInterruptIfRunning) {

            /*
             * By definition, once the table statement is issued and
             * sent to the server, it is running. If the caller specifies
             * that the statement should not be interrupted, we will always
             * return false if the command has not finished.
             */
            return false;
        }

        try {
            cancelRequested = true;
            ExecutionInfo newInfo = cancelAndGetStatus(statementExec,
                                                       planId,
                                                       authContext);
            applyNewInfo(newInfo, null);
            statementExec.updateWaiters(newInfo);
            return isCancelled();
        } catch (AdminFaultException e) {
            throw new FaultException("Problem cancelling "  +
                new String(statement) + ", planId = " + planId + ": " + e,
                false);
        } catch (RemoteException e) {
            throw new FaultException("Communication problem cancelling " +
                                     " for " + new String(statement) + ", " +
                                     "plan " +  planId + ", retry:" + e, false);
        }
    }

    /**
     * Block until the plan has finished.
     * @return the outcome of the plan.
     */
    @Override
    public StatementResult get()
        throws InterruptedException,
               ExecutionException {
        if (isCancelled) {
            throw new CancellationException();
        }

        if (!isDone) {
            statementExec.startPolling(planId, this);
        }
        countdown.await();
        checkForError();
        return getLastStatus();
    }

    /**
     * Block until the plan has finished or the timeout period is exceeded.
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    @Override
    public StatementResult get(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException, ExecutionException {

        if (isCancelled) {
            throw new CancellationException();
        }

        if (!isDone) {
            statementExec.startPolling(planId, this);
        }
        boolean finished = countdown.await(timeout, unit);
        if (!finished) {
            throw new TimeoutException("Statement '" + new String(statement) +
                "' did not finish within " + timeout + " " + unit);
        }

        checkForError();
        return getLastStatus();
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public boolean isDone() {
        updateStatus();
        return isDone;
    }

    /**
     * Reach out to the kvstore server and get current status. Update this
     * future.
     */
    @Override
    public StatementResult updateStatus() {

        if (isDone) {
            return getLastStatus();
        }

        try {
            ClientAdminServiceAPI ca = statementExec.getClientAdminService();
            ExecutionInfo newInfo = ca.getExecutionStatus(planId,
                                                          authContext);
            newInfo = checkForNeedsCancel(newInfo,
                                          statementExec,
                                          planId,
                                          authContext);
            applyNewInfo(newInfo, null);
        } catch (AdminFaultException e) {
            throw new FaultException("Problem updating status for '" +
                new String(statement) + "', planId = " + planId + ": " + e,
                false);
        } catch (RemoteException e) {
            throw new FaultException("Communication problem updating status " +
                " for '" + new String(statement) + "', plan " +
                planId + ", retry:" + e, false);
        }
        return getLastStatus();
     }

    /**
     * Return the last information known about the execution. Does not require
     * communication with the server.
     */
    @Override
    public synchronized StatementResult getLastStatus() {

        /*
         * synchronized so we get a consistent snapshot of lastExecInfo,
         * isDone, isCancelled.
         */
        return new AdminResult(planId, lastExecInfo, isDone, isCancelled);
    }

    /**
     * Return the statement which has been executed.
     */
    @Override
    public String getStatement() {
        return new String(statement);
    }

    /**
     * If the operation failed and it's a security issue, throw a
     * KVSecurityException. Otherwise, throw an ExecutionException which wraps
     * the original exception
     */
    private synchronized void checkForError()
        throws ExecutionException, KVSecurityException {

        if ((firstProblem != null) &&
            (firstProblem instanceof KVSecurityException)) {
            KVSecurityException k = (KVSecurityException) firstProblem;
            throw k;
        }

        /* All other problems are wrapped in ExecutionException */
        if ((lastExecInfo != null) &&
            (lastExecInfo.getErrorMessage() != null)) {
            throw new ExecutionException
            (new FaultException(lastExecInfo.getErrorMessage(), false));
        }
    }

    /**
     * A new status has been obtained, update the future.
     */
    synchronized void applyNewInfo(ExecutionInfo newInfo, Throwable t) {
        /* Already done, nothing to do */
        if (isDone) {
            return;
        }

        /*
         * Update all the state required before releasing latch and
         * letting others see state.
         */
        if (firstProblem == null) {
            /* Save the first problem. Note that t may be null */
            firstProblem = t;
        }

        lastExecInfo = newInfo;
        isDone = newInfo.isTerminated();
        isCancelled = newInfo.isCancelled() && cancelRequested;

        if (newInfo.isTerminated() || t != null) {
            countdown.countDown();
        }
    }

    static ExecutionInfo checkForNeedsCancel(ExecutionInfo info,
                                            DdlStatementExecutor statementExec,
                                            int planId,
                                            AuthContext authCtx)
        throws RemoteException {
        if (info.needsTermination()) {
            return cancelAndGetStatus(statementExec, planId, authCtx);
        }

        return info;
    }

    static ExecutionInfo cancelAndGetStatus(DdlStatementExecutor statementExec,
                                            int planId,
                                            AuthContext authCtx)
        throws RemoteException {
        ClientAdminServiceAPI ca = statementExec.getClientAdminService();
        return ca.interruptAndCancel(planId, authCtx);
    }

    /**
     * Return a serialized version of the ExecutionFuture.
     */
    @Override
    public byte[] toByteArray() {
        String status = null;
        String statusAsJson = null;
        String result = null;
        if (lastExecInfo != null) {
            status = lastExecInfo.getInfo();
            statusAsJson = lastExecInfo.getJSONInfo();
            result = lastExecInfo.getResult();
        }
        return toByteArray(planId, statement, status, statusAsJson, result,
                           authContext);
    }

    public static byte[] toByteArray(int planId) {
        return toByteArray(planId, new char[0], null, null, null, null);
    }

    /**
     * Internal use method to get plan id
     */
    public int getPlanId() {
        return planId;
    }

    /**
     * Convert a planId and statement to a serialized DDLFuture handle. Public
     * for support for the deprecated TableAPI.getFuture();
     *
     * In version 1, we chose to serialize the result only for a successfully
     * completed statement, when the result is not available on the server (the
     * planId is 0). That means we can infer what the value of other pieces of
     * the future and statement result should be, i.e.
     *  - the operation is finished (isDone == true)
     *  - isCancelled = false
     *  - the error message is null
     *
     * Version 1 serialization is:
     * (short) version,
     * (int) planId
     * (short) statement length
     * (string) statement
     * (int) authContext byte array length
     * (byte array) authContext byte array
     *  -- optional --
     * (int) status length
     * (string) status
     * (int) status as Json length
     * (string) statusAsJson
     * (int) result length
     * (string) result
     */
    private static byte[] toByteArray(int planId,
                                      char[] statement,
                                      String status,
                                      String statusAsJson,
                                      String result,
                                      AuthContext authContext) {

        /*
         * Doing version 1 serialization. Figure out the buffer length
         * version + planId + statement length = 8
         */
        int serializedLen = 8;

        ByteBuffer bb = UTF8.encode(CharBuffer.wrap(statement));
        byte[] statementBytes = bb.array();
        int statementLength = bb.remaining();
        serializedLen += statementLength;

        int statusLen = 0;
        int statusAsJsonLen = 0;
        int resultLen = 0;
        byte[] statusBytes = null;
        byte[] statusAsJsonBytes = null;
        byte[] resultBytes = null;
        if (planId == 0) {
            if (status != null) {
                statusBytes = status.getBytes(UTF8);
                statusLen = statusBytes.length;
            }

            if (statusAsJson != null) {
                statusAsJsonBytes = statusAsJson.getBytes(UTF8);
                statusAsJsonLen = statusAsJsonBytes.length;
            }

            if (result != null) {
                resultBytes = result.getBytes(UTF8);
                resultLen = resultBytes.length;
            }
            serializedLen += (12 + statusLen + statusAsJsonLen + resultLen);
        }

        int authBytesLen = 0;
        byte[] authBytes = null;
        if (authContext != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream out = new DataOutputStream(baos)) {
                authContext.writeFastExternal(out, SerialVersion.CURRENT);
                out.flush();
                authBytesLen = baos.size();
                authBytes = baos.toByteArray();
            } catch (IOException ioe) {
                throw new IllegalArgumentException(
                    "Fail to convert authContext", ioe);
            }
            serializedLen += authBytesLen;
        }

        ByteBuffer buf = ByteBuffer.allocate(serializedLen);

        buf.putShort(CURRENT_VERSION);
        buf.putInt(planId);
        buf.putShort((short) statementLength);
        if (statementLength > 0) {
            buf.put(statementBytes, 0, statementLength);
        }

        /*
         * Serialize the result if the statement is finished, and the result
         * will not be available on the server. This is only true if the planId
         * is 0.
         * If the planId is nonzero, and there is a corresponding plan on the
         * server, don't serialize the result, even if the operation was
         * done. In those cases, the client will have to make a request to the
         * server to get status, so that it can get the detailed execution
         * history that is available for plans.
         */
        if (planId == 0) {
            buf.putInt(statusLen);
            if (statusLen > 0) {
                buf.put(statusBytes);
            }

            buf.putInt(statusAsJsonLen);
            if (statusAsJsonLen > 0) {
                buf.put(statusAsJsonBytes);
            }

            buf.putInt(resultLen);
            if (resultLen > 0) {
                buf.put(resultBytes);
            }
        }

        if (authContext != null) {
            buf.putInt(authBytesLen);
            if (authBytesLen > 0) {
                buf.put(authBytes);
            }
        }

        return buf.array();
    }

    public AuthContext getAuthContext() {
        return authContext;
    }
}
