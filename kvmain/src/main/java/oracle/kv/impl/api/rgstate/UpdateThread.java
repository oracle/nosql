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

package oracle.kv.impl.api.rgstate;

import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.rmi.ConnectIOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.async.exception.GetUserException;
import oracle.kv.impl.rep.admin.IllegalRepNodeServiceStateException;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ThreadUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Base class for update threads.
 *
 * TODO: pass stats out to Monitor when in an RN. Log when in a client? TODO:
 * Limit the rate of log messages on error?
 */
public abstract class UpdateThread extends Thread {

    /**
     * The max number of RNs to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_RNS = 100;

    /**
     * 1 min in millis
     */
    private static final int ONE_MINUTE_MS = 60 * 1000;

    /**
     * The dispatcher associated with this updater thread.
     */
    protected final RequestDispatcher requestDispatcher;

    /**
     * The thread pool used to resolve request handles in parallel.
     */
    protected final UpdateThreadPoolExecutor threadPool;

    /**
     * Shutdown can only be executed once. The shutdown field protects against
     * multiple invocations.
     */
    protected final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     *  The time interval between executions of a concurrent update pass.
     */
    protected final int periodMs;

    /**
     *  The amount of time to wait when resolving a handle.
     */
    private final int resolutionTimeoutMs = 10000;

    private volatile int resolveCount;
    private volatile int resolveFailCount;
    private volatile int resolveExceptionCount;

    protected final Logger logger;

    /*
     * Encapsulates the above logger to limit the rate of log messages
     * associated with a specific RN.
     */
    protected final RateLimitingLogger<RepNodeId> rateLimitingLogger;

    /**
     * Creates the RN state update thread.
     *
     * @param requestDispatcher the request dispatcher associated with this
     * thread.
     *
     * @param periodMs the time period between passes over the RNs to see if
     * they need updating.
     *
     * @param logger the logger used by the update threads.
     */
    protected UpdateThread(RequestDispatcher requestDispatcher,
                           int periodMs,
                           UncaughtExceptionHandler handler,
                           final Logger logger) {
        super();
        this.requestDispatcher = requestDispatcher;
        this.periodMs = periodMs;
        this.logger = logger;
        this.rateLimitingLogger = new RateLimitingLogger<RepNodeId>
            (ONE_MINUTE_MS, LIMIT_RNS, logger);
        final String name = " " + requestDispatcher.getDispatcherId() +
                            " " + getClass().getSimpleName();
        this.setName(name);
        this.setDaemon(true);
        this.setUncaughtExceptionHandler(handler);

        /* Reclaim threads if they have not been used over 5 time periods. */
        final int keepAliveTimeMs =  periodMs * 5;
        threadPool =
            new UpdateThreadPoolExecutor
              (logger, keepAliveTimeMs,
               new UpdateThreadFactory(logger, name, handler));
    }

    /**
     * For test use only.
     */
    UpdateThreadPoolExecutor getThreadPool() {
        return threadPool;
    }

    public int getResolveCount() {
        return resolveCount;
    }

    public int getResolveFailCount() {
        return resolveFailCount;
    }

    public int getResolveExceptionCount() {
        return resolveExceptionCount;
    }

    @Override
    public void run() {

        logger.log(Level.INFO, "{0} started", this);
        try {
            while (!shutdown.get()) {
                doUpdate();

                if (shutdown.get()) {
                    return;
                }
                try {
                    Thread.sleep(periodMs);
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "{0} interrupted", this);
                    throw new IllegalStateException(e);
                }
            }
        } catch (Throwable t) {
            requestDispatcher.shutdown(t);
        } finally {
            shutdown();
        }
    }

    /**
     * Called periodically to perform update.
     */
    protected abstract void doUpdate();

    /**
     * Returns the list of RNs whose state must be maintained. If the
     * request dispatcher belongs to the client the state associated with all
     * RNs is maintained. If it's associated with an RN, both states associated
     * with its RG and states in other RGs whose requestHandler needs repair
     * are maintained. This behavior will need to be updated with support for
     * partition migration, or if support light weight clients which don't
     * themselves maintain RN state.
     *
     * @return the list of RNs whose state is to be maintained.
     */
    protected Collection<RepNodeState> getRNs() {

        final ResourceId dispatcherId = requestDispatcher.getDispatcherId();
        final RepGroupStateTable rgst =
            requestDispatcher.getRepGroupStateTable();

        if (dispatcherId.getType() == ResourceType.CLIENT) {
            return rgst.getRepNodeStates();

        } else if (dispatcherId.getType() == ResourceType.REP_NODE) {
            final Collection<RepNodeState> rns =
                rgst.getRNStatesNeedingRepair();
            final RepNodeId rnId = (RepNodeId)dispatcherId;
            rns.addAll(getRNs(new RepGroupId(rnId.getGroupId())));
            return rns;

        } else {
            throw new IllegalStateException
                ("Unexpected dispatcher: " + dispatcherId);
        }
    }

    /**
     * Returns the rep group state of the specified group.
     * @param groupId a group Id
     * @return a rep group state
     */
    protected RepGroupState getRepGroupState(RepGroupId groupId) {
        final RepGroupStateTable rgst =
            requestDispatcher.getRepGroupStateTable();
        return rgst.getGroupState(groupId);
    }

    /**
     * Returns the list of RNs in the specified group.
     *
     * @param groupId a group Id
     * @return the list of RNs in the group
     */
    protected Collection<RepNodeState> getRNs(RepGroupId groupId) {
        return getRepGroupState(groupId).getRepNodeStates();
    }

    /**
     * Returns true if the handle of the specified state needs resolution.
     * If true a thread is started to resolve asynchronously, so that a single
     * network problem does not tie up resolution of all handles.
     *
     * @param rnState target node
     * @return true if the handle needs resolution
     */
    protected boolean needsResolution(RepNodeState rnState) {
        if (!rnState.reqHandlerNeedsResolution()) {
            return false;
        }
        threadPool.execute(new ResolveHandler(rnState));
        return true;
    }

    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            /* Already shutdown. */
            return;
        }
        logger.log(Level.INFO, "{0} shutdown", this);
        threadPool.shutdownNow();
    }


    /**
     * The task that's actually executed by the threads in the thread pool to
     * resolve handles.
     */
    private class ResolveHandler implements UpdateThreadPoolExecutor.UpdateTask {
        final RepNodeState rns;

        ResolveHandler(RepNodeState rns) {
            super();
            this.rns = rns;
        }

        @Override
        public RepNodeId getResourceId() {
            return rns.getRepNodeId();
        }

        @Override
        public void run() {
            final RegistryUtils regUtils = requestDispatcher.getRegUtils();
            if (regUtils == null) {

                /*
                 * The request dispatcher has not initialized itself as
                 * yet. Retry later.
                 */
                return;
            }
            logger.log(Level.FINEST, "Probing RN state: {0}", rns);
            rns.resolveReqHandlerRef(regUtils, resolutionTimeoutMs)
                .whenComplete(unwrapExceptionVoid(this::handleRef));
        }

        private void handleRef(Boolean ref, Throwable e) {
            if (ref == null) {
                if (e == null) {
                    e = new IllegalArgumentException("Ref is null");
                }
                final boolean expected =
                    ((e instanceof GetUserException) &&
                     ((GetUserException) e).isExpectedException());
                logger.log(expected ? Level.FINEST : Level.WARNING,
                           e,
                           () -> String.format(
                               "Exception in ResolveHandler thread " +
                               "rn=%s, " +
                               "UpdateThread=%s(%s), " +
                               "ResolveHandler=%s, " +
                               "resolveExceptionCount=%s",
                               rns.getRepNodeId(),
                               UpdateThread.this.getName(),
                               ThreadUtils.threadId(UpdateThread.this),
                               ResolveHandler.this,
                               resolveExceptionCount));
                resolveExceptionCount++;
            } else if (!ref) {
                logger.log(Level.FINEST, "Resolve RN failed: {0}",
                           rns);
                resolveFailCount++;
            } else {
                logger.log(Level.FINEST, "Resolve RN succeeded: {0}",
                           rns);
                resolveCount++;
            }
        }
    }

    /**
     * Produces a brief message (whenever possible) describing the exception.
     * In particular, it avoids use of FaultException.toString which always
     * logs the complete stack trace.
     *
     * @param level the logging level to be used
     * @param msgPrefix the message prefix text
     * @param exception the exception
     */
    protected void logBrief(RepNodeId rnId,
                            Level level,
                            String msgPrefix,
                            Exception exception) {

        if (exception instanceof FaultException) {
            final FaultException fe = (FaultException) exception;
            final String fcName = fe.getFaultClassName();
            if ((fcName != null) && (fe.getMessage() != null)) {
                /* Avoid use of FaultException.toString and stack trace */
                rateLimitingLogger.log(rnId, level,
                                       msgPrefix +
                                       " Fault class:" + fcName +
                                       " Problem:" + fe.getMessage());
                return;
            }
        }

        if (exception instanceof java.rmi.ConnectException ||
            exception instanceof ConnectIOException) {
            rateLimitingLogger.log(rnId, level,
                                   msgPrefix +
                                   " Problem:" + exception.toString());
            return;
        }

        /* Default to use of toString */
        logger.log(level, "{0} Problem:{1}",
                   new Object[]{msgPrefix, exception.toString()});
    }

    /**
     * Logs a suitable message on failure.
     */
    protected void logOnFailure(RepNodeId rnId,
                                Exception exception,
                                final String changesMsg) {
        if (exception == null) {
            logger.info("Error. "  + changesMsg);
            return;
        }

        final String irnsseName =
            IllegalRepNodeServiceStateException.class.getName();

        if ((exception instanceof RepNodeAdminFaultException) &&
            ((RepNodeAdminFaultException) exception).
            getFaultClassName().equals(irnsseName)) {
            /* log an abbreviated message. */
            rateLimitingLogger.log(rnId, Level.INFO,
                                   changesMsg + "Exception message:" +
                                   exception.getMessage());
        } else {
            logBrief(rnId, Level.INFO, changesMsg, exception);
        }
    }

    /**
     * Name the update thread pool threads, and pass any uncaught exceptions
     * to the logger.
     */
    private class UpdateThreadFactory extends KVThreadFactory {
        private final String name;
        private final UncaughtExceptionHandler handler;

        UpdateThreadFactory(Logger logger,
                            String name,
                            UncaughtExceptionHandler handler) {
            super(null, logger);
            this.name = name  + "_Updater";
            this.handler = handler;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return handler;
        }
    }
}
