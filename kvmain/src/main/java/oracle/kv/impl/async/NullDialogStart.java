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

package oracle.kv.impl.async;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Helper class to quickly fail a dialog upon its start. Failing the dialog this
 * way must ensure that there is no way the dialog can already be aborted
 * through some other code paths, e.g., scheduled time out ([KVSTORE-2228]). In
 * general, this class is used to fail a dialog when the context is not created
 * yet. Otherwise, consider using
 * DialogContextImpl#onLocalAbortConnectionException.
 */
public class NullDialogStart {

    /** An executor that rejects all requests. */
    public static final ScheduledExecutorService NULL_EXECUTOR =
        new NullExecutor();
    /**
     * An executor that executes Runnable in-place, but rejects all scheduled
     * tasks.
     */
    public static final ScheduledExecutorService IN_PLACE_EXECUTE_EXECUTOR =
        new NullExecutor() {
            @Override
            public void execute(@Nullable Runnable command) {
                if (command != null) {
                    command.run();
                }
            }
    };


    private static final NullContext nullContext = new NullContext();

    /**
     * A thread local flag to indicate a fail dialog procedure is in progress.
     *
     * This is necessary to prevent too much recursion, i.e., upper layer can
     * start new dialogs in onAbort which will likely to fail again.
     */
    private static final ThreadLocal<Boolean> failingDialog =
        ThreadLocal.withInitial(() -> false);

    /**
     * Fails the dialog.
     *
     * @param handler the dialog handler
     * @param cause the cause of the fail
     * @param executor an executor to do the failing task if the upper layer
     * starts new dialog in onAbort
     */
    public static void fail(final DialogHandler handler,
                            final Throwable cause,
                            final ExecutorService executor) {
        if (!failingDialog.get()) {
            doFail(handler, cause);
            return;
        }
        try {
            executor.submit(() -> doFail(handler, cause));
            return;
        } catch(RejectedExecutionException e) {
        }
        /*
         * That didn't work, which probably means there is a race condition
         * during store shutdown. We cannot create a separate new thread due to
         * the promise that DialogHandler callbacks are run in the same thread.
         * Plus, the caller should not recurse when we are shutting down.
         * Therefore, just throw an exception.
         */
        throw new IllegalStateException(
                      String.format("Detected recursion inside " +
                                    "the onAbort method of handler [%s] " +
                                    "for cause [%s] " +
                                    "when the executor is unavailable",
                                    handler, cause));
    }

    private static void doFail(DialogHandler handler,
                               Throwable cause) {
        failingDialog.set(true);
        try {
            handler.onStart(nullContext, true);
        } finally {
            try {
                handler.onAbort(nullContext, cause);
            } catch (Throwable t) {
                /*
                 * There is nothing we can do here. If the problem is
                 * persistent, it will be logged when the dialog is actually
                 * started.
                 */
            }
            failingDialog.set(false);
        }
    }

    /**
     * A null context provided to the failed dialog.
     */
    private static class NullContext implements DialogContext {

        @Override
        public void cancel(Throwable t) {
        }

        @Override
        public boolean write(MessageOutput mesg, boolean finished) {
            return false;
        }

        @Override
        public @Nullable MessageInput read() {
            return null;
        }

        @Override
        public long getDialogId() {
            return 0;
        }

        @Override
        public long getConnectionId() {
            return 0;
        }

        @Override
        public NetworkAddress getRemoteAddress() {
            return InetNetworkAddress.ANY_LOCAL_ADDRESS;
        }

        @Override
        public ScheduledExecutorService getSchedExecService() {
            return NULL_EXECUTOR;
        }

        @Override
        public String toString() {
            return "NullContext";
        }
    }

    /**
     * A null scheduled executor service provided to the null context.
     */
    private static class NullExecutor
            extends AbstractExecutorService
            implements ScheduledExecutorService {

        @Override
        public boolean awaitTermination(long timeout,
                                        @Nullable TimeUnit unit) {
            return true;
        }

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public boolean isTerminated() {
            return true;
        }

        @Override
        public void shutdown() {
            return;
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public void execute(@Nullable Runnable command) {
            throw new RejectedExecutionException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(@Nullable Callable<V> callable,
                                               long delay,
                                               @Nullable TimeUnit unit) {
            throw new RejectedExecutionException();
        }

        @Override
        public ScheduledFuture<?> schedule(@Nullable Runnable command,
                                           long delay,
                                           @Nullable TimeUnit unit) {
            throw new RejectedExecutionException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(@Nullable
                                                      Runnable command,
                                                      long initialDelay,
                                                      long period,
                                                      @Nullable
                                                      TimeUnit unit) {

            throw new RejectedExecutionException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(@Nullable
                                                         Runnable command,
                                                         long initialDelay,
                                                         long delay,
                                                         @Nullable
                                                         TimeUnit unit) {
            throw new RejectedExecutionException();
        }
    }
}
