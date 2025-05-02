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

import static java.util.logging.Level.WARNING;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.RequestTimeoutException;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class used to implement remote calls by initiators (clients) of
 * asynchronous service interfaces. Subclasses should register the associated
 * dialog type family in {@link StandardDialogTypeFamily}, implement {@link
 * #getSerialVersionCall getSerialVersionCall} to support the implementation of
 * {@link #getSerialVersion getSerialVersion}, and implement other service
 * methods by calling {@link #startDialog startDialog} with method-specific
 * implementations of {@link AsyncVersionedRemote.MethodCall}.
 *
 * <p>Implementations that are called by clients should only use fast
 * externalizable serialization, not Java serialization. Although we have not
 * done it so far, we may at some point want to implement a direct client in a
 * language other than Java. Fast externalizable is not specific to Java, so it
 * provides us a way that we could implement a non-Java version.
 *
 * <p>Implementations that are performance critical should also avoid using
 * reflection to marshal arguments or unmarshal responses.
 *
 * <p>Two classes are performance critical and used by clients, and so
 * implement their own fast externalizable serialization and do their own
 * marshaling and unmarshaling:
 *
 * <ul>
 * <li> AsyncRequestHandlerInitiator
 * <li> ServiceRegistryInitiator
 * </ul>
 *
 * <p>These classes have their own asynchronous interfaces and were put into
 * use in release 19.5, although they were added to the codebase earlier.
 *
 * <p>All other interfaces use the AbstractAsyncInitiatorProxy subclass, which
 * uses a dynamic proxy to convert the method arguments for an RMI remote
 * interface to an async one.
 *
 * <p>Implementations that are called by the client and are not performance
 * critical use AsyncInitiatorProxy, which extends AbstractAsyncInitiatorProxy,
 * to create dynamic proxies. They use MethodCallClass annotations to record
 * what MethodClass implements fast externalizable serialization for each
 * interface method.
 *
 * <p>Three classes are in this category:
 *
 * <ul>
 * <li> AsyncClientAdminServiceInitiator
 * <li> AsyncClientRepNodeAdminInitiator
 * <li> AsyncUserLoginInitiator
 * </ul>
 *
 * <p>These classes also have their own asynchronous interfaces and were added
 * in release 21.2.
 *
 * <p>Implementations that are not called by clients can use Java serialization,
 * which can be performed automatically by using reflection. These cases use
 * JavaSerialInitiatorProxy to create dynamic proxies that also perform
 * serialization. These classes currently only provide synchronous
 * implementations: there are no associated asynchronous interfaces.
 *
 * <p>All remaining remote interface classes do this:
 *
 * <ul>
 * <li> ArbNodeAdminInitiator
 * <li> CommandServiceInitiator
 * <li> MonitorAgentInitiator
 * <li> RemoteTestInterfaceInitiator
 * <li> RepNodeAdminInitiator
 * <li> StorageNodeAgentInterfaceInitiator
 * <li> TrustedLoginInitiator
 * </ul>
 *
 * These changes were added in release 21.3.
 *
 * <p>This class implements content-based equals and hashCode methods to permit
 * callers to determine if two remote proxies are equivalent. Subclasses should
 * override these methods as appropriate.
 *
 * @see AsyncVersionedRemote
 */
public abstract class AsyncVersionedRemoteInitiator
        extends AsyncBasicLogging
        implements AsyncVersionedRemote {

    private static final String LOG_PREFIX = "async-remote-initiator: ";

    protected final CreatorEndpoint endpoint;
    protected final DialogType dialogType;

    /**
     * Creates an instance of this class.
     *
     * @param endpoint the associated endpoint
     * @param dialogType the dialog type
     * @param logger the logger
     */
    protected AsyncVersionedRemoteInitiator(CreatorEndpoint endpoint,
                                            DialogType dialogType,
                                            Logger logger) {
        super(logger);
        this.endpoint = checkNull("endpoint", endpoint);
        this.dialogType = checkNull("dialogType", dialogType);
    }

    /**
     * Returns the method call object that should be used to implement {@link
     * #getSerialVersion} calls.
     */
    protected abstract MethodCall<Short> getSerialVersionCall();

    @Override
    public CompletableFuture<Short> getSerialVersion(final short serialVersion,
                                                     final long timeoutMillis)
    {
        return startDialog(serialVersion, getSerialVersionCall(),
                           timeoutMillis);
    }

    /**
     * Starts a dialog to perform a asynchronous remote method call.
     *
     * @param <T> the response type of the call
     * @param serialVersion the serial version
     * @param methodCall for marshaling and unmarshaling the request and
     * response
     * @param timeoutMillis the dialog timeout
     * @return a future that returns the response
     */
    protected <T> CompletableFuture<T>
        startDialog(final short serialVersion,
                    final MethodCall<T> methodCall,
                    final long timeoutMillis) {
        final Supplier<String> dialogInfo =
            () -> String.format(": initiator=%s" +
                                " serialVersion=%d" +
                                " timeoutMillis=%s",
                                this,
                                serialVersion,
                                timeoutMillis);
        logger.finest(() -> LOG_PREFIX + "Entered" +
                      dialogInfo.get() +
                      " methodCall=" + methodCall.describeCall());
        if (timeoutMillis <= 0) {
            logger.fine(() -> LOG_PREFIX + "Call fails: Already timed out" +
                        dialogInfo.get() +
                        " methodCall=" + methodCall.describeCall());
            return failedFuture(
                new RequestTimeoutException(
                    (int) timeoutMillis, "Request timed out", null, false));
        }

        CompletableFuture<T> future = new CompletableFuture<>();
        final DialogHandler dialogHandler;
        final MessageOutput out = new MessageOutput();
        try {

            /*
             * Write the call data: method op, serial version, parameters, and
             * timeout
             */
            try {
                writePackedInt(out, methodCall.getMethodOp().getValue());
                out.writeShort(serialVersion);
                methodCall.writeFastExternal(out, serialVersion);
                if (!methodCall.includesTimeout()) {
                    writePackedLong(out, timeoutMillis);
                }
                logger.finest(() -> LOG_PREFIX + "Wrote call output" +
                              dialogInfo.get() +
                              " methodCall=" + methodCall.describeCall() +
                              " outputSize=" + out.size());
            } catch (IOException e) {
                logger.fine(() -> LOG_PREFIX +
                            "Call fails: Serialization failed" +
                            dialogInfo.get() +
                            " methodCall=" + methodCall.describeCall() +
                            getExceptionLogging(e));
                throw new IllegalStateException(
                    "Unexpected problem writing request: " + e, e);
            }
            dialogHandler = new AsyncVersionedRemoteDialogInitiator<>(
                out, logger, methodCall, future);
            logger.finest(() -> LOG_PREFIX + "Created dialog handler" +
                          dialogInfo.get() +
                          " dialogHandler=" + dialogHandler);
        } catch (Throwable e) {
            logger.warning(LOG_PREFIX + "Call fails:" +
                           " Unexpected exception starting request" +
                           dialogInfo.get() +
                           " methodCall=" + methodCall.describeCall() +
                           getExceptionLogging(e, WARNING));
            out.discard();
            return failedFuture(e);
        }
        try {
            logger.finer(() -> LOG_PREFIX + "Call starts" +
                         dialogInfo.get() +
                         " dialogHandler=" + dialogHandler);
            endpoint.startDialog(dialogType.getDialogTypeId(),
                                 dialogHandler, timeoutMillis);
            logger.finest(() -> LOG_PREFIX + "Started dialog" +
                          dialogInfo.get() +
                          " dialogHandler=" + dialogHandler);
            if (logger.isLoggable(Level.FINE)) {
                future = future.whenComplete(
                    unwrapExceptionVoid(
                        (result, e) ->
                        logger.log((e == null) ? Level.FINER : Level.FINE,
                                   LOG_PREFIX + "Call ends" +
                                   dialogInfo.get() +
                                   " dialogHandler=" + dialogHandler +
                                   " result=" + result +
                                   getExceptionLogging(e))));
            }
            return future;
        } catch (Throwable e) {
            logger.warning(LOG_PREFIX +
                           "Call ends: Unexpected exception starting request" +
                           dialogInfo.get() +
                           " dialogHandler=" + dialogHandler +
                           getExceptionLogging(e, WARNING));
            return failedFuture(e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, dialogType);
    }

    @Override
    public boolean equals(@Nullable Object object) {
        if ((object == null) || !getClass().equals(object.getClass())) {
            return false;
        }
        final AsyncVersionedRemoteInitiator other =
            (AsyncVersionedRemoteInitiator) object;
        return endpoint.equals(other.endpoint) &&
            dialogType.equals(other.dialogType);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getAbbreviatedClassName());
        sb.append('[');
        getToStringBody(sb);
        sb.append(']');
        return sb.toString();
    }

    protected void getToStringBody(StringBuilder sb) {
        sb.append("endpoint=").append(endpoint);
        sb.append(" dialogType=").append(dialogType);
    }
}
