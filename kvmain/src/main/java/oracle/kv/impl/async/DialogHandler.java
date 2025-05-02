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

import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.TemporaryDialogException;
import oracle.kv.impl.async.exception.DialogUnknownException;

/**
 * Handles the events of a dialog.
 *
 * <p>Upper layers implement the following methods for a handler:
 * <ul>
 * <li>{@code onStart}<br>
 * called once as the first method of the handler;</li>
 * <li>{@code onCanWrite}<br>
 * called once, for a successful {@link DialogContext#write} with message
 * <i>M</i>, when (1) <i>M</i> has been entirely written to the network buffer;
 * and (2) at least one {@code DialogContext#write} failed due to <i>M</i>.
 * <li>{@code onCanRead}<br>
 * called once each time when a new input message arrived for the handler;</li>
 * <li>{@code onAbort}<br>
 * called when the dialog is aborted.</li>
 * </ul>
 *
 * <p> Each of the handler methods has a parameter {@code context} which is the
 * {@link DialogContext} object associated with the handler. Handlers can
 * manage message reads and writes by calling {@link DialogContext#read} and
 * {@link DialogContext#write}.
 *
 * <p>All methods for an handler of an individual dialog will be called
 * sequentially; only one method will be called at a time and the next method
 * will not be called until the previous method exits.
 *
 * <p>Implementations of all methods on this interface should complete in a
 * timely manner to avoid keeping the invoking thread from dispatching to other
 * handlers.
 *
 * <p>There are two ways in which a dialog is considered finished with respect
 * to this handler:
 * <ul>
 * <li>The dialog finished normally,<br>
 * in which case all of the following conditions occurred: (1) the {@code
 * onCanRead} method was called with parameter {@code finished} set to {@code
 * true}, which indicates the remote had finished writing messages, (2) the
 * {@link DialogContext#read} method returns {@code null} after (1) occured,
 * which indicates all incoming messages are retrieved, and (3) The {@code
 * DialogContext#write} had been called successfully with parameter {@code
 * finished} set to {@code true}, which indicates the dialog had finished
 * writing messages from this side. </li>
 * <li>The dialog finished abruptly,<br> in which case either the {@link
 * DialogContext#cancel} is called or the {@code onAbort} method was called
 * with the parameter {@code cause}. </li>
 * </ul>
 *
 * <p>After the dialog is finished normally or the {@link #onAbort} method is
 * called, no method of this interface will be called. The {@link #onCanWrite}
 * or {@link #onCanRead} methods may still be called after {@link
 * DialogContext#cancel} is called.
 */
public interface DialogHandler {
    /**
     * Called once when the handler starts.
     *
     * <p>The method will be called as the first method for the handler even if it
     * is already aborted.
     *
     * <p>Any error occurred inside the callback should be handled properly by
     * the method itself. {@link DialogContext#cancel} should be called to
     * signal the cancellation of the dialog. Throwing an exception from this
     * method may cause other dialogs to fail or even the JVM to restart.
     *
     * @param context the dialog context
     * @param aborted {@code true} if dialog is already aborted
     */
    void onStart(DialogContext context, boolean aborted);

    /**
     * Called to notify that a previously failed {@link DialogContext#write}
     * may succeed.
     *
     * <p>A {@code DialogContext#write} can fail and return {@code false} if
     * there is already a message <i>M</i> (written by a previous successful
     * write) pending to send to the network buffer. In that case, when
     * <i>M</i> has been entirely written to the network buffer, this method
     * will be called once. The upper layer can attempt to write again when or
     * after this method is called.
     *
     * <p>Any error occurred inside the callback should be handled properly by
     * the method itself. {@link DialogContext#cancel} should be called to
     * signal the cancellation of the dialog. Throwing an exception from this
     * method may cause other dialogs to fail or even the JVM to restart.
     *
     * @param context the dialog context
     */
    void onCanWrite(DialogContext context);

    /**
     * Called once each time when a new {@link MessageInput} arrived for the
     * handler.
     *
     * <p>{@link DialogContext#read} can be called to retrieve arrived
     * messages.
     *
     * <p>Note that the following case may occur: a new message arrives, but
     * before {@code onCanRead} is called, {@code DialogContext#read} gets
     * called multiple times and retrieved all input messages including the
     * newly arrived one. This result in a situation that a {@code
     * DialogContext#read} call after a {@code onCanRead} call can return
     * {@code null}. Moreover, if the dialog is aborted in between entering
     * this method and {@code DialogContext#read} is called, the read can
     * return {@code null} as well.
     *
     * <p>Any error occurred inside the callback should be handled properly by
     * the method itself. {@link DialogContext#cancel} should be called to
     * signal the cancellation of the dialog. Throwing an exception from this
     * method may cause other dialogs to fail or even the JVM to restart.
     *
     * @param context the dialog context
     * @param finished {@code true} if this is the last onCanRead call and no
     * more input message will arrive
     */
    void onCanRead(DialogContext context, boolean finished);

    /**
     * Called when the dialog is aborted.
     *
     * <p>The handler is considered cancelled after entering this method, i.e.,
     * calling {@link DialogContext#read} will return {@code null} and calling
     * {@link DialogContext#write} will return {@code false}.  Calling {@link
     * DialogContext#cancel} has no effect.
     *
     * <p>Calling {@link DialogContext#cancel} will not result in this method
     * being called.
     *
     * <p>After this method is called, no subsequent method of this handler
     * will be called.
     *
     * <p>The {@code cause} of the abort is either a {@link
     * RequestTimeoutException} or a {@link DialogException}.
     *
     * <p>{@link RequestTimeoutException} is thrown when the dialog is not
     * finished normally before the configured timeout interval. The dialog may
     * have side effects on the remote.
     *
     * <p>{@link DialogException} is thrown for errors other than dialog
     * timeout. The exception captures whether the exception is reported from
     * remote ({@link DialogException#fromRemote}) and whether the dialog has
     * some side effects on the remote ({@link DialogException#hasSideEffect}).
     * The exception should be an instance of one of the three sub classes:
     * <dl>
     * <dt>{@link PersistentDialogException}
     * <dd>which is thrown when the causing problem is more persistent, e.g.,
     * unknown host error, etc. The problem is not expected to disappear
     * without more involved intervention and therefore the caller should
     * backoff and not retry immediately.
     * <dt>{@link TemporaryDialogException}
     * <dd>which is thrown when the causing problem is likely a temporary
     * error, e.g., connection is timed out, etc. The caller could retry the
     * dialog upont this exception.
     * <dt>{@link DialogUnknownException}
     * <dd>which is thrown when an unexpected error occurs. This usually means
     * bugs in the code.
     * </dl>
     *
     * <p>Note that if there has already constructed {@link MessageOutput}
     * objects for this dialog, but has not yet been written with {@link
     * DialogContext#write}, they must be discarded by calling {@link
     * MessageOutput#discard}.
     *
     * @param context the dialog context
     * @param cause cause of the abort
     */
    void onAbort(DialogContext context, Throwable cause);
}
