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

/**
 * INTERNAL: Implements the dialog and transport layer for async project.
 *
 * <p>
 * The dialog layer provides an async API and implementation for the upper
 * layer to exchange requests/responses in the form of dialogs. It also
 * implements the <a
 * href="https://sleepycat-tools.us.oracle.com/trac/wiki/JEKV/AsyncDialogProtocol">Dialog
 * Protocol</a>
 *
 * <p>
 * The transport layer implements the actual data read/write for the dialog
 * layer. The code currently provides two versions of the transport layer
 * implementation: one with native java nio library; the other with netty.
 *
 * <h1>Async Design Notes</h1>
 *
 * <h2>Regarding the Reactive Stream API</h2>
 *
 * <p>This async package uses several different callback APIs to represent
 * async execution including the Java Flow package (reactive stream API), the
 * CompletionStage interface and customized callback interfaces. We gradually
 * start to realize that the reactive stream API is the most general one, i.e.,
 * all other APIs can be expressed with the reactive stream API. However, we
 * seldom use the reactive stream API throughout the async package mainly due
 * to the fact that our data exchange model is request-response based instead
 * of stream-based. There are other convenience reasons such as a channel
 * handler needs to subscribe to both read and write events. Nevertheless, we
 * learn lessons from the reactive stream API.
 *
 * <h2>Regarding Error Handling Paradigm</h2>
 *
 * <p>One lesson we learned from the reactive stream API is error handling. We
 * adopt the folowing paradigm throughout our code base. A callback handler is
 * a handler that processes events. An execution agent is the underlying object
 * that does the execution and notifies the handlers of related events.
 * <ol>
 * <li>The callback handler is submitted to the execution agent for execution.
 * If an error occurrs to the submission, the handler must be notified of the
 * error. The submission method itself should not throw the error. (Reactive
 * stream Publisher rule 9). This means methods returning a CompletionStage or
 * Subscription must catch Throwable. In the case of Error where we are usually
 * in the mindset of not catching it but letting it bubble up to a uncaught
 * exception handler and treat it as fatal, such mindset does not apply in this
 * case. To me, this choice of not giving special treatment of Error has the
 * benefit of reducing code complexity: we do not need to check whether a
 * suitable uncaught-exception-handler is always on the stack of any invocation
 * method.</li>
 * <li>To deal with errors happend inside handler callbacks, a method must be
 * provided for a handler to cancel from the execution.  The cancel method must
 * be called when such errors occurred. (Reactive stream Subscriber rule 13).
 * </li>
 * <li>To deal with errors happened to the execution agent while processing
 * events related to a handler, a method, in the form of callback or simply a
 * shutdown method, must be provided by the handler so that the execution agent
 * can notify the handler of such error. (Reactive stream Publisher rule 4).
 * This method, if called, is always the last callback of the handler
 * interface. (Reactive stream Publisher rule 7). The method should prevent
 * recursion, i.e., if this method triggers a cancellation of the handler, the
 * cancellation should not be treated as an execution error and calls this
 * method again. (Reactive stream Subscriber rule 3).</li>
 * <li>The callback methods of the handler should make best effort to deal with
 * all errors. Errors thrown from the callback methods may affect other
 * handlers and may even restart the JVM.</li>
 * </ol>
 */
@NonNullByDefault
package oracle.kv.impl.async;

import oracle.kv.impl.util.NonNullByDefault;
