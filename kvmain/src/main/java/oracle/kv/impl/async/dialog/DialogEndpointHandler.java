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

package oracle.kv.impl.async.dialog;

import java.util.logging.Logger;

import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.IOBufSliceList;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;

/**
 * The handler for contexts and protocol handling.
 */
interface DialogEndpointHandler extends EndpointHandler {
    /**
     * Returns the logger.
     *
     * @return the logger.
     */
    Logger getLogger();

    /**
     * Returns the description of the underlying chanenl.
     *
     */
    ChannelDescription getChannelDescription();

    /**
     * Returns the {@link ProtocolReader}.
     */
    ProtocolReader getProtocolReader();

    /**
     * Returns the {@code ProtocolWriter}.
     *
     * @return the protocol writer
     */
    ProtocolWriter getProtocolWriter();

    /**
     * Asserts that the method is called inside the executor thread.
     *
     * Throws {@link IllegalStateException} if not.
     */
    void assertInExecutorThread();

    /**
     * Called when {@code DialogContextImpl} is finished normally or aborted.
     *
     * @param context the dialog context
     */
    void onContextDone(DialogContextImpl context);

    /**
     * Called when {@code DialogContextImpl} has new message to write.
     *
     * @param context the dialog context
     */
    void onContextNewWrite(DialogContextImpl context);

    /**
     * Gets the max value of the size of a {@link MessageInput}.
     *
     * @return the max value
     */
    int getMaxInputTotLen();

    /**
     * Gets the max value of the size of a {@link MessageOutput}.
     *
     * @return the max value
     */
    int getMaxOutputTotLen();

    /**
     * Gets the max length for incoming protocol message.
     *
     * @return the max length
     */
    int getMaxInputProtocolMesgLen();

    /**
     * Gets the max length for outgoing protocol message.
     *
     * @return the max length
     */
    int getMaxOutputProtocolMesgLen();

    /**
     * Writes a DialogStart message for {@code DialogContextImpl}.
     *
     * @param finish {@code true} if the frame is of the last message
     * @param cont {@code false} if the frame is the last of the message
     * @param typeno typeno of the dialog
     * @param timeoutMillis dialog timeout
     * @param frame the frame
     * @param context the context
     * @return the assigned dialogId
     */
    long writeDialogStartForContext(boolean finish,
                                    boolean cont,
                                    int typeno,
                                    long timeoutMillis,
                                    IOBufSliceList frame,
                                    DialogContextImpl context);

    /**
     * Flushes the already written messages, terminates the endpoint handler if
     * there is an error.
     */
    void flushOrTerminate();
}
