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

package com.sleepycat.je.rep.net;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.sleepycat.je.rep.subscription.StreamAuthenticator;

/**
 * An interface that associates a delegate socketChannel for network I/O, which
 * provides ByteChannel, GatheringByteChannel, and ScatteringByteChannel,
 * interfaces for callers.
 *
 * <p> The interface supports both blocking/non-blocking socketChannel as well
 * as normal and SSL connection, which complicates the semantics of its
 * methods.
 *
 * <h2 id="asyncIO">Asynchronous IO operation incompletion and
 * continuation</h2>
 *
 * For a non-blocking channel, the {@link #read}, {@link #write}, {@link
 * #flush} and {@link #closeAsync} methods may be incomplete due to some
 * requirements not met, e.g., channel write is busy or certain handshake steps
 * have not yet done for SSL. If the {@link #read} and {@link #write} methods
 * return {@code 0}, the operation may be incomplete. The {@link #flush} and
 * {@link closeAsync} methods return {@code false} if incomplete. The
 * application should call {@link getAsyncIOContinueAction} to determine what
 * action to take. When the action finishes, the operation should be retried.
 *
 * <h2 id="flush">Flushing the channel</h2>
 *
 * For a blocking channel, the data {@linkplain #write written} is always
 * flushed. For a non-blocking channel, the {@link #write} method only flushes
 * data when necessary. The application should call {@link flush} (multiple
 * times if neccessary to ensure its completion) to make sure the written data
 * is flushed.
 *
 * <h2 id="close">Closing the channel</h2>
 *
 * The channel supports methods to close gracefully for both {@linkplain #close
 * blocking} and {@linkplain #closeAsync non-blocking} channel. A method to
 * {@linkplain #closeForcefully close forcefully} is also provided.
 *
 * When any of the close method is called, if there is any thread concurrently
 * under an I/O operation (blocking or non-blocking) upon this channel, the
 * thread will receive an {@link AsynchronousCloseException} and the channel
 * will be closed forcefully.
 *
 * The channel is closed if any of the following occurs: (1) any of the close
 * method is called and throws exception; (2) the methods {@link close} or
 * {@link closeForcefully} return successfully; or (3) the method {@link
 * closeAsync} returns {@code true}. Any I/O operation upon the channel throws
 * {@link ClosedChannelException} after the channel is closed.
 */
public interface DataChannel extends ByteChannel,
                                     GatheringByteChannel,
                                     ScatteringByteChannel {

    /**
     * Checks whether the channel is connected.
     *
     * @return {@code true} if the channel is connected
     */
    boolean isConnected();

    /**
     * Retrieves a socket associated with this channel.
     *
     * @return a socket associated with this channel.
     */
    Socket socket();

    /**
     * Returns the remote address to which this channel's socket is connected.
     */
    SocketAddress getRemoteAddress() throws IOException;

    /**
     * Adjusts this channel's blocking mode.
     */
    void configureBlocking(boolean block) throws IOException;

    /**
     * Tells whether or not every I/O operation on this channel will block
     * until it completes.
     */
    boolean isBlocking();

    /**
     * Checks whether the channel encrypted.
     *
     * @return true if the data channel provides network privacy
     */
    boolean isSecure();

    /**
     * Checks whether  the channel capable of determining peer trust.
     *
     * @return true if the data channel implementation has the capability
     * to determine trust.
     */
    boolean isTrustCapable();

    /**
     * Checks whether the channel peer is trusted.
     *
     * @return true if the channel has determined that the peer is trusted.
     */
    boolean isTrusted();

    /**
     * Accessor for the underlying SocketChannel.
     *
     * Use of this accessor is discouraged. An implementation may have special
     * treatment for methods in SocketChannel. For example, SSLDataChannel
     * caches the blocking mode to avoid some blocking issue. Therefore, using
     * the above wrap methods are preferrable.
     *
     * @return the socket channel underlying this data channel instance
     */
    SocketChannel getSocketChannel();

    /**
     * {@inheritDoc}
     *
     * <p>See also <a href="DataChannel.html#asyncIO">asynchronous IO operation
     * incompletion and continuation</a> and <a
     * href="DataChannel.html#close">channel close</a>.
     */
    @Override
    long read(ByteBuffer[] dsts, int offset, int length) throws IOException;

    /**
     * {@inheritDoc}
     *
     * <p>See also <a href="DataChannel.html#asyncIO">asynchronous IO operation
     * incompletion and continuation</a>, <a
     * href="DataChannel.html#flush">flush</a> and <a
     * href="DataChannel.html#close">channel close</a>.
     */
    @Override
    long write(ByteBuffer[] srcs, int offset, int length) throws IOException;

    /**
     * Attempts to flush written data to the underlying socket channel.
     *
     * The method does nothing for a blocking channel.
     *
     * <p>See also <a href="DataChannel.html#asyncIO">asynchronous IO operation
     * incompletion and continuation</a>, <a
     * href="DataChannel.html#flush">flush</a> and <a
     * href="DataChannel.html#close">channel close</a>.
     *
     * @return {@code true} if all available written data has been flushed (or
     * no data to flush)
     */
    boolean flush() throws IOException;

    /**
     * {@inheritDoc}
     *
     * <p>It is considered a coding error to call this method if the channel is
     * configured as non-blocking: use {@link #closeAsync} or {@link
     * #closeForcefully} instead.  If called for a non-blocking channel, this
     * method closes the channel forcefully and throws {@link
     * IllegalStateException}.
     *
     * <p>The method cleanly closes the channel, flushing all written data,
     * blocking if necessary
     *
     * <p>If an error occurs, the method attempts to forcefully close the
     * channel, before throwing the exception. Only the first encountered
     * exception is thrown.
     *
     * <p>See also <a href="DataChannel.html#close">channel close</a>.
     *
     * @throws IOException if an error occurs
     */
    @Override
    void close() throws IOException;

    /**
     * Closes this channel, which should be a non-blocking channel.
     *
     * <p>It is considered a coding error to call this method if the channel is
     * configured as blocking: use {@link #close} or {@link #closeForcefully}
     * instead. If called for a blocking channel, this method closes the
     * channel forcefully and throws {@link IllegalStateException}.
     *
     * <p>The method cleanly closes the channel, flushing all written data.
     *
     * <p>The close is incomplete if this method returns {@code false}. The
     * application should keep calling this method until it returns {@code
     * true} or throws exception. See <a
     * href="DataChannel.html#asyncIO">asynchronous IO operation incompletion
     * and continuation</a>.
     *
     * <p>If an error occurs, the method attempts to forcefully close the
     * channel, before throwing the exception. Only the first encountered
     * exception is thrown.
     *
     * <p>See also <a href="DataChannel.html#close">channel close</a>.
     *
     * @return {@code true} if the channel is cleanly closed
     * @throws IOException if there is an error
     */
     boolean closeAsync() throws IOException;

    /**
     * Closes this channel forcefully.
     *
     * <p>The method returns immediately, forcefully close the channel without
     * flushing written data or any procedure required for a clean close.
     *
     * <p>See also <a href="DataChannel.html#close">channel close</a>.
     *
     * @throws IOException if an error occurs
     */
    void closeForcefully() throws IOException;

    public interface AsyncIO {
        /**
         * The types of async IO.
         */
        public enum Type {
            READ, WRITE, FLUSH, CLOSE,
        }

        /**
         * The action that needs to be taken to continue processing an
         * incomplete async IO operation.
         */
        public enum ContinueAction {
            /**
             * No unresolved issue, call the IO method again.
             */
            RETRY_NOW,
            /**
             * Register for channel read and call the IO method again when
             * notified read available. A read, write, flush or close operation
             * is incomplete due to there is no data available. Operations
             * other than read may need this action due to an underlying
             * handshake needs to receive more data.
             */
            WAIT_FOR_CHNL_READ,
            /**
             * Register for channel write, wait to be notified of write
             * availability, then call flush and call the IO method again. A
             * read, write, flush or close operation is incomplete due to the
             * channel is too busy for write (some platform write buffer is
             * full). Read operations may need this action due to an underlying
             * handshake needs to write data.
             *
             * <p>The application must flush again when notified write
             * available, before retry its operation, to avoid livelock.
             */
            WAIT_FOR_CHNL_WRITE_THEN_FLUSH,
            /**
             * Read from the channel, and call the IO method again. A write or
             * flush operation is incomplete because application had not been
             * reading data frequently enough.
             *
             * <p> This could happen when a concurrent handshake needs to
             * complete, but to do that the application data needs to be
             * cleared before we can read the handshake data. Need to make sure
             * the application is reading.
             */
            APP_READ,
            /**
             * Call {@link executeTasks} and call the IO method again when
             * notified completion. A read, write, flush or clean operation is
             * incomplete due to some handshake tasks need to be executed.
             */
            WAIT_FOR_TASKS_EXECUTION,
        }
    }

    /**
     * Returns the action to continue the incomplete an async IO operation.
     *
     * @param type the type of the operation
     * @return the action
     */
    AsyncIO.ContinueAction getAsyncIOContinueAction(AsyncIO.Type type);

    /**
     * Executes the tasks required for handshake with the provided {@code
     * executor}.
     *
     * @return the future completed when the task completes
     */
    CompletableFuture<Void> executeTasks(ExecutorService executor);

    /**
     * Sets stream authenticator for the channel
     * @param authenticator stream authenticator
     */
    void setStreamAuthenticator(StreamAuthenticator authenticator);

    /**
     * Gets a channel stream authenticator, or null if not available
     * @return a chanenel stream authenticator, or null
     */
    StreamAuthenticator getStreamAuthenticator();

    /**
     * Returns a channel ID
     * @return channel ID
     */
    String getChannelId();

    /**
     * Helper method, return true if the data channel needs security check,
     * false otherwise
     * @param channel data channel
     * @return  true if the data channel needs security check
     */
    static boolean needSecurityCheck(DataChannel channel) {
        return channel.isTrustCapable() && !channel.isTrusted();
    }
}

