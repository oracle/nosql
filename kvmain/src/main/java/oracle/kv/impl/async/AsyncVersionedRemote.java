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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.async.registry.ServiceRegistry;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.SerializeExceptionUtil;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base interface for all asynchronous service interfaces.
 *
 * <p>This class is analogous to the {@link VersionedRemote} interface used for
 * synchronous service interfaces. Like that class, this one includes
 * facilities for negotiating the serial version used for communication, to
 * support upgrades, in addition to basic facilities for performing remote
 * calls. For synchronous services, remote call facilities are provided by RMI.
 * For asynchronous services, we provide our own remote call implementation
 * based on the dialog layer. Unlike RMI, implementations of asynchronous call
 * initiation, response, and serialization and deserialization of requests and
 * responses can require hand coding at the interface and method level.
 *
 * <p>When present, the async remote call interface is represented by a service
 * interface that extends the {@link AsyncVersionedRemote} interface and whose
 * methods represent the remote calls. In some cases there is no async
 * interface class defined. Instead, the initiator uses the network format for
 * method requests and response defined below to match what the interface
 * methods would have looked like but without actually creating the interface
 * class.
 *
 * <p>Regardless of the approach, the parameters of each remote method should
 * start with a {@code short} that represents the serial version in use for the
 * call, followed by method-specific parameters, and ending with a {@code long}
 * that represents the dialog timeout in milliseconds. The remote method should
 * return a {@link CompletableFuture} for delivering the result.
 *
 * <p>The remote interface implements its network format by defining several
 * components. A {@link DialogType} value provides a unique ID for the remote
 * interface, and is usually registered as a {@link StandardDialogTypeFamily}.
 * An {@code enum} that implements {@link MethodOp} is often used to provide
 * unique IDs for each remote method. Each method is represented by a {@link
 * MethodCall} that implements the marshaling and unmarshaling of requests and
 * responses for that method.
 *
 * <p>The implementation of the calling side of the remote interface should use
 * a subclass of {@link AsyncVersionedRemoteInitiator} to initiate remote
 * calls.
 *
 * <p>If supplied, the API class provided to callers should be implemented as a
 * subclass of {@link AsyncVersionedRemoteAPI}, which negotiates the serial
 * version to support upgrades. As with the remote call interface, sometimes
 * only a synchronous interface is provided.
 *
 * <p>In addition to providing a server-side implementation of the service
 * interface, the service implementation should also provide a subclass of
 * {@link AsyncVersionedRemoteDialogResponder} to implement the responder side
 * of the remote call infrastructure. The responder should be registered with
 * the dialog layer to provide support for incoming calls. Applications can
 * also use the {@link ServiceRegistry}, analogous to the RMI registry, to
 * provide a way for clients to look up remote services.
 *
 * <p>Remote calls are implemented using the dialog layer, with a single frame
 * to represent the requested call and a single frame representing the
 * response.
 *
 * <p>The request frame has the format:
 * <ul>
 * <li> ({@link SerializationUtil#writePackedInt packed int}) <i>method op
 *      value</i>
 * <li> ({@code short}) <i>serial version</i>
 * <li> Method arguments are written in {@link FastExternalizable} format
 * <li> ({@link SerializationUtil#writePackedLong packed long}) <i>timeout</i>
 * </ul>
 *
 * <p>The response frame has the format:
 * <ul>
 * <li> ({@link ResponseType ResponseType}) <i>response type</i>
 * <li> If the response type is {@link ResponseType#SUCCESS SUCCESS}:
 *   <ul>
 *   <li> ({@code short}) <i>serial version</i>
 *   <li> The result object is written in {@code FastExternalizable} format
 *   </ul>
 * <li> If the response type is {@link ResponseType#FAILURE FAILURE}, the
 *      exception is written by {@link SerializeExceptionUtil#writeException
 *      SerializeExceptionUtil.writeException}
 * </ul>
 *
 * @see AsyncVersionedRemoteAPI
 * @see AsyncVersionedRemoteInitiator
 * @see AsyncVersionedRemoteDialogResponder
 * @see ServiceRegistry
 */
public interface AsyncVersionedRemote {

    /**
     * The types of responses to asynchronous method calls.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    enum ResponseType implements FastExternalizable {

        /** A successful result. */
        SUCCESS(0),

        /** An exception from a failure. */
        FAILURE(1);

        private static final ResponseType[] VALUES = values();

        ResponseType(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /**
         * Reads an instance from the input stream.
         *
         * @param in the input stream
         * @param serialVersion the version of the serialized form
         * @throws IOException if an I/O error occurs or if the format of the
         * input data is invalid
         */
        public static ResponseType readFastExternal(DataInput in,
                                                    short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException(
                    "Wrong ordinal for ResponseType: " + ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>value</i> // {@link #SUCCESS}=0, {@link
         *      #FAILURE}=1
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /**
     * An identifier for an asynchronous method. Subclasses of {@link
     * AsyncVersionedRemote} should define a enumeration that implements this
     * interface and whose values represent the methods supported by the remote
     * interface.
     */
    interface MethodOp {

        /** The integer value associated with the method. */
        int getValue();

        /** Returns the method call object associated with this operation. */
        MethodCall<?> readRequest(DataInput in, short serialVersion)
            throws IOException;
    }

    /**
     * An object that represents a call to a method on an asynchronous remote
     * interface, used to serialize and deserialize requests and responses.
     * Instances typically provide a constructor used by the client that
     * specifies the method arguments, and one that satisfies the
     * FastExternalizable interface for deserializing a request on the server.
     *
     * @param <R> the type of the response
     */
    interface MethodCall<R> extends FastExternalizable {

        /** Returns the associated MethodOp. */
        MethodOp getMethodOp();

        /** Writes the parameters for this request. */
        @Override
        void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException;

        /** Writes the specified response for this request. */
        void writeResponse(R response, DataOutput out, short serialVersion)
            throws IOException;

        /** Reads the response for this request. */
        R readResponse(DataInput in, short serialVersion)
            throws IOException;

        /**
         * Whether the method call includes the timeout value, in which case
         * it should not be supplied separately.
         */
        default boolean includesTimeout() {
            return false;
        }

        /**
         * Returns the timeout value that is included in the method call, if
         * present, or else throws IllegalStateException.
         */
        default long getTimeoutMillis() {
            throw new IllegalStateException(
                "This method call does not include a timeout: " + this);
        }

        /** Returns a string that describes this call. */
        String describeCall();
    }

    /**
     * A convenience base class for implementing method calls with Void
     * response types.
     */
    abstract class MethodCallVoid implements MethodCall<Void> {
        protected MethodCallVoid() { }
        @Override
        public @Nullable Void readResponse(final DataInput in,
                                           final short serialVersion) {
            return null;
        }
        @Override
        public void writeResponse(final @Nullable Void response,
                                  final DataOutput out,
                                  final short serialVersion) {
        }
    }

    /** A common base class for implementing getSerialVersion method calls. */
    abstract class AbstractGetSerialVersionCall implements MethodCall<Short> {
        protected AbstractGetSerialVersionCall() { }
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion) { }
        @Override
        public Short readResponse(final DataInput in,
                                  final short serialVersion)
            throws IOException
        {
            return in.readShort();
        }
        @Override
        public void writeResponse(final @Nullable Short response,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            out.writeShort(checkNull("response", response));
        }
    }

    /**
     * Gets the serial version of the call responder.  Queries the server for
     * its serial version and returns the result as a future. <p>
     *
     * If the server does not support the client version, then the future will
     * complete exceptionally with an {@link UnsupportedOperationException}.
     *
     * @param serialVersion the serial version of the call initiator
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @return a future that returns the serial version
     */
    CompletableFuture<Short> getSerialVersion(short serialVersion,
                                              long timeoutMillis);
}
