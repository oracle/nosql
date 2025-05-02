/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.util.TestUtils.fastSerialize;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.util.SerialVersion;

/** Utilities for testing MethodCalls. */
public class MethodCallUtils {

    /**
     * Uses {@link MethodOp#readRequest} to deserialize a {@link MethodCall} in
     * order to check that the reader for the method op matches the method op
     * for the method call.
     *
     * @param <M> the type of the method call
     * @param call the method call
     * @return the deserialized method call
     * @throws IOException if there is a problem serializing or deserializing
     * the call
     */
    public static <M extends MethodCall<?>> M serializeMethodCall(M call)
        throws IOException
    {
        final MethodCall<?> deserialized = fastSerialize(
            call, (in, sv) -> call.getMethodOp().readRequest(in, sv));
        /*
         * Can't check the compile-time type, but fastSerialize will confirm
         * that the deserialized object has the same runtime type.
         */
        @SuppressWarnings("unchecked")
        final M result = (M) deserialized;
        return result;
    }

    /**
     * Checks that calling readResponse on the call to create a deserialized
     * copy of the specified response results an an object that is equal to the
     * specified response.
     */
    public static <T> void checkFastSerializeResponse(MethodCall<T> call,
                                                      T response)
        throws IOException
    {
        final Object result = fastSerializeResponse(call, response);
        assertEquals(response, result);
        assertEquals(Objects.hashCode(response), Objects.hashCode(result));
    }

    /**
     * Returns the result of calling readResponse on the call to create a
     * deserialized copy of the response.
     */
    public static <T> T fastSerializeResponse(MethodCall<T> call, T response)
        throws IOException
    {
        final Class<?> responseClass =
            (response == null) ? null : response.getClass();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(baos)) {
            call.writeResponse(response, out, SerialVersion.CURRENT);
        }

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(baos.toByteArray());
        try (final DataInputStream in = new DataInputStream(bais)) {
            final T result =
                call.readResponse(in, SerialVersion.CURRENT);
            assertEquals("Expected EOF after reading serialized object data",
                         -1, in.read());
            final Class<?> resultClass =
                (result == null) ? null : result.getClass();
            assertEquals(responseClass, resultClass);
            return result;
        }
    }
}
