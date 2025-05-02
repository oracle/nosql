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

package oracle.kv.impl.fault;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.FastExternalizableException;

/**
 * A wrapper exception used to wrap known serializable java runtime exceptions
 * that the client is guaranteed to have on its classpath back to the client.
 * The exception is wrapped as its cause. The RequestDispatcher at the client
 * unwraps the <i>cause</i> and throws the wrapped exception. Note that this
 * type of wrapping is different from the wrapping done via a FaultException
 * which is passed "as is" back to the client.
 *
 * The prototypical example motivating this type of wrapping is an
 * IllegalArgumentException in instances where the check can only be performed
 * remotely on the RN and not locally at the client.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class WrappedClientException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    public WrappedClientException(RuntimeException wrappedException) {
        super(wrappedException);
    }

    /**
     * Creates an instance from the input stream.
     */
    public WrappedClientException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }
}
