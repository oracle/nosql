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

package oracle.kv.impl.async.exception;

import java.io.IOException;

/**
 * This exception is thrown because the remote endpoint cannot create a
 * suitable handler for this dialog type.
 *
 * We follows the mechanism in the RequestDispatcherImpl which is designed
 * before async and treat this problem as a persistent error. Most likely,
 * however, this exception occurs when the client is requesting a dialog that
 * is of a correct type, but the server has not managed to register the handler
 * yet. Therefore, ideally, this problem should be handled differently than,
 * say, ConnectException, and the upper layer should backoff and retry. But
 * that is for future work.
 */
public class DialogNoSuchTypeException extends PersistentDialogException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     */
    public DialogNoSuchTypeException(String message) {
        super(false /* no side effect */,
              true /* from remote */,
              message,
              null /* no cause */);
    }

    /**
     * Return an {@link IOException} since this exception probably means that
     * the server is not yet available and does not include an underlying
     * exception cause.
     */
    @Override
    public Throwable getUserException() {
        return new IOException(getMessage(), this);
    }
}

