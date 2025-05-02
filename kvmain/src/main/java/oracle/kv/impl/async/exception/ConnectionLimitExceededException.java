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

import oracle.kv.impl.async.dialog.ChannelDescription;

/**
 * This exception is thrown because the local endpoint has already started the
 * maximum amount of dialogs when the dialog is being started. This exception
 * corresponds to DialogLimitExceededException.
 *
 * This exception is somewhat different from other connection exceptions in that
 * the network is not in a failing state. However, we can still perceive this
 * exception as the connection being in a state unable to serve dialogs.
 */
public class ConnectionLimitExceededException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    private final int localMaxDlgs;

    /**
     * Constructs the exception.
     *
     * @param localMaxDlgs the local dialog limit
     * @param isHandshakeDone whether the handshake is done
     * @param channelDescription the description of the connection channel
     */
    public ConnectionLimitExceededException(
        int localMaxDlgs,
        boolean isHandshakeDone,
        ChannelDescription channelDescription)
    {
        super(false /* from remote */, isHandshakeDone, channelDescription,
              String.format(
                  "Dialog limit exceeded, the limit is %d", localMaxDlgs),
              null);
        this.localMaxDlgs = localMaxDlgs;
    }

    @Override
    public DialogException getDialogException(boolean hasSideEffect) {
        checkSideEffect(hasSideEffect);
        return new DialogLimitExceededException(localMaxDlgs);
    }

    /**
     * Returns {@code true} since the thhis host will probably be busy for a
     * while, no need to retry immediately.
     */
    @Override
    public boolean isPersistent() {
        return true;
    }

    /**
     * Checks for side effect.
     */
    @Override
    public void checkSideEffect(boolean hasSideEffect) {
        if (hasSideEffect) {
            throw new IllegalArgumentException(
                "This exception should have no side effects");
        }
    }

    /**
     * Return an {@link IllegalStateException} since this exception represents
     * an unexpected situation and does not include an underlying exception
     * cause. The layer managing the dialog, e.g., the request dispatcher,
     * should already be enforcing request limits, so if we get this exception
     * something has gone wrong.
     */
    @Override
    public Throwable getUserException() {
        return new IllegalStateException(getMessage(), this);
    }
}

