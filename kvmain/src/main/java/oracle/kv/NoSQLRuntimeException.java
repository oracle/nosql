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

package oracle.kv;

import oracle.kv.impl.util.NoSQLMessagesResourceBundle;
import oracle.kv.util.ErrorMessage;

import java.text.MessageFormat;

/**
 * Generic exception class for generating runtime exceptions whose messages are
 * derived from a locale specific message file.
 *
 * @since 2.0
 */
public class NoSQLRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static NoSQLMessagesResourceBundle resourceBundle = null;
    private static final Object lock = new Object();

    private String message;

    /**
     * For internal use only.
     * @hidden
     */
    public NoSQLRuntimeException(final ErrorMessage messageKey) {
        synchronized (lock) {
            if (resourceBundle == null) {
                resourceBundle = new NoSQLMessagesResourceBundle();
            }
        }

        message = (String)
            resourceBundle.handleGetObject(messageEnumToKey(messageKey));
    }

    /**
     * For internal use only.
     * @hidden
     */
    public NoSQLRuntimeException(final ErrorMessage messageKey,
                                 Object ... args) {
        synchronized (lock) {
            if (resourceBundle == null) {
                resourceBundle = new NoSQLMessagesResourceBundle();
            }
        }

        message = MessageFormat.format
            ((String) resourceBundle.handleGetObject
             (messageEnumToKey(messageKey)), args);
    }

    @Override
    public String getMessage() {
        return message;
    }

    /**
     * Convert the supplied Enum into a String key that can be used for message
     * file lookup.
     *
     * @param messageKey The Enum to convert
     *
     * @return A token that can be used to look up the error message in the
     * messages file
     */
    private String messageEnumToKey(final ErrorMessage messageKey) {
        final String[] tokens = messageKey.name().split("_");
        return tokens[1];
    }
}
