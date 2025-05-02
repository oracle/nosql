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
package oracle.kv.impl.security;

import java.io.IOException;

/**
 * An exception indicating that there is an error accessing a password store.
 */
public class PasswordStoreException extends IOException {

    private static final long serialVersionUID = 1L;

    public PasswordStoreException(String message) {
        super(message);
    }

    public PasswordStoreException(Throwable cause) {
        super(cause);
    }

    public PasswordStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
