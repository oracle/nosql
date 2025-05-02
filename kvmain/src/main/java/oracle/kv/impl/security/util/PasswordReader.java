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
package oracle.kv.impl.security.util;

import java.io.IOException;

/**
 * A simple interface for reading password input.  Implementations may choose
 * to delegate to the console or other sources.
 */
public interface PasswordReader {
    /**
     * Returns password from input reading.  May return null if fails to read
     * anything from input.
     */
    char[] readPassword(String prompt)
        throws IOException;
}

