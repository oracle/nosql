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

package oracle.kv.impl.security.pwchecker;

/**
 * The result of password check.
 */
public class PasswordCheckerResult {

    /**
     * Whether the check is passed.
     */
    private final boolean passed;

    /**
     * The message returned from a check. It is up to the checker rule to
     * control what's the text to inform user in the message. It could be a
     * pass message or fail message. The message could be null in some fail
     * cases.
     */
    private final String message;

    public PasswordCheckerResult(boolean isPassed, String message) {
        this.passed = isPassed;
        this.message = message;
    }

    public boolean isPassed() {
        return passed;
    }

    public String getMessage() {
        return message;
    }
}
