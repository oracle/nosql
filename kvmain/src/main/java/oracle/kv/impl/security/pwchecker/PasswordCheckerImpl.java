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

import java.util.ArrayList;
import java.util.List;

/**
 * Password checker implementation.
 *
 * Holds the password checker rules that need to be processed through.
 */
public class PasswordCheckerImpl implements PasswordChecker {

    /**
     * Rules to be checked.
     */
    private final List<PasswordCheckerRule> rules;

    public PasswordCheckerImpl() {
        rules = new ArrayList<PasswordCheckerRule>();
    }

    /**
     * Add a password checker rule into the password checker implementation.
     */
    void addCheckerRule(PasswordCheckerRule rule) {
        rules.add(rule);
    }

    /**
     * Check if given password violate the rules the checker contains, collects
     * the results messages from each check.
     */
    @Override
    public PasswordCheckerResult checkPassword(char[] password) {
        final StringBuilder sb = new StringBuilder();
        boolean isPassed = true;

        if (password == null || password.length == 0) {
            throw new IllegalArgumentException(
                "The password must be non-null and not empty");
        }

        for (PasswordCheckerRule pcr : rules) {
            final PasswordCheckerResult currentResult =
                pcr.checkPassword(password);
            if (!currentResult.isPassed()) {
                isPassed = false;
                sb.append(currentResult.getMessage());
            }
        }
        return new PasswordCheckerResult(isPassed, sb.toString());
    }
}
