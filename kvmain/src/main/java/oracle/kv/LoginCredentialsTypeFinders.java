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

import java.util.ArrayList;
import java.util.List;

import oracle.kv.LoginCredentials.LoginCredentialsType;
import oracle.kv.LoginCredentials.LoginCredentialsTypeFinder;
import oracle.kv.LoginCredentials.StdLoginCredentialsType;

/**
 * Class for registering and using LoginCredentialsTypeFinder instances.
 *
 * @hidden For internal use
 */
public class LoginCredentialsTypeFinders {
    private static final List<LoginCredentialsTypeFinder> finders =
        new ArrayList<>();
    static {
        addFinder(StdLoginCredentialsType::valueOf);
    }

    /** Registers a LoginCredentialsTypeFinder. */
    public static void addFinder(LoginCredentialsTypeFinder finder) {
        finders.add(finder);
    }

    /**
     * Uses all registered LoginCredentialsTypeFinders to find the
     * LoginCredentialsType associated with the specified integer value,
     * throwing an exception if not found.
     */
    static LoginCredentialsType findLoginCredentialsType(int intValue) {
        for (final LoginCredentialsTypeFinder finder : finders) {
            final LoginCredentialsType credentialsType =
                finder.getLoginCredentialsType(intValue);
            if (credentialsType != null) {
                return credentialsType;
            }
        }
        throw new IllegalArgumentException(
            "Wrong integer value for LoginCredentialsType: " + intValue);
    }
}
