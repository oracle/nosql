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

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;

/**
 * Define the interface for management of authenticator.
 * <p>
 * An authenticator is used for user authentication, each authentication method
 * must has its own authenticator factory implementation.
 */
public interface AuthenticatorFactory {

    /**
     * Generate an authenticator.
     *
     * @param secParams security parameters identifying the actual configuration
     *         of the authenticator of this authentication method.
     * @param globalParams global parameters identifying the global
     *         configuration of the authenticator.
     * @return an authenticator initialized based on given security parameter
     * @throws IllegalArgumentException if authenticator configurations cannot
     *         be found in given security parameter or environment does not
     *         match parameters.
     */
    Authenticator getAuthenticator(SecurityParams secParams,
                                   GlobalParams globalParams)
        throws IllegalArgumentException;
}
