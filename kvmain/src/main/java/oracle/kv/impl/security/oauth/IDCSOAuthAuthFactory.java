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

package oracle.kv.impl.security.oauth;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.Authenticator;
import oracle.kv.impl.security.AuthenticatorFactory;

/**
 * Factory for IDCS OAuth2 implementation of authenticator.
 */
public class IDCSOAuthAuthFactory implements AuthenticatorFactory {

    @Override
    public Authenticator getAuthenticator(SecurityParams secParams,
                                          GlobalParams globalParams)
        throws IllegalArgumentException {

        return new IDCSOAuthAuthenticator(secParams, globalParams);
    }
}
