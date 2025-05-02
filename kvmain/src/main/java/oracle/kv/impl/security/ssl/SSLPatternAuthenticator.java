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

package oracle.kv.impl.security.ssl;

import javax.net.ssl.SSLSession;

import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * SSL socket authentication implementation based on certificate DN pattern
 * match.
 */
public class SSLPatternAuthenticator
    extends SSLPatternMatcher
    implements SSLAuthenticator {

    /**
     * Construct an SSLAuthenticator that will verify peers based on a
     * match of the Distinguished Name in the peer certificate to the
     * configured pattern.
     * @param regexPattern a string that conforms to Java regular expression
     *    format rules
     */
    public SSLPatternAuthenticator(String regexPattern) {
        super(regexPattern);
    }

    /**
     * Verify that the peer should be trusted based on the configured DN
     * pattern match.
     */
    @Override
    public boolean isTrusted(SSLSession sslSession) {
        return super.verifyPeer(sslSession);
    }
}
