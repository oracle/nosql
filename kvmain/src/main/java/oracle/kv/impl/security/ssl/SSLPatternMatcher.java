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

import java.security.Principal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * SSL socket authentication implementation based on certificate DN
 * pattern match.
 */
class SSLPatternMatcher {

    private final Pattern pattern;

    /**
     * Construct an pattern matcher that will verify peers based on a
     * match of the Distinguished Name in the peer certificate to the
     * configured pattern.
     *
     * @param regexPattern a string that conforms to Java regular expression
     *    format rules
     */
    SSLPatternMatcher(String regexPattern) {
        this.pattern = Pattern.compile(regexPattern);
    }

    boolean verifyPeer(SSLSession sslSession) {
        Principal principal = null;
        try {
            principal = sslSession.getPeerPrincipal();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (principal != null) {
            if (principal instanceof X500Principal) {
                final X500Principal x500Principal = (X500Principal) principal;
                final String name =
                    x500Principal.getName(X500Principal.RFC1779);
                final Matcher m = pattern.matcher(name);
                if (m.matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]",
                             getClass().getSimpleName(),
                             pattern);
    }
}
