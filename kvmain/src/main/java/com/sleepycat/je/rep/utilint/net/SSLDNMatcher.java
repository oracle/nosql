/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.utilint.net;

import static java.util.logging.Level.FINE;

import java.security.Principal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.InstanceParams;

/**
 * This is an implementation of SSLAuthenticator which authenticates based
 * on the Distinguished Name (DN) in the SSL peer's certificate.  Matching
 * is done using Java regular expressions against the RFC1779-formatted DN.
 * This is typically used to match against the CN portion of the name.
 */

class SSLDNMatcher {

    private final Pattern pattern;
    private final InstanceLogger logger;

    /**
     * Construct an SSLDNMatcher
     *
     * @param params The instantiation params.  The classParams must be
     * a pattern to be matched to a Distinguished Name in an SSL certificate.
     * The match pattern must be a valid Java regular expression.
     * @throws IllegalArgumentException if the pattern is not a valid
     * regular expression
     */
    SSLDNMatcher(InstanceParams params)
        throws IllegalArgumentException {

        this.pattern = compileRegex(params.getClassParams());
        this.logger =
            params.getContext().getLoggerFactory().getLogger(getClass());
    }

    /*
     * Check whether the peer certificate matches the configured expression.
     */
    public boolean peerMatches(SSLSession sslSession) {
        Principal principal = null;
        try {
            principal = sslSession.getPeerPrincipal();
        } catch (SSLPeerUnverifiedException pue) {
            logger.log(
                FINE,
                () -> String.format("Error get peer principal: %s", pue));
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

        /* assing a final to use in lambda */
        final Principal p = principal;
        logger.log(
            FINE,
            () -> String.format("Principal does not match: " +
                                "pattern=%s, principal=%s",
                                pattern, p));
        return false;
    }

    private static Pattern compileRegex(String regex)
        throws IllegalArgumentException {
        try {
            return Pattern.compile(regex);
        } catch(PatternSyntaxException pse) {
            throw new IllegalArgumentException(
                "pattern is invalid", pse);
        }
    }

    static void validateRegex(String regex)
        throws IllegalArgumentException {

        /* ignore the result */
        compileRegex(regex);
    }
}


