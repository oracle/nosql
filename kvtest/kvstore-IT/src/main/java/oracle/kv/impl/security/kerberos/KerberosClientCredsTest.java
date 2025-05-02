/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.security.auth.Subject;

import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.login.KerberosClientCreds.KrbServicePrincipals;

import org.junit.Test;

/**
 * Test building client Kerberos credentials.
 */
public class KerberosClientCredsTest {

    private static final String HOST_PRINC_PAIR =
        "nosql1:oraclenosql/nosql1.example.com, " +
        "nosql2:oraclenosql/nosql2.example.com";
    private static final String HOST_PRINC_PAIR_WITH_REALM =
        "nosql1:oraclenosql/nosql1.example.com@EXAMPLE.COM, " +
        "nosql2:oraclenosql/nosql2.example.com@EXAMPLE.COM";
    private static final String USER_PRINC = "krbuser@EXAMPLE.COM";

    @Test
    public void testBasic() {
        final Subject subj = new Subject();
        final boolean mutualAuth = false;
        KerberosClientCreds creds;

        /* Test building credentials with null value */
        try {
             creds = new KerberosClientCreds(null,
                                             subj,
                                             HOST_PRINC_PAIR,
                                             mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("username must not be null",
                       iae.getMessage(),
                       containsString("username argument must not be null"));
        }
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            null,
                                            HOST_PRINC_PAIR,
                                            mutualAuth);
           fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("subject must not be null", iae.getMessage(),
                       containsString("subject must not be null"));
        }

        /* Test building credentials with invalid host principal pair */
        String invalidHostPrincPair = "abc:   ";
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            subj,
                                            invalidHostPrincPair,
                                            mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("Invalid pair",
                       iae.getMessage(), containsString("Invalid pair of"));
        }
        invalidHostPrincPair = "abc:abc.oracle.com, cbd:   ";
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            subj,
                                            invalidHostPrincPair,
                                            mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("Invalid pair",
                       iae.getMessage(), containsString("Invalid pair of"));
        }

        /* Test host principal pairs in wrong pattern */
        invalidHostPrincPair = "abc:abc.oracle.com;cbd:";
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            subj,
                                            invalidHostPrincPair,
                                            mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("Invalid pattern",
                       iae.getMessage(),
                       containsString("does not match the pattern"));
        }
        invalidHostPrincPair = "abc:abc.oracle.com:::";
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            subj,
                                            invalidHostPrincPair,
                                            mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("Invalid pattern",
                       iae.getMessage(),
                       containsString("does not match the pattern"));
        }

        /* Test principal have the same service name */
        invalidHostPrincPair = "host1:abc/oracle.com,host2:def/oracle.com";
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            subj,
                                            invalidHostPrincPair,
                                            mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("Invalid service name",
                       iae.getMessage(),
                       containsString("service name must be the same"));
        }

        /* Test principal in the same realm */
        invalidHostPrincPair = "host1:abc/oracle.com@EXAMPLE.COM," +
                               "host2:abc/oracle.com@EXAMPLE1.COM";
        try {
            creds = new KerberosClientCreds(USER_PRINC,
                                            subj,
                                            invalidHostPrincPair,
                                            mutualAuth);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertThat("Invalid realm name",
                       iae.getMessage(),
                       containsString("must specify the same realm"));
        }

        /* Test normal case with principals having empty realm */
        creds = new KerberosClientCreds(USER_PRINC,
                                        subj,
                                        HOST_PRINC_PAIR,
                                        mutualAuth);
        KrbServicePrincipals princs = creds.getKrbServicePrincipals();
        assertEquals(princs.getHelperhostPrincipals().size(), 2);
        assertEquals(princs.getPrincipal("nosql1"),
                     "oraclenosql/nosql1.example.com");
        assertEquals(princs.getPrincipal("nosql2"),
                     "oraclenosql/nosql2.example.com");
        assertNull(princs.getDefaultRealm());
        assertEquals(princs.getServiceName(), "oraclenosql");

        /* Test normal case with principals having realm */
        creds = new KerberosClientCreds(USER_PRINC,
                                        subj,
                                        HOST_PRINC_PAIR_WITH_REALM,
                                        mutualAuth);
        princs = creds.getKrbServicePrincipals();
        assertEquals(princs.getHelperhostPrincipals().size(), 2);
        assertEquals(princs.getPrincipal("nosql1"),
                     "oraclenosql/nosql1.example.com@EXAMPLE.COM");
        assertEquals(princs.getPrincipal("nosql2"),
                     "oraclenosql/nosql2.example.com@EXAMPLE.COM");
        assertEquals(princs.getDefaultRealm(), "EXAMPLE.COM");
        assertEquals(princs.getServiceName(), "oraclenosql");
    }

    @Test
    public void testAddPrincipal() {
        final Subject subj = new Subject();
        final boolean mutualAuth = false;
        KerberosClientCreds creds = new KerberosClientCreds(
            USER_PRINC, subj, HOST_PRINC_PAIR, mutualAuth);
        creds.addServicePrincipal("nosql3", "nosql3.example.com");
        KrbServicePrincipals princs = creds.getKrbServicePrincipals();
        assertEquals(princs.getPrincipal("nosql3"),
                     "oraclenosql/nosql3.example.com");

        /* Test add principal with empty instance */
        creds.addServicePrincipal("nosql4", "");
        princs = creds.getKrbServicePrincipals();
        assertEquals(princs.getPrincipal("nosql4"), "oraclenosql");

        /* Test add principal with null instance */
        creds.addServicePrincipal("nosql5", null);
        princs = creds.getKrbServicePrincipals();
        assertEquals(princs.getPrincipal("nosql5"), "oraclenosql");

        creds = new KerberosClientCreds(USER_PRINC, subj,
                                        HOST_PRINC_PAIR_WITH_REALM, mutualAuth);
        creds.addServicePrincipal("nosql3", "nosql3.example.com");
        princs = creds.getKrbServicePrincipals();
        assertEquals(princs.getPrincipal("nosql3"),
                     "oraclenosql/nosql3.example.com@EXAMPLE.COM");
    }
}
