/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.kerberos.KerberosConfig.StoreKrbConfiguration;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

public class KerberosContextTest extends KerberosTestBase {

    private static final String SERVICE_PRINCIPAL = "oraclenosql/localhost";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        startKdc();
    }

    @Override
    public void tearDown() throws Exception {
        stopKdc();
    }

    @Test
    public void testGetContext() throws Exception {
        final File secDir = new File(TestUtils.getTestDir(), "security");
        assertTrue(secDir.mkdir());
        final File keytab = new File(secDir, "store.keytab");
        addPrincipal(keytab, SERVICE_PRINCIPAL);

        final SecurityParams secParams =
            makeSecurityParams(secDir, keytab, "localhost" /* instance name */);
        KerberosContext.setConfiguration(new StoreKrbConfiguration(secParams));
        KerberosContext.createContext();

        /* Context should contains a subject with Kerberos principal and keys */
        final Subject subj = KerberosContext.getContext().getSubject();
        assertEquals(1, subj.getPrincipals().size());
        assertEquals(KerberosPrincipal.class,
                     subj.getPrincipals().iterator().next().getClass());
        assertEquals(SERVICE_PRINCIPAL + "@" + getRealm(),
                     subj.getPrincipals().iterator().next().getName());

        for (KerberosTicket ticket :
             subj.getPrivateCredentials(KerberosTicket.class)) {
            assertEquals(SERVICE_PRINCIPAL + "@" + getRealm(),
                         ticket.getClient().getName());
        }

        /* Test if subject can be reused by context */
        KerberosContext.runWithContext(
            subj,
            () -> {
                KerberosContext.setConfiguration(
                    new StoreKrbConfiguration(secParams));
                KerberosContext.getCurrentContext();
                return null;
            });
        assertEquals(subj, KerberosContext.getContext().getSubject());
    }
}
