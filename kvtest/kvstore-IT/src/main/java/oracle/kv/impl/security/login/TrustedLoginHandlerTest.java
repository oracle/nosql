/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.TestBase;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ResourceId;

import org.junit.Test;

/**
 * Test the TrustedLoginHandler API.
 */
public class TrustedLoginHandlerTest extends TestBase {

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void testLoginInternalLocal() {

        final ResourceId rid = new AdminId(1);
        final TrustedLoginHandler tlh =
            new TrustedLoginHandler(rid, true /* localId */);

        final LoginResult lr = tlh.loginInternal("localhost");
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());
        assertEquals(IdScope.LOCAL,
                     lr.getLoginToken().getSessionId().getIdValueScope());
    }

    @Test
    public void testLoginInternalNonLocal() {

        final ResourceId rid = new AdminId(1);
        final TrustedLoginHandler tlh =
            new TrustedLoginHandler(rid, false /* localId */);

        final LoginResult lr = tlh.loginInternal("localhost");
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());
        assertEquals(IdScope.STORE,
                     lr.getLoginToken().getSessionId().getIdValueScope());
    }

    @Test
    public void testValidateLoginToken() {
        final ResourceId rid = new AdminId(1);
        final TrustedLoginHandler tlhNOP =
            new TrustedLoginHandler(rid, true /* localId */);
        final TrustedLoginHandler tlh =
            new TrustedLoginHandler(rid, true /* localId */);

        final LoginResult lr = tlh.loginInternal("localhost");

        /* Validate a token against the wrong handler */
        Subject subject = tlhNOP.validateLoginToken(lr.getLoginToken(), null);
        assertNull(subject);

        /* Validate a token against the right handler */
        subject = tlh.validateLoginToken(lr.getLoginToken(), null);
        assertNotNull(subject);
        final Set<Principal> principals = subject.getPrincipals();
        assertNotNull(principals);
        assertTrue(principals.contains(KVStoreRolePrincipal.INTERNAL));

        /* Now, log out the session and try again */
        tlh.logout(lr.getLoginToken());

        /* Validate a token against the right handler */
        subject = tlh.validateLoginToken(lr.getLoginToken(), null);
        assertNull(subject);
    }
}
