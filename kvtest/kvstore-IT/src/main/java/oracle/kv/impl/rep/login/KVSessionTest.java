/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.login;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.TestBase;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.login.LoginSession;

import org.junit.Test;


/**
 * This is a basic test to make sure that KVSession objects can be serialized
 * correctly.
 */
public class KVSessionTest extends TestBase {

    static final String USER_NAME = "Freddy";
    static final String USER_ID = "u32";
    static final String HOST = "localhost";

    /*
     * Test that we can serialize out and back
     */
    @Test
    public void testBasic()
        throws Exception {

        final LoginSession ls = makeLoginSession();

        final KVSession sess1 = new KVSession(ls);
        byte[] serialized = sess1.toByteArray();
        final KVSession sess2 = KVSession.fromByteArray(serialized);
        final LoginSession ls2 = sess2.makeLoginSession();
        assertEquals(ls.getId(), ls2.getId());
        assertEquals(ls.getExpireTime(), ls2.getExpireTime());
    }

    /*
     * Test that we can serialize in and back out when we are given a
     * Version+1 form serialized object.
     */
    @Test
    public void testNextVersion()
        throws Exception {

        final LoginSession ls = makeLoginSession();

        final KVSession sess = new KVSession(ls);
        /* Cheat so that we mark this as NEXT_VERSION */
        sess.setVersion((byte) KVSession.NEXT_VERSION);
        final byte[] serialized = sess.toByteArray();

        /* we will add a second copy of the serialized form */
        final byte[] extended = Arrays.copyOf(serialized,
                                              2 * serialized.length);
        System.arraycopy(serialized, 0, extended, serialized.length,
                         serialized.length);

        final KVSession sess2 = KVSession.fromByteArray(extended);
        final byte[] extended2 = sess2.toByteArray();

        assertEquals(extended.length, extended2.length);
        assertArrayEquals(extended, extended2);

        final LoginSession ls2 = sess2.makeLoginSession();
        assertEquals(ls.getId(), ls2.getId());
        assertEquals(ls.getExpireTime(), ls2.getExpireTime());
    }

    /*
     * Make sure that if we get a v1 object with too much data, that we
     * complain.
     */
    @Test
    public void testTooMuchData()
        throws Exception {

        final LoginSession ls = makeLoginSession();

        final KVSession sess = new KVSession(ls);
        final byte[] serialized = sess.toByteArray();

        /* we will add a byte to the end of the serialized form */
        final byte[] extended = Arrays.copyOf(serialized,
                                              serialized.length + 1);

        try {
            KVSession.fromByteArray(extended);
            fail("expected exception");
        } catch (IOException ioe) {
        }
    }

    /*
     * Make sure that if we get a v1 object with too little data, that we
     * complain.
     */
    @Test
    public void testTooLittleData()
        throws Exception {

        final LoginSession ls = makeLoginSession();

        final KVSession sess = new KVSession(ls);
        final byte[] serialized = sess.toByteArray();
        final byte[] shortened = Arrays.copyOf(serialized,
                                               serialized.length - 1);

        try {
            KVSession.fromByteArray(shortened);
            fail("expected exception");
        } catch (IOException ioe) {
        }
    }

    private LoginSession makeLoginSession() {
        /* Make a subject with known content */
        final Set<Principal> princs = new HashSet<Principal>();
        princs.add(new KVStoreUserPrincipal(USER_NAME, USER_ID));
        princs.add(KVStoreRolePrincipal.PUBLIC);
        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();
        final Subject subj =
            new Subject(true, princs, publicCreds, privateCreds);

        /* Make a session id */
        final byte[] sid = new byte[13];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) (i + 27);
        }
        final LoginSession.Id id = new LoginSession.Id(sid);
        final LoginSession ls = new LoginSession(id, subj, HOST,
                                                 true /* persistent */);
        final long later = System.currentTimeMillis() + 3600*1000L;
        ls.setExpireTime(later);

        return ls;
    }
}
