/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static oracle.kv.impl.util.TestUtils.fastSerialize;
import static oracle.kv.impl.util.TestUtils.serialize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import oracle.kv.TestBase;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;

import org.junit.Test;

/**
 * Test the AuthContext API.
 */
public class AuthContextTest extends TestBase {

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void testBasic()
        throws Exception {
        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        final long et = 1234567890123456L;
        final LoginToken lt = new LoginToken(new SessionId(sid), et);

        final AuthContext sc1 = new AuthContext(lt);
        assertEqualLoginTokens(lt, sc1.getLoginToken());
        assertNull(sc1.getForwarderLoginToken());
        assertNull(sc1.getClientHost());

        final AuthContext sc2 = serialize(sc1);
        assertEqualLoginTokens(lt, sc2.getLoginToken());
        assertNull(sc2.getForwarderLoginToken());
        assertNull(sc2.getClientHost());

        final AuthContext sc3 = fastSerialize(sc1);
        assertEqualLoginTokens(lt, sc3.getLoginToken());
        assertNull(sc3.getForwarderLoginToken());
        assertNull(sc3.getClientHost());
    }

    @Test
    public void testForwarded()
        throws Exception {

        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        final long et = 1234567890123456L;
        final LoginToken lt = new LoginToken(new SessionId(sid), et);

        final byte[] fsid = new byte[20];
        for (int i = 0; i < fsid.length; i++) {
            fsid[i] = (byte) i;
        }
        final long fet = 2345678901234567L;
        final LoginToken flt = new LoginToken(new SessionId(fsid), fet);

        final String ch = "10.10.20.32";

        final AuthContext sc1 = new AuthContext(lt, flt, ch);
        assertEqualLoginTokens(lt, sc1.getLoginToken());
        assertEqualLoginTokens(flt, sc1.getForwarderLoginToken());
        assertEquals(ch, sc1.getClientHost());

        final AuthContext sc2 = serialize(sc1);
        assertEqualLoginTokens(lt, sc2.getLoginToken());
        assertEqualLoginTokens(flt, sc2.getForwarderLoginToken());
        assertEquals(ch, sc2.getClientHost());

        final AuthContext sc3 = fastSerialize(sc1);
        assertEqualLoginTokens(lt, sc3.getLoginToken());
        assertEqualLoginTokens(flt, sc3.getForwarderLoginToken());
        assertEquals(ch, sc3.getClientHost());
    }

    private static void assertEqualLoginTokens(LoginToken lt1,
                                               LoginToken lt2) {
        assertNotNull(lt2);
        assertArrayEquals(lt1.getSessionId().getIdValue(),
                          lt2.getSessionId().getIdValue());
    }
}
