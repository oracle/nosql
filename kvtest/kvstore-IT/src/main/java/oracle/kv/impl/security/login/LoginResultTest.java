/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import oracle.kv.TestBase;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test the LoginResult API.
 */
public class LoginResultTest extends TestBase {

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
        final LoginResult lr1 = new LoginResult(lt);

        final LoginToken lt1 = lr1.getLoginToken();
        assertEquals(IdScope.PERSISTENT,
                     lt1.getSessionId().getIdValueScope());
        assertNull(lt1.getSessionId().getAllocator());
        final byte[] sid1 = lt1.getSessionId().getIdValue();
        assertArrayEquals(sid, sid1);
        assertEquals(et, lt1.getExpireTime());

        /* serialize it */
        final LoginResult lr2 = TestUtils.serialize(lr1);
        final LoginToken lt2 = lr2.getLoginToken();

        assertEquals(IdScope.PERSISTENT,
                     lt2.getSessionId().getIdValueScope());
        assertTrue(null == lt2.getSessionId().getAllocator());
        final byte[] sid2 = lt2.getSessionId().getIdValue();
        assertArrayEquals(sid, sid2);
        assertEquals(et, lt2.getExpireTime());

        /* fast serialize it */
        final LoginResult lr3 = TestUtils.fastSerialize(lr1);
        final LoginToken lt3 = lr3.getLoginToken();

        assertEquals(IdScope.PERSISTENT,
                     lt3.getSessionId().getIdValueScope());
        assertNull(lt3.getSessionId().getAllocator());
        final byte[] sid3 = lt3.getSessionId().getIdValue();
        assertArrayEquals(sid, sid3);
        assertEquals(et, lt3.getExpireTime());
    }
}
