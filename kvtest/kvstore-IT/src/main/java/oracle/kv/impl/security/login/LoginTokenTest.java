/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.util.EnumSet;

import oracle.kv.TestBase;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test the LoginToken API.
 */
public class LoginTokenTest extends TestBase {

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void testPersistent()
        throws Exception {
        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }

        final long et = 1234567890123456L;
        final LoginToken lt1 = new LoginToken(new SessionId(sid), et);

        assertNull(lt1.getSessionId().getAllocator());
        assertEquals(IdScope.PERSISTENT,
                     lt1.getSessionId().getIdValueScope());
        final byte[] sid1 = lt1.getSessionId().getIdValue();
        assertFalse(sid == sid1);
        assertEquals(sid.length, sid1.length);
        assertArrayEquals(sid, sid1);
        assertEquals(et, lt1.getExpireTime());

        /* check serialization */
        final LoginToken lt2 = TestUtils.serialize(lt1);

        assertNull(lt2.getSessionId().getAllocator());
        assertEquals(IdScope.PERSISTENT,
                     lt2.getSessionId().getIdValueScope());
        final byte[] sid2 = lt2.getSessionId().getIdValue();
        assertNotSame(sid, sid2);
        assertEquals(sid.length, sid2.length);
        assertArrayEquals(sid, sid2);
        assertEquals(et, lt2.getExpireTime());

        /* fromByteArray */
        final byte[] bytes = lt1.toByteArray();
        final LoginToken lt3 = LoginToken.fromByteArray(bytes);

        assertNull(lt3.getSessionId().getAllocator());
        assertEquals(IdScope.PERSISTENT,
                     lt3.getSessionId().getIdValueScope());
        final byte[] sid3 = lt3.getSessionId().getIdValue();
        assertNotSame(sid, sid3);
        assertEquals(sid.length, sid3.length);
        assertArrayEquals(sid, sid3);
        assertEquals(et, lt3.getExpireTime());

        /* check fast serialization */
        final LoginToken lt4 = TestUtils.fastSerialize(lt1);

        assertNull(lt4.getSessionId().getAllocator());
        assertEquals(IdScope.PERSISTENT,
                     lt4.getSessionId().getIdValueScope());
        final byte[] sid4 = lt4.getSessionId().getIdValue();
        assertNotSame(sid, sid4);
        assertEquals(sid.length, sid4.length);
        assertArrayEquals(sid, sid4);
        assertEquals(et, lt4.getExpireTime());
    }

    @Test
    public void testNonPersistent()
        throws Exception {

        for (IdScope scope : EnumSet.of(IdScope.LOCAL, IdScope.STORE)) {
            final String ctx = "Scope: " + scope;
            final byte[] sid = new byte[20];
            for (int i = 0; i < sid.length; i++) {
                sid[i] = (byte) i;
            }
            final long et = 1234567890123456L;
            final ResourceId rid = new StorageNodeId(17);

            final LoginToken lt1 =
                new LoginToken(new SessionId(sid, scope, rid), et);

            assertEquals(ctx, scope, lt1.getSessionId().getIdValueScope());
            assertEquals(ctx, rid, lt1.getSessionId().getAllocator());
            final byte[] sid1 = lt1.getSessionId().getIdValue();
            assertNotSame(ctx, sid, sid1);
            assertEquals(ctx, sid.length, sid1.length);
            assertArrayEquals(ctx, sid, sid1);
            assertEquals(ctx, et, lt1.getExpireTime());

            /* serialization */
            final LoginToken lt2 = TestUtils.serialize(lt1);

            assertEquals(ctx, rid, lt2.getSessionId().getAllocator());
            final byte[] sid2 = lt2.getSessionId().getIdValue();
            assertNotSame(ctx, sid, sid2);
            assertEquals(ctx, sid.length, sid2.length);
            assertArrayEquals(ctx, sid, sid2);
            assertEquals(ctx, et, lt2.getExpireTime());

            /* toByteArray */
            final byte[] bytes = lt1.toByteArray();
            final LoginToken lt3 = LoginToken.fromByteArray(bytes);

            assertEquals(ctx, rid, lt3.getSessionId().getAllocator());
            final byte[] sid3 = lt3.getSessionId().getIdValue();
            assertNotSame(ctx, sid, sid3);
            assertEquals(ctx, sid.length, sid3.length);
            assertArrayEquals(ctx, sid, sid3);
            assertEquals(ctx, et, lt3.getExpireTime());

            /* fast serialization */
            final LoginToken lt4 = TestUtils.fastSerialize(lt1);

            assertEquals(ctx, rid, lt4.getSessionId().getAllocator());
            final byte[] sid4 = lt4.getSessionId().getIdValue();
            assertNotSame(ctx, sid, sid4);
            assertEquals(ctx, sid.length, sid4.length);
            assertArrayEquals(ctx, sid, sid4);
            assertEquals(ctx, et, lt4.getExpireTime());
        }
    }
}
