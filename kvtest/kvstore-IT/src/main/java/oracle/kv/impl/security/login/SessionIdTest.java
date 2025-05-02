/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import oracle.kv.TestBase;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test the SessionId API.
 */
public class SessionIdTest extends TestBase {

    /* A valid session id value length that is used for most testing */
    private static final int VALID_SID_LEN = 20;

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    @SuppressWarnings("unused")
    @Test
    public void testPersistent()
        throws Exception {

        /* verify session id length checking */
        try {
            new SessionId(new byte[SessionId.SESSION_ID_MAX_SIZE + 1]);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("sessionId length"));
        }

        /* verify max session id is ok */
        new SessionId(new byte[SessionId.SESSION_ID_MAX_SIZE]);

        final byte[] sidVal = new byte[VALID_SID_LEN];
        for (int i = 0; i < sidVal.length; i++) {
            sidVal[i] = (byte) i;
        }

        final SessionId sid1 = new SessionId(sidVal);

        assertNull(sid1.getAllocator());
        assertEquals(IdScope.PERSISTENT, sid1.getIdValueScope());
        final byte[] sidVal1 = sid1.getIdValue();
        assertNotSame(sidVal, sidVal1);
        assertEquals(sidVal.length, sidVal1.length);
        assertArrayEquals(sidVal, sidVal1);

        /* check serialization */
        final SessionId sid2 = TestUtils.serialize(sid1);

        assertNull(sid2.getAllocator());
        assertEquals(IdScope.PERSISTENT, sid2.getIdValueScope());
        final byte[] sidVal2 = sid2.getIdValue();
        assertNotSame(sidVal, sidVal2);
        assertEquals(sidVal.length, sidVal2.length);
        assertArrayEquals(sidVal, sidVal2);

        /* check fast serialization */
        final SessionId sid4 = TestUtils.fastSerialize(sid1);

        assertNull(sid4.getAllocator());
        assertEquals(IdScope.PERSISTENT, sid4.getIdValueScope());
        final byte[] sidVal4 = sid4.getIdValue();
        assertNotSame(sidVal, sidVal4);
        assertEquals(sidVal.length, sidVal4.length);
        assertArrayEquals(sidVal, sidVal4);
    }

    @Test
    @SuppressWarnings("unused")
    public void testNonPersistent()
        throws Exception {

        /* verify session id length checking */
        try {
            new SessionId(new byte[SessionId.SESSION_ID_MAX_SIZE + 1],
                          IdScope.LOCAL,
                          new StorageNodeId(1));
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("sessionId length"));
        }

        /* verify max session id is ok */
        new SessionId(new byte[SessionId.SESSION_ID_MAX_SIZE],
                      IdScope.LOCAL,
                      new StorageNodeId(1));

        /* Check scope */
        try {
            new SessionId(new byte[VALID_SID_LEN],
                          IdScope.PERSISTENT,
                          new StorageNodeId(1));
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("invalid scope"));
        }

        for (IdScope scope : EnumSet.of(IdScope.LOCAL, IdScope.STORE)) {
            final String ctx = "Scope: " + scope;
            final byte[] sidVal = new byte[VALID_SID_LEN];
            for (int i = 0; i < sidVal.length; i++) {
                sidVal[i] = (byte) i;
            }
            final ResourceId rid = new StorageNodeId(17);
            final SessionId sid1 = new SessionId(sidVal, scope, rid);

            assertEquals(ctx, scope, sid1.getIdValueScope());
            assertEquals(ctx, rid, sid1.getAllocator());
            final byte[] sidVal1 = sid1.getIdValue();
            assertNotSame(ctx, sidVal, sidVal1);
            assertEquals(ctx, sidVal.length, sidVal1.length);
            assertArrayEquals(ctx, sidVal, sidVal1);

            /* serialization */
            final SessionId sid2 = TestUtils.serialize(sid1);

            assertEquals(ctx, rid, sid2.getAllocator());
            final byte[] sidVal2 = sid2.getIdValue();
            assertNotSame(ctx, sidVal, sidVal2);
            assertEquals(ctx, sidVal.length, sidVal2.length);
            assertArrayEquals(ctx, sidVal, sidVal2);

            /* fast serialization */
            final SessionId sid4 = TestUtils.fastSerialize(sid1);

            assertEquals(ctx, rid, sid4.getAllocator());
            final byte[] sidVal4 = sid4.getIdValue();
            assertNotSame(ctx, sidVal, sidVal4);
            assertEquals(ctx, sidVal.length, sidVal4.length);
            assertArrayEquals(ctx, sidVal, sidVal4);
        }
    }
}
