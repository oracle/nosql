/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.TestUtils.checkFastSerialize;
import static oracle.kv.impl.util.TestUtils.checkSerialize;
import static oracle.kv.util.TestUtils.checkAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.SerialVersion;

/**
 * Tests Version class in isolation.
 */
public class VersionTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    /**
     * Tests Version.toByteArray, fromByteArray, writeFastExternal, and
     * standard serialization.
     */
    @Test
    public void testSerialization()
        throws Exception {

        final UUID uuid = UUID.randomUUID();
        final long vlsn = 123456789;
        final long lsn = 0x1234567812345678L;
        final RepNodeId repNodeId = new RepNodeId(1234, 1234);

        Version v1 = new Version(uuid, vlsn);
        byte[] bytes = v1.toByteArray();
        Version v2 = Version.fromByteArray(bytes);
        assertEquals(v1, v2);
        checkSerialize(v1);
        checkFastSerialize(v1, Version::createVersion);

        v1 = new Version(uuid, vlsn, repNodeId, lsn);
        bytes = v1.toByteArray();
        v2 = Version.fromByteArray(bytes);
        assertEquals(v1, v2);
        checkSerialize(v1);
        checkFastSerialize(v1, Version::createVersion);

        /* check that corrupted byte[] throws IAE */
        bytes[0] = 0;
        try {
            v2 = Version.fromByteArray(bytes);
            fail("IAE should have been thrown");
        } catch (IllegalArgumentException iae) {
            // success
        } catch (Exception e) {
            fail("IAE should have been thrown, not " + e.getClass());
        }

        /* use null bytes */
        try {
            v2 = Version.fromByteArray(null);
            fail("IAE should have been thrown");
        } catch (IllegalArgumentException iae) {
            // success
        } catch (Exception e) {
            fail("IAE should have been thrown, not " + e.getClass());
        }
    }

    /**
     * Tests Version.toByteArray
     */
    @Test
    public void testToByteArray1() {
        final UUID uuid = UUID.randomUUID();
        final long vlsn = 123456789;

        Version v1 = new Version(uuid, vlsn);

        // old Version.toByteArray() implementation
        final ByteArrayOutputStream baos1 = new ByteArrayOutputStream(50);
        try {
            final ObjectOutputStream oos1 = new ObjectOutputStream(baos1);

            oos1.writeShort(SerialVersion.CURRENT);
            v1.writeFastExternal(oos1, SerialVersion.CURRENT);

            oos1.flush();
            baos1.flush();
        } catch (IOException e) {
            /* Should never happen. */
            Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
        }

        byte[] expected1 = baos1.toByteArray();

        // current Version.toByteArray() implementation
        byte[] actual1 = v1.toByteArray();

        Assert.assertArrayEquals("Version.toByteArray() doesn't match old " +
                "implementation.", expected1, actual1);
    }

    /**
     * Tests Version.toByteArray
     */
    @Test
    public void testToByteArray2() {
        final UUID uuid = UUID.randomUUID();
        final long vlsn = 123456789;
        final long lsn = 0x1234567812345678L;
        final RepNodeId repNodeId = new RepNodeId(1234, 1234);

        Version v2 = new Version(uuid, vlsn, repNodeId, lsn);

        // old Version.toByteArray() implementation
        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream(50);
        try {
            final ObjectOutputStream oos2 = new ObjectOutputStream(baos2);

            oos2.writeShort(SerialVersion.CURRENT);
            v2.writeFastExternal(oos2, SerialVersion.CURRENT);

            oos2.flush();
            baos2.flush();
        } catch (IOException e) {
            /* Should never happen. */
            Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
        }

        byte[] expected2 = baos2.toByteArray();

        // current Version.toByteArray() implementation
        byte[] actual2 = v2.toByteArray();

        Assert.assertArrayEquals("Version.toByteArray() doesn't match old " +
            "implementation.", expected2, actual2);
    }

    @Test
    public void testSerialVersion() {
        checkAll(
            Stream.of(
                serialVersionChecker(
                    new Version(new UUID(1, 2), 3, null /* repNodeId */, 0),
                    0xf89f47b27b86f7edL),
                serialVersionChecker(
                    new Version(new UUID(1, 2), 3, new RepNodeId(4, 5), 6),
                    0x3c91dd861ea82af3L))
            .map(svc -> svc.reader(Version::createVersion)));
    }
}
