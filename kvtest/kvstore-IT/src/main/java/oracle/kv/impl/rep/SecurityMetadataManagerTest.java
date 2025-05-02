/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.SecurityMetadataTest;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.KVRepTestConfig;

import org.junit.Test;

public class SecurityMetadataManagerTest extends RepNodeTestBase {

    private KVRepTestConfig config;
    private static final RepNodeId rnId = new RepNodeId(1, 1);
    private SecurityMetadataManager smm = null;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        /* Build a 1*1 kvstore */
        config = new KVRepTestConfig(this, 1 /* nDC */, 1 /* nSN */,
                                     1 /* RF */, 1 /* nPartition */);
        config.startupRNs();
    }

    @Override
    public void tearDown() throws Exception {

        if (smm != null) {
            smm.closeDbHandles();
            smm = null;
        }
        config.stopRNs();
        config = null;
        super.tearDown();
    }

    @SuppressWarnings("unused")
    @Test
    public void testInitialization() {
        RepNode rn = config.getRN(rnId);

        /* Null Repnode */
        try {
            new SecurityMetadataManager(null, kvstoreName, logger);
            fail("Expected NullpointerException");
        } catch (NullPointerException npe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Negative max change limit */
        try {
            new SecurityMetadataManager(rn, kvstoreName, -3, logger);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException npe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        smm = new SecurityMetadataManager(rn, kvstoreName, logger);

        /* Not opened securityMD database */
        try {
            smm.getSecurityMetadata();
            fail("Expected RNUnavailableException");
        } catch (RNUnavailableException rnue) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Initial state: empty metadata */
        smm.updateDbHandles(rn.getEnv(1));
        assertNull(smm.getSecurityMetadata());
    }

    @Test
    public void testUpdate() {
        RepNode rn = config.getRN(rnId);
        SecurityMetadata sm = null;
        smm = new SecurityMetadataManager(rn, kvstoreName, logger);
        smm.updateDbHandles(rn.getEnv(1));

        /* Test updating with null */
        try {
            smm.update(sm);
            fail("Expected NullPointerException");
        } catch (NullPointerException npe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Test updating data with mismatched store name */
        sm = new SecurityMetadata("foo");
        try {
            smm.update(sm);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ise) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        /* Verify updating with full metadata */
        sm = getTestSecurityMd();
        smm.update(sm.getCopy());
        SecurityMetadataTest.assertSecurityMDEquals(
            sm, smm.getSecurityMetadata());

        /* Verify updating using changes with existing sequence number */
        final int oldSeqNum = sm.getSequenceNumber();
        smm.update(sm.getChangeInfo(1));
        assertEquals(oldSeqNum, smm.getSecurityMetadata().getSequenceNumber());

        /* Bump the sequence number */
        sm.addUser(KVStoreUser.newInstance("u10"));
        sm.addUser(KVStoreUser.newInstance("u11"));
        sm.addUser(KVStoreUser.newInstance("u12"));
        sm.addRole(new RoleInstance("r10"));
        sm.addRole(new RoleInstance("r11"));
        sm.addRole(new RoleInstance("r12"));
        sm.addKerberosInstanceName("i1", new StorageNodeId(1));
        sm.addKerberosInstanceName("i2", new StorageNodeId(2));
        sm.addKerberosInstanceName("i3", new StorageNodeId(3));

        /* Verify updating with non-continuous sequence number */
        SecurityMetadata oldCopy = smm.getSecurityMetadata().getCopy();
        int newSeqNum = smm.update(sm.getChangeInfo(oldSeqNum + 2)); /* gap */
        assertEquals(oldSeqNum, newSeqNum);
        SecurityMetadataTest.assertSecurityMDEquals(
            oldCopy, smm.getSecurityMetadata());

        /* Verify updating with valid change info */
        newSeqNum = smm.update(sm.getCopy().getChangeInfo(oldSeqNum));
        assertEquals(newSeqNum, smm.getSecurityMetadata().getSequenceNumber());
        SecurityMetadataTest.assertSecurityMDEquals(
            sm, smm.getSecurityMetadata());
    }

    @Test
    public void testPrune() {
        RepNode rn = config.getRN(rnId);
        SecurityMetadata sm = getTestSecurityMd();
        int maxChanges = 17;
        smm = new SecurityMetadataManager(rn, kvstoreName, maxChanges, logger);
        smm.updateDbHandles(rn.getEnv(1));
        int originalSize = sm.getChanges().size();

        assertTrue(originalSize < maxChanges);

        /* Test no pruning in whole metadata updating */
        smm.update(sm.getCopy());
        assertEquals(
            originalSize, smm.getSecurityMetadata().getChanges().size());
        smm.closeDbHandles();

        /* Test no pruning during incremental updating */
        smm = new SecurityMetadataManager(rn, kvstoreName, maxChanges, logger);
        smm.updateDbHandles(rn.getEnv(1));
        smm.update(sm.getChangeInfo(1));
        assertEquals(smm.getSecurityMetadata().getSequenceNumber(), 16);
        for (int idx = sm.getSequenceNumber(); idx < 13; idx++) {
            SecurityMetadata updateSm = smm.getSecurityMetadata();
            final int currSeqNum = updateSm.getSequenceNumber();

            sm.addUser(KVStoreUser.newInstance("newUser" + idx));
            sm.addRole(new RoleInstance("newRole" + idx));
            sm.addKerberosInstanceName("newInstance", new StorageNodeId(1));
            assertTrue(sm.getSequenceNumber() != updateSm.getSequenceNumber());
            assertTrue(sm.getChanges().size() != updateSm.getChanges().size());

            smm.update(sm.getChangeInfo(currSeqNum));
            updateSm = smm.getSecurityMetadata();
            assertEquals(sm.getSequenceNumber(), updateSm.getSequenceNumber());
            assertEquals(sm.getChanges().size(), updateSm.getChanges().size());
        }

        /* Test pruning during incremental updating */
        for (int idx = 13; idx < 20; idx++) {
            SecurityMetadata updateSm = smm.getSecurityMetadata();
            final int currSeqNum = updateSm.getSequenceNumber();

            sm.addUser(KVStoreUser.newInstance("newUser" + idx));
            sm.addRole(new RoleInstance("newRole" + idx));
            sm.addKerberosInstanceName("newInstance", new StorageNodeId(idx));
            assertTrue(sm.getChanges().size() > maxChanges);
            assertTrue(sm.getSequenceNumber() != updateSm.getSequenceNumber());
            assertTrue(sm.getChanges().size() != updateSm.getChanges().size());

            smm.update(sm.getChangeInfo(currSeqNum));
            updateSm = smm.getSecurityMetadata();

            /*
             * Verify the metadata is updated to latest sequence number, but
             * the number of changes is no larger than maxChanges.
             */
            assertEquals(sm.getSequenceNumber(), updateSm.getSequenceNumber());
            assertEquals(maxChanges, updateSm.getChanges().size());
        }
        smm.closeDbHandles();

        /* Test pruning during full updating */
        smm = new SecurityMetadataManager(rn, kvstoreName, maxChanges, logger);
        smm.updateDbHandles(rn.getEnv(1));
        assertTrue(sm.getChanges().size() > maxChanges);
        smm.update(sm.getCopy());
        assertEquals(sm.getSequenceNumber(),
                     smm.getSecurityMetadata().getSequenceNumber());
        assertEquals(maxChanges, smm.getSecurityMetadata().getChanges().size());
    }

    /* Get a security metadata object with some data */
    private SecurityMetadata getTestSecurityMd() {
        SecurityMetadata sm = new SecurityMetadata(kvstoreName);
        sm.addUser(KVStoreUser.newInstance("u1"));
        sm.addRole(new RoleInstance("r1"));
        sm.addKerberosInstanceName("i1", new StorageNodeId(1));
        KVStoreUser toRemove = sm.addUser(KVStoreUser.newInstance("u3"));
        KVStoreUser toUpdate = sm.addUser(KVStoreUser.newInstance("u4"));
        sm.updateUser(toUpdate.getElementId(), KVStoreUser.newInstance("u5"));
        sm.addUser(KVStoreUser.newInstance("u6"));
        sm.removeUser(toRemove.getElementId());

        RoleInstance toRemoveRole = sm.addRole(new RoleInstance("r3"));
        RoleInstance toUpdateRole = sm.addRole(new RoleInstance("r4"));
        sm.updateRole(toUpdateRole.getElementId(), new RoleInstance("r5"));
        sm.addRole(new RoleInstance("r6"));
        sm.removeRole(toRemoveRole.getElementId());

        sm.addKerberosInstanceName("i3", new StorageNodeId(3));
        sm.addKerberosInstanceName("i4", new StorageNodeId(4));
        sm.removeKrbInstanceName(new StorageNodeId(3));
        return sm;
    }
}
