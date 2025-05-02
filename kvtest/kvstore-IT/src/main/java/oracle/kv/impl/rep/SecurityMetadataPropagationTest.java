/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;

import org.junit.Test;

public class SecurityMetadataPropagationTest extends RepNodeTestBase {

    private KVRepTestConfig config;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        /* Build a 2*2 kvstore */
        config = new KVRepTestConfig(this, 1 /* nDC */, 2 /* nSN */,
                                     2 /* RF */, 10 /* nPartition */);
        config.startRepNodeServices();
    }

    @Override
    public void tearDown() throws Exception {

        config.stopRepNodeServices();
        config = null;
        super.tearDown();
    }

    @Test
    public void testPropagation() {
        final RepNode rns[] = {
            config.getRN(new RepNodeId(1, 1)),
            config.getRN(new RepNodeId(1, 2)),
            config.getRN(new RepNodeId(2, 1)),
            config.getRN(new RepNodeId(2, 2))
        };

        final SecurityMetadata secmd =
                new SecurityMetadata(kvstoreName, "md-test" /* id */);
        secmd.addUser(KVStoreUser.newInstance("testUser"));
        secmd.addRole(new RoleInstance("testRole"));
        secmd.addKerberosInstanceName("testInstance", new StorageNodeId(1));

        /* rg1rn1 initiates the propagation */
        rns[0].updateMetadata(secmd.getCopy());

        assertEquals(secmd.getSequenceNumber(),
                     (int)rns[0].getMetadataSeqNum(MetadataType.SECURITY));

        /* Check the new metadata shows up in all other rns */
        for (final RepNode rn : rns) {
            waitForUpdate(rn, secmd);
        }

        /* Bump the seq number, and let rg2rn1 initiate another propagation */
        secmd.addUser(KVStoreUser.newInstance("newTestUser"));
        rns[2].updateMetadata(secmd.getCopy());

        secmd.addRole(new RoleInstance("newTestRole"));
        rns[2].updateMetadata(secmd.getCopy());

        secmd.addKerberosInstanceName("newInstance", new StorageNodeId(2));
        rns[2].updateMetadata(secmd.getCopy());

        /* Check the new metadata shows up in all other rns */
        for (final RepNode rn : rns) {
            waitForUpdate(rn, secmd);
        }

        assertEquals(secmd.getSequenceNumber(),
            (int)rns[2].getMetadataSeqNum(MetadataType.SECURITY));
    }

    /* Polling to check the metadata on rn is updated or not */
    private void waitForUpdate(final RepNode rn, final Metadata<?> md) {
        boolean success = new PollCondition(500, 20000) {
            @Override
            protected boolean condition() {
                return rn.getMetadataSeqNum(md.getType()) >=
                        md.getSequenceNumber();
            }
        }.await();
        assertTrue(success);
    }
}
