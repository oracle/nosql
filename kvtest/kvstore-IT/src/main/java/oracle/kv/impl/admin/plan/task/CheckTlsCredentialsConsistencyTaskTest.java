/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan.task;

import static oracle.kv.impl.admin.plan.task.CheckTlsCredentialsConsistencyTask.checkConsistent;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes.HashInfo;
import oracle.kv.impl.topo.StorageNodeId;

import org.junit.Test;

public class CheckTlsCredentialsConsistencyTaskTest extends TestBase {

    @Test
    public void testCheckConsistent() {
        assertEquals(null,
                     checkConsistent(
                         Map.of(new StorageNodeId(1),
                                new HashInfo("1111111111", false)),
                         "keystore"));
        assertEquals(null,
                     checkConsistent(
                         Map.of(new StorageNodeId(1),
                                new HashInfo("1111111111", false),
                                new StorageNodeId(2),
                                new HashInfo("1111111111", false),
                                new StorageNodeId(3),
                                new HashInfo("1111111111", false)),
                         "keystore"));
        assertEquals(null,
                     checkConsistent(
                         Map.of(new StorageNodeId(1),
                                new HashInfo("1111111111", false),
                                new StorageNodeId(2),
                                new HashInfo("1111111111", true),
                                new StorageNodeId(3),
                                new HashInfo("1111111111", false)),
                         "keystore"));
        assertEquals("Updates for keystore files are required to make" +
                     " installed files consistent: " +
                     "{sn1=1111111111/install," +
                     " sn2=2222222222/install," +
                     " sn3=3333333333/install}",
                     checkConsistent(
                         Map.of(new StorageNodeId(1),
                                new HashInfo("1111111111", false),
                                new StorageNodeId(2),
                                new HashInfo("2222222222", false),
                                new StorageNodeId(3),
                                new HashInfo("3333333333", false)),
                         "keystore"));
        assertEquals("Updates for keystore files are inconsistent: " +
                     "{sn1=1111111111/install," +
                     " sn2=2222222222/update," +
                     " sn3=3333333333/update}",
                     checkConsistent(
                         Map.of(new StorageNodeId(1),
                                new HashInfo("1111111111", false),
                                new StorageNodeId(2),
                                new HashInfo("2222222222", true),
                                new StorageNodeId(3),
                                new HashInfo("3333333333", true)),
                         "keystore"));
        assertEquals("Updates for truststore files are required for SNs: " +
                     "sn1, sn2: " +
                     "{sn1=1111111111/install," +
                     " sn2=2222222222/install," +
                     " sn3=3333333333/update," +
                     " sn4=3333333333/update}",
                     checkConsistent(
                         Map.of(new StorageNodeId(1),
                                new HashInfo("1111111111", false),
                                new StorageNodeId(2),
                                new HashInfo("2222222222", false),
                                new StorageNodeId(3),
                                new HashInfo("3333333333", true),
                                new StorageNodeId(4),
                                new HashInfo("3333333333", true)),
                         "truststore"));
    }
}
