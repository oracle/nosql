/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;

import org.junit.Test;

/** Test serial version compatibility */
public class DurabilitySerialTest extends TestBase {

    @Test
    public void testSerialVersion() throws Exception {
        checkAll(serialVersionChecker(Durability.COMMIT_SYNC,
                                      0xc7a623fd2bbc05bL),
                 serialVersionChecker(Durability.COMMIT_NO_SYNC,
                                      0x12a340d7897fe713L),
                 serialVersionChecker(Durability.COMMIT_WRITE_NO_SYNC,
                                      0x1c96044ca1e79a09L),
                 serialVersionChecker(new Durability(SyncPolicy.SYNC,
                                                     SyncPolicy.SYNC,
                                                     ReplicaAckPolicy.ALL),
                                      0x29e2dcfbb16f63bbL),
                 serialVersionChecker(new Durability(SyncPolicy.SYNC,
                                                     SyncPolicy.SYNC,
                                                     ReplicaAckPolicy.NONE),
                                      0x2547cc736e951fa4L));
    }
}
