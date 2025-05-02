/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import java.util.stream.Stream;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.TestBase;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/** Test serial version compatibility for the Request class */
public class RequestSerialTest extends TestBase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        Request.testReadSerialVersion = 0;
        Request.testWriteSerialVersion = 0;
    }

    /**
     * Test serialization of Request objects, checking objects with field
     * values that produce different serialized forms without worrying about
     * varying the sub-objects themselves, which can be tested separately.
     *
     * Factors tested include:
     * - Partition ID vs. shard ID
     * - Write vs. read
     * - Forwarding RNs
     * - Read zones
     */
    @Test
    public void testSerialVersion() {
        checkAll(
            Stream.of(
                /* Partition ID, read */
                serialVersionChecker(
                    new Request(new Get(new byte[] { 1, 2, 3 }),
                                new PartitionId(4),
                                false /* write */,
                                null /* durability */,
                                Consistency.ABSOLUTE,
                                5 /* ttl */,
                                6 /* topoSeqNumber */,
                                new ClientId(7) /* dispatcherId */,
                                8 /* timeoutMs */,
                                null /* readZoneIds */),
                    SerialVersion.MINIMUM, 0x479a6e8e21562239L),
                /* Shard ID, read, read zones */
                serialVersionChecker(
                    new Request(new Get(new byte[] { 1, 2, 3 }),
                                new RepGroupId(4),
                                false /* write */,
                                null /* durability */,
                                Consistency.ABSOLUTE,
                                5 /* ttl */,
                                6 /* topoSeqNumber */,
                                new ClientId(7) /* dispatcherId */,
                                8 /* timeoutMs */,
                                new int[] { 9, 10 } /* readZoneIds */),
                    SerialVersion.MINIMUM, 0x1cf4cadeb78c0019L),
                /* Partition ID, write */
                serialVersionChecker(
                    new Request(new Get(new byte[] { 1, 2, 3 }),
                                new PartitionId(4),
                                true /* write */,
                                Durability.COMMIT_NO_SYNC,
                                null /* consistency */,
                                5 /* ttl */,
                                6 /* topoSeqNumber */,
                                new ClientId(7) /* dispatcherId */,
                                8 /* timeoutMs */,
                                null /* readZoneIds */),
                    SerialVersion.MINIMUM, 0xc3e022e6fb9f353dL),
                /* Shard ID, write, forwarding RNs */
                serialVersionChecker(
                    addForwardingRNs(
                        new Request(new Get(new byte[] { 1, 2, 3 }),
                                    new RepGroupId(4),
                                    true /* write */,
                                    Durability.COMMIT_NO_SYNC,
                                    null /* consistency */,
                                    5 /* ttl */,
                                    6 /* topoSeqNumber */,
                                    new RepNodeId(7, 8) /* dispatcherId */,
                                    9 /* timeoutMs */,
                                    null /* readZoneIds */),
                        10, 11 /* forwarding RNs */),
                    SerialVersion.MINIMUM, 0xedf27fa00b0eb508L))
            /*
             * Serialize using the specified serial version but store 1 as the
             * serial version in the serialized form so that the bytes stay the
             * same over time.
             */
            .map(svc -> svc.writer((obj, out, sv) -> {
                        obj.setSerialVersion(sv);
                        Request.testWriteSerialVersion = 1;
                        obj.writeFastExternal(out, sv);
                    }))
            /* And reestablish the serial version for reading in */
            .map(svc -> svc.reader((in, sv) -> {
                        Request.testReadSerialVersion = sv;
                        return new Request(in);
                    })));
    }

    private static Request addForwardingRNs(Request request,
                                            int... forwardingRNs) {
        for (int forwardingRN : forwardingRNs) {
            request.updateForwardingRNs(new RepNodeId(forwardingRN, 1), 1);
        }
        return request;
    }
}
