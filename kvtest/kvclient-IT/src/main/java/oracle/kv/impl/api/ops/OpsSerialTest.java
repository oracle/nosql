/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static java.util.Collections.singletonList;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerialVersion.CLOUD_MR_TABLE;
import static oracle.kv.impl.util.SerialVersion.ROW_METADATA_VERSION;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.bulk.BulkPut.KVPair;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.TimeToLive;

import org.junit.Test;

/** Test serial version compatibility for the various non-table Ops classes. */
public class OpsSerialTest extends OpsSerialTestBase {
    private static final byte[] KEY_BYTES = { 2, 4, 6 };
    private static final byte[] VALUE_BYTES = { 13, 17 };
    private static final Value VALUE = Value.createValue(VALUE_BYTES);
    private static final int TABLE_ID = 8;
    private static final Version VERSION =
        new Version(new UUID(1, 2), 3, null, 0);
    private static final KeyRange KEY_RANGE =
        new KeyRange("a", false, "b", false);
    private static final int regionId = Integer.MAX_VALUE;

    @Test
    public void testDelete() {
        checkOps(serialVersionChecker(
                     new Delete(KEY_BYTES, Choice.VALUE),
                     SerialVersion.MINIMUM, 0x4c952989d59e23a1L,
                     CLOUD_MR_TABLE, 0xa0dada8a5d6bb4cbL,
                     ROW_METADATA_VERSION, 0x22fe249ac2907e8L),
                 serialVersionChecker(
                     new Delete(KEY_BYTES, Choice.ALL, TABLE_ID,
                                false /* doTombstone */, null /* rowMetadata */),
                     SerialVersion.MINIMUM, 0xc10e687be7c11fd7L,
                     CLOUD_MR_TABLE, 0xb5e3328ea7588e25L,
                     ROW_METADATA_VERSION, 0x57e5ff46962347ceL),
                 serialVersionChecker(
                     new Delete(KEY_BYTES, Choice.ALL, TABLE_ID,
                                true /* doTombstone */, null /* rowMetadata */),
                     CLOUD_MR_TABLE, 0x86c55edb51d0e544L,
                     ROW_METADATA_VERSION, 0x670ce448218a0a40L));
    }

    @Test
    public void testDeleteIfVersion() {
        checkOps(serialVersionChecker(
                     new DeleteIfVersion(KEY_BYTES, Choice.VALUE, VERSION),
                     SerialVersion.MINIMUM, 0xb6a2cf000c45c209L,
                     CLOUD_MR_TABLE, 0xc0d34f8c460721feL,
                     ROW_METADATA_VERSION, 0x7c9feb5f08881e80L),
                 serialVersionChecker(
                     new DeleteIfVersion(KEY_BYTES, Choice.VALUE, VERSION,
                                         TABLE_ID, false /* doTombstone */,
                             null /* rowMetadata */),
                     SerialVersion.MINIMUM, 0x971cbeda8ede03bL,
                     CLOUD_MR_TABLE, 0x79a3aef8e614aeffL,
                     ROW_METADATA_VERSION, 0x2511a05b1b53e6f7L),
                 serialVersionChecker(
                     new DeleteIfVersion(KEY_BYTES, Choice.VALUE, VERSION,
                                         TABLE_ID, true /* doTombstone */,
                                         null /* rowMetadata */),
                     CLOUD_MR_TABLE, 0xdac26edb2c1e6c91L,
                     ROW_METADATA_VERSION, 0x4672dca893599b70L));
    }

    @Test
    public void testExecute() {
        final Execute.OperationFactoryImpl factory =
            new Execute.OperationFactoryImpl(
                KeySerializer.PROHIBIT_INTERNAL_KEYSPACE);
        final List<Execute.OperationImpl> ops =
            Collections.singletonList(
                (Execute.OperationImpl)
                factory.createPut(Key.fromByteArray(KEY_BYTES), VALUE));
        checkOps(serialVersionChecker(
                     new Execute(ops, TABLE_ID),
                     0x197cb3ddf2a31a79L));
    }

    @Test
    public void testGet() {
        checkOps(serialVersionChecker(
                     new Get(KEY_BYTES),
                     SerialVersion.MINIMUM, 0x1a7a99f8d5c29558L),
                 serialVersionChecker(
                     new Get(KEY_BYTES, TABLE_ID,
                             true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x4d7ce0ccb7d7d1c3L),
                 serialVersionChecker(
                     new Get(KEY_BYTES, TABLE_ID,
                             false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x9653cc4fa79e10e7L));
    }

    @Test
    public void testGetIdentityAttrsAndValues() {
        checkOps(serialVersionChecker(
                     new GetIdentityAttrsAndValues(
                         KEY_BYTES,
                         1 /* curVersion */,
                         2 /* clientIdentityCacheSize */,
                         true /* needAttributes */,
                         false /* needNextSequence */,
                         "seqGen"),
                     0xcff5355c3c6a4b07L));
    }

    @Test
    public void testMultiDelete() {
        checkOps(serialVersionChecker(
                     new MultiDelete(KEY_BYTES,
                                     null /* keyRange */,
                                     null /* depth */,
                                     null /* lobSuffixBytes */),
                     0x94409e77c53d8382L),
                 serialVersionChecker(
                     new MultiDelete(KEY_BYTES, KEY_RANGE, Depth.CHILDREN_ONLY,
                                     new byte[] { 7, 8 } /* lobSuffixBytes */),
                     0x8d0d9703feefb79bL));

    }

    @Test
    public void testMultiGet() {
        checkOps(serialVersionChecker(
                     new MultiGet(KEY_BYTES,
                                  null /* subRange */,
                                  null /* depth */,
                                  true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x6442e213de1a1fefL),
                 serialVersionChecker(
                     new MultiGet(KEY_BYTES,
                                  KEY_RANGE /* subRange */,
                                  Depth.CHILDREN_ONLY,
                                  true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x8002a560ca3a24eeL),
                 serialVersionChecker(
                     new MultiGet(KEY_BYTES,
                                  KEY_RANGE /* subRange */,
                                  Depth.CHILDREN_ONLY,
                                  false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x5e1b0f323de5963eL));
    }

    @Test
    public void testMultiGetBatchIterate() {
        checkOps(serialVersionChecker(
                     new MultiGetBatchIterate(singletonList(KEY_BYTES),
                                              null /* resumeKey */,
                                              null /* subRange */,
                                              null /* depth */,
                                              1 /* batchSize */,
                                              true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x863daffadff4ac67L),
                 serialVersionChecker(
                     new MultiGetBatchIterate(
                         singletonList(KEY_BYTES),
                         new byte[] { 1, 2 } /* resumeKey */,
                         KEY_RANGE /* subRange */,
                         Depth.CHILDREN_ONLY /* depth */,
                         1 /* batchSize */,
                         true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x6a53011e2f18cb2L),
                 serialVersionChecker(
                     new MultiGetBatchIterate(
                         singletonList(KEY_BYTES),
                         new byte[] { 1, 2 } /* resumeKey */,
                         KEY_RANGE /* subRange */,
                         Depth.CHILDREN_ONLY /* depth */,
                         1 /* batchSize */,
                         false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x7899c3227b1fe3b0L));
    }

    @Test
    public void testMultiGetBatchKeysIterate() {
        checkOps(serialVersionChecker(
                     new MultiGetBatchKeysIterate(singletonList(KEY_BYTES),
                                                  null /* resumeKey */,
                                                  null /* subRange */,
                                                  null /* depth */,
                                                  1 /* batchSize */,
                                                  true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xecc0b0ffb42c8e9bL),
                 serialVersionChecker(
                     new MultiGetBatchKeysIterate(
                         singletonList(KEY_BYTES),
                         new byte[] { 1, 2 } /* resumeKey */,
                         KEY_RANGE /* subRange */,
                         Depth.CHILDREN_ONLY /* depth */,
                         1 /* batchSize */,
                         true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x182cce50a4e5de6dL),
                 serialVersionChecker(
                     new MultiGetBatchKeysIterate(
                         singletonList(KEY_BYTES),
                         new byte[] { 1, 2 } /* resumeKey */,
                         KEY_RANGE /* subRange */,
                         Depth.CHILDREN_ONLY /* depth */,
                         1 /* batchSize */,
                         false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xfdbf0c99c69b891cL));
    }

    @Test
    public void testMultiGetIterate() {
        checkOps(serialVersionChecker(
                     new MultiGetIterate(KEY_BYTES,
                                         null /* subRange */,
                                         null /* depth */,
                                         Direction.FORWARD,
                                         1 /* batchSize */,
                                         null /* resumeKey */,
                                         true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x8c08f1e2a3db0ae9L),
                 serialVersionChecker(
                     new MultiGetIterate(KEY_BYTES,
                                         KEY_RANGE /* subRange */,
                                         Depth.CHILDREN_ONLY,
                                         Direction.FORWARD,
                                         1 /* batchSize */,
                                         new byte[] { 1, 2 } /* resumeKey */,
                                         true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xf1210acb9d279b57L),
                 serialVersionChecker(
                     new MultiGetIterate(KEY_BYTES,
                                         KEY_RANGE /* subRange */,
                                         Depth.CHILDREN_ONLY,
                                         Direction.FORWARD,
                                         1 /* batchSize */,
                                         new byte[] { 1, 2 } /* resumeKey */,
                                         false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xc4029ae9d123ec93L));
    }

    @Test
    public void testMultiGetKeys() {
        checkOps(serialVersionChecker(
                     new MultiGetKeys(KEY_BYTES,
                                      null /* subRange */,
                                      null /* depth */,
                                      true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xb625746f8924f953L),
                 serialVersionChecker(
                     new MultiGetKeys(KEY_BYTES,
                                      KEY_RANGE /* subRange */,
                                      Depth.CHILDREN_ONLY,
                                      true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xc972783d6df6f6a8L),
                 serialVersionChecker(
                     new MultiGetKeys(KEY_BYTES,
                                      KEY_RANGE /* subRange */,
                                      Depth.CHILDREN_ONLY,
                                      false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x76a7268b91a36da1L));
    }

    @Test
    public void testNop() {
        checkOps(serialVersionChecker(new NOP(),
                                      0x5ba93c9db0cff93fL));
    }

    @Test
    public void testPut() {
        checkOps(serialVersionChecker(
                     new Put(KEY_BYTES,
                             VALUE,
                             Choice.VALUE,
                             TABLE_ID,
                             TimeToLive.DO_NOT_EXPIRE,
                             false /* updateTTL */,
                             false /* isSQLUpdate */),
                     0x4cdf48f797e94eabL));
    }

    @Test
    public void testPutBatch() {
        final KVPair kvPair =
            new KVPair(KEY_BYTES,
                       VALUE_BYTES,
                       1 /* ttlVal */,
                       TimeToLive.HOURS_ORDINAL,
                       0 /* creationTime */,
                       0 /* mod time */,
                       false /* isTombstone */,
                       -1 /* streamId */);
        final KVPair kvPairNew =
            new KVPair(KEY_BYTES,
                       VALUE_BYTES,
                       1 /* ttlVal */,
                       TimeToLive.HOURS_ORDINAL,
                       0 /* creationTime */,
                       12345L /* mod time */,
                       true /* isTombstone */,
                       -1 /* streamId */);
        checkOps(
            serialVersionChecker(
                new PutBatch(singletonList(kvPair),
                             null  /* tableIds */,
                             true  /* overwrite */,
                             false /* usePutResolve */,
                             0     /* localRegionId */
                             ),
                SerialVersion.MINIMUM, 0xc44845d191603b9bL,
                SerialVersion.BULK_PUT_RESOLVE, 0xe6eb825dae9ba8c5L,
                SerialVersion.CREATION_TIME_VER, 0x64137f6745c83c26L),
            serialVersionChecker(
                new PutBatch(singletonList(kvPair),
                             new long[] { TABLE_ID },
                             false /* overwrite */,
                             false /* usePutResolve */,
                             0     /* localRegionId */
                             ),
                SerialVersion.MINIMUM, 0xf6c32911ff2ba5b4L,
                SerialVersion.BULK_PUT_RESOLVE, 0xbdeb11e582a8e7f7L,
                SerialVersion.CREATION_TIME_VER, 0xb891815470aab01cL),
            serialVersionChecker(
                new PutBatch(singletonList(kvPair),
                             new long[] { TABLE_ID },
                             false /* overwrite */,
                             true  /* usePutResolve */,
                             1     /* localRegionId */
                             ),
                SerialVersion.BULK_PUT_RESOLVE, 0x533dbaa3e1916793L,
                SerialVersion.CREATION_TIME_VER, 0xb678c859b775c2d4L),
            serialVersionChecker(
                new PutBatch(singletonList(kvPairNew),
                             new long[] { TABLE_ID },
                             false /* overwrite */,
                             true  /* usePutResolve */,
                             1     /* localRegionId */
                             ),
                SerialVersion.BULK_PUT_RESOLVE, 0x3ee4206c17be7b5bL,
                SerialVersion.CREATION_TIME_VER, 0xa3a29583759a1173L));
    }

    @Test
    public void testPutIfAbsent() {
        checkOps(serialVersionChecker(
                     new PutIfAbsent(KEY_BYTES,
                                     VALUE,
                                     Choice.VALUE,
                                     TABLE_ID,
                                     TimeToLive.DO_NOT_EXPIRE,
                                     false /* updateTTL */),
                     0x4bc367459bf9b1afL));
    }

    @Test
    public void testPutIfPresent() {
        checkOps(serialVersionChecker(
                     new PutIfPresent(KEY_BYTES,
                                      VALUE,
                                      Choice.VALUE,
                                      TABLE_ID,
                                      TimeToLive.DO_NOT_EXPIRE,
                                      false /* updateTTL */),
                     0x7b6a565c3fc38bbfL));
    }

    @Test
    public void testPutIfVersion() {
        checkOps(serialVersionChecker(
                     new PutIfVersion(KEY_BYTES,
                                      VALUE,
                                      Choice.VALUE,
                                      VERSION,
                                      TABLE_ID,
                                      TimeToLive.DO_NOT_EXPIRE,
                                      false /* updateTTL */),
                     0x7b6dc78bc36ba3c6L));
    }

    @Test
    public void testPutResolve() {
        checkOps(serialVersionChecker(
                     new PutResolve(KEY_BYTES,
                                    VALUE,
                                    TABLE_ID,
                                    Choice.VALUE,
                                    1 /* expirationTimeMs */,
                                    true /* updateTTL */,
                                    false /* isTombstone */,
                                    0 /* creationTime */,
                                    2 /* timestamp */,
                                    Region.NULL_REGION_ID),
                     SerialVersion.MINIMUM, 0x20e1758fb369dcf5L,
                     CLOUD_MR_TABLE, 0x3606734cab6b7d93L,
                     SerialVersion.CREATION_TIME_VER, 0x3d59d52bacc906baL),
                 serialVersionChecker(
                     new PutResolve(KEY_BYTES,
                                    VALUE,
                                    TABLE_ID,
                                    Choice.VALUE,
                                    1 /* expirationTimeMs */,
                                    true /* updateTTL */,
                                    false /* isTombstone */,
                                    0 /* creationTime */,
                                    2 /* timestamp */,
                                    regionId),
                     CLOUD_MR_TABLE, 0x6c1ddf4968294fe2L,
                     SerialVersion.CREATION_TIME_VER, 0x61e5b0df88952406L));
    }

    @Test
    public void testStoreIterate() {
        checkOps(serialVersionChecker(
                     new StoreIterate(KEY_BYTES,
                                      KEY_RANGE,
                                      Depth.CHILDREN_ONLY,
                                      Direction.FORWARD,
                                      1 /* batchSize */,
                                      null /* resumeKey */,
                                      true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x9fb30cce4ed0a1e3L),
                 serialVersionChecker(
                     new StoreIterate(KEY_BYTES,
                                      KEY_RANGE,
                                      Depth.CHILDREN_ONLY,
                                      Direction.FORWARD,
                                      1 /* batchSize */,
                                      new byte[] { 1, 2 } /* resumeKey */,
                                      true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x3c14c4c098ea2696L),
                 serialVersionChecker(
                     new StoreIterate(KEY_BYTES,
                                      KEY_RANGE,
                                      Depth.CHILDREN_ONLY,
                                      Direction.FORWARD,
                                      1 /* batchSize */,
                                      new byte[] { 1, 2 } /* resumeKey */,
                                      false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x16515199a34bd685L));
    }

    @Test
    public void testStoreKeysIterate() {
        checkOps(serialVersionChecker(
                     new StoreKeysIterate(KEY_BYTES,
                                          KEY_RANGE,
                                          Depth.CHILDREN_ONLY,
                                          Direction.FORWARD,
                                          1 /* batchSize */,
                                          null /* resumeKey */,
                                          true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0x891f29ea7cb1a047L),
                 serialVersionChecker(
                     new StoreKeysIterate(KEY_BYTES,
                                          KEY_RANGE,
                                          Depth.CHILDREN_ONLY,
                                          Direction.FORWARD,
                                          1 /* batchSize */,
                                          new byte[] { 1, 2 } /* resumeKey */,
                                          true /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xe35e36bd0c4385b0L),
                 serialVersionChecker(
                     new StoreKeysIterate(KEY_BYTES,
                                          KEY_RANGE,
                                          Depth.CHILDREN_ONLY,
                                          Direction.FORWARD,
                                          1 /* batchSize */,
                                          new byte[] { 1, 2 } /* resumeKey */,
                                          false /* excludeTombstones */),
                     SerialVersion.MINIMUM, 0xc2b1063ba69b99f0L));
    }
}
