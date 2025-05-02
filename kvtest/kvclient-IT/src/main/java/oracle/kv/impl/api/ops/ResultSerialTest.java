/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import java.util.UUID;
import java.util.stream.Stream;

import oracle.kv.TestBase;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.Result.BulkGetIterateResult;
import oracle.kv.impl.api.ops.Result.BulkGetKeysIterateResult;
import oracle.kv.impl.api.ops.Result.DeleteResult;
import oracle.kv.impl.api.ops.Result.ExecuteResult;
import oracle.kv.impl.api.ops.Result.GetIdentityResult;
import oracle.kv.impl.api.ops.Result.GetResult;
import oracle.kv.impl.api.ops.Result.IndexKeysIterateResult;
import oracle.kv.impl.api.ops.Result.IndexRowsIterateResult;
import oracle.kv.impl.api.ops.Result.IterateResult;
import oracle.kv.impl.api.ops.Result.KeysIterateResult;
import oracle.kv.impl.api.ops.Result.MultiDeleteResult;
import oracle.kv.impl.api.ops.Result.NOPResult;
import oracle.kv.impl.api.ops.Result.PutBatchResult;
import oracle.kv.impl.api.ops.Result.PutResult;
import oracle.kv.impl.api.table.SequenceImpl.SGAttrsAndValues;
import oracle.kv.impl.util.SerialTestUtils.SerialVersionChecker;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/**
 * Test serial version compatibility for non-table Result subclasses in the ops
 * package.
*/
public class ResultSerialTest extends TestBase {
    private static final byte[] KEY_BYTES = { 2, 4, 6 };
    private static final byte[] VALUE_BYTES = { 2, 13, 17 };
    private static final Version VERSION =
        new Version(new UUID(1, 2), 3, null, 0);
    private static final ResultValueVersion RESULT_VALUE_VERSION_EXP =
        new ResultValueVersion(VALUE_BYTES,
                               VERSION,
                               3 /* expirationTime */,
                               0 /* modificationTime */,
                               -1 /* storageSize */);
    private static final ResultValueVersion RESULT_VALUE_VERSION_MOD =
        new ResultValueVersion(VALUE_BYTES,
                               VERSION,
                               3 /* expirationTime */,
                               4 /* modificationTime */,
                               -1 /* storageSize */);
    private static final ResultValueVersion RESULT_VALUE_VERSION_STORAGE =
        new ResultValueVersion(VALUE_BYTES,
                               VERSION,
                               3 /* expirationTime */,
                               4 /* modificationTime */,
                               5 /* storageSize */);
    private static final ResultKeyValueVersion RESULT_KEY_VALUE_VERSION_EXP =
        new ResultKeyValueVersion(KEY_BYTES,
                                  VALUE_BYTES,
                                  VERSION,
                                  3 /* expirationTime */,
                                  0 /* modificationTime */,
                                  false /* isTombstone */);
    private static final ResultKeyValueVersion RESULT_KEY_VALUE_VERSION_MOD =
        new ResultKeyValueVersion(KEY_BYTES,
                                  VALUE_BYTES,
                                  VERSION,
                                  3 /* expirationTime */,
                                  4 /* modificationTime */,
                                  false /* isTombstone */);
    private static final ResultKeyValueVersion RESULT_KEY_VALUE_VERSION_TB =
        new ResultKeyValueVersion(KEY_BYTES,
                                  VALUE_BYTES,
                                  VERSION,
                                  3 /* expirationTime */,
                                  4 /* modificationTime */,
                                  true /* isTombstone */);
    /**
     * Override the default version, since clearing the test directory is only
     * possible in server-side tests.
     */
    @Override
    protected void clearTestDirectory() { }

    @Test
    public void testGetResult() {
        checkResult(
            serialVersionChecker(
                new GetResult(OpCode.GET,
                              1 /* readKB */,
                              2 /* writeKB */,
                              RESULT_VALUE_VERSION_EXP),
                SerialVersion.MINIMUM, 0x1c79a005443f68cbL),
            serialVersionChecker(
                new GetResult(OpCode.GET,
                              1 /* readKB */,
                              2 /* writeKB */,
                              RESULT_VALUE_VERSION_MOD),
                SerialVersion.MINIMUM, 0xa6b456f464e3c768L),
            serialVersionChecker(
                new GetResult(OpCode.GET,
                              1 /* readKB */,
                              2 /* writeKB */,
                              RESULT_VALUE_VERSION_STORAGE),
                SerialVersion.MINIMUM, 0x5512c63d261c2e17L));
    }

    @Test
    public void testPutResult() {
        checkResult(
            serialVersionChecker(
                new PutResult(OpCode.PUT,
                              1 /* readKB */,
                              2 /* writeKB */,
                              RESULT_VALUE_VERSION_EXP,
                              VERSION,
                              10 /* expTime */,
                              false /* wasUpdate */,
                              0 /* modificationTime */,
                              -1 /* storageSize */,
                              -1 /* shard */),
                SerialVersion.MINIMUM, 0x961816f3e524fa7dL),
            serialVersionChecker(
                new PutResult(OpCode.PUT,
                              1 /* readKB */,
                              2 /* writeKB */,
                              RESULT_VALUE_VERSION_MOD,
                              VERSION,
                              10 /* expTime */,
                              false /* wasUpdate */,
                              11 /* modificationTime */,
                              -1 /* storageSize */,
                              -1 /* shard */),
                SerialVersion.MINIMUM, 0x65051cf4a2615e1dL),
            serialVersionChecker(
                new PutResult(OpCode.PUT,
                              1 /* readKB */,
                              2 /* writeKB */,
                              RESULT_VALUE_VERSION_STORAGE,
                              VERSION,
                              10 /* expTime */,
                              false /* wasUpdate */,
                              11 /* modificationTime */,
                              12 /* storageSize */,
                              13 /* shard */),
                SerialVersion.MINIMUM, 0x4035eb2620422159L));
    }

    @Test
    public void testDeleteResult() {
        checkResult(
            serialVersionChecker(
                new DeleteResult(OpCode.DELETE,
                                 1 /* readKB */,
                                 2 /* writeKB */,
                                 RESULT_VALUE_VERSION_EXP,
                                 true /* success */),
                SerialVersion.MINIMUM, 0xd6b58ee93336952dL),
            serialVersionChecker(
                new DeleteResult(OpCode.DELETE,
                                 1 /* readKB */,
                                 2 /* writeKB */,
                                 RESULT_VALUE_VERSION_MOD,
                                 true /* success */),
                SerialVersion.MINIMUM, 0x66bbe208857f246dL),
            serialVersionChecker(
                new DeleteResult(OpCode.DELETE,
                                 1 /* readKB */,
                                 2 /* writeKB */,
                                 RESULT_VALUE_VERSION_STORAGE,
                                 true /* success */),
                SerialVersion.MINIMUM, 0x25cb8a869de8fb84L));
    }

    @Test
    public void testMultiDeleteResult() {
        checkResult(
            serialVersionChecker(
                new MultiDeleteResult(OpCode.MULTI_DELETE,
                                      1 /* readKB */,
                                      2 /* writeKB */,
                                      3 /* nDeletions */,
                                      null /* resumeKey */),
                0x33271a24f2e9622L),
            serialVersionChecker(
                new MultiDeleteResult(OpCode.MULTI_DELETE,
                                      1 /* readKB */,
                                      2 /* writeKB */,
                                      3 /* nDeletions */,
                                      new byte[] { 4, 5 } /* resumeKey */),
                0x23f96249da913cc9L));
    }

    @Test
    public void testNopResult() {
        checkResult(
            serialVersionChecker(new NOPResult(), 0x22f36e30159ec10L));
    }

    @Test
    public void testExecuteResult() {
        checkResult(
            serialVersionChecker(
                new ExecuteResult(
                    OpCode.EXECUTE,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(
                        new DeleteResult(OpCode.DELETE,
                                         1 /* readKB */,
                                         2 /* writeKB */,
                                         RESULT_VALUE_VERSION_EXP,
                                         true /* success */))),
                SerialVersion.MINIMUM, 0xfbc341b841ec19b0L),
            serialVersionChecker(
                new ExecuteResult(
                    OpCode.EXECUTE,
                    1 /* readKB */,
                    2 /* writeKB */,
                    3 /* failureIndex */,
                    new DeleteResult(OpCode.DELETE,
                                     1 /* readKB */,
                                     2 /* writeKB */,
                                     RESULT_VALUE_VERSION_EXP,
                                     false /* success */)),
                SerialVersion.MINIMUM, 0xdb0549d4ee8b9d3aL));
    }

    @Test
    public void testPutBatchResult() {
        checkResult(
            serialVersionChecker(
                new PutBatchResult(1 /* readKB */,
                                   2 /* writeKB */,
                                   3 /* numKVPairs */,
                                   emptyList() /* keysPresent */),
                0x84f9f73c4f5df936L),
            serialVersionChecker(
                new PutBatchResult(1 /* readKB */,
                                   2 /* writeKB */,
                                   3 /* numKVPairs */,
                                   singletonList(2) /* keysPresent */),
                0x4749cbd5177c5e41L));
    }

    @Test
    public void testIterateResult() {
        checkResult(
            serialVersionChecker(
                new IterateResult(OpCode.STORE_ITERATE,
                                  1 /* readKB */,
                                  2 /* writeKB */,
                                  singletonList(RESULT_KEY_VALUE_VERSION_EXP),
                                  true /* moreElements */),
                SerialVersion.MINIMUM, 0x748e650ff4aabeeaL,
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0x226a94684e4b9475L),
            serialVersionChecker(
                new IterateResult(OpCode.STORE_ITERATE,
                                  1 /* readKB */,
                                  2 /* writeKB */,
                                  singletonList(RESULT_KEY_VALUE_VERSION_MOD),
                                  true /* moreElements */),
                SerialVersion.MINIMUM, 0x967a8e1f86a512bbL,
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0x451c2971bf01bc22L),
            serialVersionChecker(
                new IterateResult(OpCode.STORE_ITERATE,
                                  1 /* readKB */,
                                  2 /* writeKB */,
                                  singletonList(RESULT_KEY_VALUE_VERSION_TB),
                                  true /* moreElements */),
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0xb430af80b284951fL));
    }

    @Test
    public void testKeysIterateResult() {
        checkResult(
            serialVersionChecker(
                new KeysIterateResult(
                    OpCode.STORE_KEYS_ITERATE,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(
                        new ResultKey(KEY_BYTES,
                                      3 /* expirationTime */)),
                    true /* moreElements */),
                0x3e7beb161306d1e9L));
    }

    @Test
    public void testIndexKeysIterateResult() {
        checkResult(
            serialVersionChecker(
                new IndexKeysIterateResult(
                    OpCode.INDEX_KEYS_ITERATE,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(
                        new ResultIndexKeys(
                            KEY_BYTES,
                            new byte[] { 66, 77 } /* indexKeyBytes */,
                            3 /* expirationTime */)),
                    true /* moreElements */),
                0xc0d9b0b8458eb48L));
    }

    @Test
    public void testIndexRowsIterateResult() {
        checkResult(
            serialVersionChecker(
                new IndexRowsIterateResult(
                    OpCode.INDEX_ITERATE,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(
                        new ResultIndexRows(
                            new byte[] { 66, 77 } /* indexKeyBytes */,
                            KEY_BYTES,
                            VALUE_BYTES,
                            VERSION,
                            3 /* expirationTime */,
                            0 /* modificationTime */)),
                    true /* moreElements */),
                SerialVersion.MINIMUM, 0xfba1899f70ebbb03L,
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0xe9d5fdbc25f9656cL),
            serialVersionChecker(
                new IndexRowsIterateResult(
                    OpCode.INDEX_ITERATE,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(
                        new ResultIndexRows(
                            new byte[] { 66, 77 } /* indexKeyBytes */,
                            KEY_BYTES,
                            VALUE_BYTES,
                            VERSION,
                            3 /* expirationTime */,
                            4 /* modificationTime */)),
                    true /* moreElements */),
                SerialVersion.MINIMUM, 0xe14fa82463ae23d0L,
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0xb85aa6567512024aL));
    }

    @Test
    public void testBulkGetIterateResult() {
        checkResult(
            serialVersionChecker(
                new BulkGetIterateResult(
                    OpCode.MULTI_GET_BATCH,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(RESULT_KEY_VALUE_VERSION_EXP),
                    true /* moreElements */,
                    3 /* resumeParentKeyIndex */),
                SerialVersion.MINIMUM, 0x37c53d0c655f61b3L,
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0xe6bb01ba5d56a3bdL),
            serialVersionChecker(
                new BulkGetIterateResult(
                    OpCode.MULTI_GET_BATCH,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(RESULT_KEY_VALUE_VERSION_MOD),
                    true /* moreElements */,
                    3 /* resumeParentKeyIndex */),
                SerialVersion.MINIMUM, 0x7b10fe7bbe8748ebL,
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0xf86b4a12b5d94633L));
    }

    @Test
    public void testBulkGetKeysIterateResult() {
        checkResult(
            serialVersionChecker(
                new BulkGetKeysIterateResult(
                    OpCode.MULTI_GET_BATCH_KEYS,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(new ResultKey(KEY_BYTES,
                                                3 /* expirationTime */)),
                    true /* moreElements */,
                    3 /* lastParentKeyIndex */),
                0x9ef08550527a45f1L));
    }

    @Test
    public void testGetIdentityResult() {
        checkResult(
            serialVersionChecker(
                new GetIdentityResult(OpCode.GET_IDENTITY,
                                      1 /* readKB */,
                                      2 /* writeKB */,
                                      new SGAttrsAndValues()),
                0xd87c439754a778fdL));
    }

    @SafeVarargs
    @SuppressWarnings({"all","varargs"})
    private static <T extends Result>
        void checkResult(SerialVersionChecker<T>... checkers)
    {
        checkAll(Stream.of(checkers)
                 .map(svc -> svc.reader(Result::readFastExternal)));
    }
}
