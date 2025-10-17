/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonList;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerialVersion.CLOUD_MR_TABLE;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_14;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_17;
import static oracle.kv.impl.util.SerialVersion.ROW_METADATA_VERSION;

import java.math.MathContext;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.IndexIterate;
import oracle.kv.impl.api.ops.IndexKeysIterate;
import oracle.kv.impl.api.ops.MultiDeleteTable;
import oracle.kv.impl.api.ops.MultiGetBatchTable;
import oracle.kv.impl.api.ops.MultiGetBatchTableKeys;
import oracle.kv.impl.api.ops.MultiGetTable;
import oracle.kv.impl.api.ops.MultiGetTableKeys;
import oracle.kv.impl.api.ops.OpsSerialTestBase;
import oracle.kv.impl.api.ops.TableIterate;
import oracle.kv.impl.api.ops.TableKeysIterate;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.CodeGenerator;
import oracle.kv.impl.query.compiler.ExprConst;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.runtime.ConstIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/** Test serial version compatibility for the various table Ops classes. */
public class TableOpsSerialTest extends OpsSerialTestBase {
    private static final TableImpl TABLE =
        TableBuilder.createTableBuilder("myName")
        .addInteger("id")
        .addString("val")
        .primaryKey("id")
        .buildTable();
    private static final byte[] PARENT_KEY = new byte[] { 2, 4 };
    private static final byte[] RESUME_KEY = new byte[] { 3, 5 };
    private static final KeyRange KEY_RANGE = new KeyRange("prefix");
    private static final IndexRange INDEX_RANGE =
        new IndexRange(new IndexImpl("indexName", TABLE,
                                     singletonList("val"),
                                     singletonList(null),
                                     "description")
                       .createIndexKey(),
                       null /* getOptions */,
                       Direction.FORWARD);
    private static final int regionId = Integer.MAX_VALUE;

    @Test
    public void testIndexIterate() {
        checkOps(serialVersionChecker(
                     new IndexIterate(
                         "myIndexName",
                         new TargetTables(TABLE, null, null),
                         INDEX_RANGE,
                         new byte[] { 1 } /* resumeSecondaryKey */,
                         new byte[] { 2 } /* resumePrimaryKey */,
                         3 /* batchSize */,
                         4 /* maxReadKB */,
                         5 /* emptyReadFactor */),
                     0x75d6f4f9998abd11L),
                 serialVersionChecker(
                     new IndexIterate(
                         "myIndexName",
                         new TargetTables(TABLE, null, null),
                         INDEX_RANGE,
                         null /* resumeSecondaryKey */,
                         null /* resumePrimaryKey */,
                         3 /* batchSize */,
                         4 /* maxReadKB */,
                         5 /* emptyReadFactor */),
                     0xf29f041549ced00aL));
    }

    @Test
    public void testIndexKeysIterate() {
        checkOps(serialVersionChecker(
                     new IndexKeysIterate(
                         "myIndexName",
                         new TargetTables(TABLE, null, null),
                         INDEX_RANGE,
                         new byte[] { 1 } /* resumeSecondaryKey */,
                         new byte[] { 2 } /* resumePrimaryKey */,
                         3 /* batchSize */,
                         4 /* maxReadKB */,
                         5 /* emptyReadFactor */),
                     0x3012ba975dd8851aL),
                 serialVersionChecker(
                     new IndexKeysIterate(
                         "myIndexName",
                         new TargetTables(TABLE, null, null),
                         INDEX_RANGE,
                         null /* resumeSecondaryKey */,
                         null /* resumePrimaryKey */,
                         3 /* batchSize */,
                         4 /* maxReadKB */,
                         5 /* emptyReadFactor */),
                     0xe8126700a491aec0L));
    }

    @Test
    public void testMultiDeleteTable() {
        checkOps(serialVersionChecker(
                     new MultiDeleteTable(PARENT_KEY,
                                          new TargetTables(TABLE, null, null),
                                          KEY_RANGE,
                                          RESUME_KEY,
                                          7,
                                          false /* doTombstone */,
                                          null /* rowMetadata */),
                     SerialVersion.MINIMUM, 0xf1263fcede36a88dL,
                CLOUD_MR_TABLE, 0xae8e2fc911747f58L,
                     ROW_METADATA_VERSION, 0x260462ecb0234090L),
                 serialVersionChecker(
                     new MultiDeleteTable(null, /* parentKey */
                                          new TargetTables(TABLE, null, null),
                                          null, /* subRange */
                                          null, /* resumeKey */
                                          8,
                                          false /* doTombstone */,
                                          null /* rowMetadata */),
                     SerialVersion.MINIMUM, 0x648b1dc049dec2bdL,
                     CLOUD_MR_TABLE, 0xf3b61b1030c629fL,
                     ROW_METADATA_VERSION, 0xdc4a82ec4c3b4503L),
                 serialVersionChecker(
                     new MultiDeleteTable(null, /* parentKey */
                                          new TargetTables(TABLE, null, null),
                                          null, /* subRange */
                                          null, /* resumeKey */
                                          8,
                                          true  /* doTombstone */,
                                          null /* rowMetadata */),
                     CLOUD_MR_TABLE, 0xb6b9fa2e043df94aL,
                     ROW_METADATA_VERSION, 0xdb30f86c7d4bc186L));
    }

    @Test
    public void testMultiGetBatchTable() {
        checkOps(serialVersionChecker(
                     new MultiGetBatchTable(
                         singletonList(PARENT_KEY),
                         RESUME_KEY,
                         new TargetTables(TABLE, null, null),
                         KEY_RANGE,
                         7),
                     0x458511c032b81719L),
                 serialVersionChecker(
                     new MultiGetBatchTable(
                         singletonList(PARENT_KEY),
                         null, /* resumeKey */
                         new TargetTables(TABLE, null, null),
                         null, /* subRange */
                         7),
                     0xcf89e1df510ba565L));
    }

    @Test
    public void testMultiGetBatchTableKeys() {
        checkOps(serialVersionChecker(
                     new MultiGetBatchTableKeys(
                         singletonList(PARENT_KEY),
                         RESUME_KEY,
                         new TargetTables(TABLE, null, null),
                         KEY_RANGE,
                         7),
                     0x7bc1755a02cb9024L),
                 serialVersionChecker(
                     new MultiGetBatchTableKeys(
                         singletonList(PARENT_KEY),
                         null, /* resumeKey */
                         new TargetTables(TABLE, null, null),
                         null, /* subRange */
                         7),
                     0x3412b2c125afd123L));
    }

    @Test
    public void testMultiGetTable() {
        checkOps(serialVersionChecker(
                     new MultiGetTable(
                         PARENT_KEY,
                         new TargetTables(TABLE, null, null),
                         KEY_RANGE),
                     0x60813081db464cf0L),
                 serialVersionChecker(
                     new MultiGetTable(
                         null, /* parentKey */
                         new TargetTables(TABLE, null, null),
                         null /* subRange */),
                     0xa644081c2d79d59aL));
    }

    @Test
    public void testMultiGetTableKeys() {
        checkOps(serialVersionChecker(
                     new MultiGetTableKeys(
                         PARENT_KEY,
                         new TargetTables(TABLE, null, null),
                         KEY_RANGE,
                         5),
                     0x2346028250d0bbf0L),
                 serialVersionChecker(
                     new MultiGetTableKeys(
                         null, /* parentKey */
                         new TargetTables(TABLE, null, null),
                         null /* subRange */,
                         6),
                     0x2b7a373311cf65e8L));
    }

    @Test
    public void testTableIterate() {

        /* exclude tombstones */
        checkOps(serialVersionChecker(
                     new TableIterate(PARENT_KEY,
                                      new TargetTables(TABLE, null, null),
                                      true, /* majorComplete */
                                      4,
                                      RESUME_KEY,
                                      false /* inclTombstones */),
                     SerialVersion.MINIMUM, 0xaaf9df58158c59dL,
                     SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                     0x4aed0f4afa7a2b7bL),
                 serialVersionChecker(
                     new TableIterate(PARENT_KEY,
                                      new TargetTables(TABLE, null, null),
                                      false, /* majorComplete */
                                      4,
                                      null /* resumeKey */,
                                      false /* includeTombstones */),
                     SerialVersion.MINIMUM, 0x504d4adf5c35cb8bL,
                     SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                     0xb5a90d9cb14fdf78L));

        /*
         * include tombstones, only supported version should pass
         */
        checkOps(
            serialVersionChecker(
                new TableIterate(PARENT_KEY,
                                 new TargetTables(TABLE, null, null),
                                 true, /* majorComplete */
                                 4,
                                 RESUME_KEY,
                                 true /* inclTombstones */),
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0xabe9d562d1502a8dL),
            serialVersionChecker(
                new TableIterate(PARENT_KEY,
                                 new TargetTables(TABLE, null, null),
                                 false, /* majorComplete */
                                 4,
                                 null /* resumeKey */,
                                 true /* includeTombstones */),
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0x4ff58fc7e7379f0cL));
    }

    @Test
    public void testTableKeysIterate() {

        /* store iterator parameter excluding tombstones */
        final StoreIteratorParams sip = new StoreIteratorParams(
            Direction.FORWARD,
            1, /* batchSize */
            PARENT_KEY,
            KEY_RANGE,
            Depth.PARENT_AND_CHILDREN,
            Consistency.NONE_REQUIRED,
            2, /* timeout */
            TimeUnit.MILLISECONDS,
            true /* excludeTombstones */);
        checkOps(serialVersionChecker(
                     new TableKeysIterate(sip,
                                          new TargetTables(TABLE, null, null),
                                          true, /* majorComplete */
                                          RESUME_KEY,
                                          6),
                     SerialVersion.MINIMUM, 0xa062d33b166520fcL,
                     SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                     0xe319735b3de3899eL),
                 serialVersionChecker(
                     new TableKeysIterate(sip,
                                          new TargetTables(TABLE, null, null),
                                          false, /* majorComplete */
                                          null, /* resumeKey */
                                          7),
                     SerialVersion.MINIMUM, 0x4b2331a446e7d93L,
                     SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                     0x73c5d8dca785713bL));


        /* including tombstones, only supported version should pass */
        final StoreIteratorParams sipTombstone = new StoreIteratorParams(
            Direction.FORWARD,
            1, /* batchSize */
            PARENT_KEY,
            KEY_RANGE,
            Depth.PARENT_AND_CHILDREN,
            Consistency.NONE_REQUIRED,
            2, /* timeout */
            TimeUnit.MILLISECONDS,
            false /* excludeTombstones */);
        checkOps(
            serialVersionChecker(
                new TableKeysIterate(sipTombstone,
                                     new TargetTables(TABLE, null, null),
                                     true, /* majorComplete */
                                     RESUME_KEY,
                                     6),
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0x7ce66ebc507fad61L),
            serialVersionChecker(
                new TableKeysIterate(sipTombstone,
                                     new TargetTables(TABLE, null, null),
                                     false, /* majorComplete */
                                     null, /* resumeKey */
                                     7),
                SerialVersion.TABLE_ITERATOR_TOMBSTONES_VER,
                0x3cbc72ca016d313cL));
    }

    @Test
    public void testTableQuery() {
        final QueryControlBlock qcb =
            new QueryControlBlock((TableAPIImpl) null, /* tableAPI */
                                  null, /* options */
                                  null, /* queryString */
                                  null, /* sctx */
                                  null, /* namespace */
                                  null /* prepareCallback */);
        qcb.setCodegen(new CodeGenerator(qcb));
        checkOps(serialVersionChecker(
                     new TableQuery(
                         "foo",
                         DistributionKind.SINGLE_PARTITION,
                         new AnyDefImpl(),
                         true, /* mayReturnNull */
                         new ConstIter(
                             new ExprConst(
                                 qcb,
                                 null, /* sctx */
                                 new QueryException.Location(
                                     1, /* startLine */
                                     2, /* startColumn */
                                     3, /* endLine */
                                     4 /* endColumn */),
                                 new IntegerValueImpl(3)),
                             1, /* resultReg */
                             new IntegerValueImpl(3),
                             false /* forCloud */),
                         new FieldValueImpl[] { new IntegerValueImpl(3) },
                         2, /* numIterators */
                         3, /* numRegisters */
                         4, /* tableId */
                         MathContext.UNLIMITED,
                         (byte) 5, /* traceLevel */
                         true, /* doLogFileTracing*/
                         6, /* batchSize */
                         7, /* maxReadKB */
                         8, /* currentMaxReadKB */
                         9, /* currentMaxWriteKB */
                         new ResumeInfo((RuntimeControlBlock)null /* rcb */),
                         10, /* emptyReadFactor */
                         11, /* deleteLimit */
                         10, /* updateLimit */
                         Region.NULL_REGION_ID,
                         false /* doTombstone */,
                         0,
                         false /* performsWrite */,
                         null /* rowMetadata */),
                     SerialVersion.MINIMUM, 0x3144c2b760b8c32cL,
                     CLOUD_MR_TABLE, 0x790589f5083ee0edL,
                     QUERY_VERSION_14, 0x929514f53334c1e3L,
                     QUERY_VERSION_16, 0xff61efe8a32c1ea7L,
                     QUERY_VERSION_17, 0x71e479f809d5a33eL,
                     ROW_METADATA_VERSION, 0xe5efeda932111fb0L),
                 serialVersionChecker(
                     new TableQuery(
                         "testQuery",
                         DistributionKind.SINGLE_PARTITION,
                         new AnyDefImpl(),
                         true, /* mayReturnNull */
                         new ConstIter(
                             new ExprConst(
                                 qcb,
                                 null, /* sctx */
                                 new QueryException.Location(
                                     1, /* startLine */
                                     2, /* startColumn */
                                     3, /* endLine */
                                     4 /* endColumn */),
                                 new IntegerValueImpl(3)),
                             1, /* resultReg */
                             new IntegerValueImpl(3),
                             false /* forCloud */),
                         new FieldValueImpl[] { new IntegerValueImpl(3) },
                         2, /* numIterators */
                         3, /* numRegisters */
                         4, /* tableId */
                         MathContext.UNLIMITED,
                         (byte) 5, /* traceLevel */
                         true, /*doLogFileTracing */
                         6, /* batchSize */
                         7, /* maxReadKB */
                         8, /* currentMaxReadKB */
                         9, /* currentMaxWriteKB */
                         new ResumeInfo((RuntimeControlBlock)null /* rcb */),
                         10, /* emptyReadFactor */
                         11, /* deleteLimit */
                         10, /* updateLimit */
                         regionId,
                         true /* doTombstone */,
                         0,
                         false /* performsWrite */,
                         null /* rowMetadata */),
                     CLOUD_MR_TABLE, 0x1767f1dc5bbf651eL,
                     QUERY_VERSION_14, 0x8a7933f2058d74c8L,
                     QUERY_VERSION_16, 0x8c562212955c1835L,
                     QUERY_VERSION_17, 0x3a8b98d37b8fecdbL,
                     ROW_METADATA_VERSION, 0xfb04422829bc5a73L));
    }
}
