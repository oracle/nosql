/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonList;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import java.util.stream.Stream;

import oracle.kv.TestBase;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.Result.QueryResult;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.util.SerialTestUtils.SerialVersionChecker;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/**
 * Test serial version compatibility for table-related Result subclasses in the
 * ops package.
 */
public class TableResultSerialTest extends TestBase {

    @Test
    public void testQueryResult() {
        checkResult(
            serialVersionChecker(
                new QueryResult(
                    OpCode.QUERY_SINGLE_PARTITION,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(new IntegerValueImpl(3)) /* results */,
                    new IntegerDefImpl("desc") /* resultDef */,
                    true /* mayReturnNULL */,
                    true /* moreElements */,
                    new ResumeInfo(),
                    true /* exceededSizeLimit */,
                    null /* pids */,
                    null /* numResultsPerPid */,
                    null /* resumeInfos */,
                    null /* batchName */,
                    null /* batchTrace */),
                SerialVersion.MINIMUM, 0xf33c95a3b23e1312L,
                SerialVersion.V33, 0xdeadecf263303fddL,
                SerialVersion.V37, 0x3acb4a03846bdae7L),
            serialVersionChecker(
                new QueryResult(
                    OpCode.QUERY_SINGLE_PARTITION,
                    1 /* readKB */,
                    2 /* writeKB */,
                    singletonList(new IntegerValueImpl(3)) /* results */,
                    new IntegerDefImpl("desc") /* resultDef */,
                    true /* mayReturnNULL */,
                    true /* moreElements */,
                    new ResumeInfo(),
                    true /* exceededSizeLimit */,
                    new int[] { 4 } /* pids */,
                    new int[] { 5 } /* numResultsPerPid */,
                    new ResumeInfo[] { new ResumeInfo() } /* resumeInfos */,
                    null /* batchName */,
                    null /* batchTrace */),
                SerialVersion.MINIMUM, 0xa1acdc0db6b2facbL,
                SerialVersion.V33, 0x2c477723d43ff6cL,
                SerialVersion.V37, 0x8e3def5ae86e0da3L));
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
