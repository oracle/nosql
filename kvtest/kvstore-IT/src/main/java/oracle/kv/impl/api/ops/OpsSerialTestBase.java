/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static oracle.kv.util.TestUtils.checkAll;

import java.util.stream.Stream;

import oracle.kv.impl.util.SerialTestUtils.SerialVersionChecker;

/** Base class for testing serial version compatibility for Ops classes. */
public class OpsSerialTestBase extends BasicClientTestBase {

    /**
     * Check internal operations, using InternalOperation.readFastExternal as
     * the reader.
     */
    @SafeVarargs
    @SuppressWarnings({"all","varargs"})
    protected static <T extends InternalOperation>
        void checkOps(SerialVersionChecker<T>... checkers)
    {
        checkOps(Stream.of(checkers));
    }

    /**
     * Check a stream of internal operations, using
     * InternalOperation.readFastExternal as the reader.
     */
    protected static <T extends InternalOperation>
        void checkOps(Stream<SerialVersionChecker<T>> checkers)
    {
        checkAll(
            checkers.map(
                svc -> svc.reader(InternalOperation::readFastExternal)));
    }
}
