/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import oracle.kv.TestBase;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/** Base class for tests for subclasses of BasicCompressingFileHandler. */
public abstract class BasicCompressingFileHandlerTestBase
    <H extends BasicCompressingFileHandler>
        extends TestBase {

    protected H handler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (handler != null) {
            handler.close();
        }
    }

    @Test
    public void testConstructor() throws Exception {
        final File dir = TestUtils.getTestDir();

        checkException(() -> createFileHandler(
                           "", 5, 10, true /* extraLogging */ ),
                       IllegalArgumentException.class);
        checkException(() -> createFileHandler(
                           dir + "/%g%g", 50_000, 10, true /* extraLogging */),
                       IllegalArgumentException.class,
                       "%g%g");
        checkException(() -> createFileHandler(
                           dir + "/%g%u", 50_000, 10, true /* extraLogging */),
                       IllegalArgumentException.class,
                       "%g%u");
        checkException(() -> createFileHandler(
                           dir + "/%g%%", 50_000, 10, true /* extraLogging */),
                       IllegalArgumentException.class,
                       "%%");
        checkException(() -> createFileHandler(
                           dir + "/foo", 50_000, 10, false /* extraLogging */),
                       IllegalArgumentException.class,
                       "%g");

        checkException(() -> createFileHandler(
                           dir + "/foo%g", -1, 5, true /* extraLogging */),
                       IllegalArgumentException.class);

        checkException(() -> createFileHandler(
                           dir + "/foo%g", 1000, 0, true /* extraLogging */),
                       IllegalArgumentException.class);

        handler = createFileHandler(
            dir + "/a_%g", 0, 1, true /* extraLogging */);
        assertEquals(0, handler.getLimit());
        assertEquals(1, handler.getCount());
    }

    /* Other classes and methods */

    abstract protected H createFileHandler(String pattern,
                                           int limit,
                                           int count,
                                           boolean extraLogging)
        throws IOException;
}
