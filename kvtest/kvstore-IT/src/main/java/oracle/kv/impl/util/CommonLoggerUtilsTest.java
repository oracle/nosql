/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.util.function.BiConsumer;

import oracle.kv.FaultException;
import oracle.kv.TestBase;

import org.junit.Test;

public class CommonLoggerUtilsTest extends TestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FaultException.testNoCurrentInMessage = true;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        FaultException.testNoCurrentInMessage = false;
    }

    @Test
    public void testExceptionString() {
        final BiConsumer<String, Throwable> test = (result, exception) ->
            assertEquals(result, exceptionString(exception));

        test.accept("java.lang.Exception: msg", new Exception("msg"));
        test.accept("java.lang.Exception: ", new Exception(""));
        test.accept("java.lang.Exception: null", new Exception());
        test.accept("oracle.kv.FaultException: msg",
                    new FaultException("msg", new Exception("blah"),
                                       true /* isRemote */));
    }
}
