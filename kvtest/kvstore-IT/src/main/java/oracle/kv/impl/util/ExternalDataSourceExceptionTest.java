/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test the ExternalDataSourceException class.
 */
public class ExternalDataSourceExceptionTest extends TestBase {

    @Test
    public void testStringCtor() {
        final String exceptionMessage = "A sample message";
        try {
            throw new ExternalDataSourceException(exceptionMessage);
        } catch (ExternalDataSourceException edse) {
            assertEquals(edse.getMessage(), exceptionMessage);
            assertTrue(null == edse.getCause());
       }

        try {
            throw new ExternalDataSourceException(null);
        } catch (ExternalDataSourceException edse) {
            assertTrue(null == edse.getMessage());
            assertTrue(null == edse.getCause());
        }
    }

    @Test
    public void testThrowableCtor() {
        final String exceptionMessage = "A sample message";
        final Exception aThrowable = new Exception("dummy");

        try {
            throw new ExternalDataSourceException
                (exceptionMessage, aThrowable);
        } catch (ExternalDataSourceException edse) {
            assertEquals(edse.getMessage(), exceptionMessage);
            assertTrue(aThrowable == edse.getCause());
        }

        try {
            throw new ExternalDataSourceException(null, aThrowable);
        } catch (ExternalDataSourceException edse) {
            assertTrue(null == edse.getMessage());
            assertTrue(aThrowable == edse.getCause());
        }
    }
}
