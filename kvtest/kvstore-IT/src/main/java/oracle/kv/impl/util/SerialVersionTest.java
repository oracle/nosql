/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertNotNull;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test {@link SerialVersion}.
 */
public class SerialVersionTest extends TestBase {

    @Test
    public void testGetKVVersion() {
        assertNotNull(SerialVersion.getKVVersion(SerialVersion.CURRENT));
    }
}
