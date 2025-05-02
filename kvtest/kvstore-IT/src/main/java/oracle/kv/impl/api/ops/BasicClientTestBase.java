/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import oracle.kv.TestBase;

/**
 * Base class for client tests.
 */
public class BasicClientTestBase extends TestBase {

    /**
     * Override the default version, since clearing the test directory is only
     * possible in server-side tests.
     */
    @Override
    protected void clearTestDirectory() { }
}
