/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import java.util.logging.Logger;

import oracle.kv.TestBase;

/** Base class for unit tests */
public class JUnitTestBase extends TestBase {

    /**
     * Use a standard Java logger since the KV one requires the logger name to
     * start with "oracle.kv".
     */
    @Override
    protected Logger getLogger() {
        return Logger.getLogger(getClass().getName());
    }
}
