/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.topo;

import static oracle.kv.impl.util.TestUtils.checkFastSerialize;

import oracle.kv.TestBase;

import org.junit.Test;

/** Test {@link RepNodeId}. */
public class RepNodeIdTest extends TestBase {

    @Test
    public void testSerializeDeserialize() throws Exception {
        checkFastSerialize(new RepNodeId(1, 2), ResourceId::readFastExternal);
    }
}
