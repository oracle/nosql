/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.TestBase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests RemoteFile.
 */
public class EnvParamsTest extends TestBase {

    @Test
    public void testBasic() {
        File dir1 = new File("test.file");
        File dir2 = new File("test.file");
        SNAInfo si1 = new SNAInfo("mystore", "sn1", "localhost", "~/kvroot1");
        SNAInfo si2 = new SNAInfo("mystore", "sn1", "localhost", "~/kvroot1");
        JavaVersionVerifier javaVerifier1 = new JavaVersionVerifier();
        JavaVersionVerifier javaVerifier2 = new JavaVersionVerifier();
        Map<SNAInfo, Boolean> map = new HashMap<SNAInfo, Boolean>();
        map.put(si1, true);
        EnvParams envParams = new EnvParams(100, javaVerifier1, map, dir1);

        assertEquals(envParams.getLatency(), 100);
        assertEquals(envParams.getJavaVersion().toString(),
                     javaVerifier2.toString());

        Map<SNAInfo, Boolean> snaMap = envParams.getNetworkConnectionMap();
        Map.Entry<SNAInfo, Boolean> entry = snaMap.entrySet().iterator().next();
        assertEquals(entry.getKey(), si2);
        assertTrue(entry.getValue());

        assertEquals(envParams.getSaveFolder(), dir2);
    }
}
