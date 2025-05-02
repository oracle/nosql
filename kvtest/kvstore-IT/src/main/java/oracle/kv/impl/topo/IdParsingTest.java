/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.topo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test that each component id can parse a variety of representations of its
 * name.
 */
public class IdParsingTest extends TestBase {

    private final boolean verbose;

    public IdParsingTest() {
        verbose = Boolean.getBoolean("test.verbose");
    }

    /** StorageNodeIds can be represented as N, snN, SNN */
    @Test
    public void testParseStorageNodeId() {

        String[] good = 
            new String[] {"1", "sn1", "SN1", "Sn1", "sN1"};
        for (String s : good) {
            StorageNodeId snId = StorageNodeId.parse(s);
            assertEquals(1, snId.getStorageNodeId());
        }

        good = new String[] {"123", "sn123", "SN123", "Sn123", "sN123"};
        for (String s : good) {
            StorageNodeId snId = StorageNodeId.parse(s);
            assertEquals(123, snId.getStorageNodeId());
        }

        String[] bad = new String[] {"SN-10", "x0", "s10"};
        for (String s : bad) {
            try {
                StorageNodeId.parse(s);
                fail(s + " is not valid, should fail");
            } catch (IllegalArgumentException expected) {
                if (verbose) {
                    System.err.println(expected);
                }
            }
        }
    }

    @Test
    public void testParseRepNodeId() {
        String[] good = 
            new String[] {"rg10-rn5","RG10-RN5","Rg10-Rn5", "10,5"};
        for (String s : good) {
            RepNodeId rnId = RepNodeId.parse(s);
            assertEquals(10, rnId.getGroupId());
            assertEquals(5, rnId.getNodeNum());
        }

        good = new String[] {"rg1-rn1","RG1-RN1","Rg1-Rn1", "1,1"};
        for (String s : good) {
            RepNodeId rnId = RepNodeId.parse(s);
            assertEquals(1, rnId.getGroupId());
            assertEquals(1, rnId.getNodeNum());
        }

        String[] bad = new String[] {"rn10-rg5", "rg-10-rn-5"};
        for (String s : bad) {
            try {
                RepNodeId.parse(s);
                fail(s + " is not valid, should fail");
            } catch (IllegalArgumentException expected) {
                if (verbose) {
                    System.err.println(expected);
                }
            }
        }
    }

    @Test
    public void testParseAdminId() {
        String[] good = 
            new String[] {"10", "Admin10", "admin10", "ADMIN10"};
        for (String s : good) {
            AdminId id = AdminId.parse(s);
            assertEquals(10, id.getAdminInstanceId());
        }
        good = new String[] {"1", "Admin1", "admin1", "ADMIN1"};
        for (String s : good) {
            AdminId id = AdminId.parse(s);
            assertEquals(1, id.getAdminInstanceId());
        }

        String[] bad = new String[] {"adminX", "admin-10"};
        for (String s : bad) {
            try {
                AdminId.parse(s);
                fail(s + " is not valid, should fail");
            } catch (IllegalArgumentException expected) {
                if (verbose) {
                    System.err.println(expected);
                }
            }
        }
    }

    @Test
    public void testParseDatacenterId() {
        String[] good = new String[] {
            "10", "zn10", "ZN10", "zn10", "dc10", "DC10", "dc10"
        };
        for (String s : good) {
            DatacenterId id = DatacenterId.parse(s);
            assertEquals(10, id.getDatacenterId());
        }
        good = new String[] {"1", "zn1", "ZN1", "zn1", "dc1", "DC1", "dc1"};
        for (String s : good) {
            DatacenterId id = DatacenterId.parse(s);
            assertEquals(1, id.getDatacenterId());
        }

        String[] bad = new String[] {
            "znX", "zn-10", "Zone10", "dcX", "dc-10", "DataCenter10"
        };
        for (String s : bad) {
            try {
                DatacenterId.parse(s);
                fail(s + " is not valid, should fail");
            } catch (IllegalArgumentException expected) {
                if (verbose) {
                    System.err.println(expected);
                }
            }
        }
    }
}
