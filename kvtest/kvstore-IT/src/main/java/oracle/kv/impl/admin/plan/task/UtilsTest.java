/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import oracle.kv.impl.admin.AdminTestBase;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.StorageNodeId;

public class UtilsTest extends AdminTestBase {

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test for verifying the consistency of search parameters,
     * and for acquiring a copy of those parameters.
     */
    @Test
    public void testVerifyAndGetSearchParams() {

        final String clusterName1 = "JiminyCricket";
        final String clusterMembers1 = "42.42.42.42:42";

        Parameters p = new Parameters("test");

        StorageNodeParams snp1 =
            new StorageNodeParams(new ParameterMap(ParameterState.SNA_TYPE,
                                                    ParameterState.SNA_TYPE));
        snp1.setStorageNodeId(new StorageNodeId(1));
        StorageNodeParams snp2 =
            new StorageNodeParams(new ParameterMap(ParameterState.SNA_TYPE,
                                                    ParameterState.SNA_TYPE)); 
        snp2.setStorageNodeId(new StorageNodeId(2));
        StorageNodeParams snp3 =
            new StorageNodeParams(new ParameterMap(ParameterState.SNA_TYPE,
                                                    ParameterState.SNA_TYPE));
        snp3.setStorageNodeId(new StorageNodeId(3));

        p.add(snp1);

        ParameterMap pm = Utils.verifyAndGetSearchParams(p);

        assertEquals("", pm.getOrDefault
                     (ParameterState.SN_SEARCH_CLUSTER_NAME).asString());

        snp1.setSearchClusterName(clusterName1);

        Exception iseThrown = null;
        try {
            Utils.verifyAndGetSearchParams(p);
        } catch (CommandFaultException ise) {
            iseThrown = ise;
        }
        assertTrue(iseThrown != null);
        assertTrue(iseThrown.getMessage().contains
                   ("sn1's search cluster parameters are not consistent."));
        snp1.setSearchClusterMembers(clusterMembers1);

        pm = Utils.verifyAndGetSearchParams(p);

        assertEquals(
                     pm.getOrDefault
                     (ParameterState.SN_SEARCH_CLUSTER_NAME).asString(),
                     clusterName1);

        snp2.setSearchClusterName(clusterName1);
        snp2.setSearchClusterMembers(clusterMembers1);

        p.add(snp2);

        pm = Utils.verifyAndGetSearchParams(p);

        assertEquals(
                     pm.getOrDefault
                     (ParameterState.SN_SEARCH_CLUSTER_MEMBERS).asString(),
                     clusterMembers1);

        snp3.setSearchClusterName("IncorrectName");
        snp3.setSearchClusterMembers(clusterMembers1);
        
        p.add(snp3);

        iseThrown = null;
        try {
            Utils.verifyAndGetSearchParams(p);
        } catch (CommandFaultException ise) {
            iseThrown = ise;
        }
        assertTrue(iseThrown != null);
        assertTrue(iseThrown.getMessage().contains
                   ("sn3's search cluster parameters do not match other SNs."));
    }
}

