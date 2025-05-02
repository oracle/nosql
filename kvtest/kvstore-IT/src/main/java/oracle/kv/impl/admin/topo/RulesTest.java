/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.topo;

import static oracle.kv.impl.admin.topo.Rules.validateAllowArbiters;
import static oracle.kv.impl.admin.topo.Rules.validateChangeReplicationFactor;
import static oracle.kv.impl.admin.topo.Rules.validateMasterAffinity;
import static oracle.kv.impl.admin.topo.Rules.validateReplicationFactor;
import static oracle.kv.impl.topo.DatacenterType.PRIMARY;
import static oracle.kv.impl.topo.DatacenterType.SECONDARY;
import static oracle.kv.util.TestUtils.checkException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.IllegalCommandException;

import oracle.kv.impl.topo.Datacenter;
import org.junit.Test;

/** Tests for the Rules class */
public class RulesTest extends TestBase {
    private static final boolean ALLOW_ARBITERS = true;
    private static final boolean NO_ARBITERS = false;
    private static final boolean MASTER_AFFINITY = true;
    private static final boolean NO_AFFINITY = false;

    @Test
    public void testValidateReplicationFactor() {
        validateReplicationFactor(0, PRIMARY, ALLOW_ARBITERS);
        checkException(
            () -> validateReplicationFactor(0, PRIMARY, NO_ARBITERS),
            IllegalCommandException.class,
            "Replication factor 0 is not permitted on primary zones that" +
            " don't allow arbiters.");
        validateReplicationFactor(1, PRIMARY, ALLOW_ARBITERS);
        validateReplicationFactor(1, PRIMARY, NO_ARBITERS);
        validateReplicationFactor(5, PRIMARY, ALLOW_ARBITERS);
        validateReplicationFactor(5, PRIMARY, NO_ARBITERS);

        /*
         * Note that the arbiter setting isn't permitted here, but this method
         * only checks the RF
         */
        validateReplicationFactor(0, SECONDARY, ALLOW_ARBITERS);

        validateReplicationFactor(0, SECONDARY, NO_ARBITERS);
        validateReplicationFactor(1, SECONDARY, NO_ARBITERS);
        validateReplicationFactor(5, SECONDARY, NO_ARBITERS);
    }

    @Test
    public void testValidateChangeReplicationFactor() {
        final Datacenter primary = Datacenter.newInstance(
            "primary", 2, PRIMARY, NO_ARBITERS, NO_AFFINITY);
        final Datacenter secondary = Datacenter.newInstance(
            "secondary", 2, SECONDARY, NO_ARBITERS, NO_AFFINITY);
        checkException(
            () -> validateChangeReplicationFactor(1, primary),
            IllegalCommandException.class,
            "The replication factor of a primary zone cannot be made" +
            " smaller.");
        validateChangeReplicationFactor(3, primary);
        validateChangeReplicationFactor(1, secondary);
        validateChangeReplicationFactor(3, secondary);
    }

    @Test
    public void testValidateAllowArbiters() {
        validateAllowArbiters(ALLOW_ARBITERS, PRIMARY, 0);
        checkException(
            () -> validateAllowArbiters(NO_ARBITERS, PRIMARY, 0),
            IllegalCommandException.class,
            "Allowing arbiters is required on primary zones with" +
            " replication factor 0.");
        validateAllowArbiters(ALLOW_ARBITERS, PRIMARY, 1);
        validateAllowArbiters(NO_ARBITERS, PRIMARY, 1);
        checkException(
            () -> validateAllowArbiters(ALLOW_ARBITERS, SECONDARY, 0),
            IllegalCommandException.class,
            "Allowing arbiters is not permitted on secondary zones.");
        validateAllowArbiters(NO_ARBITERS, SECONDARY, 0);
        checkException(
            () -> validateAllowArbiters(ALLOW_ARBITERS, SECONDARY, 1),
            IllegalCommandException.class,
            "Allowing arbiters is not permitted on secondary zones.");
        validateAllowArbiters(NO_ARBITERS, SECONDARY, 1);
    }

    @Test
    public void testValidateMasterAffinity() {
        checkException(
            () -> validateMasterAffinity(MASTER_AFFINITY, PRIMARY, 0),
            IllegalCommandException.class,
            "Master affinity is not allowed for primary zones with" +
            " replication factor 0.");
        validateMasterAffinity(NO_AFFINITY, PRIMARY, 0);
        validateMasterAffinity(MASTER_AFFINITY, PRIMARY, 1);
        validateMasterAffinity(NO_AFFINITY, PRIMARY, 1);
        checkException(
            () -> validateMasterAffinity(MASTER_AFFINITY, SECONDARY, 0),
            IllegalCommandException.class,
            "Master affinity is not allowed for secondary zones.");
        validateMasterAffinity(NO_AFFINITY, SECONDARY, 0);
        checkException(
            () -> validateMasterAffinity(MASTER_AFFINITY, SECONDARY, 1),
            IllegalCommandException.class,
            "Master affinity is not allowed for secondary zones.");
        validateMasterAffinity(NO_AFFINITY, SECONDARY, 1);
    }
}
