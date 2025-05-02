/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import java.io.IOException;

import com.sleepycat.je.rep.stream.ReplicaFeederSyncup;
import com.sleepycat.je.utilint.TestHookAdapter;

/**
 * Implements the JE TestHook class.
 * Support adding a rollback key in java property.
 * If the hook execution sees the rollback key, it will do the JE level
 * rollback test hook injection.
 */
public class RollbackTestHook extends TestHookAdapter<ReplicaFeederSyncup> {
    public static final String ROLLBACK_TEST_HOOK_KEY =
        "oracle.kv.test.rollback";

    public static void installIfSet() {
        if (Boolean.getBoolean(ROLLBACK_TEST_HOOK_KEY)) {
            ReplicaFeederSyncup.setRollbackTestHook(new RollbackTestHook());
        }
    }

    @Override
    public void doHook(ReplicaFeederSyncup sync) {
        try {
            sync.setupHardRecovery();
        } catch (IOException e) /* CHECKSTYLE:OFF */ {
            /* Ignore the failure of failure injection */
        } /* CHECKSTYLE:ON */
        finally {
            /* Clean up the test hook */
            ReplicaFeederSyncup.setRollbackTestHook(null);
        }
    }
}
