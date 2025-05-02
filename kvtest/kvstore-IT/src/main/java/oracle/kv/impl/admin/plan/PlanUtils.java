/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan;

import oracle.kv.impl.admin.CommandServiceAPI;

public class PlanUtils {

    /**
     * Use the command service to run the plan with the specified ID and
     * confirm that the plan succeeds.
     */
    public static void runPlan(CommandServiceAPI cs, int planId)
        throws Exception
    {
        cs.approvePlan(planId);
        cs.executePlan(planId, false /* force */);
        cs.awaitPlan(planId, 0 /* timeout */, null);
        cs.assertSuccess(planId);
    }
}
