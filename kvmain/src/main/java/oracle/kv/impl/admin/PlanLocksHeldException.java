/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.admin;

import oracle.kv.ContingencyException;

/**
 * A checked exception used internally to communicate the fact that a given
 * plan can't obtain its catalog locks. The id of the plan that does own the 
 * contested lock is made available.
 */
public class PlanLocksHeldException extends ContingencyException {

    private final int owningPlanId;
    private static final long serialVersionUID = 1L;

    public PlanLocksHeldException(String message, int runningPlanId) {
        super(message);
        this.owningPlanId = runningPlanId;
    }

    public int getOwningPlanId() {
        return owningPlanId;
    }
}
