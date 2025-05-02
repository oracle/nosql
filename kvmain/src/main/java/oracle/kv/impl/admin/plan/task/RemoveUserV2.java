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

package oracle.kv.impl.admin.plan.task;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MultiMetadataPlan;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Remove user task used by MultiMetadataPlan.
 */
public class RemoveUserV2 extends RemoveUser {

    private static final long serialVersionUID = 1L;

    private final MultiMetadataPlan multiMetadataPlan;

    public static RemoveUserV2 newInstance(MultiMetadataPlan plan,
                                           String userName) {
        final RemoveUserV2 removeUserV2 = new RemoveUserV2(plan, userName);
        removeUserV2.guardLastSysadminUser();
        return removeUserV2;
    }

    private RemoveUserV2(MultiMetadataPlan plan, String userName) {
        super(null, userName);
        this.multiMetadataPlan = plan;
    }

    @Override
    protected SecurityMetadata getMetadata() {
        return multiMetadataPlan.getSecurityMetadata();
    }

    @Override
    protected SecurityMetadata getMetadata(Transaction txn) {
        return multiMetadataPlan.getSecurityMetadata(txn);
    }

    @Override
    protected AbstractPlan getPlan() {
        return this.multiMetadataPlan;
    }
}
