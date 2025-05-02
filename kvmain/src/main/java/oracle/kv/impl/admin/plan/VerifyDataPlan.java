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

package oracle.kv.impl.admin.plan;

import java.util.List;

import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.test.TestHook;

/**
 * NOTE: This class is for backward compatibility only, it has been replaced
 * by VerifyDataPlanV2.
 *
 */
public class VerifyDataPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;
    public volatile static TestHook<Object[]> VERIFY_HOOK;


    private VerifyDataPlan() {
        super(null, null);
    }

    @Override
    public String getDefaultName() {
        return "Verify Data";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
