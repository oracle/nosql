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

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;

/**
 * A convenience class to package all the parameter components used by the
 * Admin service.
 */
public class AdminServiceParams {

    private volatile SecurityParams securityParams;
    private volatile GlobalParams globalParams;
    private final StorageNodeParams storageNodeParams;
    private volatile AdminParams adminParams;

    public AdminServiceParams(SecurityParams securityParams,
                              GlobalParams globalParams,
                              StorageNodeParams storageNodeParams,
                              AdminParams adminParams) {
        super();
        this.securityParams = securityParams;
        this.globalParams = globalParams;
        this.storageNodeParams = storageNodeParams;
        this.adminParams = adminParams;
    }

    public SecurityParams getSecurityParams() {
        return securityParams;
    }

    public GlobalParams getGlobalParams() {
        return globalParams;
    }

    public StorageNodeParams getStorageNodeParams() {
        return storageNodeParams;
    }

    public AdminParams getAdminParams() {
        return adminParams;
    }

    public void setAdminParams(AdminParams newParams) {
        adminParams = newParams;
    }

    public void setGlobalParams(GlobalParams newParams) {
        globalParams = newParams;
    }

    public void setSecurityParams(SecurityParams newParams) {
        securityParams = newParams;
    }
}
