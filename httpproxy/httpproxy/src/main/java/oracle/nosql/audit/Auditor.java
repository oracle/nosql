/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.audit;

import java.io.IOException;

/**
 *  An interface used for doing the audit work.
 */
public interface Auditor {

    /**
     * To audit AuditContext.
     */
    public void audit(AuditContext auditContext) throws IOException;
}
