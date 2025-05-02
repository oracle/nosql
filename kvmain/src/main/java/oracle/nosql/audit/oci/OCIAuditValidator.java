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
package oracle.nosql.audit.oci;

import oracle.nosql.audit.AuditContext;
import oracle.nosql.audit.AuditContext.AuditContextValidator;

/**
 * To validate OCIAuditContext.
 */
public class OCIAuditValidator implements AuditContextValidator {

    @Override
    public boolean validate(AuditContext context) {
        if (!(context instanceof OCIAuditContext)) {
            return false;
        }
        final OCIAuditContext ociContext = (OCIAuditContext) context;
        final String eventType = ociContext.eventType;
        if (eventType == null || eventType.isEmpty()) {
            return false;
        }
        final String eventId = ociContext.eventId;
        if (eventId == null || eventId.isEmpty()) {
            return false;
        }
        final String partitionId = ociContext.getPartitionId();
        if (partitionId == null || partitionId.isEmpty()) {
            return false;
        }
        return true;
    }

}
