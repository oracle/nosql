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

/**
 * An interface used for recording audit context which will be used to generate
 * audit content.
 */
public interface AuditContext {

    /**
     * An interface for building an AuditContext.
     */
    public interface AuditContextBuilder {
        /**
         * @return build an AuditContext.
         */
        AuditContext[] build();
    }

    /**
     * An interface for validating AuditContext.
     */
    public interface AuditContextValidator {
        /**
         * @param context to be validated.
         * @return true if AuditContext is valid.
         */
        public boolean validate(AuditContext context);
    }
}
