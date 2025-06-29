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

package oracle.kv.impl.util;

/**
 * Runtime Exception for external data source.
 */
public class ExternalDataSourceException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ExternalDataSourceException(String msg) {
        super(msg);
    }

    public ExternalDataSourceException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
