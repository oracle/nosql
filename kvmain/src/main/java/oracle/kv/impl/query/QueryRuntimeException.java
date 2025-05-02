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

package oracle.kv.impl.query;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

/**
 * The QueryRuntimeException is internal used at server side only.
 *
 * A wrapper exception used to wrap RuntimeException thrown from executing
 * query plan at server side. The RuntimeException is wrapped as its cause,
 * and the RequestHandler at the server attempts to handle the cause. If the
 * cause is not handled, the QueryRuntimeException is rethrown, and the
 * ServiceFaultHandler arranges to throw the original RuntimeException to the
 * client.
 *
 * Note that the cause is always a non-null RuntimeException.
 */
public class QueryRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueryRuntimeException(RuntimeException wrappedException) {
        super(checkNull("wrappedException", wrappedException));
    }
}
