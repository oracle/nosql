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
package oracle.kv.impl.security;


import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;

/**
 * An interface defining the required support behavior for checking
 * access rights for proxy object access.
 */
public interface AccessChecker {

    /**
     * Given a auth context object, locate a Subject object that describes
     * the requestor.
     *
     * @param authCtx the security context, which might be null, that
     *        identifies the requestor
     * @return a Subject representing the caller, or null if there is
     *         no user context.
     * @throws SessionAccessException
     */
    Subject identifyRequestor(AuthContext authCtx)
        throws SessionAccessException;


    /**
     * Check that a pending invocation of a method is valid.
     *
     * @param execCtx an object describing the context in which the security
     *        check is being made.
     * @param opCtx an object describing the operation  in which the security
     *        check is being made.
     * @throws AuthenticationRequiredException if security checking is
     *        in force and the security context does not contain a
     *        valid identity.
     * @throws UnauthorizedException if security checking is in force and
     *        the identity indicated by the security context does not
     *        have the necessary access rights to make the call.
     * @throws SessionAccessException
     */
    void checkAccess(ExecutionContext execCtx, OperationContext opCtx)
        throws AuthenticationRequiredException, UnauthorizedException,
               SessionAccessException;

    /**
     * Given a Subject object, find the privileges associated with it.
     *
     * @param subj the subject who's privileges are to be looked up, which
     * might be null
     * @return a Collection of KVStorePrivileges associated with the requestor
     * Subject. Null will be returned if the subject is null.
     */
    Set<KVStorePrivilege> identifyPrivileges(Subject subj);
}
