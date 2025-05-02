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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.fault.ProcessFaultHandler.Operation;
import oracle.kv.impl.fault.ProcessFaultHandler.Procedure;

/**
 * ExecutionContext contains the logic to manage the user identity and
 * operation description. It includes methods for recording the execution
 * for Thread-specific access at later points in the execution of an operation.
 */
public final class ExecutionContext {

    /**
     * Thread-local state tracking the per-thread ExecutionContext.
     * This allows deferred access checks to locate the relevant security
     * information.
     */
    private static final ThreadLocal<ExecutionContext> currentContext =
        new ThreadLocal<ExecutionContext>();

    /**
     * The AuthContext object provided in a remote call, which provides
     * identification of the caller.  This may be null.
     */
    private final AuthContext requestorCtx;

    /**
     * The identity and capabilities of the caller derived from the AuthContext
     * object provided in a remote call. This may be null.
     */
    private final Subject requestorSubj;

    /**
     * An object describing the operation and required authorization for the
     * operation.
     */
    private final OperationContext opCtx;

    /**
     * A string identifying the host from which the access originated.
     */
     private final String clientHost;

     private final PrivilegeCollection privileges;

    /**
     * Construct an operation context object, which collects the
     * interesting bits of information about a thread of execution for
     * the purpose of security validation, etc.
     *
     * @param requestorCtx the identifying information provided by the caller.
     *   This may be null
     * @param requestorSubject the identity of the caller, derrived from the
     *   requestorCtx.  This may be null, and will be null if requestorCtx is
     *   null.
     * @param opCtx the operation description object
     */
    private ExecutionContext(AuthContext requestorCtx,
                             Subject requestorSubject,
                             PrivilegeCollection requestorPrivis,
                             OperationContext opCtx) {
        this.requestorCtx = requestorCtx;
        this.requestorSubj = requestorSubject;
        this.opCtx = opCtx;
        this.clientHost = AsyncVersionedRemoteDialogResponder.getClientHost();
        this.privileges = requestorPrivis;
    }

    /**
     * Get the context object that identifies and authorizes the requestor
     * to perform an action.
     */
    public AuthContext requestorContext() {
        return requestorCtx;
    }

    /**
     * Get the Subject that describes the requestor and the associated
     * authorizations.
     */
    public Subject requestorSubject() {
        return requestorSubj;
    }

    /**
     * Get the host from which the request originated.
     */
    public String requestorHost() {
        return clientHost;
    }

    /**
     * Get the context object that describes the action being performed
     * by the user.
     */
    public OperationContext operationContext() {
        return opCtx;
    }

    /**
     * Gets the privileges associated with the requestor.
     */
    public PrivilegeCollection requestorPrivileges() {
        return privileges;
    }

    /**
     * Create an execution context object and perform the initial
     * security check on it, based on the access requirements defined by
     * the accessCheck.
     *
     * @param accessCheck the access checker for the environment, which
     *   performs security checking operations.
     * @param requestorCtx the context object identifying the caller
     * @param opCtx the context object identifying the type of operation
     *   being performed, for the purpose of reporting.
     */
    public static ExecutionContext create(AccessChecker accessCheck,
                                          AuthContext requestorCtx,
                                          OperationContext opCtx)
        throws AuthenticationRequiredException, UnauthorizedException,
               SessionAccessException {

        final Subject reqSubj;
        final PrivilegeCollection subjPrivis;
        if (accessCheck == null) {
            reqSubj = null;
            subjPrivis = null;
        } else {
            reqSubj = accessCheck.identifyRequestor(requestorCtx);
            subjPrivis = new PrivilegeCollection(
                accessCheck.identifyPrivileges(reqSubj));
        }

        final ExecutionContext execCtx =
            new ExecutionContext(requestorCtx, reqSubj, subjPrivis, opCtx);

        if (accessCheck != null) {
            accessCheck.checkAccess(execCtx, opCtx);
        }

        return execCtx;
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static <R, E extends Exception> R runWithContext(
        final Operation<R, E> operation, final ExecutionContext execCtx)
        throws E {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            return operation.execute();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static <R> R runWithContext(
        final Supplier<R> operation, final ExecutionContext execCtx) {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            return operation.get();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static <E extends Exception> void runWithContext(
        final Procedure<E> procedure, final ExecutionContext execCtx)
        throws E {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            procedure.execute();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static void runWithContext(
        final Runnable procedure, final ExecutionContext execCtx) {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            procedure.run();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Find out the current execution context.
     */
    public static ExecutionContext getCurrent() {
        return currentContext.get();
    }

    /**
     * Find out the current execution user Subject.
     */
    public static Subject getCurrentUserSubject() {
        final ExecutionContext execCtx = getCurrent();

        if (execCtx == null) {
            return null;
        }

        return execCtx.requestorSubject();
    }

    /**
     * Find out the current execution user client host.
     */
    public static String getCurrentUserHost() {
        final ExecutionContext execCtx = getCurrent();

        if (execCtx == null) {
            return null;
        }

        return execCtx.requestorHost();
    }

    /**
     * Find the user principal associated with the current execution user
     * Subject.
     */
    public static KVStoreUserPrincipal getCurrentUserPrincipal() {
        return getSubjectUserPrincipal(getCurrentUserSubject());
    }

    /**
     * Finds the privileges associated with current execution context. The
     * result may be null.
     */
    public static PrivilegeCollection getCurrentPrivileges() {
        final ExecutionContext execCtx = getCurrent();

        if (execCtx == null) {
            return null;
        }

        return execCtx.requestorPrivileges();
    }

    /**
     * Find the user principal associated with the specified Subject.
     */
    public static KVStoreUserPrincipal getSubjectUserPrincipal(Subject subj) {
        if (subj == null) {
            return null;
        }

        final Set<KVStoreUserPrincipal> userPrincs =
            subj.getPrincipals(KVStoreUserPrincipal.class);

        if (userPrincs.isEmpty()) {
            return null;
        }

        if (userPrincs.size() != 1) {
            throw new IllegalStateException(
                "Current user has multiple user principals");
        }

        return userPrincs.iterator().next();
    }

    /**
     * Find the role principals associated with the specified Subject.
     * @return a newly allocated array containing the associated role
     * principals.
     */
    public static KVStoreRolePrincipal[] getSubjectRolePrincipals(
        Subject subj) {

        if (subj == null) {
            return null;
        }

        final Set<KVStoreRolePrincipal> rolePrincs =
            subj.getPrincipals(KVStoreRolePrincipal.class);

        return rolePrincs.toArray(new KVStoreRolePrincipal[rolePrincs.size()]);
    }

    /**
     * Find the roles associated with the specified Subject.
     * @return a newly allocated array containing the names of associated roles.
     */
    public static String[] getSubjectRoles(Subject subj) {

        if (subj == null) {
            return null;
        }

        final Set<KVStoreRolePrincipal> rolePrincs =
            subj.getPrincipals(KVStoreRolePrincipal.class);

        final List<String> roles = new ArrayList<String>();
        for (KVStoreRolePrincipal princ : rolePrincs) {
            roles.add(princ.getName());
        }
        return roles.toArray(new String[roles.size()]);
    }

    /**
     * Tests whether the ExecutionContext Subject has the specified role
     * assigned to it.
     * @param roleName name of the RoleInstance to look for
     * @return true if the subject has the specified role
     */
    public boolean hasRole(final String roleName) {
        return subjectHasRole(requestorSubj, roleName);
    }

    /**
     * Tests whether a particular Subject has the specified role assigned to it.
     * @param subj a Subject to inspect
     * @param roleName name of RoleInstance to look for
     * @return true if the subject has the specified role
     */
    public static boolean subjectHasRole(Subject subj, String roleName) {
        /* implemented inline to minimize overhead */
        for (Object princ : subj.getPrincipals()) {
            if (KVStoreRolePrincipal.class.isAssignableFrom(princ.getClass())) {
                final KVStoreRolePrincipal rolePrinc =
                    (KVStoreRolePrincipal) princ;
                if (rolePrinc.getName().equals(roleName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Tests whether the ExecutionContext Subject has all the specified
     * privileges.
     *
     * @param privis collection of privileges to look for
     * @return true if all the specified privileges are granted to or owned by
     * the subject
     */
    public boolean hasAllPrivileges(
        final Collection<? extends KVStorePrivilege> privis) {

        return privileges != null && privileges.impliesAll(privis);
    }

    /**
     * Tests whether the ExecutionContext Subject has the specified privilege.
     *
     * @param privi the privilege to look for
     * @return true if the specified privilege is granted to or owned by the
     * subject
     */
    public boolean hasPrivilege(KVStorePrivilege privi) {
        return privileges != null && privileges.implies(privi);
    }

    public static class PrivilegeCollection {

        private final Set<KVStorePrivilege> privilegeSet;

        PrivilegeCollection(final Set<KVStorePrivilege> privileges) {
            privilegeSet = privileges;
        }

        @Override
        public String toString() {
            return "PrivilegeCollection" + privilegeSet;
        }

        public boolean implies(final KVStorePrivilege privilege) {
            if (privilegeSet == null) {
                return false;
            }

            /*
             * If the privilege exists, return true; otherwise check the
             * implication.
             */
            if (privilegeSet.contains(privilege)) {
                return true;
            }

            final KVStorePrivilege[] implications =
                privilege.implyingPrivileges();
            if (implications != null) {
                for (final KVStorePrivilege privi : implications) {
                    if (privilegeSet.contains(privi)) {
                        return true;
                    }
                }
            }

            return false;
        }

        public boolean impliesAll(
            final Collection<? extends KVStorePrivilege> privileges) {

            if (privileges == null) {
                return false;
            }

            for (final KVStorePrivilege priviToCheck : privileges) {
                if (!implies(priviToCheck)) {
                    return false;
                }
            }
            return true;
        }
    }
}
