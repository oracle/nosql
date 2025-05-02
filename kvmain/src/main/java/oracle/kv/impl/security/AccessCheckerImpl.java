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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.TokenVerifier;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.util.BloomFilter;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.RateLimitingLogger;

import oracle.nosql.common.cache.Cache;
import oracle.nosql.common.cache.CacheBuilder;
import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * Standard implementation of AccessChecker.
 */
public class AccessCheckerImpl implements AccessChecker {

    /**
     * The max number of faults to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_FAULTS = 20;

    /**
     * 1 min in millis
     */
    private static final int ONE_MINUTE_MS = 60 * 1000;

    private final TokenVerifier verifier;
    private final RoleResolver roleResolver;
    private volatile Logger logger;

    /*
     * Encapsulates the above logger to limit the rate of log messages
     * associated with a specific fault.
     */
    private volatile RateLimitingLogger<String> rateLimitingLogger;

    /* Cache for mapping a user to all his privileges */
    private final Cache<String, PrivilegeEntry> userPrivCache;

    /**
     * Construct a AccessChecker that uses the provided TokenVerifier
     * to validate method calls. If the cache config is set to null, caches will
     * be disabled.
     */
    public AccessCheckerImpl(TokenVerifier verifier,
                             RoleResolver resolver,
                             CacheConfig config,
                             Logger logger) {
        this(verifier, resolver, config, logger, ONE_MINUTE_MS, LIMIT_FAULTS);
    }

    /*
     * For testing.
     */
    protected AccessCheckerImpl(TokenVerifier verifier,
                                RoleResolver resolver,
                                CacheConfig config,
                                Logger logger,
                                int logPeriod,
                                int faultsLimit) {
        this.verifier = verifier;
        this.roleResolver = resolver;
        this.logger = logger;
        this.rateLimitingLogger = new RateLimitingLogger<String>(
            logPeriod, faultsLimit, logger);
        if (config != null) {
            userPrivCache = CacheBuilder.build(config);
        } else {
            userPrivCache = null;
        }
    }

    /** Stop any threads associated with this instance. */
    public void stop() {
        if (userPrivCache != null) {
            userPrivCache.stop(false);
        }
    }

    /**
     * Log a message describing an access error.
     * @param msg a general message describing the cause of the error
     * @param execCtx the ExecutionContext that encountered the error
     * @param opCtx the OperationContext that was being attempted
     */
    public void logError(String msg,
                         ExecutionContext execCtx,
                         OperationContext opCtx) {

        AccessCheckUtils.logSecurityError(
            msg, opCtx.describe(), execCtx, rateLimitingLogger);
    }

    /**
     * Updates the logger used by this instance.  The logger must always be
     * non-null but it may be changed.
     */
    public void setLogger(Logger logger) {
        this.logger = logger;
        this.rateLimitingLogger = new RateLimitingLogger<String>(
            ONE_MINUTE_MS, LIMIT_FAULTS, logger);
    }

    /**
     * Identifies the requestor of an operation.
     * @param context the identifying context provided by the caller.
     *   This is null allowable.
     * @return a Subject object if the identity could be determined,
     *   or else null.
     * @throws SessionAccessException
     */
    @Override
    public Subject identifyRequestor(AuthContext context)
        throws SessionAccessException {

        if (context == null) {
            return null;
        }

        final LoginToken token = context.getLoginToken();

        if (token == null) {
            return null;
        }

        try {
            return verifier.verifyToken(token);
        } catch (SessionAccessException sae) {
            /*
             * rethrow indicating that the access exception applies to the
             * token supplied with the AuthContext.
             */
            throw new SessionAccessException(sae,
                                             false /* isReturnSignal */);
        }
    }

    /**
     * Check the authorization of the requestor against the requirements
     * of the operation.
     */
    @Override
    public void checkAccess(ExecutionContext execCtx, OperationContext opCtx)
        throws AuthenticationRequiredException, UnauthorizedException {

        final List<? extends KVStorePrivilege> requiredPrivileges =
            opCtx.getRequiredPrivileges();

        if (requiredPrivileges.size() == 0) {
            /*
             * subject could be null here, either because token was null
             * or because it couldn't be validated, but since there are
             * no authentication requirements, we don't worry about it
             * here.
             */
            return;
        }

        final Subject subject = execCtx.requestorSubject();

        if (subject == null) {
            final AuthContext secCtx = execCtx.requestorContext();
            if (secCtx == null || secCtx.getLoginToken() == null) {
                logError("Attempt to call method without authentication",
                         execCtx, opCtx);
                throw new AuthenticationRequiredException(
                    "Authentication required for access",
                    false /* isReturnSignal */);
            }

            /*
             * All of internal login sessions are allocated by Storage Nodes,
             * so use the allocator in login token to distinguish the access
             * type, and log different warning messages to make errors from
             * internal accesses clearer and more descriptive.
             */
            final ResourceId resourceId =
                secCtx.getLoginToken().getSessionId().getAllocator();
            if (resourceId != null && resourceId.getType().isStorageNode()) {
                logInvalidInternalTokenError(execCtx, opCtx);
            } else {
                logError("Attempt to call method with invalid " +
                         "authentication: the login token is invalid or " +
                         "expired, and should either be renewed " +
                         "automatically or reauthentication should be " +
                         "performed.",
                         execCtx, opCtx);
            }
            throw new AuthenticationRequiredException(
                "Authentication required for access",
                false /* isReturnSignal */);
        }

        /*
         * Checks whether all required privileges are implied by requestor's
         * granted privileges.
         */
        if (!execCtx.hasAllPrivileges(requiredPrivileges)) {
            /* Permission check failed. */
            logError("Insufficient access rights", execCtx, opCtx);
            throw new UnauthorizedException(
                "Insufficient access rights granted. requestorPrivileges:" +
                    execCtx.requestorPrivileges() +
                "  requiredPrivileges:" + opCtx.getRequiredPrivileges());
        }
    }

    /*
     * Simply logging token validation failure as access error is confusing for
     * internal authentication, the internal nodes' accesses are expected to
     * renew login token after capture the access errors. Note that this is
     * kind of assumption, since we cannot tell at this point if the token is
     * allocated by Storage node or faked by malicious user. However, if
     * malicious user can successfully send a token at this point that means
     * the internal trusted connections are already compromised, logging a
     * warning here seems meaningless, just log with INFO level for monitoring
     * the internal accesses events.
     */
    private void logInvalidInternalTokenError(ExecutionContext execCtx,
                                              OperationContext opCtx) {
        if (rateLimitingLogger.getInternalLogger() == null) {
            return;
        }

        final KVStoreUserPrincipal user = ExecutionContext.
            getSubjectUserPrincipal(execCtx.requestorSubject());
        if (user != null) {
            logError("Unexpected access by internal node as user " + user,
                     execCtx, opCtx);
            return;
        }
        rateLimitingLogger.log(opCtx.describe(), Level.INFO,
            String.format("Internal operation %s access from %s, accessing " +
                          "with an expired or non-existent login token, " +
                          "will attempt to acquire a new token.",
                          opCtx.describe(), execCtx.requestorHost()));
    }

    /**
     * Identifies privileges of a specified Subject
     */
    @Override
    public Set<KVStorePrivilege> identifyPrivileges(Subject reqSubj) {
        if (reqSubj == null) {
            return null;
        }

        final KVStoreUserPrincipal user =
            ExecutionContext.getSubjectUserPrincipal(reqSubj);

        /*
         * For subjects from internal login and anonymous login, the user
         * principal could be null. We do not cache in these cases, since both
         * subjects have a limit number of system built-in roles, and thus
         * their privileges can be resolved quickly.
         */
        if (userPrivCache != null && user != null) {
            final PrivilegeEntry privEntry =
                userPrivCache.get(user.getUserId());
            if (privEntry != null) {
                return privEntry.getPrivileges();
            }
        }

        /*
         * No cached subj privileges, try to resolve by recursively traversing
         * the granted role
         */
        final Set<KVStorePrivilege> subjPrivSet =
            new HashSet<KVStorePrivilege>();
        final Set<String> subjRoleSet = new HashSet<String>();

        final Set<KVStoreRolePrincipal> reqRoles =
            reqSubj.getPrincipals(KVStoreRolePrincipal.class);
        for (final KVStoreRolePrincipal princ : reqRoles) {
            final String roleName = princ.getName();
            recursiveGetRolesAndPrivis(roleName, subjRoleSet, subjPrivSet);
        }

        if (userPrivCache != null  && user != null) {

            /* Do not cache IDCS OAuth user privilege resolution results */
            final String userId = user.getUserId();
            if (userId != null &&
                userId.startsWith(SecurityUtils.IDCS_OAUTH_USER_ID_PREFIX)) {
                return subjPrivSet;
            }
            userPrivCache.put(user.getUserId(),
                              new PrivilegeEntry(user.getUserId(),
                                                 subjPrivSet,
                                                 subjRoleSet));
        }
        return subjPrivSet;
    }

    /*
     * For testing.
     */
    RateLimitingLogger<String> getRateLimitingLogger() {
        return rateLimitingLogger;
    }

    /**
     * Get role privileges recursively.
     *
     * @param roleName of role that need to get all privileges recursively.
     * @param roleSet contains all leaf roles granted to this role
     * @param priviSet contains all privileges of given role and its granted
     * roles.
     */
    private void recursiveGetRolesAndPrivis(String roleName,
                                            Set<String> roleSet,
                                            Set<KVStorePrivilege> priviSet) {
        final RoleInstance role = roleResolver.resolve(roleName);
        if (role == null) {
            logger.info("Could not resolve role with name of " + roleName);
        } else {
            logger.fine("Role " + roleName + " resolved successfully.");
            priviSet.addAll(role.getPrivileges());
            roleSet.add(roleName);
            for (final String grantedRole : role.getGrantedRoles()) {
                recursiveGetRolesAndPrivis(grantedRole, roleSet, priviSet);
            }
        }
    }

    /**
     * Updates the cache due to role definition change, including the changes
     * of granted roles or privileges of this role.
     *
     * @return true if there are privilege entries contains this role
     * information and removed successfully.
     */
    public boolean updateRoleDefinition(RoleInstance role) {
        /* signal indicate if any entry is removed from the cache*/
        boolean removed = false;
        final Collection<PrivilegeEntry> allPrivEntries =
                userPrivCache.getAllValues();
        for (PrivilegeEntry privEntry : allPrivEntries) {
            if (privEntry.hasRole(role.name())) {
                PrivilegeEntry entry =
                    userPrivCache.remove(privEntry.getPrincId());

                if (!removed) {
                    removed = (entry != null);
                }
            }
        }
        return removed;
    }

    /**
     * Updates the cache due to user definition change, which is the changes
     * of granted roles of this user.
     *
     * @return true if there is user in privilege cache and removed
     * successfully.
     */
    public boolean updateUserDefinition(KVStoreUser user) {

        return (userPrivCache.remove(user.getElementId()) != null);
    }

    /**
     * Privilege entry for subject-privilege cache.  Besides privileges, each
     * entry also contains a bloom filter of leaf roles the subject contains,
     * in order to ease the user lookup when a role change comes for cache
     * update. Note that using BllomFilter to check role existence would have
     * false positives. However, false positives here could lead to only
     * excessive cache remove and update, and thus do not harm the whole
     * correctness.
     */
    public static class PrivilegeEntry {
        private final Set<KVStorePrivilege> privsSet;
        private final String princId;
        private final byte[] leafRoleBf;

        public PrivilegeEntry(String princId,
                              Set<KVStorePrivilege> privsSet,
                              Set<String> leafRoles) {
            this.privsSet = privsSet;
            this.princId = princId;

            /* Build flat role bloom filter */
            if (leafRoles == null || leafRoles.isEmpty()) {
                leafRoleBf = null;
            } else {
                final int bfSize = BloomFilter.getByteSize(leafRoles.size());
                leafRoleBf = new byte[bfSize];
                final BloomFilter.HashContext hc =
                    new BloomFilter.HashContext();
                for (String role : leafRoles) {
                    BloomFilter.add(
                        leafRoleBf,
                        RoleInstance.getNormalizedName(role).getBytes(),
                        hc);
                }
            }
        }

        /**
         * Get the id of principal owns this entry, could be a user id.
         */
        String getPrincId() {
            return princId;
        }

        public Set<KVStorePrivilege> getPrivileges() {
            return Collections.unmodifiableSet(privsSet);
        }

        public boolean hasRole(String name) {
            if (leafRoleBf == null) {
                return false;
            }
            return BloomFilter.contains(
                leafRoleBf, RoleInstance.getNormalizedName(name).getBytes());
        }
    }
}
