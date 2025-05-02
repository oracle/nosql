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

package oracle.kv.impl.security.metadata;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;

/**
 * A class to help update user and role information maintained in security
 * components once there are changes of user and role definition.
 */
public class SecurityMDUpdater {

    private final Set<UserChangeUpdater> userUpdaters =
        Collections.synchronizedSet(new HashSet<UserChangeUpdater>());

    private final Set<RoleChangeUpdater> roleUpdaters =
        Collections.synchronizedSet(new HashSet<RoleChangeUpdater>());

    public void addUserChangeUpdaters(UserChangeUpdater... updaters) {
        for (final UserChangeUpdater updater : updaters) {
            userUpdaters.add(updater);
        }
    }

    public void addRoleChangeUpdaters(RoleChangeUpdater... updaters) {
        for (final RoleChangeUpdater updater : updaters) {
            roleUpdaters.add(updater);
        }
    }

    /**
     * User definition change listener. Registered user definition updaters will
     * be notified of the changes.
     */
    public class UserChangeListener implements SecurityMDListener {

        @Override
        public void notifyMetadataChange(SecurityMDChange mdChange) {
            if (mdChange.getElementType() != SecurityElementType.KVSTOREUSER) {
                return;
            }

            for (final UserChangeUpdater userUpdater : userUpdaters) {
                userUpdater.newUserDefinition(mdChange);
            }
        }
    }

    /**
     * Role definition change listener. Registered role definition updaters will
     * be notified of the changes.
     */
    public class RoleChangeListener implements SecurityMDListener {

        @Override
        public void notifyMetadataChange(SecurityMDChange mdChange) {
            if (mdChange.getElementType() != SecurityElementType.KVSTOREROLE) {
                return;
            }

            for (final RoleChangeUpdater roleUpdater : roleUpdaters) {
                roleUpdater.newRoleDefinition(mdChange);
            }
        }
    }

    /**
     * Updater interface for user definition change
     * 
     */
    public interface UserChangeUpdater {
        /**
         * Notifies a new user definition change.
         *
         * @param mdChange the security metadata change which should contains
         * the new KVStoreUser instance
         */
        void newUserDefinition(SecurityMDChange mdChange);
    }

    /**
     * Updater interface for role definition change
     */
    public interface RoleChangeUpdater {
        /**
         * Notifies a new role definition change.
         *
         * @param mdChange the security metadata change which should contains
         * the new RoleInstance instance
         */
        void newRoleDefinition(SecurityMDChange mdChange);
    }
}
