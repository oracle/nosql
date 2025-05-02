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

package oracle.kv.impl.async;

import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.api.AsyncRequestHandler;
import oracle.kv.impl.arb.admin.ArbNodeAdmin;
import oracle.kv.impl.async.registry.ServiceRegistry;
import oracle.kv.impl.client.admin.AsyncClientAdminService;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.rep.admin.AsyncClientRepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.security.login.AsyncUserLogin;
import oracle.kv.impl.security.login.TrustedLogin;
import oracle.kv.impl.sna.StorageNodeAgentInterface;
import oracle.kv.impl.test.RemoteTestInterface;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The standard dialog type families for asynchronous service interfaces.
 */
public enum StandardDialogTypeFamily implements DialogTypeFamily {

    /** Dialog type family for {@link ServiceRegistry}. */
    SERVICE_REGISTRY_TYPE_FAMILY(0),

    /** Dialog type family for {@link AsyncRequestHandler}. */
    REQUEST_HANDLER_TYPE_FAMILY(1),

    /** Dialog type family for {@link AsyncClientAdminService}. */
    CLIENT_ADMIN_SERVICE_TYPE_FAMILY(2),

    /**
     * Dialog type family for {@link AsyncClientRepNodeAdmin} and {@link
     * RepNodeAdmin}.
     */
    REP_NODE_ADMIN_TYPE_FAMILY(3),

    /** Dialog type family for {@link AsyncUserLogin}. */
    USER_LOGIN_TYPE_FAMILY(4),

    /** Dialog type family for {@link ArbNodeAdmin}. */
    ARB_NODE_ADMIN_TYPE_FAMILY(5),

    /** Dialog type family for {@link MonitorAgent}. */
    MONITOR_AGENT_TYPE_FAMILY(6),

    /** Dialog type family for {@link TrustedLogin}. */
    TRUSTED_LOGIN_TYPE_FAMILY(7),

    /** Dialog type family for {@link CommandService}. */
    COMMAND_SERVICE_TYPE_FAMILY(8),

    /** Dialog type family for {@link StorageNodeAgentInterface}. */
    STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY(9),

    /** Dialog type family for {@link RemoteTestInterface}. */
    REMOTE_TEST_INTERFACE_TYPE_FAMILY(10);

    /**
     * The bootstrap dialog type used for the service registry, which has a
     * known dialog type ID.
     */
    public static final DialogType SERVICE_REGISTRY_DIALOG_TYPE =
        new DialogType(0, SERVICE_REGISTRY_TYPE_FAMILY);

    private static final StandardDialogTypeFamily[] VALUES = values();

    private StandardDialogTypeFamily(int ordinal) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
        DialogType.registerTypeFamily(this);
    }

    @Override
    public int getFamilyId() {
        return ordinal();
    }

    @Override
    public String getFamilyName() {
        return name();
    }

    /**
     * Returns the enum constant with the specified ordinal value, or null if
     * the ordinal is out of bounds.
     */
    public static @Nullable StandardDialogTypeFamily getOrNull(int ordinal) {
        return ((ordinal >= 0) && (ordinal < VALUES.length)) ?
            VALUES[ordinal] :
            null;
    }

    @Override
    public String toString() {
        return name() + '(' + ordinal() + ')';
    }
}
