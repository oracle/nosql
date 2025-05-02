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

package oracle.kv.impl.admin.plan.task;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.plan.UpdateTlsCredentialsPlan;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes.HashInfo;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.NonNullByDefault;
import oracle.kv.util.ErrorMessage;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Check that installed keystore and truststore files will be consistent across
 * all SNs in the store after updates are applied.
 *
 * <p>This task has no side effects.
 */
@NonNullByDefault
public class CheckTlsCredentialsConsistencyTask extends SingleJobTask {
    private static final long serialVersionUID = 1;
    private final UpdateTlsCredentialsPlan plan;

    public CheckTlsCredentialsConsistencyTask(UpdateTlsCredentialsPlan plan) {
        this.plan = plan;
    }

    @Override
    protected UpdateTlsCredentialsPlan getPlan() {
        return plan;
    }

    /** Don't proceed if the credentials check fails. */
    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public State doWork() {
        final Map<StorageNodeId, CredentialHashes> snCredentialHashes =
            plan.getAllCredentialHashes();
        final @Nullable String keystoreResult = checkConsistent(
            snCredentialHashes.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(),
                                      e -> e.getValue().keystore)),
            "keystore");
        final @Nullable String truststoreResult = checkConsistent(
            snCredentialHashes.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(),
                                      e -> e.getValue().truststore)),
            "truststore");
        if ((keystoreResult == null) && (truststoreResult == null)) {
            return State.SUCCEEDED;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("Problems found with credentials:");
        if (keystoreResult != null) {
            sb.append('\n').append(keystoreResult);
        }
        if (truststoreResult != null) {
            sb.append('\n').append(truststoreResult);
        }
        final String msg = sb.toString();
        throw new CommandFaultException(msg, new IllegalStateException(msg),
                                        ErrorMessage.NOSQL_5200,
                                        CommandResult.NO_CLEANUP_JOBS);
    }

    /**
     * Checks SN keystore or truststore hashes for consistency, returning a
     * string that describes any problems found, or null if the hashes are
     * consistent.
     */
    static
        @Nullable String checkConsistent(Map<StorageNodeId, HashInfo> hashes,
                                         String fileType) {
        /* Check for consistent updates */
        @Nullable String update = null;
        for (final HashInfo hashInfo : hashes.values()) {
            if (!hashInfo.update) {
                continue;
            }
            if (update == null) {
                update = hashInfo.hash;
            } else if (!update.equals(hashInfo.hash)) {
                return "Updates for " + fileType +
                    " files are inconsistent: " + new TreeMap<>(hashes);
            }
        }
        if (update == null) {
            /* No updates, check for consistent installs */
            @Nullable String install = null;
            for (final HashInfo hashInfo : hashes.values()) {
                if (install == null) {
                    /*
                     * Note that we don't expect any of the supplied hash
                     * values to be null
                     */
                    install = hashInfo.hash;
                } else if (!install.equals(hashInfo.hash)) {
                    return "Updates for " + fileType + " files are required" +
                        " to make installed files consistent: " +
                        new TreeMap<>(hashes);
                }
            }
            return null;
        }
        /* Check for installs that need updates */
        @Nullable StringBuilder sb = null;
        hashes = new TreeMap<>(hashes);
        for (final Entry<StorageNodeId, HashInfo> e : hashes.entrySet()) {
            final HashInfo hashInfo = e.getValue();
            if (hashInfo.update) {
                continue;
            }
            final String hash = hashInfo.hash;
            if (!update.equals(hash)) {
                final StorageNodeId snId = e.getKey();
                if (sb == null) {
                    sb = new StringBuilder();
                    sb.append("Updates for ").append(fileType)
                        .append(" files are required for SNs: ")
                        .append(snId);
                } else {
                    sb.append(", ").append(snId);
                }
            }
        }
        if (sb != null) {
            return sb.toString() + ": " + hashes;
        }
        return null;
    }
}
