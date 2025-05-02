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

package oracle.kv.impl.admin.plan;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Base class for plans which operate on multiple metadata types.
 *
 * When a MultiMetadataPlan is constructed, the metadata sequence numbers of
 * the metadata objects are saved so that a check can be made when the plan
 * runs, making sure the metadata has not regressed. Note that, if the plan
 * itself changes any metadata, it should track the changes using
 * Plan.updatingMetadata().
 */
public abstract class MultiMetadataPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    protected static final Set<MetadataType> TABLE_SECURITY_TYPES =
        Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(MetadataType.TABLE, MetadataType.SECURITY)));

    /*
     * The sequence numbers of the metadata objects for the plan when it was
     * created. A plan should check if MD sequence numbers have regressed.
     * Also if a plan makes multiple changes to MD over the course of
     * execution it should update the basis so that the plan can be restarted.
     */
    private final Map<MetadataType, Integer> bases = new HashMap<>();

    protected MultiMetadataPlan(String planName, Planner planner) {
        super(planName, planner);

        for (MetadataType mdType : getMetadataTypes()) {
            final Metadata<? extends MetadataInfo> md = getMetadata(mdType);
            bases.put(mdType, (md == null) ? Metadata.EMPTY_SEQUENCE_NUMBER :
                               md.getSequenceNumber());
        }
    }

    /**
     * Returns the metadata types used by this plan.
     *
     * @return the metadata types
     */
    protected abstract Set<MetadataType> getMetadataTypes();

    /* Simpler methods to get table and security metadata */
    public TableMetadata getTableMetadata() {
        return (TableMetadata) getMetadata(MetadataType.TABLE);
    }

    public SecurityMetadata getSecurityMetadata() {
        return (SecurityMetadata) getMetadata(MetadataType.SECURITY);
    }

    /* Simpler methods to get table and security metadata using Transaction */
    public TableMetadata getTableMetadata(Transaction txn) {
        return getAdmin().getMetadata(TableMetadata.class,
                                      MetadataType.TABLE,
                                      txn);
    }

    public SecurityMetadata getSecurityMetadata(Transaction txn) {
        return getAdmin().getMetadata(SecurityMetadata.class,
                                      MetadataType.SECURITY,
                                      txn);
    }

    /**
     * Get metadata with given metadata type. Only table and security metadata
     * can be loaded using this method.
     *
     * @param type
     * @return metadata of given type
     */
    private Metadata<? extends MetadataInfo> getMetadata(MetadataType type) {
        switch (type) {
        case TABLE:
            return getAdmin().getMetadata(TableMetadata.class, type);
        case SECURITY:
            return getAdmin().getMetadata(SecurityMetadata.class, type);
        case TOPOLOGY:
            return getAdmin().getCurrentTopology();
        default:
            throw new IllegalArgumentException(
                "Unknown metadata type: " + type);
        }
    }

    @Override
    public String getDefaultName() {
        return "Multiple Metadata Plan";
    }

    /**
     * Updates the basis for this plan if the specified metadata is one of
     * metadata types this plan uses and the metadata's sequence number is
     * greater than the basis. If the basis is updated, true is returned.
     *
     * @param metadata the updated metadata
     * @return true if the plan's basis of this type of metadata was updated
     */
    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        final MetadataType mdType = metadata.getType();
        if (!bases.keySet().contains(mdType)) {
            return false;
        }
        return updateBasis(mdType, metadata.getSequenceNumber());
    }

    /**
     * Updates the basis of given metadata type for this plan. If newBasis is
     * greater than the current basis, the current basis will be set to
     * newBasis and true is returned, otherwise false is returned.
     *
     * @param mdType the metadata type
     * @param newBasis the new basis
     * @return true if the basis was updated
     *
     * @throws IllegalStateException if the newBasis is less than the current
     * basis of given metadata type
     */
    private boolean updateBasis(MetadataType mdType, int newBasis) {
        if (bases.get(mdType) == newBasis) {
            return false;
        }

        if (bases.get(mdType) > newBasis) {
            throw new IllegalStateException(
                this + " attempting to persist older version of " +
                mdType + "  metadata");
        }
        bases.put(mdType, newBasis);
        return true;
    }
}
