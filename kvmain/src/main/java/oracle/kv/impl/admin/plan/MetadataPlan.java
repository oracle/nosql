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

import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;

import com.sleepycat.je.Transaction;

/**
 * Base class for plans which operate on a single metadata type.
 *
 * When a MetadataPlan is constructed the metadata sequence number of the
 * metadata is saved so that a check can be made when the plan runs, making sure
 * the metadata has not changed out from under the plan. Note that if the plan
 * itself changes the metadata it should track the changes using
 * Plan.updatingMetadata().
 */
public abstract class MetadataPlan<T extends Metadata<? extends MetadataInfo>>
                                                          extends AbstractPlan {
    private static final long serialVersionUID = 1L;

    /*
     * The metadata sequence number of the plan when it was created. A plan
     * should check if the MD seq# has changed when it executes. Also if a
     * plan makes multiple changes to the MD over the course of execution
     * it should update the basis so that the plan can be restarted.
     */
    private int basis;

    /**
     * Constructor for subclass. When constructed the metadata sequence number
     * basis is set by calling getMetadata() and storing the seq # of the
     * returned metadata. If the returned metadata is null the basis is set
     * to Metadata.EMPTY_SEQUENCE_NUMBER.
     */
    protected MetadataPlan(String planName,
                           Planner planner,
                           boolean systemPlan) {
        super(planName, planner, systemPlan);

        final T md = getMetadata();
        basis = (md == null) ? Metadata.EMPTY_SEQUENCE_NUMBER :
                               md.getSequenceNumber();
    }

    /**
     * Gets the metadata type of this plan.
     *
     * @return the metadata type
     */
    protected abstract MetadataType getMetadataType();

    /**
     * Gets the class of the metadata used by this plan.
     *
     * @return the metadata class
     */
    protected abstract Class<T> getMetadataClass();

    /**
     * Gets the metadata for read-only use by this plan.
     *
     * @return the metadata object
     */
    public T getMetadata() {
        return getAdmin().getMetadata(getMetadataClass(), getMetadataType());
    }

    /**
     * Gets the metadata for use by this plan in an existing Transaction
     *
     * @return the metadata object
     */
    public T getMetadata(Transaction txn) {
        return getAdmin().getMetadata(getMetadataClass(),
                                      getMetadataType(),
                                      txn);
    }

    /**
     * Checks the current metadata's sequence number with the basis of this
     * plan. If they do not match an IllegalStateException is thrown. This
     * method checks the metadata object returned from getMetadata().
     */
    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        final T md = getMetadata();
        final int seqNum = (md == null) ? Metadata.EMPTY_SEQUENCE_NUMBER :
                                          md.getSequenceNumber();

        if (seqNum != basis) {
            throw new IllegalCommandException
                    ("Plan " + this + " was based on the " +
                     (md != null ? md.getType() : "") + " metadata at " +
                     "sequence " + basis +
                     " but the current metadata is at sequence " +
                     (md != null ? md.getSequenceNumber() : "unknown") +
                     ". Please cancel this plan and create a new plan.");
        }
    }

    /**
     * Updates the basis for this plan if the specified metadata is of the type
     * of this plan and the metadata's sequence number is greater than the
     * basis. If the basis is updated, true is returned.
     *
     * @param metadata the updated metadata
     * @return true if the plan's basis was updated
     */
    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        return metadata.getType().equals(getMetadataType()) ?
                                    updateBasis(metadata.getSequenceNumber()) :
                                    false;
    }


    /**
     * Updates the basis for this plan. If newBasis is greater than the current
     * basis, the current basis will be set to newBasis and true is returned,
     * otherwise false is returned.
     *
     * @param newBasis the new basis
     * @return true if the basis was updated
     *
     * @throws IllegalStateException if the newBasis is less than the current
     * basis
     */
    private boolean updateBasis(int newBasis) {
        if (basis == newBasis) {
            return false;
        }

        if (basis > newBasis) {
              throw new IllegalStateException(
                  this + " attempting to persist older version of " +
                  getMetadataType() + "  metadata");
        }
        basis = newBasis;
        return true;
    }

    /**
     * Checks whether all nodes in the store are with target version or later.
     * If not, an {@link IllegalCommandException} will be thrown with specified
     * error message.
     *
     * @param targetVersion version to be checked
     * @param errMsg message showed to caller when current version is lower
     * than the target version
     * @throws IllegalCommandException if not all nodes in store are with
     * the target version or later, or the current lowest version of the store
     * cannot be confirmed
     */
    static void checkVersion(Admin admin,
                             KVVersion targetVersion,
                             String errMsg) {
        if (!Utils.storeHasVersion(admin, targetVersion)){
            throw new IllegalCommandException(
                String.format(
                    "%s The highest version supported by all nodes is lower " +
                    "than the required version of %s or later.",
                    errMsg, targetVersion.getNumericVersionString()));
        }
    }
}
