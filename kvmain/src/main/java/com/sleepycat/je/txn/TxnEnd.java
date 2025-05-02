/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.txn;

import java.util.function.Supplier;

import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Timestamp;

/**
 * The base class for records that mark the end of a transaction.
 */
public abstract class TxnEnd implements Loggable {

    long id;
    Timestamp time;
    long lastLsn;

    /* For replication - master node which wrote this record. */
    int masterId;
    long masterTerm = MasterTerm.NULL;
    final Supplier<MasterIdTerm> masterTermIdSupplier;

    /**
     * The txn commit VLSN that was acknowledged by at least a majority of the
     * nodes either at the time of this commit, or eventually via a heartbeat.
     * This VLSN must typically be less than the VLSN associated with the
     * TxnEnd itself, when it's written to the log. In cases of mixed mode
     * operation (when a pre-DTVLSN is serving as a feeder to a DTVLSN aware
     * replica) it may be equal to the VLSN associated with the TxnEnd.
     */
    long dtvlsn;

    TxnEnd(long id,
           long lastLsn,
           Supplier<MasterIdTerm> masterTermIdSupplier,
           long dtvlsn) {
        this.id = id;
        time = new Timestamp(TimeSupplier.currentTimeMillis());
        this.lastLsn = lastLsn;
        this.masterTermIdSupplier = masterTermIdSupplier;
        updateMasterIdTerm();
        this.dtvlsn = dtvlsn;
    }

    /**
     * For constructing from the log during tests.
     */
    public TxnEnd() {
        lastLsn = DbLsn.NULL_LSN;
        masterTermIdSupplier = () -> MasterIdTerm.PRIMODAL_ID_TERM;
    }

    /*
     * Accessors.
     */
    public long getId() {
        return id;
    }

    public Timestamp getTime() {
        return time;
    }

    long getLastLsn() {
        return lastLsn;
    }

    public int getMasterId() {
        return masterId;
    }

    public long getMasterTerm() {
        return masterTerm;
    }

    public MasterIdTerm getMasterIdTerm() {
        return new MasterIdTerm(masterId, masterTerm);
    }

    @Override
    public long getTransactionId() {
        return id;
    }

    public long getDTVLSN() {
        return dtvlsn;
    }

    public void setDTVLSN(long dtvlsn) {
        this.dtvlsn = dtvlsn;
    }

    /**
     * Returns true if there are changes that have been logged for this entry.
     * It's unusual for such a record to not have associated changes, since
     * such commit/abort entries are typically optimized away. When present
     * they typically represent records used to persist uptodate DTVLSN
     * information as part of the entry.
     */
    public boolean hasLoggedEntries() {
        return (lastLsn != DbLsn.NULL_LSN);
    }

    protected abstract String getTagName();

    public void updateMasterIdTerm() {
        MasterIdTerm masterIdTerm = masterTermIdSupplier.get();
        this.masterId = masterIdTerm.nodeId;
        this.masterTerm = masterIdTerm.term;
    }
}
