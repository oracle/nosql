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

package com.sleepycat.je.rep.elections;

import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.utilint.NotSerializable;

/**
 * Indicates that a master ready to connect to or has been connected to is
 * obsolete.
 *
 * A master becomes obsolete when (1) the acceptor is to make or has made a
 * promise for a higher term, or (2) a master of higher term is known.
 */
@SuppressWarnings("serial")
public class MasterObsoleteException extends Exception implements NotSerializable {

    /**
     * Constructs the exception resulted from the MasterStatus not in sync.
     */
    public MasterObsoleteException(MasterStatus masterStatus) {
        super(String.format(
            "Master needs sync: " +
            "nodeMaster=%s, groupMaster=%s",
            masterStatus.getNodeMasterNameId(),
            masterStatus.getGroupMasterNameId()));
    }

    /**
     * Constructs the exception resulted from a new master is learned while
     * connecting to the old.
     */
    public MasterObsoleteException(NameIdPair oldMaster,
                                   NameIdPair newMaster) {
        super(String.format(
            "Learned new master: " +
            "oldMaster=%s, newMaster=%s",
            oldMaster, newMaster));
    }

    /**
     * Constructs the exception resulted from the acceptor is to make or has
     * made a promise for a new term.
     */
    public MasterObsoleteException(long learnedTerm,
                                   long allowedTerm) {
        super(String.format(
            "Learned term but obsolete: " +
            "learnedTerm=%s, allowdTerm=%s",
            MasterTerm.logString(learnedTerm),
            MasterTerm.logString(allowedTerm)));
    }
}
