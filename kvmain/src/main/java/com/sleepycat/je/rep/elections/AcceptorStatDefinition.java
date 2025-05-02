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

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for each Acceptors statistics.
 */
public class AcceptorStatDefinition {

    public static final String GROUP_NAME = "Acceptor";
    public static final String GROUP_DESC =
        "stats related to the Acceptor of the Elections ";

    public static final String N_PROPOSE_ACCEPTOR_ACCEPTED =
        "nProposeAcceptorAccepted";
    public static final String N_PROPOSE_ACCEPTOR_ACCEPTED_DESC =
        "Number of propose messages accepted by the acceptor";

    public static StatDefinition PROPOSE_ACCEPTOR_ACCEPTED =
        new StatDefinition
        (N_PROPOSE_ACCEPTOR_ACCEPTED,
         N_PROPOSE_ACCEPTOR_ACCEPTED_DESC);

    public static final String N_PROPOSE_ACCEPTOR_IGNORED =
        "nProposeAcceptorIgnored";
    public static final String N_PROPOSE_ACCEPTOR_IGNORED_DESC =
        "Number of propose messages ignored by the acceptor";

    public static StatDefinition PROPOSE_ACCEPTOR_IGNORED =
        new StatDefinition
        (N_PROPOSE_ACCEPTOR_IGNORED,
         N_PROPOSE_ACCEPTOR_IGNORED_DESC);

    public static final String N_PROPOSE_ACCEPTOR_REJECTED =
        "nProposeAcceptorRejected";
    public static final String N_PROPOSE_ACCEPTOR_REJECTED_DESC =
        "Number of propose messages rejected by the acceptor";

    public static StatDefinition PROPOSE_ACCEPTOR_REJECTED =
        new StatDefinition
        (N_PROPOSE_ACCEPTOR_REJECTED,
         N_PROPOSE_ACCEPTOR_REJECTED_DESC);

    public static final String N_ACCEPT_ACCEPTOR_ACCEPTED =
        "nAcceptAcceptorAccepted";
    public static final String N_ACCEPT_ACCEPTOR_ACCEPTED_DESC =
        "Number of accept messages accepted by the acceptor";

    public static StatDefinition ACCEPT_ACCEPTOR_ACCEPTED =
        new StatDefinition
        (N_ACCEPT_ACCEPTOR_ACCEPTED,
         N_ACCEPT_ACCEPTOR_ACCEPTED_DESC);

    public static final String N_ACCEPT_ACCEPTOR_REJECTED =
        "nAcceptAcceptorRejected";
    public static final String N_ACCEPT_ACCEPTOR_REJECTED_DESC =
        "Number of accept messages rejected by the acceptor";

    public static StatDefinition ACCEPT_ACCEPTOR_REJECTED =
        new StatDefinition
        (N_ACCEPT_ACCEPTOR_REJECTED,
         N_ACCEPT_ACCEPTOR_REJECTED_DESC);
}
