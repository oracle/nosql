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

package com.sleepycat.je.rep.impl.node;

import java.time.Instant;

/**
 * Defines the term associated with a master. A term starts when a master is
 * elected and ends when the node fails for some reason. All log entries
 * written by the master during this interval constitute a term.
 *
 * An RN's log can be then be modeled as a sequence of terms as follows:
 *
 * log -> durable_term* [ part_durable_term ] nondurable_term*
 *
 * durable_term -> [durable_log_entry]+
 *
 * part_durable_term -> [durable_log_entry]+ [nondurable_log_entry]+
 *
 * nondurable_term -> nondurable_log_entry+
 *
 * Durable log entries are durable by virtue of being present in a majority of
 * the RN logs. At least a majority of nodes share the common log prefix of
 * durable entries. This ensures that during an election at least one of the
 * nodes in this set will participate and win, preserving the prefix of durable
 * entries. That winner, the new master, starts another term and extends the
 * durable prefix.
 *
 * The primary motivation for the term is so that it can be used to select the
 * correct master: one that preserves the durable prefix, in an election.
 *
 * All commit and abort log entries written by the master contain the term
 * number.
 *
 * Nondurable entries become durable as the master gathers new acknowledgments.
 * A nondurable entry written in a preceding term may become durable in a
 * subsequent term if the master of that term, or masters of subsequent terms
 * that also have it in their logs and can propagate it to a majority of nodes.
 * That is, a no durable entry may take more than one term to become durable as
 * each term moves it further to the quorum replication it needs to become
 * durable. A nondurable entry will be rolled back if any new master does not
 * have the entry in its logs.
 *
 * Only durable results are available to be read by an application, regardless
 * of whether the read is at the master or at one of the replicas.
 *
 * Log entries additionally contain a dtvlsn. The last commit/abort entry in
 * each log identifies via its dtvlsn, the highest commit log entry, preceding
 * it in the log, that is known to be durable. This value typically lags the
 * actual highest durable commit entry, since the master may have crashed
 * between the time it determined that an entry was durable and its update of
 * the dtvlsn on disk. Additionally, replicas may lag as they are informed
 * about dtvlsns via sequential log commit entries or via heartbeats
 * (in future).
 */
public abstract class MasterTerm {

    /* Used when the state does not allow for a valid masterTerm, e.g.
     * during initialization or in non replicated txn commits and aborts.
     */
    public static long NULL = 0;

    /* The time when terms were first introduced */
    public static long MIN_TERM =
        Instant.parse("2021-06-03T00:00:00.00Z").toEpochMilli();

    public static long MAX_TERM = MIN_TERM +
        (1000l * 60 * 60 * 24 * 365 * 100 /* years */);

    /* The distinguished time used to indicate that a log entry was generated
     * or replicated by a pre-term Master
     */
    public static long PRETERM_TERM = MIN_TERM - 1;

    public static boolean inRange(long masterTerm) {
        return (masterTerm >= MIN_TERM) && (masterTerm <= MAX_TERM);
    }

    /* Returns true, if the masterTerm is a legal value */
    public static boolean isValid(long masterTerm) {
        return isPreTerm(masterTerm) || inRange(masterTerm);
    }

    /* Returns true, if the masterTerm is the distinguished PRETERM_TERM value */
    public static boolean isPreTerm(long masterTerm) {
        return masterTerm == PRETERM_TERM;
    }

    /**
     * Returns true if termj can validly follow termi in sequential commit or
     * abort entries in the log.
     *
     * Valid log entry commit sequences can be expressed by the following
     * regular expression sequence, where preTerm represents an old (pre term
     * changes) log commit entry and postTerm represents the new log commit
     * entries:
     *
     * [preTerm | postTerm]* postTerm+
     *
     * The first component: [preTerm|postTerm]* represents the portion of the
     * log while the replication group was being upgraded and preTerm or new
     * nodes can become masters. Once the group has been upgraded, the log will
     * only contain postTerm entries represented by the suffix: postTerm+
     *
     */
    public static boolean follows(long termi, long termj) {
        return (termi <= termj) ?
                /* Increasing terms */
                true :
                /* Term decreased */
                /* Old log entry term, transition is ok */
                (termj == PRETERM_TERM);
    }

    /*
     * Throws IAE if the master term is not a legal value. Otherwise returns
     * the validated masterTerm
     */
    public static long check(long masterTerm) {
        if (!isValid(masterTerm)) {
            final String msg =
                String.format("Bad master term: %,d (%s)",
                              masterTerm,
                              Instant.ofEpochMilli(masterTerm).toString());
            throw new IllegalArgumentException(msg);
        }
        return masterTerm;
    }

    public static String logString(long masterTerm) {
        final String termString;
        if (masterTerm == MIN_TERM) {
            termString = "MIN_TERM";
        } else if (masterTerm == MAX_TERM) {
            termString = "MAX_TERM";
        } else if (masterTerm == PRETERM_TERM) {
            termString = "PRETERM_TERM";
        } else if (masterTerm == NULL) {
            termString = "NULL_TERM";
        } else {
            termString = Instant.ofEpochMilli(masterTerm).toString();
        }

        return String.format("%x(%s)", masterTerm, termString);
    }
}
