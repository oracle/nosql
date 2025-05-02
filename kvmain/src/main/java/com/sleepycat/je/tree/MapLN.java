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

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Loggable;

/**
 * {@literal
 * A MapLN represents a Leaf Node in the JE Db Mapping Tree.
 *
 * MapLNs contain a DatabaseImpl, which in turn contains three categories of
 * information - database configuration information, the per-database File
 * Summary utilization information, and each database's btree root. While LNs
 * are written to the log as the result of API operations which create new data
 * records, MapLNs are written to the log as a result of configuration changes
 * or updates to the btree which cascade up
 * the tree and result in a new root. Because they serve as a bridge between
 * the application data btree and the db mapping tree, MapLNs must be written
 * with special rules, and should only be written from DbTree.modifyDbRoot.
 * The basic rule is that in order to ensure that the MapLN contains the
 * proper btree root, the btree root latch is used to protect both any logging
 * of the MapLN, and any updates to the root lsn.
 *
 * Updates to the internal btree nodes obey a strict bottom up approach, in
 * accordance with the log semantics which require that later log entries are
 * known to supercede earlier log entries. In other words, for a btree that
 * looks like
 *      MapLN
 *        |
 *       IN
 *        |
 *       BIN
 *        |
 *       LN
 * we know that update operations cause the btree nodes must be logged in this
 * order: LN, BIN, IN, MapLN, so that the reference to each on disk node is
 * correct. (Note that logging order is special and different when the btree
 * is initially created.)
 *
 * However, MapLNs may need to be written to disk at arbitrary points in time
 * in order to save database config or utilization data. Those writes don't
 * have the time and context to be done in a cascading-upwards fashion.  We
 * ensure that MapLNs are not erroneously written with an out of sync root by
 * requiring that DbTree.modifyDbRoot takes the root latch for the application
 * data btree. RootINs are also written with the root latch, so it serves to
 * ensure that the root doesn't change during the time when the MapLN is
 * written. For example, suppose thread 1 is doing a cascading-up MapLN write,
 * and thread 2 is doing an arbitrary-point MapLN write:
 *
 * Thread 1                   Thread 2
 * --------                   --------
 * latch root                 latch BIN parent of MapLN
 * log root IN
 * log MapLN (Tree root)       wants to log MapLN too -- but has to take
 *  to refer to new root IN    root latch, so we'll get the right rootIN
 *
 * Without latching the root this could produce the following, incorrect log
 *  30 LNa
 *  40 BIN
 *  50 IN (first version of root)
 *  60 MapLN, refers to IN(50)
 *  ...
 *  90 LNb
 *  100 BIN
 *  110 IN (second version of root)
 *  120 CkptStart (the tree is not dirty, no IN will be logged during the
 *   ckpt interval))
 *   ..  something arbirarily writes out the MapLN
 *  130 MapLN refers to first root, IN(50)    <------ impossible
 *
 * While a MapLN can't be written out with the wrong root, it's possible
 * for a rootIN to be logged without the MapLN, and for that rootIN not
 * to be processed at recovery. Suppose a checkpoint begins and ends
 * in the window between when a rootIN is written, and DbTree.modifyDbRoot is
 * called:
 *   300 log new root IN,
 *   update root reference in tree
 *   unlatch root
 *
 *   310 Checkpoint starts
 *   320 Checkpoint ends
 *   ...if we crash here, before the MapLN is logged, , we won't see the new
 *   root IN at lsn 300. However, the IN is non-txnal and will be recreated
 *   during reply of txnal information (LNs) by normal recovery processing.
 * }
 */
public final class MapLN extends LN {

    private static final String BEGIN_TAG = "<mapLN>";
    private static final String END_TAG = "</mapLN>";

    private final DatabaseImpl databaseImpl;
    private boolean deleted;

    /**
     * Create a new MapLn to hold a new databaseImpl. In the ideal world, we'd
     * have a base LN class so that this MapLN doesn't have a superfluous data
     * field, but we want to optimize the LN class for size and speed right
     * now.
     */
    public MapLN(DatabaseImpl db) {
        super(new byte[0]);
        databaseImpl = db;
        deleted = false;
    }

    /**
     * Create an empty MapLN, to be filled in from the log.
     */
    public MapLN() {
        super();
        databaseImpl = new DatabaseImpl();
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    void makeDeleted() {
        deleted = true;

        /* Release all references to nodes held by this database. */
        databaseImpl.getTree().setRoot(null, true);
    }

    public DatabaseImpl getDatabase() {
        return databaseImpl;
    }

    /**
     * Returns true if the MapLN can be evicted, which is allowed when DB
     * eviction is configured, the DB is not in use (not in the DbTree cache
     * or can be evicted), and the root IN for the DB is not resident.
     *
     * <p>The latch for the BIN containing the MapLN must be held when this
     * method is called.</p>
     */
    @Override
    boolean isEvictable() {
        final EnvironmentImpl envImpl = databaseImpl.getEnv();
        /*
         * Check for resident root IN _after_ DB eviction to prevent
         * another thread from fetching the root IN.
         */
        return envImpl.getDbEviction() &&
            envImpl.getDbTree().evictDb(databaseImpl) &&
            !databaseImpl.getTree().isRootResident();
    }

    @Override
    public void initialize(DatabaseImpl db) {
        super.initialize(db);
        databaseImpl.setEnvironmentImpl(db.getEnv());
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes. Don't count the treeAdmin memory, because
     * that goes into a different bucket.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        return MemoryBudget.MAPLN_OVERHEAD;
    }

    /*
     * Dumping
     */

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.dumpString(nSpaces, dumpTags));
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<deleted val=\"").append(Boolean.toString(deleted));
        sb.append("\">");
        sb.append('\n');
        sb.append(databaseImpl.dumpString(nSpaces));
        return sb.toString();
    }

    /*
     * Logging
     */

    /**
     * Return the correct log entry type for a MapLN depends on whether it's
     * transactional.
     */
    @Override
    protected LogEntryType getLogType(boolean isInsert,
                                      boolean isTransactional,
                                      DatabaseImpl db) {
        assert(!isTransactional);
        return LogEntryType.LOG_MAPLN;
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return super.getLogSize(logVersion, forReplication) +
            databaseImpl.getLogSize() +
            1; // deleted
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        super.writeToLog(logBuffer, logVersion, forReplication);
        databaseImpl.writeToLog(logBuffer);
        byte booleans = (byte) (deleted ? 1 : 0);
        logBuffer.put(booleans);
    }

    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer itemBuffer,
                            int entryVersion) {
        super.readFromLog(envImpl, itemBuffer, entryVersion);
        databaseImpl.readFromLog(envImpl, itemBuffer, entryVersion);
        byte booleans = itemBuffer.get();
        deleted = (booleans & 1) != 0;

        /*
         * Clear DbFileSummaryMap unless this is an old deleted MapLN, in
         * which case we may use it for utilization counting.
         */
        if (entryVersion < 16 || !deleted) {
            databaseImpl.clearDbFileSummaries();
        }
    }

    /**
     * Should never be replicated.
     */
    @Override
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * Dump additional fields. Done this way so the additional info can be
     * within the XML tags defining the dumped log entry.
     */
    @Override
    protected void dumpLogAdditional(StringBuilder sb, boolean verbose) {
        databaseImpl.dumpLog(sb, verbose);
    }
}
