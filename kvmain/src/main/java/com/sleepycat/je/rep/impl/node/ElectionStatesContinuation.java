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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.elections.Proposer.DefaultFormattedProposal;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.json_simple.JsonException;
import com.sleepycat.json_simple.JsonObject;
import com.sleepycat.json_simple.Jsoner;

/**
 * Manages the continuation of election states across node restart.
 *
 * <p>
 * Due to historic reasons, the main mechanism for persisting states is the
 * replicated log. Non-replicated entries can be created interleaving with the
 * replicated entries but they can be rolled back when we switch masters. The
 * obvious down side of this approach is that we cannot persist node-specific
 * states; the up side being that without the necessity of the maintenance of
 * node-specific state, we can restore the database with any replicated log.
 *
 * <p>
 * Paxos (which we use for election) as well as our log replication protocol
 * (which is similar to Raft) require persisting node-specific states. Paxos
 * and Raft require persisting the latest promise of the acceptor. Paxos in
 * addition require persisting the latest accepted value of the acceptor. We
 * satisfy these requirements with two approaches:
 * <ol>
 * <li>We rely on the synchronization on time. The proposal term and round are
 * generated according to the current time for a node. With node time being
 * synchronized, we can be sure of a maximum term and round this node has
 * participated in before its restart, hence guarantee the equivalent safety as
 * Paxos and Raft. Time synchronization is achieved through NTP which is always
 * required for our store to run (e.g., to guarantee TimeConsistency). The time
 * synchronization requirement can be configured through
 * {@link ReplicationConfig#MAX_CLOCK_DELTA}. The configured max skew is
 * checked frequently for connected node peers i.e., in this object during
 * election as well as during the heartbeat exchange in the replay stream.</li>
 * <li>We persist these election states with this class as well. This approach
 * can serve as an optimization If the file is not present. We can fall back to
 * the above time approach.</li>
 * </ol>
 *
 * <p>
 * We persist the election states in json for the following benefits: (1) human
 * readability such that we could quickly check or manually modify the file if
 * necessary; (2) well defined so that I do not need to deal with complication
 * such as null values.
 *
 * <p>
 * When we fall back to the time approach when no persisted file is found, we
 * create a continuation barrier on time such that we do not initiate any
 * election or ack activity until past the barrier. The barrier timestamp is
 * computed such that under the configured max clock skew, after passing the
 * barrier, the invariant of no ack of a lower term after promising a higher
 * term is guaranteed even if the promise is forgotten. See
 * computeBarrierTimestamp.
 */
public class ElectionStatesContinuation extends ElectionStates {

    /**
     * Disables waiting past barrier. Used for testing.
     */
    public static volatile boolean barrierDisabled = false;
    /**
     * Disables the clock skew check. Used for testing.
     */
    public static volatile boolean clockSkewCheckDisabled = false;

    public static final String PERSIST_FILE_NAME = "election.persisted";
    public static final String TEMP_FILE_NAME = "election.persisted.temp";

    /** The start timestamp. */
    private final long startTimestamp = TimeSupplier.currentTimeMillis();

    /*
     * All fields are accessed within the synchronization block of this
     * object after the object construction.
     */

    /** The environment. */
    private final RepImpl envImpl;
    /** The maximum clock skew. */
    private final long maxClockSkew;
    /** The persisted file. */
    private final File persistedFile;
    /** The temp file for atomically writing to the persisted file. */
    private final File tempFile;

    /**
     * Marks that we have waited past the continuation barrier.
     */
    private boolean pastContinuationBarrier = false;

    /**
     * Constructs the continuation management for a rep node.
     */
    public ElectionStatesContinuation(RepImpl envImpl) {
        this.envImpl = envImpl;
        this.maxClockSkew =
            envImpl.getConfigManager()
            .getDuration(RepParams.MAX_CLOCK_DELTA);
        this.persistedFile =
            new File(envImpl.getEnvironmentHome(), PERSIST_FILE_NAME);
        this.tempFile =
            new File(envImpl.getEnvironmentHome(), TEMP_FILE_NAME);
    }

    /**
     * Initializes the persisted states.
     *
     * @param needBarrier if barrier is needed. Barrier may not needed if the
     * node could not have made any promise, e.g., it has never joined a group.
     */
    public synchronized void init(boolean needBarrier) {
        final boolean persistedFileExists = persistedFile.exists();
        /*
         * If persisted file is not present, handle it which will initialize our
         * voting state with a barrier timestamp if needBarrier is true.
         */
        if (!persistedFileExists) {
            handleNoPersistedFile(needBarrier);
            return;
        }

        /*
         * If persisted file is present and the content is read successfully,
         * then we have initialized our voting state correctly with our past
         * memory and there is no need to wait for a barrier.
         */
        if (readPersisted()) {
            pastContinuationBarrier = true;
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format("%s initialized election states from file to %s",
                    envImpl.getNameIdPair().getName(), this));
            return;
        }
        /*
         * Read failed because the existing persisted file is corrupted. Delete
         * the file and handle as the persisted file is not present.
         */
        deleteCorruptedPersistedFile();
        handleNoPersistedFile(needBarrier);
    }

    private void deleteCorruptedPersistedFile() {
        try {
            Files.deleteIfExists(persistedFile.toPath());
        } catch (IOException e) {
            /*
             * Cannot delete a file which seems quite unexpected. For such kind
             * of unknown error, it seems safer to fail the whole environment.
             */
            throw new IllegalStateException(
                String.format("Error deleting corrupted persisted file: %s",
                    persistedFile),
                e);
        }
    }

    /**
     * Handles the case where the persisted file is not present. Attempts to
     * write the persisted file with the computed proposal timestamps possibly
     * taking barrier into consideration. Also calls init to initialize the
     * in-memory proposal values.
     */
    private void handleNoPersistedFile(boolean needBarrier) {
        final JsonObject persistingContent;
        if (!needBarrier || barrierDisabled) {
            persistingContent = initWithMinTerm();
            pastContinuationBarrier = true;
        } else {
            /* Establish barrier */
            persistingContent = initWithBarrier();
            pastContinuationBarrier = false;
        }
        init(persistingContent);
    }

    /**
     * Returns {@code true} if this object is initialized, that is its init()
     * method has been invoked.
     */
    public synchronized boolean initialized() {
        return (promisedProposal != null);
    }

    /**
     * Initializes the promised term with one that is far in the past and
     * returns the persisting object. This will equivalently disable barrier.
     */
    private JsonObject initWithMinTerm() {
        final JsonObject obj = new JsonObject();
        obj.put(PROMISED_PROPOSAL_KEY.getKey(),
                (new DefaultFormattedProposal(MasterTerm.MIN_TERM)
                 .wireFormat()));
        persist(obj);
        LoggerUtils.info(
            envImpl.getLogger(), envImpl,
            String.format(
                "%s no barrier needed, " +
                "initialized promised term to %s",
                envImpl.getNameIdPair().getName(),
                MasterTerm.logString(MasterTerm.MIN_TERM)));
        return obj;
    }

    /**
     * Initializes the promised term with a barrier considering the clock skew
     * and returns the persisting object.
     */
    private JsonObject initWithBarrier() {
        final long barrierTimestamp = computeBarrierTimestamp();
        final JsonObject obj = new JsonObject();
        obj.put(PROMISED_PROPOSAL_KEY.getKey(),
                (new DefaultFormattedProposal(barrierTimestamp))
                .wireFormat());
        persist(obj);
        LoggerUtils.info(
            envImpl.getLogger(), envImpl,
            String.format(
                "%s initialized promised term with barrier to %s",
                envImpl.getNameIdPair().getName(),
                MasterTerm.logString(barrierTimestamp)));
        return obj;
    }

    /**
     * Reads the persisted from the file and initializes the in-memory proposal
     * values. Returns {@code false} if the file does not exist or is
     * ill-formatted, {@code true} otherwise succeeded.
     */
    private boolean readPersisted() {
        if (!persistedFile.exists()) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format("Unexpected non-exist file %s", persistedFile));
            return false;
        }
        if (persistedFile.isDirectory()) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format(
                    "Unexpected existing directory %s", persistedFile));
            return false;
        }
        final JsonObject content = readPersistedObject();
        if (content == null) {
            return false;
        }
        init(content);
        return true;
    }

    /**
     * Reads the persisted as a json object, returns {@code null} if there is
     * any problem.
     */
    private JsonObject readPersistedObject() {
        return readPersistedObject(persistedFile, envImpl);
    }

    /**
     * Static helper to read a json object from a file.
     */
    public static JsonObject readPersistedObject(File persistedFile,
                                                 RepImpl envImpl)
    {
        try (final BufferedReader reader =
            new BufferedReader(new FileReader(persistedFile)))
        {
            final Object obj = Jsoner.deserialize(reader);
            if (!(obj instanceof JsonObject)) {
                LoggerUtils.info(envImpl.getLogger(), envImpl,
                    String.format("Expected json object, got %s", obj));
                return null;
            }
            return (JsonObject) obj;
        } catch (IOException e) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format("Cannot read persisted file %s", persistedFile));
            return null;
        } catch (JsonException e) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format(
                    "Error reading json object from persisted file %s",
                    persistedFile));
            return null;
        }
    }

    /**
     * Computes the continuation barrier timestamp based on maxClockSkew.
     *
     * <p>
     * This computation is based on the assumption that NTP never adjusts clock
     * backwards. If the assumption does not stand, the method
     * computeBarrierTimestampWithBackwradClockAdjustment should be used.
     *
     * <p>
     * After passing the barrier, we must guarantee we will not ack any term
     * lower than we have ever promised even if we forgot about the promise.
     *
     * <p>
     * The barrier timestamp is set at b. The earliest replay stream after
     * restart is of term r connected at local time t. The requirement is
     * {@code r >= u} which is satisfied by setting {@code b = w + S}.
     *
     * <pre>
     * {@code
     *                 --u
     *                /
     * promise    v <-
     *
     * restart    w
     * barrier    b
     *                 ----r
     *                /
     * replay     t <-
     * }
     * </pre>
     *
     * <p>
     * Proof sketch: (I) We only allow replay terms higher than b after
     * restart, hence {@code r >= b}. (II) Since clock is never adjusted
     * backwards, we have {@code w >= v}. (III) Since when our code is running,
     * the node is well-synchronized with each other, and therefore {@code u <=
     * v + S}.
     *
     * Therefore, we have the following inequalities:
     * {@code r > b = w + S >= v + S >= u}.
     */
    private long computeBarrierTimestamp() {
        return computeBarrierTimestamp(startTimestamp, maxClockSkew);
    }

    /**
     * Utility class shared by the unit tests.
     */
    public static long computeBarrierTimestamp(long currTime,
                                               long maxClockSkew) {
        if (barrierDisabled) {
            return MasterTerm.MIN_TERM;
        }
        return currTime + maxClockSkew;
    }

    /**
     * Computes the continuation barrier timestamp based on maxClockSkew.
     *
     * <p>
     * This computation is based on an assumption clocks may go backwards.
     * Normally, NTP adjusts the clock frequency to catch up or slow down clock
     * and therefore clocks do not usually go backwards. However, it seems to
     * me there is a threshold that NTP might adjust clock backwards when the
     * offset is too large.
     *
     * <p>
     * Currently this computation is not used since we think it is quite rare
     * that NTP would adjust clock backwards. The computeBarrierTimestamp is
     * used. However, if this is not the case this method should be used.
     *
     * <p>
     * After passing the barrier, we must guarantee we will not ack any term
     * lower than we have ever promised even if we forgot about the promise.
     *
     * <p>
     * We compute the barrier with the following assumptions: (1) When our code
     * is running, the time of the node is well-synchronized. (2) The time
     * measurements of any two well-synchronized nodes at the same time is less
     * than the maximum clock skew. This description is less accurate since it
     * ignores relativity, i.e., assumes zero network latency. I think this
     * does not change the computation. (3) At least one other node is running
     * and hence well-synchronized when this node restarts. (4) The clock of
     * running nodes are monotonically increasing, i.e., NTP does not adjust
     * the clock backwards when our code is running.
     *
     * <p>
     * Suppose the max clock skew is S when nodes are well-synchronized.
     * Suppose before this node N restarts, it has made a promise of timestamp
     * u at N's local time v. The local time of N reads w after restart.  The
     * barrier timestamp is set at b. The earliest replay stream after restart
     * is of term r connected at local time t. The requirement is {@code r >=
     * u} which is satisfied by setting {@code b = w + 3S}. The following
     * computation also assumes there is a running node that is well-synced and
     * its clock reads v' at the same time corresponding to v; and w' that to
     * w.
     *
     * <pre>
     * {@code
     *                 --u
     *                /
     * promise    v <-    -------- v'
     *
     * restart    w       -------- w'
     * barrier    b
     *                 ----r
     *                /
     * replay     t <-
     * }
     * </pre>
     *
     * <p>
     * Proof sketch: (I) We only allow replay terms higher than b after
     * restart, hence {@code
     * r >= b}. (II) According to (3) there is at least one node running during
     * N restart. Suppose the corresponding time measurements of v and w are v'
     * and w'.
     *
     * Therefore, we have the following inequalities: (i) {@code w' >= v'}
     * according to (4); (ii) {@code w >= w' - S} and {@code v <= v' + S}
     * according to (1) and (2). (III) The node N and the proposal node is
     * well-synchronized and thus {@code u <= v + S} according to (1) and (2).
     * Hence {@code r > b = w + 3S >= w' + 2S >= v' + 2S >= v + S >= u}.
     */
    @SuppressWarnings("unused")
    private long computeBarrierTimestampWithBackwardClockAdjustment() {
        if (barrierDisabled) {
            return MasterTerm.MIN_TERM;
        }
        return TimeSupplier.currentTimeMillis() + 3 * maxClockSkew;
    }

    @Override
    public synchronized boolean setPromised(Proposal proposal) {
        if (!ensureValidProposal(proposal)) {
            return false;
        }
        if (super.setPromised(proposal)) {
            persist();
            return true;
        }
        return false;
    }

    /**
     * Ensures the validity of the proposal arrival. We check two things: (1)
     * we have passed the continuation barrier if any; (2) the proposal
     * timestamp is not too far in the future.
     *
     * Note that for check (2), We cannot check if the timestamp is too far in
     * the history due to network latency and activities such as gc. Also, when
     * this check fails, we do not throw EnvironmentFailureException but simply
     * log it with WARNING with the expectation that admin will be alerted by
     * such message. The decision to not thrown exception is to adhere to the
     * principle of HA as the top priority. Since we do not know which node is
     * at fault (the proposer or the acceptor), we may invalidate all the
     * healthy nodes due to one out-of-sync proposer causing disruption to
     * availability. TODO: ideally, we would want to invalidate the culprit
     * node if we can gain enough confidence over the situation. This, however,
     * would require multiple validation across time and multiple nodes.
     *
     * @return {@code true} if passed the validation
     */
    private boolean ensureValidProposal(Proposal proposal) {
        if (!pastContinuationBarrier) {
            throw new IllegalStateException(String.format(
                "Unexpected arrival of election proposal %s " +
                "before waited past election continuation barrier"));
        }
        if (!clockSkewCheckDisabled) {
            final long currTime = TimeSupplier.currentTimeMillis();
            final long proposalTime = proposal.getTimeMs();
            if (proposalTime > currTime + maxClockSkew) {
                LoggerUtils.logMsg(
                    envImpl.getReplicationWarningLogger(),
                    envImpl,
                    EnvironmentFailureReason.TIME_OUT_OF_SYNC.toString(),
                    Level.WARNING,
                    String.format(
                        "Proposal %s is too far in the future: " +
                        "currentTime=%s, maxClockSkew=%s",
                        proposal, currTime, maxClockSkew));
                return false;
            }
        }
        return true;
    }

    /**
     * Persists the {@code persisted} field.
     */
    private void persist() {
        ensureHoldLock();
        final JsonObject obj = new JsonObject();
        obj.put(PROMISED_PROPOSAL_KEY.getKey(),
                (promisedProposal == null)
                ? null : promisedProposal.wireFormat());
        obj.put(ACCEPTED_PROPOSAL_KEY.getKey(),
                (acceptedProposal == null)
                ? null : acceptedProposal.wireFormat());
        obj.put(ACCEPTED_VALUE_KEY.getKey(),
                (acceptedValue == null)
                ? null : acceptedValue.wireFormat());
        persist(obj);
    }

    /**
     * Utility method to persist an object. Successfully persisting the object
     * to the file is not essential since we have a way to handle such file not
     * being present or even corrupted. Therefore, ignoring all error during
     * persistence by simply logging them.
     */
    private void persist(JsonObject obj) {
        ensureHoldLock();
        try {
            /*
             * This method is invoked to persist the information used to make an election
             * in order to be accessed it if a crash occurs right after the election.
             */
            envImpl.getLogManager().flushSync();
        } catch (DatabaseException e) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                    String.format("Cannot be applied f-Sync because of: " + e.getMessage()));
        }
        try (final FileOutputStream fos = new FileOutputStream(tempFile)) {
            try (final PrintWriter writer = new PrintWriter(fos)) {
                writer.write(obj.toJson());
                writer.flush();
                /*
                 * It seems to me that the try-with-resource can guarantee the
                 * bytes are flushed, but not necessarily synced. Manually sync
                 * just to be safe.
                 */
                fos.getFD().sync();
            }
        } catch (IOException e) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format("Cannot write temp file: %s", tempFile));
            return;
        }
        try {
            Files.move(tempFile.toPath(), persistedFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LoggerUtils.info(envImpl.getLogger(), envImpl,
                String.format("Cannot move temp file %s to persisted file %s",
                    tempFile, persistedFile));
            return;
        }
        LoggerUtils.info(
            envImpl.getLogger(), envImpl,
            String.format(
                "%s persisted to election state file: %s",
                envImpl.getNameIdPair().getName(),
                obj.toJson()));
    }

    @Override
    public synchronized boolean setAccept(Accept accept) {
        if (!ensureValidProposal(accept.getProposal())) {
            return false;
        }
        if (super.setAccept(accept)) {
            persist();
            return true;
        }
        return false;
    }

    /**
     * Waits past the continuation barrier.
     *
     * <p>
     * This method should be called before the election and replay threads are
     * initiated.
     *
     * <p>
     * We do not actually need to wait to past the barrier since the election
     * code should sets the minimum allowed proposal term with the barrier
     * timestamp. However, if we do not wait the RepNode is set up to just
     * initiating elections based on timestamp which can interfere with current
     * elections. Just wait for simplicity.
     */
    public void waitPastBarrier() throws InterruptedException {
        while (true) {
            final long waitTimeMillis;
            synchronized(this) {
                if (pastContinuationBarrier) {
                    return;
                }
                ensureInitialized();
                final long barrierTimestamp = promisedProposal.getTimeMs();
                final long currentTimeMillis = TimeSupplier.currentTimeMillis();
                waitTimeMillis = barrierTimestamp - currentTimeMillis;
                if (waitTimeMillis <= 0) {
                    pastContinuationBarrier = true;
                    return;
                }
                LoggerUtils.info(
                    envImpl.getLogger(), envImpl,
                    String.format(
                        "Node %s waiting %s ms to pass barrier %s",
                        envImpl.getNameIdPair().getName(),
                        waitTimeMillis, promisedProposal));
            }
            Thread.sleep(waitTimeMillis);
        }
    }

    /**
     * Returns whether the barrier has been passed.
     */
    public synchronized boolean pastContinuationBarrier() {
        return pastContinuationBarrier;
    }
}
