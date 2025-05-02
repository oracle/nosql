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

package oracle.kv.impl.tif;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.esResponse.GetResponse;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.topo.PartitionId;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Object to manage and send checkpoint TIF state to ES cluster.
 */
class CheckpointManager {

    /* Index properties for the checkpoint index.
     * TODO: make these values parameters.
     */
    static final Map<String, String> indexProperties;
    static {
        indexProperties = new HashMap<>();
        indexProperties.put(ElasticsearchHandler.SHARDS_PROPERTY, "1");
        indexProperties.put(ElasticsearchHandler.REPLICAS_PROPERTY, "1");
    }

    /* for logging */
    private final Logger logger;
    private final SimpleDateFormat df =
        new SimpleDateFormat("EEE," + " d MMM yyyy HH:mm:ss Z");

    /* ES client and admin client used to do ES operations */
    private final ElasticsearchHandler esHandler;

    /* name of ES index to store checkpoint state */
    private final String esCkptIndexName;
    /* per replication group key to store the checkpoint state in ES */
    private final String esCheckpointKey;

    /* parent TIF of the checkpoint manager */
    private final TextIndexFeeder feeder;

    /* schedule handle and thread pool to schedule timed tasks */
    private ScheduledExecutorService scheduledThreadPool;
    private ScheduledFuture<?> schedulerHandle;

    /* checkpoint interval in seconds */
    private long ckptIntervalSecs;

    /* number of checkpoints have been made since manager starts */
    private long numCheckpointDone;
    private long lastCheckpointTime;

    /*
     * Checkpoint Manager depends upon proper communication
     * with ES and hence throws IOException.
     *
     * Checkpoint indices and mappings are created on ES
     * during construction of this object.
     *
     */
    CheckpointManager(TextIndexFeeder feeder,
                      String esCkptIndexName,
                      String esCheckpointKey,
                      ElasticsearchHandler esHandler,
                      long checkpointIntervalMs,
                      Logger logger)
                              throws IllegalStateException, IOException {
        this.feeder = feeder;
        this.esCkptIndexName = esCkptIndexName;
        this.esCheckpointKey = esCheckpointKey;
        this.esHandler = esHandler;
        this.logger = logger;

        schedulerHandle = null;
        numCheckpointDone = 0;
        lastCheckpointTime = 0;

        /* initialized as constant but can be adjusted by setter API */
        ckptIntervalSecs = checkpointIntervalMs / 1000;
        ensureCkptESIndexAndMapping();
    }

    /* only for test, create manager without TIF */
    CheckpointManager(String indexName,
                      ElasticsearchHandler handler,
                      Logger l) throws IllegalStateException, IOException {

        this(null, indexName, TextIndexFeeder.CHECKPOINT_KEY_PREFIX,
             handler, 120000, l);
    }

    /**
     * Stops checkpoint manager, cancel scheduled checkpoint if exists
     */
    void stop() {

        if (schedulerHandle != null && !schedulerHandle.isCancelled()) {
            schedulerHandle.cancel(false);
            schedulerHandle = null;
        }

        scheduledThreadPool.shutdown();
        while (!scheduledThreadPool.isTerminated()) {
            /* wait for all tasks to finish */
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Starts checkpoint activity
     */
    void startCheckpoint() {
        /* periodically repeating checkpoint */
        startCheckpoint(true);
    }

    /**
     * Starts checkpoint activity
     *
     * @param repeatCheckpoint  true if repeating checkpoint periodically,
     *                          false if once-time checkpoint
     */
    void startCheckpoint(boolean repeatCheckpoint) {
        scheduledThreadPool = Executors.newScheduledThreadPool(1);

        /* schedule the first checkpoint */
        final CheckpointThread firstCkpt =
            new CheckpointThread(repeatCheckpoint);
        schedulerHandle = scheduledThreadPool.schedule(firstCkpt,
                                                       getCkptIntervalSecs(),
                                                       TimeUnit.SECONDS);
        final long next =
            System.currentTimeMillis() + getCkptIntervalSecs() * 1000;
        logger.log(Level.INFO,
                   "The first checkpoint to ES index {0} scheduled {1} " +
                   "seconds later at time {2}",
                   new Object[]{esCkptIndexName, getCkptIntervalSecs(),
                       df.format(new Date(next))});
    }

    /**
     * Cancels all future checkpoints
     */
    void cancelCheckpoint() {

        /* if checkpoint is in-progress, allow it to finish */
        if (schedulerHandle != null) {
            schedulerHandle.cancel(false);
            logger.log(Level.INFO,
                       "Future checkpoint cancelled for ES index {0}.",
                       esCkptIndexName);
        }
    }

    /**
     * Gets the checkpoint interval in seconds
     *
     * @return interval in seconds
     */
    long getCkptIntervalSecs() {
        return ckptIntervalSecs;
    }

    /**
     * Sets a new checkpoint interval in seconds. The new interval
     * will be effective when scheduling the next checkpoint
     *
     * @param interval checkpoint interval in seconds
     */
    void setCkptIntervalSecs(long interval) {
        ckptIntervalSecs = interval;
    }

    /**
     * Fetches a checkpoint state from ES index
     *
     * @return checkpoint state, null if no checkpoint state is found in ES
     * index.
     */
    CheckpointState fetchCkptFromES() {

        try {
            final GetResponse response =
                esHandler.get(esCkptIndexName,
                              esCheckpointKey);

            /* For SR27091 - test the response for null; to avoid NPE. */
            if (response == null) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info(" ");
                    logger.info("CheckpointManager.fetchCkptFromES: " +
                                "checkpoint request resulted in null " +
                                "response from ElasticsearchHandler");
                    logger.info(" ");
                }
                return null;
            }

            final Map<String, Object> rawState = response.sourceAsMap();
            if (!response.isFound() || rawState == null) {
                return null;
            }
            return new CheckpointState(rawState);
        } catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(" ");
                logger.log(Level.WARNING,
                    "CheckpointManager.fetchCkptFromES: checkpoint request " +
                    "resulted in exception from ElasticsearchHandler " +
                    "[exception=" + e + ", cause=" +e.getCause() + "]", e);
                logger.warning(" ");
            }
            return null;
        }
    }

    /**
     * Deletes a checkpoint state from ES index
     */
    void deleteCheckpointFromES() throws IllegalStateException {

        try {
            esHandler.del(esCkptIndexName, esCheckpointKey);
        } catch (IOException e) {
            logger.severe("Failed to delete checkpoint due to:" + e);
            throw new IllegalStateException(e);
        }

    }

    /**
     * Gets number of successful checkpoints to ES
     *
     * @return number of successful checkpoints to ES
     */
    long getNumCheckpointDone() {
        return numCheckpointDone;
    }

    /**
     * Gets the time of the last successful checkpoint
     *
     * @return the time of the last successful checkpoint
     */
    Date getLastCheckpointTime() {
        return new Date(lastCheckpointTime);
    }

    /**
     * Commits a checkpoint to ES index
     *
     * @param state state of checkpoint to commit
     */
    void doCheckpoint(CheckpointState state) {

        if (state == null) {
            /* no checkpoint to commit */
            return;
        }

        try {
            /* The returned IndexResponse will indicate only success.  Failures
             * at this interface are reported by exception.
             */
            esHandler.index(makePutOpFromCkpt(state));
            numCheckpointDone++;
            lastCheckpointTime = System.currentTimeMillis();
        } catch (Exception e) {
            /*
             * It is undocumented what types of exceptions may be thrown
             * here.  The most likely issue is that ES is unavailable,
             * which should be a temporary situation.  If the checkpoint
             * fails repeatedly, it is likely that other indexing
             * operations are also failing. For now, log the failure but
             * don't log the stack trace (which may un-necessarily alarm
             * or confuse the user); and hope for success the next time
             * around.
             */
            logger.log(Level.WARNING,
                       "Checkpoint failed, reason: " + e.getMessage());
        }
    }

    /*
     * Returns a put operation containing a JSON representation of a checkpoint
     * state suitable for indexing in ES.
     */
    private IndexOperation makePutOpFromCkpt(CheckpointState state) {

        try {
            /* create a document with all checkpoint field */
            final ESJsonBuilder document = ESJsonBuilder.builder();
            document.startStructure();

            for (Map.Entry<String, String> entry :
                state.getFieldsNameValue().entrySet()) {

                final String k = entry.getKey();
                final String v = entry.getValue();

                document.field(k, v);
            }

            document.endStructure();
            /* create an input op from document and key */
            return new IndexOperation(esCkptIndexName,
                                      esCheckpointKey,
                                      document.byteArray(),
                                      IndexOperation.OperationType.PUT);
        } catch (IOException e) {
            throw new IllegalStateException(
                          "Unable to serialize ES document " +
                          "for checkpoint: " + toString());
        }
    }

    /* Ensures the checkpoint ES index and mapping exist */
    private void ensureCkptESIndexAndMapping()
        throws IllegalStateException,
        IOException {
        /* create a new ckpt ES index if it does not exists */
        if (!esHandler.existESIndex(esCkptIndexName)) {
            esHandler.createESIndex(esCkptIndexName, indexProperties);
        }

        /* create a ckpt ES index mapping if it does not exists */
        final JsonGenerator mappingSpec = buildCkptMappingSpec();
        if (!esHandler.existESIndexMapping(esCkptIndexName)) {
            esHandler.createESIndexMapping(esCkptIndexName,
                                           mappingSpec);
        } else {
            /* if a mapping already exists, verify it first */
            final String existingMapping =
                    esHandler.getESIndexMapping(esCkptIndexName);
            if (ESRestClientUtil.isMappingResponseEqual(existingMapping,
                                                        mappingSpec,
                                                        esCkptIndexName)) {
                /* same mapping already exists, done */
                logger.log(Level.INFO,
                           "Same checkpoint mapping found in ES index {0}, " +
                           "use it.",
                           esCkptIndexName);
            } else {
                /*
                 * existing mapping is different, since unable to delete
                 * a mapping, we simply delete the whole ES index and create a
                 * new one, and create new mapping.
                 */
                logger.log(Level.INFO,
                           "Different checkpoint mapping found in ES index " +
                           "{0}, delete it and create a new index and mapping.",
                           esCkptIndexName);
                esHandler.deleteESIndex(esCkptIndexName);
                esHandler.createESIndex(esCkptIndexName, indexProperties);
                esHandler.createESIndexMapping(esCkptIndexName, mappingSpec);
            }
        }

        assert (esHandler.existESIndexMapping(esCkptIndexName));
        logger.log(Level.INFO,
                   "ES index for checkpoint {0} is ready.",
                   new Object[]{esCkptIndexName});
    }

    /* Creates a JSON to describe an ES type mapping for checkpoint state */
    private JsonGenerator buildCkptMappingSpec() {

        try {
            final ESJsonBuilder mapping = ESJsonBuilder.builder();

            mapping.startStructure().field("dynamic", "false")
                   .startStructure("properties");

            final Set<String> allFields =
                new CheckpointState().getFieldsNameValue().keySet();
            for (String key : allFields) {
                mapping.startStructure(key)
                       /* for checkpoint state, no need to index any field. */
                       .field("index", "false")
                       /* all fields are string type */
                       .field("type", "text")
                       .endStructure();
            }
            mapping.endStructure(); // properties end
            mapping.endStructure(); // mapping end

            return mapping.jsonGenarator();
        } catch (IOException e) {
            throw new IllegalStateException
            ("Unable to serialize ES mapping for checkpoint index " +
             esCkptIndexName, e);
        }
    }

    /* get a dummy checkpoint state for test only */
    private CheckpointState getDummyCkptStateForTest() {
        final CheckpointState state = new CheckpointState();
        state.setCheckpointTimeStamp(System.currentTimeMillis());
        state.setCheckpointVLSN(1000);
        state.setSrcRepNode("rg1-rn1");
        state.setRepGroupUUID(UUID.fromString
            ("5e40b8a9-cbff-460d-b7b0-618447426c5d"));
        state.setGroupName("rg1");


        final Set<PartitionId> comp = new HashSet<>();
        comp.add(new PartitionId(1));
        comp.add(new PartitionId(100));
        comp.add(new PartitionId(12345));
        state.setCompleteTransParts(comp);

        return state;
    }

    /*
     * Checkpoint thread to conduct checkpoint
     */
    private class CheckpointThread implements Runnable {
        private boolean scheduleNext;

        public CheckpointThread() {
            scheduleNext = true;
        }

        /* for test only */
        public CheckpointThread(boolean next) {
            this();
            scheduleNext = next;
        }

        @Override
        public void run() {

            final long start = System.currentTimeMillis();
            logger.log(Level.FINE,
                       "Do checkpoint to ES index {0} at time {1}.",
                       new Object[]{esCkptIndexName,
                           df.format(new Date(start))});

            /* get checkpoint from tif and commit to ES */
            if (feeder != null) {
                doCheckpoint(feeder.prepareCheckpointState());
            } else {
                /* for test only, no tif */
                doCheckpoint(getDummyCkptStateForTest());
            }

            /* scheduled next checkpoint thread */
            if (scheduleNext) {
                final CheckpointThread nextCkpt = new CheckpointThread();

                schedulerHandle =
                    scheduledThreadPool.schedule(nextCkpt,
                                                 getCkptIntervalSecs(),
                                                 TimeUnit.SECONDS);

                final long next = System.currentTimeMillis() +
                            (1000 * getCkptIntervalSecs());
                logger.log(Level.FINE,
                           "Next checkpoint to {0} is scheduled {1} seconds " +
                           "later at time {2}.",
                           new Object[]{esCkptIndexName, getCkptIntervalSecs(),
                               df.format(next) });
            }

            logger.log(Level.FINE,
                       "Finish checkpoint to ES index {0}, time elapsed: " +
                       "{1} ms",
                       new Object[]{esCkptIndexName,
                           (System.currentTimeMillis() - start)});
        }

        @Override
        public String toString() {
            return "Thread of checkpoint to ES index " + esCkptIndexName;
        }
    }
}
