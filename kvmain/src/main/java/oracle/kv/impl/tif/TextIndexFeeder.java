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

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static oracle.kv.impl.param.ParameterState.RN_TIF_CHECKPOINT_INTERVAL;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.table.Index;

import com.fasterxml.jackson.core.JsonGenerator;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.VLSN;

/**
 * TextIndexFeeder is responsible for
 *
 * 1. receiving data from source node, either via the replication stream or
 * partition transfer
 * 2. converting received data to ES operation based on the text index
 * 3. sending indexable operations to ES cluster
 * 4. conducting periodical checkpoint to ES cluster
 */
public class TextIndexFeeder {

    /* all es indices managed by TIF start with ondb */
    public static final String ES_INDEX_NAME_PREFIX = "ondb";
    /* es index for internal use, _prefix to avoid conflict */
    public static final String ES_INDEX_NAME_FOR_CKPT = "_checkpoint";
    /* key to store ckpt info */
    public static final String CHECKPOINT_KEY_PREFIX = "tif_ckpt_key_";
    /* prefix of tif env diretory */
    public static final String TIF_ENV_DIR_PREFIX = "tif_env_";

    private static final byte[] emptyBytes = {};

    /* logger */
    private final Logger logger;

    /* table meta from KV store */
    private volatile TableMetadata tableMetadata;

    /* ES client and admin client */
    private final ElasticsearchHandler esHandler;

    /* configuration, subscription manager, and checkpoint manager */
    private final SubscriptionConfig config;
    private final SubscriptionManager subManager;
    private final CheckpointManager ckptManager;

    /* info about source RN */
    private final SourceRepNode sourceRN;
    /* name of kvstore from which TIF streams data  */
    private final String storeName;

    /* txn agenda for replication stream */
    private final TransactionAgenda txnAgendaForRepStream;
    /* txn agenda for partition transfer */
    private final TransactionAgenda txnAgendaForPartTransfer;

    /* worker to stream data in txn agenda for replication stream */
    private TextIndexFeederWorker workerProcessRepStream;
    /* group of workers to stream data in txn agenda for partition readers */
    private ArrayList<TextIndexFeederWorker> workerPoolProcessPartTransfer;

    /* Semaphore used to stop the handling of the replication streams while
     * a new tableMetadata is being installed.
     */
    private final Semaphore dataItemConsumptionPermits;
    /* The number of permits in the semaphore above, one per worker thread. */
    private final int nDataItemConsumptionThreads;

    /**
     * Constructor of TextIndexFeeder.
     *
     * Initialize internal data structures and start worker threads.
     *
     * @param sourceRN     rep node from which to stream data
     * @param tifRN        rep node hosting TIF
     * @param esHandler    ElasticSearch handler
     * @param logger       logger
     *
     * @throws IllegalStateException
     */
    TextIndexFeeder(SourceRepNode sourceRN,
                    HostRepNode tifRN,
                    ElasticsearchHandler esHandler,
                    ParameterMap params,
                    SecurityParams securityParams,
                    Logger logger)
        throws IllegalStateException {

        this.logger = logger;
        this.sourceRN = sourceRN;
        this.esHandler = esHandler;
        storeName = sourceRN.getStoreName();
        tableMetadata = sourceRN.getTableMetadata();

        /* for each text index create ES index */
        try {
            initializeESIndices(tableMetadata.getTextIndexes());

            config = buildSubscriptionConfig(sourceRN, tifRN, securityParams);
            /* set the feeder filter */
            config.setFeederFilter(new TextIndexFeederFilter());
            subManager = new SubscriptionManager(sourceRN, tifRN, config,
                                                 logger);

            final long checkpointInterval =
                ((DurationParameter) params.getOrDefault(
                    RN_TIF_CHECKPOINT_INTERVAL)).toMillis();

            ckptManager =
                new CheckpointManager(
                        this,
                        deriveCkptESIndexName(storeName),
                        CHECKPOINT_KEY_PREFIX + config.getGroupName(),
                        esHandler, checkpointInterval, logger);
        } catch (IOException ioe) {
            logger.severe(lm("Either Indices could not be intialized or" +
                    " CheckpointManager not constructed due to:" + ioe));
            throw new IllegalStateException(ioe);
        }

        /* start time interval based checkpoint */
        ckptManager.startCheckpoint();
        /* create txn agendas */
        txnAgendaForRepStream = new TransactionAgenda(esHandler,
                                                      params,
                                                      logger,
                                                      "ta-str");
        txnAgendaForPartTransfer = new TransactionAgenda(esHandler,
                                                         params,
                                                         logger,
                                                         "ta-tra");

        /* For each worker thread, allow 1 permit in the semaphore.
         * To prevent the workers from processing data items during
         * metadata updates, acquire all the permits.  See
         * TextIndexFeederWorker.handleDataItemHelper
         */
        nDataItemConsumptionThreads =
            1 + subManager.getDOPForPartTransfer();
        dataItemConsumptionPermits =
            new Semaphore(nDataItemConsumptionThreads, true);

        /* start txn agenda workers */
        startWorkers();
    }

    /**
     * Switches to start everything under the hood!
     */
    void startFeeder() {

        /* first get the checkpoint to see where to start */
        final CheckpointState ckpt = ckptManager.fetchCkptFromES();

        /* start from day 0 if no checkpoint */
        if (ckpt == null || !isValidCheckpoint(ckpt)) {
            logger.log(Level.INFO,
                       lm("TextIndexFeeder.startFeeder: Found no checkpoint " +
                          "or invalid checkpoint, start " +
                          "stream from {0}"), FIRST_VLSN);
            if (ckpt != null) {
                ckptManager.deleteCheckpointFromES();
            }
            subManager.startStream(FIRST_VLSN);
        } else {

            /**
             * We only do checkpoint during ongoing phase, but partitions
             * may come and go during failure. After verifying a checkpoint
             * state from ES, we first check whether the list of completed
             * partitions matches the list of hosted partitions in source
             * node.
             *
             * If yes, we will resume TIF with ongoing phase from the
             * checkpoint VLSN.
             *
             * If no, the source may have newly immigrated partition
             * not in the completed list in checkpoint, such partition
             * need to be transferred by partition transfer.
             */
            final long vlsn = ckpt.getCheckpointVLSN();
            final Set<PartitionId> toTransfer =
                new HashSet<>(subManager.getManagedPartitions());
            toTransfer.removeAll(ckpt.getPartsTransferred());
            if (toTransfer.isEmpty()) {
                logger.log(Level.INFO,
                           lm("TextIndexFeeder.startFeeder: No new " +
                              "partitions found at source RN, resume " +
                              "stream from vlsn {0} in checkpoint: {1}"),
                              new Object[]{VLSN.getNext(vlsn), ckpt.toString()});
                subManager.startStream(VLSN.getNext(vlsn));
            } else {
                logger.log(Level.INFO,
                           lm("TextIndexFeeder.startFeeder: Found {0} new " +
                              "partitions at source, start stream " +
                              "from vlsn {1}, also start partition transfer " +
                              "for partitions: {2}."),
                           new Object[]{toTransfer.size(), vlsn,
                               SubscriptionManager
                                   .partitionListToString(toTransfer)});
                /* delete ckpt since go back to init phase */
                ckptManager.deleteCheckpointFromES();
                subManager.startStream(vlsn, toTransfer);
            }
        }
    }

    /**
     * Stops TIF, delete ES index if needed
     *
     * @param deleteIndex true if delete ES index
     * @param removeCheckpoint if true, delete the checkpoint for this group.
     * @throws IllegalStateException
     */
    void stopFeeder(boolean deleteIndex, boolean removeCheckpoint) {

        /* shutdown ckpt manager, no more checkpoint */
        ckptManager.stop();

        /* Shutdown TransactionAgenda's batch timer. */
        txnAgendaForRepStream.stop();
        txnAgendaForPartTransfer.stop();

        /* shutdown all subscription */
        subManager.shutdown(SubscriptionState.SHUTDOWN);

        /* shut down all workers */
        workerProcessRepStream.cancel();
        for (TextIndexFeederWorker worker : workerPoolProcessPartTransfer) {
            worker.cancel();
        }

        /*
         * Removing the checkpoint effectively resets the TIF so that it will
         * start over populating the indexes from the beginning, the next time
         * it starts up.
         */
        if (removeCheckpoint) {
            ckptManager.deleteCheckpointFromES();
        }

        /* finally delete ES index for each text index, if requested */
        if (deleteIndex) {
            try {
                if (tableMetadata != null) {
                    deleteESIndices(tableMetadata.getTextIndexes());
                }

                /* delete checkpoint ES index */
                final String ckptESindex = deriveCkptESIndexName(storeName);
                if (esHandler.existESIndex(ckptESindex)) {
                    esHandler.deleteESIndex(ckptESindex);
                    logger.log(Level.INFO,
                               lm("TextIndexFeeder.stopFeeder: Checkpoint " +
                                  "ES index {0} has been deleted."),
                                  ckptESindex);
                }
            } catch (IOException ioe) {
                logger.severe(lm("TextIndexFeeder.stopFeeder: failed to " +
                                 "delete indices. Cause:" + ioe.getCause()));
                /*
                 * Stop Feeder is required to happen when RN toggles between
                 * master and replica. However, for stateChangeEvents,
                 * deleteIndex and removeCheckpoint is false, so this codeblock
                 * is not entered. For TableMetadataChanges, index need to get
                 * deleted, so IllegalStateException should be thrown.
                 */
                throw new IllegalStateException(ioe);
            }
        }
    }

    /**
     * Drops an ES index.
     *
     * @param esIndexName name of ES index to drop
     */
    void dropIndex(final String esIndexName) throws IllegalStateException {

        /* Before deleting the index from Elasticsearch, remove any operations
         * intended for the index from both TransactionAgendas.
         */
        txnAgendaForRepStream.purgeOpsForIndex(esIndexName);
        txnAgendaForPartTransfer.purgeOpsForIndex(esIndexName);

        /* TransferCommits in a CommitQueue might contain standalone ops that
         * refer to the index being removed.  A flush of the commit queue will
         * get rid of them.
         */
        txnAgendaForRepStream.flushPendingCommits();
        txnAgendaForPartTransfer.flushPendingCommits();

        try {
            if (!esHandler.existESIndex(esIndexName)) {
                logger.log(Level.WARNING,
                           lm("TextIndexFeeder.dropIndex: ES index {0} does " +
                              "not exist, ignore."),
                           esIndexName);
            } else {
                esHandler.deleteESIndex(esIndexName);
            }
        } catch (IOException e) {
            // This is exception from exist index. Can be ignored.
            logger.warning("TextIndexFeeder.dropIndex: existIndex API " +
                           "called to ES failed for index:" +
                            esIndexName);
        }
    }

    /**
     * Ensures ES index and mapping exists for each text index
     *
     * @throws IllegalStateException
     */
    void ensureESIndexAndMapping() throws IllegalStateException {

        if (tableMetadata == null) {
            return;
        }

        final List<Index> indices = tableMetadata.getTextIndexes();
        for (Index index : indices) {
            if (index.getType().equals(Index.IndexType.TEXT)) {
                final String tableName = index.getTable().getFullName();
                final String namespace =
                    ((TableImpl) index.getTable()).getInternalNamespace();
                final String esIndexName = deriveESIndexName(storeName,
                                                             namespace,
                                                             tableName,
                                                             index.getName());
                final IndexImpl indexImpl = (IndexImpl) index;
                try {
                    /* ensure ES index exists */
                    ensureESIndex(esIndexName, indexImpl.getProperties());

                    /* ensure the mapping exists */
                    final JsonGenerator mappingSpec =
                        ElasticsearchHandler.generateMapping(indexImpl);

                    if (!esHandler.existESIndexMapping(esIndexName)) {
                        /* create new mapping if not exists */
                        esHandler.createESIndexMapping(esIndexName,
                                                       mappingSpec);
                        logger.log(
                            Level.INFO,
                            lm("TextIndexFeeder.ensureESIndexAndMapping: " +
                               "ES mapping created for text index {0}"),
                            new Object[] { index.getName() });
                    } else {
                        /* if mapping exist, verify it */
                        final String existingMapping =
                            esHandler.getESIndexMapping(esIndexName);
                        if (ESRestClientUtil.isMappingResponseEqual(
                                                 existingMapping,
                                                 mappingSpec,
                                                 esIndexName)) {
                            logger.log(
                               Level.INFO,
                               lm("TextIndexFeeder.ensureESIndexAndMapping: " +
                                  "Mapping already exists for index " +
                                  esIndexName));
                        } else {

                            if (logger.isLoggable(Level.INFO)) {
                                logger.log(
                                   Level.INFO,
                                   lm("TextIndexFeeder." +
                                      "ensureESIndexAndMapping: " +
                                      "Existing mapping spec does not match " +
                                      "the new spec generated for the es " +
                                      "index [name = {0}]. " +
                                      "EXPECTED SPEC = {1}, " +
                                      "EXISTING SPEC = {2}"),
                                   new Object[] {
                                       esIndexName,
                                       ElasticsearchHandler.mappingSpecStr(
                                           mappingSpec),
                                       existingMapping });
                            }

                            /*
                             * unable to delete mapping, so just delete the
                             * es index
                             */
                            esHandler.deleteESIndex(esIndexName);
                            /* recreate a new index */
                            esHandler.createESIndex(esIndexName,
                                                    indexImpl.getProperties());
                            /* create new mapping */
                            esHandler.createESIndexMapping(esIndexName,
                                                           mappingSpec);

                            if (logger.isLoggable(Level.INFO)) {
                                logger.log(
                                   Level.INFO,
                                   lm("TextIndexFeeder." +
                                      "ensureESIndexAndMapping: " +
                                      "Old mapping deleted and new mapping " +
                                      "created for es index [name = {0}]. " +
                                      "New mapping spec = {1}"),
                                   new Object[] {
                                       esIndexName,
                                       ElasticsearchHandler.mappingSpecStr(
                                           mappingSpec)});
                            }
                        }

                    }
                } catch (IOException ioe) {
                    /*
                     * The IOExceptions are due to existMapping and exist Index
                     * API. Since existing mapping could not be checked, need
                     * to throw IllegalStateException here.
                     */
                    logger.severe(
                        lm("TextIndexFeeder.ensureESIndexAndMapping: " +
                           "The ES Mapping might have existed before." +
                           "ExistMapping could not check the same" +
                           "due to:" + ioe.getCause()));

                    throw new IllegalStateException(ioe);

                }
            }
        }
    }

    /**
     * @hidden
     *
     * For internal test only: start tif from initial phase, regardless of
     * start VLSN.
     */
    void startFeederFromInitPhase() {
        subManager.startStream(subManager.getManagedPartitions());
    }

    /**
     * Adds a new incoming partition to TIF
     *
     * @param pid id of partition
     */
    synchronized void addPartition(PartitionId pid) {
        subManager.addPartition(pid);
        logger.log(Level.INFO,
                   lm("TextIndexFeeder.addPartition: Partition {0} has been " +
                      "added, and TIF will start " +
                      "receiving entries from the partition"), pid);
    }

    /**
     * Removes a partition tif no longer interested
     *
     * @param pid  id of partition to remove
     */
    synchronized void removePartition(PartitionId pid) {
        subManager.removePartition(pid);
        logger.log(Level.INFO,
                   lm("TextIndexFeeder.removePartition: Partition {0} has " +
                      "been removed, and TIF will no " +
                      "longer receive entries from the partition."), pid);
    }

    /**
    * Checks if a partition is managed by the TIF
    *
    *
    * @param pid  id of partition
    */
    boolean isManangedPartition(PartitionId pid) {
        return subManager.isManangedPartition(pid);
    }

    /**
     * Returns subscription manager
     *
     * @return subscription manager
     */
    SubscriptionManager getSubManager() {
        return subManager;
    }

    /**
     * Compares the text indices defined in current table meatadata with those
     * in the old version of the metadata to determine which need to be added.
     *
     * @param oldMetadata old table metadata
     *
     * @return a set of es indices to add
     */
    Set<String> esIndicesToAdd(TableMetadata oldMetadata) {
        /*
         * Build a list of all es index names for all text indices in both old
         * and current metadata.
         */
        final Set<String> oldEsIndexNames =
            buildESIndicesNames(storeName, oldMetadata.getTextIndexes());

        final Set<String> currentEsIndexNames =
            buildESIndicesNames(storeName, tableMetadata.getTextIndexes());

        /*
         * Subtract the set of old indexes from new, to derive the set of
         * indexes that need to be created.
         */
        final Set<String> indexesToCreate = new HashSet<>(currentEsIndexNames);
        indexesToCreate.removeAll(oldEsIndexNames);

        if (indexesToCreate.isEmpty()) {
            logger.log(Level.FINE,
                       lm("TextIndexFeeder.esIndicesToAdd: No new indexes " +
                          "were found by comparing " +
                          "old and new table metadata; nothing to add."));
        } else {
            logger.log(Level.INFO,
                       lm("TextIndexFeeder.esIndicesToAdd: ES indices to " +
                          "create: {0} "),
                       Arrays.toString(indexesToCreate.toArray()));
        }
        return indexesToCreate;
    }

    /**
     * Compares the text indices defined in table meatadata with those
     * already existent on ES, and returns a set of ES indices that need
     * be dropped. Only ES indices corresponding to the user-defined text
     * indices are considered. The internal checkpoint ES index is excluded.
     *
     * @return a set of es indices to drop
     */
    Set<String> esIndicesToDrop(TableMetadata oldMetadata) {
        /*
         * Build a list of all es index names for all text indices in both old
         * and current metadata.
         */
        final Set<String> oldEsIndexNames =
            buildESIndicesNames(storeName, oldMetadata.getTextIndexes());

        final Set<String> currentEsIndexNames =
            buildESIndicesNames(storeName, tableMetadata.getTextIndexes());


        /*
         * Subtract the set of new indexes from old, to derive the set of
         * indexes that need to be removed.
         */
        final Set<String> indexesToRemove = new HashSet<>(oldEsIndexNames);
        indexesToRemove.removeAll(currentEsIndexNames);

        if (indexesToRemove.isEmpty()) {
            logger.log(Level.FINE,
                       lm("TextIndexFeeder.esIndicesToDrop: No indexes to " +
                          "remove were found by comparing " +
                          "old and new table metadata; nothing to remove."));
        } else {
            logger.log(Level.INFO,
                       lm("TextIndexFeeder.esIndicesToDrop: ES indices to " +
                          "remove: {0}"),
                       Arrays.toString(indexesToRemove.toArray()));
        }
        return indexesToRemove;
    }

    SubscriptionState getSubscriptionState() {
        return subManager.getState();
    }

    /**
     * Register a user-defined post commit callback
     *
     * @param cbk commit callback to register
     */
    void setPostCommitCbk(TransactionPostCommitCallback cbk) {
        txnAgendaForPartTransfer.setPostCommitCbk(cbk);
        txnAgendaForRepStream.setPostCommitCbk(cbk);
    }

    /**
     * Create a mapping in ES index for a given text index. ES will use the
     * mapping to index the doc from TIF.
     *
     * @param kvstoreName   name of store
     * @param tableName     name of table
     * @param textIndexName name of the text index defined on the table
     * @throws IllegalStateException
     */
    void createMappingForTextIndex(String kvstoreName,
                                   String namespace,
                                   String tableName,
                                   String textIndexName)
        throws IllegalStateException {

        final String esIndexName = deriveESIndexName(kvstoreName, namespace,
                                                     tableName, textIndexName);
        /* create and add mapping to ES index */
        final TableImpl tableImpl = tableMetadata.getTable(namespace,
                                                           tableName, true);
        final Index textIndex = tableImpl.getTextIndex(textIndexName);
        final JsonGenerator mappingSpec =
            ElasticsearchHandler.generateMapping((IndexImpl) textIndex);

        esHandler.createESIndexMapping(esIndexName, mappingSpec);
    }

    /**
     * Create a checkpoint state for checkpoint thread.
     *
     * @return a checkpoint state or null if no checkpoint
     */
    CheckpointState prepareCheckpointState() {

        /*
         * Resume from init phase is tricky. The bookmark VLSN
         * may not be still available in source node when TIF
         * resumes. For now we only do checkpoint in ongoing phase.
         */
        if (subManager.getState()
                      .equals(SubscriptionState.REPLICATION_STREAM)) {

            return new CheckpointState(config.getGroupName(),
                                       config.getGroupUUID(),
                                       sourceRN.getSourceNodeName(),
                                       txnAgendaForRepStream
                                           .getLastCommittedVLSN(),
                                       subManager.getManagedPartitions(),
                                       System.currentTimeMillis());

        }

        return null;
    }

    /**
     * Sets new table metadata for TIF.  While installing the new version of
     * the metadata, suspend processing of the input queues.  Upon return from
     * this method, the caller is guaranteed that there is no ongoing
     * processing of data items using the old version of the metadata.  In the
     * case where the metadata delta represents deletion of indexes, this means
     * that no further data items that would add updates to the deleted indexes
     * will be added to the transaction agendas.
     *
     * @param newTmd  new table metadata
     */
    void setTableMetadata(TableMetadata newTmd) {
        /*
         * Acquire all of the available permits for threads passing through
         * handleDataItemHelper.  This effectively suspends the handling of
         * items from the input queues.
         */
        dataItemConsumptionPermits.acquireUninterruptibly
            (nDataItemConsumptionThreads);

        tableMetadata = newTmd;

        /*
         * After assigning a new tableMetadata, release the permits to allow
         * the handling of items from the input queues to resume.
         */
        dataItemConsumptionPermits.release(nDataItemConsumptionThreads);
    }

    /*---------------------------------------------------------------*/
    /*---                    static functions                    ---*/
    /*---------------------------------------------------------------*/

    /**
     * Derives ES index prefix. The prefix is "ondb.<kvstore>"
     *
     * @param storeName  name of kv store
     * @return  es index prefix
     */
    public static String deriveESIndexPrefix(final String storeName) {
        return ES_INDEX_NAME_PREFIX + "." + storeName.toLowerCase();
    }

    /**
     * Derives a unique ES index name. We would create an ES index for each
     * text index defined on a table. To uniquely identify an ES index, its
     * name is derived from the name of store, table as well as the text index
     * name itself. For example, an ES index created for text index
     * mytextindex on table mytable in store mystore will be named as
     * "ondb.mystore.mytable.mytextindex"
     *
     * @param storeName      name of the kvstore
     * @param tableName      name of the table
     * @param textIndexName  name of the text index
     *
     * @return ES index name
     */
    public static String deriveESIndexName(final String storeName,
                                           final String namespace,
                                           final String tableName,
                                           final String textIndexName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(deriveESIndexPrefix(storeName)).append(".");
        if (namespace != null) {
            sb.append(namespace.toLowerCase()).append(".");
        }
        sb.append(tableName.toLowerCase()).append(".")
          .append(textIndexName.toLowerCase());
        return sb.toString();
    }

    /**
     * Builds ES index name for checkpoint. There is one ES index for
     * checkpoint per each store. The ckpt ES index would be like
     * "ondb.kvstore.tif_checkpoint"
     *
     * @return name of ES index for checkpoint
     */
    static String deriveCkptESIndexName(final String storeName) {
        return deriveESIndexPrefix(storeName) + "." + ES_INDEX_NAME_FOR_CKPT;
    }

    /**
     * Builds list of es index names for all text indices in table metadata
     *
     * @param textIndices  list of text indices
     * @return  set of ES index names corresponding to the text indices
     */
    static Set<String> buildESIndicesNames(final String storeName,
                                           final List<Index> textIndices) {
        final Set<String> allESIndices = new HashSet<>();
        for (Index index : textIndices) {
            /* must be text index */
            assert (index.getType().equals(Index.IndexType.TEXT));

            final String esIndexName = deriveESIndexName(storeName,
                ((TableImpl) index.getTable()).getInternalNamespace(),
                index.getTable().getFullName(),
                index.getName());
            allESIndices.add(esIndexName);

        }
        return allESIndices;
    }

    /*---------------------------------------------------------------*/
    /*---                    private functions                    ---*/
    /*---------------------------------------------------------------*/
    /* check if the checkpoint state fetched from ES is valid */
    private boolean isValidCheckpoint(CheckpointState ckpt) {

        /* verify rep group name and its uuid id */
        if (!sourceRN.getGroupName().equals(ckpt.getGroupName()) ||
            !sourceRN.getGroupUUID().equals(ckpt.getGroupUUID())) {

            logger.log
                (Level.INFO,
                 lm("TextIndexFeeder.isValidCheckpoint: Mismatch rep group. " +
                    "Group name (id) in ckpt:{0}({1}) " +
                    "while the expected group name (id) is: {2}({3})"),
                       new Object[]{ckpt.getGroupName(), ckpt.getGroupUUID(),
                           sourceRN.getGroupName(), sourceRN.getGroupUUID()});

            return false;
        }

        /* if group matches but source has migrated, log it */
        if (!sourceRN.getSourceNodeName().equals(ckpt.getSrcRepNode())) {
            logger.log
                (Level.INFO,
                 lm("TextIndexFeeder.isValidCheckpoint: Mismatch source " +
                    "node, source have migrated, source " +
                    " node name in the ckpt: {0}, while expected is: {1}"),
                 new Object[]{ckpt.getSrcRepNode(),
                              sourceRN.getSourceNodeName()});
        }

        return true;
    }

    /* start all worker threads */
    private void startWorkers() {

        workerProcessRepStream =
            new TextIndexFeederWorker("rep_str_agenda_worker", subManager
                .getInputQueueRepStream());
        workerProcessRepStream.start();
        logger.log(Level.INFO,
                   lm("TextIndexFeeder.startWorkers: Worker thread " +
                      "processing rep stream started, worker id: " +
                      workerProcessRepStream.getWorkerId()));

        /* start all partition transfer worker processes, id from 1 */
        final int dopPartTrans = subManager.getDOPForPartTransfer();
        workerPoolProcessPartTransfer = new ArrayList<>(dopPartTrans);
        for (int i = 0; i < dopPartTrans; i++) {
            final TextIndexFeederWorker worker =
                new TextIndexFeederWorker(
                        "part_xfer_agenda_worker_" + i,
                        subManager.getInputQueuePartReader());
            workerPoolProcessPartTransfer.add(worker);
            worker.start();
        }
        logger.log(Level.INFO,
                   lm("TextIndexFeeder.startWorkers: # worker threads for " +
                      "partition transfer started: {0}"),
                      dopPartTrans);
    }

    /* build a subscription configuration from source and host node */
    private SubscriptionConfig buildSubscriptionConfig(
                                   SourceRepNode feederRN,
                                   HostRepNode tifHostRN,
                                   SecurityParams securityParams)
        throws IllegalStateException {

        Properties haSSLProps = null;
        if (securityParams != null && securityParams.isSecure()) {
            haSSLProps = securityParams.getJEHAProperties();
        }

        final File tifEnvDir = createEnvDirectory(tifHostRN);
        final String tifHostPort =
            HostPortPair.getString(tifHostRN.getHost(), tifHostRN.getPort());
        final String feederHostPort =
            HostPortPair.getString(feederRN.getSourceHost(),
                                   feederRN.getSourcePort());
        try {
            if (haSSLProps != null) {
                return new SubscriptionConfig(tifHostRN.getTifNodeName(),
                                              tifEnvDir.getAbsolutePath(),
                                              tifHostPort, feederHostPort,
                                              feederRN.getGroupName(),
                                              feederRN.getGroupUUID(),
                                              NodeType.SECONDARY, null,
                                              haSSLProps);
            }

            return new SubscriptionConfig(tifHostRN.getTifNodeName(),
                                          tifEnvDir.getAbsolutePath(),
                                          tifHostPort, feederHostPort,
                                          feederRN.getGroupName(),
                                          feederRN.getGroupUUID());

        } catch (UnknownHostException e) {
            logger.warning(lm("TextIndexFeeder.buildSubscriptionConfig: " +
                              "Unknown host " + e.getMessage()));
            throw new IllegalStateException("Unknown host " + e.getMessage());
        }

    }

    /* create a TIF environment directory at TIF host node */
    private File createEnvDirectory(HostRepNode host)
        throws IllegalStateException {

        final File repHome = FileNames.getServiceDir(host.getRootDirPath(),
                                                     host.getStoreName(),
                                                     null,
                                                     host.getStorageNodeId(),
                                                     host.getRepNodeId());
        final String tifDir = TIF_ENV_DIR_PREFIX + host.getTifNodeName();
        final File tifEnvDir = new File(repHome, tifDir);

        if (tifEnvDir.exists() && !FileUtils.deleteDirectory(tifEnvDir)) {
            logger.log(Level.WARNING,
                       lm("TextIndexFeeder.createEnvDirectory: Unable to " +
                          "delete an old TIF environment directory" +
                          " at: {0}."),
                       tifEnvDir.toString());

            throw new IllegalStateException("Unable to create TIF " +
                                            "env directory " + tifDir +
                                            " at TIF host node " +
                                            host.getRepNodeId()
                                                .getFullName());
        }

        if (!tifEnvDir.exists() && FileNames.makeDir(tifEnvDir)) {
            logger.log(Level.INFO,
                       lm("TextIndexFeeder.createEnvDirectory: Successfully " +
                          "created new TIF environment directory " +
                          "{0} at: {1}"),
                       new Object[]{tifDir, tifEnvDir.toString()});

            return tifEnvDir;
        }
        logger.log(Level.WARNING,
                   lm("TextIndexFeeder.createEnvDirectory: Unable to create " +
                      "a new TIF env dir {0} at: {1}"),
                   new Object[]{tifDir, tifEnvDir.toString() });

        throw new IllegalStateException("Unable to create TIF " +
                                        "environment directory at TIF " +
                                         "host node " +
                                         host.getRepNodeId()
                                             .getFullName());
    }

    /*
     * Initializes ES index for each text index
     *
     * @param textIndices  list of all text indices
     */
    private void initializeESIndices(final List<Index> textIndices)
        throws IOException {
        for (Index index : textIndices) {
            /* create ES index for each text index */
            if (index.getType().equals(Index.IndexType.TEXT)) {
                final String esIndexName =
                    deriveESIndexName(storeName,
                        ((TableImpl) index.getTable()).getInternalNamespace(),
                        index.getTable().getFullName(),
                        index.getName());
                esHandler.createESIndex(esIndexName,
                                        ((IndexImpl) index).getProperties());
                logger.log(Level.FINE,
                           lm("TextIndexFeeder.initializeESIndices: ES " +
                              "index {0} created for text index {1} in " +
                              "kvstore {2}"),
                           new Object[]{esIndexName, index.getName(),
                                        storeName});
            }
        }
    }

    /*
     * Deletes ES index for each text index
     *
     * @param textIndices  list of all text indices
     */
    private void deleteESIndices(final List<Index> textIndices)
        throws IllegalStateException,
        IOException {

        for (Index index : textIndices) {
            /* delete ES index for each text index */
            if (index.getType().equals(Index.IndexType.TEXT)) {
                final String esIndexName =
                    deriveESIndexName(storeName,
                        ((TableImpl) index.getTable()).getInternalNamespace(),
                        index.getTable().getFullName(),
                        index.getName());

                if (esHandler.existESIndex(esIndexName)) {
                    esHandler.deleteESIndex(esIndexName);
                }
                logger.log(Level.INFO,
                           lm("TextIndexFeeder.deleteESIndices: ES " +
                              "index {0} deleted for text index {1} in " +
                              "kvstore {2}"),
                           new Object[]{esIndexName, index.getName(),
                               storeName });
            }
        }
    }

    /* Ensures ES index exists, create an ES index if not */
    private void ensureESIndex(final String esIndexName,
                               Map<String, String> properties)
        throws IllegalStateException,
        IOException {
        if (!esHandler.existESIndex(esIndexName)) {
            esHandler.createESIndex(esIndexName, properties);
        }
    }

    /*
     * TextIndexFeeder worker thread to dequeue each entry from queue, and
     * post the entry to TransactionAgenda if it is a data operation. If the
     * entry is an commit/abort operation, the worker will close the transaction
     * in agenda by committing it to ES or abort it.
     */
    private class TextIndexFeederWorker extends Thread {

        private final String workerId;
        private final BlockingQueue<DataItem> queue;
        private volatile boolean cancelled;

        TextIndexFeederWorker(String workerId,
                              BlockingQueue<DataItem> queue) {
            this.workerId = workerId;
            this.queue = queue;
            cancelled = false;
            /* Prevent its hanging the process on exit. */
            setDaemon(true);
        }

        @Override
        public void run() {

            while (!cancelled) {
                DataItem entry = null;
                /* TIF worker should keep running until it is cancelled */
                try {
                    entry = queue.take();
                    if (!entry.getPartitionId().equals(PartitionId.NULL_ID)) {
                        /* has a valid part. id, meaning it is from part.
                         * transfer
                         */
                        handleDataItemHelper(entry, txnAgendaForPartTransfer);
                    } else {
                        /* no part id, meaning it is from rep stream */
                        handleDataItemHelper(entry, txnAgendaForRepStream);
                    }

                } catch (InterruptedException e) {
                    logger.log(Level.WARNING,
                               lm("TextIndexFeeder [TextIndexFeederWorker]: " +
                                  "TIF worker " + workerId +
                                  " - Interrupted input queue take " +
                                  "operation, " +
                                  "retrying. Reason: {0}."), e.getMessage());
                } catch (Exception exp) {
                    /*
                     * tif worker should not crash when it is unable to process
                     * an entry, instead, it should log the entry and continue.
                     */
                    logger.log(Level.WARNING,
                               lm("TextIndexFeeder [TextIndexFeederWorker]: " +
                                  "TIF worker '" + workerId +
                                  "' failed to process entry. \nReason: " +
                                  exp.getMessage() +
                                  ",\nentry = " +
                                  ((entry == null) ?
                                  "null\n" : entry.toString()) + "Associated" +
                                  " exception, message, stacktrace: "),
                                  exp);
                }
            }
        }

        public void cancel() {
            cancelled = true;
        }

        public String getWorkerId() {
            return workerId;
        }

        /* process each item in the input queue */
        private void handleDataItemHelper(DataItem entry,
                                          TransactionAgenda agenda) {

            try {
                /* The semaphore dataItemConsumptionPermits is initialized with
                 * a number of permits equal to the number of worker threads
                 * that might pass through here.  During normal operation,
                 * the threads to not interfere with one another.  This path
                 * is suspended for all worker threads by acquiring all of
                 * the permits, as is done in setTableMetadata.
                 */
                dataItemConsumptionPermits.acquireUninterruptibly();

                final byte[] keyBytes = entry.getKey();
                byte[] valueBytes = entry.getValue();

                valueBytes = (valueBytes == null) ? emptyBytes : valueBytes;

                /* abort or commit a txn to ES */
                if (entry.isTxnAbort()) {
                    agenda.abort(entry.getTxnID());
                    return;
                }
                if (entry.isTxnCommit()) {
                    agenda.commit(entry.getTxnID(), entry.getVLSN());
                    return;
                }

                /* must be a put or del operation with a valid key */
                assert (keyBytes != null);
                /* check each text indices and convert entry to an index op */
                for (Index ti : tableMetadata.getTextIndexes()) {
                    final String indexName = ti.getName();
                    final String tableName = ti.getTable().getFullName();
                    final String namespace = ((TableImpl) ti.getTable())
                        .getInternalNamespace();
                    final String esIndexName =
                        deriveESIndexName(storeName, namespace, tableName,
                                          indexName);
                    final IndexImpl tiImpl = (IndexImpl) ti;

                    /*
                     * deserialize a key/value pair to a RowImpl iff the key is
                     * interesting to the text index. Null RowImpl indicates
                     * key is not interesting to the text index and can be
                     * ignored.
                     */
                    final RowImpl row =
                        tiImpl.deserializeRow(keyBytes, valueBytes);
                    if (row != null) {
                        /* this entry is interesting to the text index */
                        IndexOperation op;
                        if (entry.isDelete()) {
                            op = ElasticsearchHandler
                                .makeDeleteOperation(tiImpl, esIndexName,
                                                     row);
                        } else {
                            op = ElasticsearchHandler
                                .makePutOperation(tiImpl, esIndexName,
                                                  row, logger);
                        }

                        if (op == null) {
                            continue;
                        }
                        /*
                         * if entry is a COPY op from partition transfer,
                         * directly commit to ES
                         */
                        if (entry.isCopyInPartTrans()) {
                            agenda.commit(op);
                        } else {
                            /* post op to agenda */
                            agenda.addOp(entry.getTxnID(), op);
                        }
                    }
                }
            } finally {
                dataItemConsumptionPermits.release();
            }
        }
    }

    /*
     * Preprocess a log message string to include a tag identifying the log
     * message as coming from the TIF. TODO: future grand redesign of the
     * logging system will no doubt override this localized band-aid...
     */
    private String lm(String s) {
        return "[tif] " + s;
    }
}
