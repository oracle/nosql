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
package com.sleepycat.je.rep.subscription;

import static com.sleepycat.je.log.LogEntryType.LOG_TXN_ABORT;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.InputWireRecord;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;


/**
 * Object to represent the thread created by Subscription to process messages
 * received from feeder.
 */
public class SubscriptionProcessMessageThread extends StoppableThread {

    /* wait time in ms in soft shut down */
    private final static int SOFT_SHUTDOWN_WAIT_MS = 3 * 1000;

    /* handle to stats */
    private final SubscriptionStat stats;
    /* configuration */
    private final SubscriptionConfig config;
    /* input queue from which to consume messages */
    private final BlockingQueue<Object> queue;
    /* logger */
    private final Logger logger;

    /* exit flag to specify exit type */
    private volatile ExitType exitRequest;

    /** replicated env */
    private final RepImpl repImpl;
    
    private static TestHook<InputWireRecord> verifyHook;


    /**
     * Construct a subscription thread to process messages
     *
     * @param repImpl RepImpl of the RN where thread is running
     * @param queue   Input queue from which to consume messages
     * @param config  Subscription configuration
     * @param stats   statistics handle
     */
    SubscriptionProcessMessageThread(RepImpl repImpl,
                                     BlockingQueue<Object> queue,
                                     SubscriptionConfig config,
                                     SubscriptionStat stats) {
        super(repImpl, "SubscriptionProcessMessageThread-" +
                       config.getSubNodeName());
        this.repImpl = repImpl;
        this.config = config;
        this.queue = queue;
        this.stats = stats;

        logger = repImpl.getLogger();
        exitRequest = ExitType.NONE;
        stats.setHighVLSN(NULL_VLSN);
    }

    /**
     * Implement a soft shutdown.
     *
     * @return the amount of time in ms that the shutdownThread method will
     * wait for the thread to exit.
     */
    @Override
    public int initiateSoftShutdown() {
        exitRequest = ExitType.IMMEDIATE;
        /*
         * This thread will poll entry from queue and apply subscription
         * callback to process each entry. For some client like Stream, the
         * callback would enqueue the entry into another queue for next level of
         * processing, which may block and we have no idea when the callback
         * will return, hence, we set a max wait time in soft shutdown to
         * avoid waiting too long.
         */
        return SOFT_SHUTDOWN_WAIT_MS;
    }
    
    public static void setInputWireRecordHook(TestHook<InputWireRecord> hook) {
    	verifyHook = hook;
    }


    /**
     * Implement thread run() method. Dequeue message from the queue and
     * process it via the callback.
     *
     */
    @Override
    public void run() {

        /* callback provided by client to process each message in input queue */
        final SubscriptionCallback callBack = config.getCallBack();

        LoggerUtils.info(logger, repImpl,
                         lm("Input thread started. Message queue size:" +
                            queue.remainingCapacity()));

        /* loop to process each message in the queue */
        try {
            while (exitRequest != ExitType.IMMEDIATE) {

                /* fetch next message from queue */
                final Object message =
                    queue.poll(SubscriptionConfig.QUEUE_POLL_INTERVAL_MS,
                               TimeUnit.MILLISECONDS);

                if (message == null) {
                    /*
                     * No message to consume, continue and wait for the
                     * next message.
                     */
                    continue;
                } else if (message instanceof Exception) {

                    callBack.processException((Exception) message);

                    /* exits if shutdown message from feeder */
                    if (message instanceof GroupShutdownException) {
                        exitRequest = ExitType.IMMEDIATE;
                        GroupShutdownException gse =
                            (GroupShutdownException) message;
                        LoggerUtils.info(
                            logger, repImpl,
                            lm("Received shutdown message from " +
                               config.getFeederHost() + " at VLSN " +
                               gse.getShutdownVLSN()));
                        break;
                    }
                } else {
                    
					/* use different callbacks depending on entry type */
					final InputWireRecord wireRecord = ((Protocol.Entry) message)
							.getWireRecord();

					TestHookExecute.doHookIfSet(verifyHook, wireRecord);

					final long vlsn = wireRecord.getVLSN();

					final byte type = wireRecord.getEntryType();
					final LogEntry entry = wireRecord.getLogEntry();
					final long txnId = entry.getTransactionId();

					byte[] beforeImgVal = null;
					long beforeImgTs = 0L;
					long beforeImgExp = 0L;
					boolean beforeImageEnabled = wireRecord
							.enabledBeforeImage();
					if (beforeImageEnabled) {
						beforeImgVal = wireRecord.getBeforeImageData();
						beforeImgTs = wireRecord
								.getBeforeImageModificationTime();
						beforeImgExp = wireRecord.getBeforeImageExpiration();
					}

					stats.setHighVLSN(vlsn);
					stats.getNumOpsProcessed().increment();

					/* call different proc depending on entry type */
					if (LOG_TXN_COMMIT.equalsType(type)) {
						stats.getNumTxnCommitted().increment();
						callBack.processCommit(vlsn, txnId);
						continue;
					}

					if (LOG_TXN_ABORT.equalsType(type)) {
						stats.getNumTxnAborted().increment();
						callBack.processAbort(vlsn, txnId);
						continue;
					}

					if (entry instanceof LNLogEntry) {
						/* receive a LNLogEntry from Feeder */
						final LNLogEntry<?> lnEntry = (LNLogEntry<?>) entry;
						/*
						 * We have to call postFetchInit to avoid EFE. The
						 * function will reformat the key/data if entry is from
						 * a dup DB. The default feeder filter would filter out
						 * all dup db entries for us.
						 *
						 * TODO: Note today we temporarily disabled user-defined
						 * feeder filter and thus users are unable to replace
						 * the default feeder filter with their own. So here it
						 * is safe to assume no dup db entry.
						 *
						 * We will have to address the dup db entry issue in
						 * future to make the Subscription API public, in which
						 * users will be allowed to use their own feeder filter.
						 */
						lnEntry.postFetchInit(false);
						if (lnEntry.getLN().isDeleted()) {
							/* regular delete */
							callBack.processDel(vlsn, lnEntry.getKey(),
									null/* no tombstone */, txnId,
									lnEntry.getDbId(),
									lnEntry.getModificationTime(),
									beforeImageEnabled, beforeImgVal,
									beforeImgTs, beforeImgExp);

						} else if (lnEntry.isTombstone()) {
							/* convert put representing tombstone to delete */
							callBack.processDel(vlsn, lnEntry.getKey(),
									lnEntry.getData(), txnId, lnEntry.getDbId(),
									lnEntry.getModificationTime(),
									beforeImageEnabled, beforeImgVal,
									beforeImgTs, beforeImgExp);
						} else {
							/* regular put */
							callBack.processPut(vlsn, lnEntry.getKey(),
									lnEntry.getData(), txnId, lnEntry.getDbId(),
									lnEntry.getModificationTime(),
									lnEntry.getExpirationTime(),
									beforeImageEnabled, beforeImgVal,
									beforeImgTs, beforeImgExp);
						}
					}
                }
            }
        } catch (InterruptedException exp) {
            LoggerUtils.warning(logger, repImpl,
                                lm("interrupted: " + exp.getMessage()));
            exitRequest = ExitType.IMMEDIATE;
        } catch (RuntimeException | Error exp) {
            LoggerUtils.warning(logger, repImpl,
                                lm("input thread exception: " +
                                   exp.getMessage() + "\n" +
                                   LoggerUtils.getStackTrace(exp)));
            exitRequest = ExitType.IMMEDIATE;
        } finally {
            queue.clear();
            LoggerUtils.info(
                logger, repImpl,
                lm("message queue cleared, thread exits with type: " +
                   exitRequest));
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    /* types of exits  */
    private enum ExitType {
        NONE,      /* No exit requested */
        IMMEDIATE  /* An immediate exit; ignore queued requests. */
    }

    private String lm(String msg) {
        return "[SubscriptionProcessMessageThread-" +
               config.getSubNodeName() + "] " + msg;
    }
}
