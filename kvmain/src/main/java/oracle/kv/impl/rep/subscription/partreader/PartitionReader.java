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

package oracle.kv.impl.rep.subscription.partreader;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.InternalException;

import oracle.kv.impl.rep.migration.MigrationService;
import oracle.kv.impl.rep.migration.TransferProtocol;
import oracle.kv.impl.topo.PartitionId;

public class PartitionReader implements Runnable {

    /* Service used to transfer the partition from source */
    private static final int MAX_RETRY = 3;
    private static final int RETRY_SLEEP_MS = 3000;

    private final ReplicatedEnvironment repEnv;
    private final Logger logger;
    private final PartitionId partitionId;
    private final PartitionReaderCallBack callBack;
    private final SubscriptionConfig config;

    private volatile Thread executingThread = null;
    private DataChannel channel;
    private DataInputStream stream;
    private long highestVLSN;
    private int numRetry;

    /* true if reader is canceled */
    private volatile boolean canceled = false;
    /* reader status */
    private PartitionReaderStatus status;

    public PartitionReader(ReplicatedEnvironment repEnv,
                           PartitionId partitionId,
                           PartitionReaderCallBack callBack,
                           SubscriptionConfig config,
                           Logger logger) {
        this.repEnv = repEnv;
        this.partitionId = partitionId;
        this.callBack = callBack;
        this.logger = logger;

        stream = null;
        channel = null;
        highestVLSN = NULL_VLSN;
        this.config = config;
        numRetry = 0;
        status = new PartitionReaderStatus(partitionId);
    }

    /**
     * Returns true if the replicator is alive
     *
     * @return true replicator thread is running
     */
    public boolean isAlive() {
        return (executingThread != null);
    }

    /**
     * Gets reader status
     *
     * @return status of partition reader
     */
    public PartitionReaderStatus getStatus() {
        return status;
    }

    /**
     * Starts partition reader thread to consume replication stream
     * from Partition Migration service at source RN.
     */
    @Override
    public void run() throws InternalException {

        status.setState(PartitionReaderStatus.PartitionRepState.REPLICATING);

        try {
            executingThread = Thread.currentThread();
            status.setStartTime();


            /* request source to transfer partition, retry if source is busy */
            while (!initialize()) {
                numRetry++;
                if (numRetry > MAX_RETRY) {
                    String msg = "For partition " + partitionId +
                                 ", unable to connect to source (" +
                                 config.getFeederHostAddr() + ":" +
                                 config.getFeederPort() + ") after " +
                                 numRetry + " connections.";

                    logger.log(Level.INFO, msg);
                    throw new InternalException(msg);
                }
                channel.close();
                Thread.sleep(RETRY_SLEEP_MS);
                logger.log(Level.INFO,
                           "reader waken up after sleeping {0} ms.",
                           new Object[]{RETRY_SLEEP_MS});

            }

            logger.log(Level.INFO,
                       "Ready to transfer {0}, connection established to " +
                       "source {1} at address {2}:{3}",
                       new Object[]{partitionId,
                           config.getFeederHost(),
                           config.getFeederHostAddr(),
                           config.getFeederPort()});

            /* process each entry received from source */
            processStream();
        } catch (InternalException | InterruptedException | IOException |
            ServiceDispatcher.ServiceConnectFailedException e) {
            status.incrErrors();
            logAndThrow(e);
        } finally {
            status.setEndTime();
            executingThread = null;
        }
    }

    /**
     * Gets number of retries reader try to connect to PMS
     *
     * @return number of retries
     */
    public int getNumRetry() {
        return numRetry;
    }

    /**
     * Gets the highest VLSN seen in transfer
     *
     * @return highest VLSN seen in transfer
     */
    public long getHighestVLSN() {
        return highestVLSN;
    }

    /**
     * Shuts down current partition reader
     */
    public void shutdown() {
        cancel(true);

        String msg = status.toString();
        logger.log(Level.INFO, msg);
    }

    /**
     * Cancels a running partition reader
     *
     * @param wait true if wait till thread exits
     */
    public void cancel(boolean wait) {

        canceled = true;
        if (!wait) {
            return;
        }

        final Thread thread = executingThread;
        /* Wait if there is a thread and is is running */
        if ((thread != null) && thread.isAlive()) {
            try {
                logger.log(Level.FINE,
                           "Waiting for partition reader for {0} to exit",
                           new Object[]{partitionId});
                thread.join(5000);

                if (isAlive()) {
                    logger.log(Level.FINE,
                               "Cancel of partition reader for {0} timed out",
                               new Object[]{partitionId});
                }
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    /**
     * Consumes replication stream and extract data entries out of
     * the stream. The entry will processed by callback.
     *
     * @throws IOException
     */
    private void processStream() throws IOException {

        boolean done = false;


        logger.log(Level.INFO, "Start streaming for partition {0}",
                   new Object[]{partitionId});

        while (!canceled && !done) {
            long vlsn = NULL_VLSN;
            long txnId;
            long expirationTime;
            byte[] key;
            byte[] value;

            TransferProtocol.OP op = TransferProtocol.OP.get(stream.readByte());
            int bytesRead = 1;

            if (op == null) {
                status.incrErrors();
                throw new IOException("Bad op, or unexpected EOF");
            }
            status.incrOpsRead();

            switch (op) {
                case COPY:
                    key = readDbEntry();
                    value = readDbEntry();
                    expirationTime = readExpirationTime();
                    vlsn = readVLSN();

                    if (isOpFiltered(vlsn, key)) {
                        status.incrNumFilteredOps();
                    } else {
                        callBack.processCopy(partitionId, vlsn,
                                             expirationTime,
                                             key, value);
                    }

                    bytesRead += key.length + value.length + 8;
                    break;

                case PUT:
                    txnId = readTxnId();
                    key = readDbEntry();
                    value = readDbEntry();
                    expirationTime = readExpirationTime();
                    vlsn = readVLSN();

                    if (isOpFiltered(vlsn, key)) {
                        status.incrNumFilteredOps();
                    } else {
                        callBack.processPut(partitionId, vlsn,
                                            expirationTime,
                                            key, value,
                                            txnId);
                    }

                    bytesRead += 8 + key.length + value.length + 8;
                    break;

                case DELETE:
                    txnId = readTxnId();
                    key = readDbEntry();
                    vlsn = readVLSN();
                    if (isOpFiltered(vlsn, key)) {
                        status.incrNumFilteredOps();
                    } else {
                        callBack.processDel(partitionId, vlsn, key, txnId);
                    }

                    bytesRead += 8 + key.length + 8;
                    break;

                case PREPARE:
                    txnId = readTxnId();
                    callBack.processPrepare(partitionId, txnId);
                    bytesRead += 8;
                    break;

                case ABORT:
                    txnId = readTxnId();
                    callBack.processAbort(partitionId, txnId);
                    bytesRead += 8;
                    status.incrAbortedTXNs();
                    break;

                case COMMIT : {
                    txnId = readTxnId();
                    callBack.processCommit(partitionId, txnId);
                    bytesRead += 8;
                    status.incrCommittedTXNs();
                    break;
                }
                case LAST_RECORD_MARKER : {
                    logger.log(Level.INFO,
                               "Received last record marker for {0}",
                               new Object[]{partitionId});
                    break;
                }
                case EOD : {
                    done = true;
                    logger.log(Level.INFO, "Receive EOD for {0}",
                               new Object[]{partitionId});
                    callBack.processEOD(partitionId);
                    break;
                }
                default:
                    String err = "Receive invalid operation " + op.ordinal() +
                                 " for partition " + partitionId +
                                 " from RN " + config.getFeederHost();
                    status.incrErrors();
                    status.setState(PartitionReaderStatus.PartitionRepState
                                        .ERROR);
                    throw new IOException(err);
            }
            status.incrBytesRead(bytesRead);
            highestVLSN = (vlsn > highestVLSN) ? vlsn : highestVLSN;
        }

        if (canceled) {
            status.setState(PartitionReaderStatus.PartitionRepState.CANCELLED);
        } else if (done) {
            status.setState(PartitionReaderStatus.PartitionRepState.DONE);
        }
    }

    /**
     * Establishes a channel with the partition source and creates an
     * input stream.
     *
     * @throws IOException
     * @throws ServiceDispatcher.ServiceConnectFailedException
     */
    private synchronized void openChannel()
        throws IOException, ServiceDispatcher.ServiceConnectFailedException {

        RepImpl repImpl = RepInternal.getRepImpl(repEnv);

        if (repImpl == null) {
            String msg = "Replication env not available.";
            throw new IllegalStateException(msg);
        }

        final InetAddress feederHostAddr = config.getFeederHostAddr();
        final int feederHostPort = config.getFeederPort();
        final InetSocketAddress sourceAddress =
            new InetSocketAddress(feederHostAddr, feederHostPort);

        DataChannelFactory.ConnectOptions connectOpts =
            new DataChannelFactory.ConnectOptions();
        connectOpts.setTcpNoDelay(config.TCP_NO_DELAY)
                   .setReceiveBufferSize(config.getReceiveBufferSize())
                   .setOpenTimeout((int) config
                       .getStreamOpenTimeout(TimeUnit.MILLISECONDS));

        channel = RepUtils.openBlockingChannel(
            sourceAddress, repImpl.getChannelFactory(), connectOpts);
        ServiceDispatcher.doServiceHandshake(channel,
                                             MigrationService.SERVICE_NAME);

        stream = new DataInputStream(Channels.newInputStream(channel));

        logger.log(Level.INFO,
                   "Channel opened to source at {0} to send a " +
                   "partition transfer request for {1}",
                   new Object[]{sourceAddress, partitionId});

    }


    /**
     * Sends a partition transfer request to Partition Migration Service at
     * source node and handle response from source.
     *
     * @return false if source is busy and caller may wait and retry later,
     *         true if request is sent successfully and we get OK from source.
     *
     * @throws IOException
     * @throws InternalException
     */
    private synchronized boolean sendRequest()
            throws IOException, InternalException {

        RepImpl repImpl = RepInternal.getRepImpl(repEnv);
        if (repImpl == null) {
            throw new InternalException("Replication env not available.");
        }

        /* send a request to source node PMS */
        TransferProtocol.TransferRequest.write(channel,
                                               partitionId.getPartitionId());
        final ServiceDispatcher.Response response =
            TransferProtocol.TransferRequest.readResponse(stream);
        switch (response) {
            case OK:
                logger.log(Level.INFO,
                           "OK from source, start transfer {0}",
                           new Object[]{partitionId});
                return true;

            case BUSY:
                long limit =
                    TransferProtocol.TransferRequest.readNumStreams(stream);

                logger.log(Level.INFO,
                           "Source busy by reaching maximum {0} partition " +
                           "streams, for reader of {1}",
                           new Object[]{limit, partitionId});
                return false;

            case UNKNOWN_SERVICE:
            case FORMAT_ERROR:
            case INVALID:
                String reason = "Unexpected response from source, reason is " +
                                TransferProtocol.TransferRequest
                                    .readReason(stream);
                logger.log(Level.WARNING, reason);
                throw new InternalException(reason);

            case AUTHENTICATE:
            case PROCEED:
                String err = "Error response from source " +
                             response.toString();
                logger.log(Level.WARNING, err);
                throw new InternalException(err);
            default:
                throw new InternalException("Invalid response " + response);
        }
    }

    /**
     * Initialize connection to source. Open a channel and send transfer request
     * to source. If source returns OK, it returns true to caller. If source
     * is busy, it returns false.
     *
     *
     * @return  true if successful initialization; false if source is busy
     *
     * @throws IOException
     * @throws ServiceDispatcher.ServiceConnectFailedException
     */
    private boolean initialize()
        throws IOException, ServiceDispatcher.ServiceConnectFailedException {

        /* create channel to source */
        openChannel();

        /* request source to transfer partition, retry if source is busy */
        if (sendRequest()) {
            return true;
        }

        channel.close();
        return false;
    }

    /**
     * Checks if data entry will be filtered
     *
     * @param vlsn vlsn of operation
     * @param key  key of operation
     *
     * @return true if this entry will be filtered
     */
    private boolean isOpFiltered(long vlsn, byte[] key) {
        /*
         * filter item in which VLSN = 0, meaning it is a deletion of
         * a deleted or non-existent key. or, filter any entry without
         * a valid key
         */
        return (vlsn == NULL_VLSN) || (key == null);
    }

    /**
     * Reads a transaction ID from the stream.
     */
    private long readTxnId() throws IOException {
        return stream.readLong();
    }

    /**
     * Reads a VLSN from the stream.
     */
    private long readVLSN() throws IOException {
        return stream.readLong();
    }

    /**
     * Reads expiration time from the stream.
     */
    private long readExpirationTime() throws IOException {
        return stream.readLong();
    }

    /**
     * Reads a DB entry (as a byte array) from the migration stream.
     */
    private byte[] readDbEntry() throws IOException {
        int size = stream.readInt();
        byte[] bytes = new byte[size];
        stream.readFully(bytes);
        return bytes;
    }

    /**
     * Handle exception in when receiving data from source. Log the
     * exception and throw an InternalException created from that.
     */
    private void logAndThrow(Exception e)
        throws InternalException {

        logger.log(Level.INFO, e.toString());
        throw new InternalException(e.toString());
    }

    @Override
    public String toString() {
        return "PartitionReader for partition [" + partitionId + "], " +
               "from RN: " + config.getFeederHost() + ", status: " +
               status.getState().toString();
    }
}
