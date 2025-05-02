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

/**
 * Simulate KVS network traffic patterns. It simulates both client to store
 * traffic and JE HA (RN to RN, intra-shard) traffic.
 *
 * The simulation is organized as a single client simulation process and
 * multiple simulated RN processes. At the start of the simulation, the client
 * groups the RNs into shards based on the replication factor;a RN in each shard
 * is designated to serve as the master, with the other RNs in the shard serving
 * as replicas.
 *
 * To simulate an insert, the client sends a request to master, which then
 * sends the request to its replicas. The replicas ack all received requests.
 * The master upon receiving a quorum of acks returns an ack to
 * the client.
 *
 * The simulated client/master/replica processes are all implemented by NetSim.
 * The client versus RN behavior is determined by the arguments. Within an RN
 * the role of master or replica is determined via the initialization message,
 * which contains the RNConfig as its payload, sent by the client.
 *
 * TODO:
 *
 * 1) Positional -> keyword command line arguments.
 * 2) Make replay more async, that is, make ack handling async. More below.
 * 3) Measure async ack latency explicitly.
 * 4) Don't shutdown RNs on EOF and make client reuse existing RNs.
 * 5) Allow for additional network connection config, e.g. buffer sizes.
 * 6) Allow multiple client processes
 * 7) Check for distribution of shards across processes
 * 8) Configure other network connection properties used by JE/KVS, e.g. network
 * buffer sizes.
 * 9) Simulate read/mixed loads which could go to all RNs?
 * 10) Async client (eliminate requestExecutor) to get out of request/thread
 * model and permit higher load generation. Will also be a better fit when
 * KVS async APIs are in place.
 */

package oracle.kv.util.netperfsim;

import oracle.kv.util.NetSimUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Usage instructions:
 *
 * {@literal
 * 1) Start up the simulated RNS:
 *      pdsh -w n1[1-3] 'cd sim; java -cp . NetSim `hostname`:6000 2>&1 > /tmp/sim.txt &'
 *
 * 2) Start up the client:
 *      java -cp . NetSim 3 10 1000 nshgb08:6000 nshgb09:6000 nshgb10:6000
 *
 * If capacity > 1 then arrange for "shard" RNs to be on different machines
 * to correctly simulate inter-machine network traffic.
 *
 * 3) ctrl-c the driver to stop it
 *
 * 4) Kill the simulated RNS:
 *      pdsh -w n1[1-3] 'kill -9 `pgrep -f NetSim`'
 * }
 */
public class NetSim {

     public static final String RF_FLAG                = "-rf";

     public static final String THREADS_PER_SHARD_FLAG = "-threads-per-shard";

     public static final String VALUE_BYTES_FLAG       = "-value-bytes";

     public static final String RN_HOSTPORT_FLAG       = "-rnHostPort";

     public static final String MODE_FLAG              = "-mode";

     private static final String SERVER                = "server";

     public static final String TCP_PARAMS_FLAG        = "-tcp_params";

    /**
     * The size of the value payload in bytes.
     */
    private static int valueBytes;

    /**
     * The replication factor.
     */
    private static int rf;

    /**
     * The number of threads concurrently inserting into the same shard.
     */
    private static int threadsPerShard = 1;

    /**
     * The hostname and port number needed to be used for the clients.
     */
    private static String rnHostPort;

    /**
     * Mode: "server" or "client".
     */
    private static String mode;

    /**
     * Network configuration parameters. Currently we are allowing
     * following network config parameters.
     * 1.) sendBufferSize
     * 2.) reuseAddress
     * 3.) receiveBufferSize
     * 4.) tcpNoDelay
     *
     * Usage : -tcp_params "sendBufferSize=value1,reuseAddress=value2,..."
     *
     * TODO : Need to add more parameters as per further requirements
     */
    private static String tcp_params;

    /**
     * Map for storing individual network configuration parameter
     */
    private static Map<String,String> tcp_params_value =
            new HashMap<String, String>();

    /**
     * List of the currently used network config parameters
     */
    private final static List<String> list_params =
            Arrays.asList("sendBufferSize", "reuseAddress",
                    "receiveBufferSize", "tcpNoDelay");

    /**
     * The addresses associated with all the RNs.
     */
    private static ArrayList<InetSocketAddress> rns =
            new ArrayList<InetSocketAddress>();

    /**
     * The threads used to issue requests. There is at least on thread per
     * shard, but there could be more depending on the degree of parallelism.
     */
    private static ExecutorService requestExecutor;

    /* Set to true in order to stop the simulation. */
    private static volatile boolean shutDown = false;

    public static void main(String[] args)
        throws IOException, InterruptedException {

        int argc = 0;
        int nArgs = args.length;

        while (argc < nArgs) {
            String thisArg = args[argc++];
            if (thisArg == null) {
                continue;
            }
            if (thisArg.equals(RF_FLAG)) {
                if (argc < nArgs) {
                    rf = Integer.parseInt(args[argc++]);
                } else {
                    usage();
                }
            } else if (thisArg.equals(THREADS_PER_SHARD_FLAG)) {
                if (argc < nArgs) {
                  threadsPerShard = Integer.parseInt(args[argc++]);
                } else {
                    usage();
                }
            } else if (thisArg.equals(VALUE_BYTES_FLAG)) {
                if (argc < nArgs) {
                    valueBytes = Integer.parseInt(args[argc++]);
                } else {
                    usage();
                }
            } else if (thisArg.equals(MODE_FLAG)) {
                if (argc < nArgs) {
                    mode = args[argc++];
                } else {
                    usage();
                }
            } else if (thisArg.equals(RN_HOSTPORT_FLAG)) {
                if (argc < nArgs) {
                    rnHostPort = (args[argc++]);
                } else {
                    usage();
                }
            } else if (thisArg.equals(TCP_PARAMS_FLAG)) {
                if (argc < nArgs) {
                    tcp_params = (args[argc++]);
                    tcp_params_value = getTcpParams(tcp_params);
                } else {
                    usage();
                }
            }
        }

        if (mode != null && mode.equalsIgnoreCase(SERVER)) {
            /* An RN simUlation. */
            new RN(rnHostPort).start();
            return;
        }

        /*
        Signal.handle(new Signal("INT"), new SignalHandler () {

            @Override
            public void handle(Signal sig) {
                System.err.println("Driver shutdown signal:" + sig);
                shutdownRequests();
                System.exit(0);
            }
          });*/

      Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
              System.err.println("Driver shutdown initiated");
              shutdownRequests();
          }
      });

        String[] hostPort = rnHostPort.split(" ");
        for(int i=0;i<hostPort.length;i++)
        {
            rns.add(createISA(hostPort[i]));
        }

        if ((rns.size() % rf) != 0) {

            String fmt = "Number of rns: %d, must be a multiple of rf: %d.";
            throw new IllegalArgumentException(String.format(fmt, rns.size(), rf));
        }

        /* First start up the replicas. */
        startReplicas();
        startMasters();

        startRequests();
    }

    private static void usage()
        throws IllegalArgumentException {

        System.err.println("Usage: ...NetSim " +
                           RF_FLAG + " <rf> " +
                           THREADS_PER_SHARD_FLAG + " <threads-per-shard> " +
                           VALUE_BYTES_FLAG + " <value-bytes>" +
                           RN_HOSTPORT_FLAG + " <rnHostPort>" +
                           MODE_FLAG + " <mode>" +
                           TCP_PARAMS_FLAG + " <tcp_params>");
        throw new IllegalArgumentException("Could not parse NetSim args");
    }

    private static void tcp_params_usage()
        throws IllegalArgumentException {

        System.err.println("Usage : Please pass unique network " +
                           "configuration parameters");
        throw new IllegalArgumentException
        ("Could not parse tcp specific network config args");
    }

    private static Map<String,String> getTcpParams(String tcp_parameters) {

    	Map<String, String> paramsAsMap = new HashMap<String, String>();
        String[] paramSet = tcp_parameters.split(",");
        for (String ps:paramSet) {
	        String[] eachParam = ps.split("=");
	        if (!eachParam[1].equals("") &&
                !eachParam[1].equals("NULL") &&
                list_params.contains(eachParam[0])) {
                if (!paramsAsMap.containsKey(eachParam[0])) {
                    paramsAsMap.put(eachParam[0], eachParam[1]);
                } else {
                    tcp_params_usage();
                    }
                }
	        }
        return paramsAsMap;
    }

    /**
     * Generates client side requests. Requests are generated by the RequestTask.
     * Each RequestTask effectively simulates an application server thread
     * that would be used to access a KVS.
     */
    private static void startRequests() throws InterruptedException {

        requestExecutor =
            Executors.newFixedThreadPool((rns.size() / rf) * threadsPerShard,
                                         new RequestThreadFactory());

        for (int i=0; i < rns.size(); i+= rf) {
            for (int t=0; t < threadsPerShard; t++) {
                requestExecutor.execute(new RequestTask(rns.get(i)));
            }
        }

       requestExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    }

    /**
     * Shuts down the client's generation of requests.
     */
    private static void shutdownRequests() {
        shutDown = true;
        try {
            if (requestExecutor != null) {
                requestExecutor.shutdown();
                if (!requestExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    requestExecutor.shutdownNow();
                }
            }
        } catch (InterruptedException e) {

        }
    }

    private static InetSocketAddress createISA(String arg)
        throws NumberFormatException {
        final String hostPort[] = arg.split(":");
        final InetSocketAddress isa = new InetSocketAddress(hostPort[0],
                                       Integer.valueOf(hostPort[1]));
        return isa;
    }


    /**
     * Designates each RN on a non-RF boundary (based on its position in the
     * rns list) to simulate Replica traffic and sends it the initial
     * configuration message with an RNConfig object indicating it should
     * serve as a Replica.
     */
    private static void startReplicas()
        throws IOException, SocketException {

        final RNConfig replicaConfigure = new RNConfig(false, null);

        for (int i=0; i < rns.size(); i++) {
            if (i % rf == 0) {
                /* A master */
                continue;
            }

            System.err.println("Starting replica at:" + rns.get(i));

            try (final SocketChannel sc = openConfiguredChannel(rns.get(i))) {
                int bytes = write(sc, replicaConfigure);
                readAck(sc, bytes);
            }
        }
    }

    /**
     * Designates each RN on a RF boundary (based on its position in the
     * rns list) to simulate Master traffic and sends it the initial
     * configuration message with an RNConfig object indicating it should
     * serve as a Master.
     */
    private static void startMasters() throws IOException {
        for (int i=0; i < rns.size(); i +=rf) {
            System.err.println("Starting master at:" + rns.get(i));
            try (final SocketChannel sc = openConfiguredChannel(rns.get(i))) {

                List<InetSocketAddress> replicas =
                    new LinkedList<InetSocketAddress>(rns.subList(i+1, i+rf));

                RNConfig mconfig = new RNConfig(true, replicas);

                int bytes = write(sc, mconfig);
                readAck(sc, bytes);
            }
        }
    }

    /**
     * Opens and configures a socket channel.
     */
    static SocketChannel openConfiguredChannel(InetSocketAddress isa)
        throws IOException {

        SocketChannel sc = SocketChannel.open(isa);
        sc.configureBlocking(true);
        sc = NetSimUtil.setTcpParams(sc, tcp_params_value);
        return sc;
    }

    /**
     * Writes the object using the supplied channel.
     */
    static int write(SocketChannel sc, Object o) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        final ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

        return writeMessage(sc, bb);
    }

    /**
     * Reads the object using the supplied channel, blocking if necessary.
     */
    static Object readObject(SocketChannel sc)
        throws IOException {

        final ByteBuffer message = readMessage(sc);

        writeAck(sc, message.remaining());

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(message.array());

        try (final ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes an acknowledgement, which is simply a number, that is expected by
     * the sender of the message and can be verified by it.
     */
    private static void writeAck(SocketChannel sc,
                                 long ackValue) throws IOException {
        writeLong(sc, ackValue);
    }

    /**
     * Reads an acknowledgement and verifies it.
     */
    private static void readAck(SocketChannel sc,
                                long expected)
        throws IOException {
        final long ackBytes = readLong(sc);
        if (ackBytes != expected) {
            String fmt = "expected ack value:%d actual:%d";
            throw new IllegalStateException(String.format(fmt, expected, ackBytes));
        }
    }

    /**
     * Low level method to write bytes to the channel.
     */
    static int writeMessage(SocketChannel sc, ByteBuffer bb) throws IOException {

        bb.rewind();
        final int messageBytes = bb.remaining();

        writeLong(sc, messageBytes);

        while (bb.remaining() > 0) {
            sc.write(bb);
        }

        return messageBytes;
    }

    /**
     * Low level method to read bytes from the channel.
     */
    static ByteBuffer readMessage(SocketChannel sc) throws IOException {
        final long messageBytes = readLong(sc);

        final ByteBuffer message =  ByteBuffer.allocate((int)messageBytes);

        while (message.remaining() > 0) {
            if (sc.read(message) < 0) {
                throw new IOException("Premature EOF");
            }
        }

        message.rewind();

        return message;
    }

    /**
     * Convenience methods to read/write long values.
     */
    static void writeLong(SocketChannel sc, long n) throws IOException {
        final ByteBuffer longBytes = ByteBuffer.allocate(8).putLong(n);
        longBytes.rewind();

        while (longBytes.remaining() > 0) {
            sc.write(longBytes);
        }
    }

    private static long readLong(SocketChannel sc) throws IOException {
        final ByteBuffer longBytes = ByteBuffer.allocate(8);

        while (longBytes.remaining() > 0) {
            if (sc.read(longBytes) < 0) {
                throw new IOException("Premature EOF");
            }
        }

        longBytes.rewind();
        final long ackBytes = longBytes.getLong();
        return ackBytes;
    }

    /**
     * Used during initialization to inform an RN process whether it should
     * simulate a master or a replica.
     */
    static class RNConfig implements Serializable {

        private static final long serialVersionUID = 1L;

        private final boolean isMaster;

        private final List<InetSocketAddress> replicas;

        RNConfig(boolean isMaster, List<InetSocketAddress> replicas) {
            super();
            this.isMaster = isMaster;
            this.replicas = replicas;
        }
    }

    /**
     * Thread factory used by the simulated client to issues requests.
     */
    static class RequestThreadFactory implements ThreadFactory {

        private final AtomicInteger id = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread newThread = new Thread(r, "Request" + id.incrementAndGet());
            newThread.setDaemon(true);
            newThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    e.printStackTrace(System.err);
                    shutDown = true;
                }

            });

            return newThread;
        }

    }

    /**
     * The task used to issue client requests. Each task simulates an application
     * server thread.
     */
    static class RequestTask implements Runnable {
        final InetSocketAddress rn;

        RequestTask(InetSocketAddress rn) {
            this.rn = rn;
        }

        @Override
        public void run() {
            ByteBuffer bb = ByteBuffer.allocate(valueBytes);
            try {
                SocketChannel sc = openConfiguredChannel(rn);
                System.err.println("request channel:" + sc);

                long messageCount = 0;
                long byteCount = 0;
                long startNs = System.nanoTime();

                while (!shutDown) {
                    bb.rewind();
                    bb.putLong(++messageCount);
                    bb.rewind();
                    /**
                     * TODO: Gather async ack stats. Average, 95/99 percentiles.
                     */
                    final int messageBytes = writeMessage(sc, bb);
                    byteCount += messageBytes;

                    readAck(sc, messageCount);

                    //if ((messageCount % 100000) == 0) {
                    if ((messageCount % 10000) == 0) {
                        String format =
                            "%tT Total:%,d requests(%,d MB)." +
                            " Throughput:%,d req/sec; %,d MB/sec. " +
                            " Client latency: %,d us/req.";
                        final long now =  System.nanoTime();
                        long deltaNs = now - startNs;
                        final long messageThroughput =
                            (messageCount * 1000000000l) / deltaNs;
                        final long mbPerSec =
                            (byteCount * 1000000000l) / (deltaNs * 1024 * 1024);
                        System.err.println(String.format(format,
                                                         now,
                                                         messageCount,
                                                         byteCount/(1024 * 1024),
                                                         messageThroughput,
                                                         mbPerSec,
                                                         deltaNs/(messageCount*1000)));
                    }
                }

                System.err.println("Request shutdown");

            } catch (Throwable e) {
                e.printStackTrace(System.err);
                shutDown = true;
            } finally {
                System.err.println("Request Task exited");
            }
        }
    }

    /**
     * The RN simulator. It simulates either master or replica network traffic
     * based upon the initialization message it gets from the client.
     */
    static class RN extends Thread {

        ServerSocketChannel ssc;

        SocketChannel[] rChannels ;

        //final String args[];

        RNConfig config;

        public RN(String arg) throws IOException {
            //this.args = args;
            setDaemon(false);

            ssc = ServerSocketChannel.open();
            InetSocketAddress isa = createISA(arg);
            ssc.socket().bind(isa);
        }

        @Override
        public void run() {
            try {
                System.err.println("Started rn on:" + ssc);
                try (final SocketChannel sc = ssc.accept()) {

                    /*
                     * The first message, used to configure the RN as a
                     * master or replica.
                     */
                    config = (RNConfig)readObject(sc);
                    System.err.println("Master:" + config.isMaster);
                }

                if (config.isMaster) {
                    processMessagesAsMaster();
                } else {
                    processMessagesAsReplica();
                }

            } catch (AsynchronousCloseException ace) {
                System.err.println("Server socket closed to force exit");
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                System.err.println("RN Exiting");
            }
        }

        /**
         * Simulate replica traffic. Simply eat the message and send back an
         * ack containing the message id.
         */
        void processMessagesAsReplica() throws IOException {
            try {
                SocketChannel sc = ssc.accept();
                System.err.println("Replica HA channel:"  + sc);
                sc.configureBlocking(true);
                sc = NetSimUtil.setTcpParams(sc, tcp_params_value);

                while (!shutDown) {
                    ByteBuffer message = readMessage(sc);
                    message.rewind();
                    long messageId = message.getLong();
                    final int messageBytes = message.remaining();
                    writeAck(sc, messageId);

                    if (messageBytes == 0) {
                        System.err.println("Requested shutdown");
                        ssc.close();
                        shutDown = true;
                        break;
                    }
                }
            } finally {
                System.err.println("Replica (RN) exiting");
            }
        }

        /**
         * Simulates the master generated network traffic.
         */
        void processMessagesAsMaster() throws IOException {

            requestExecutor = Executors.newCachedThreadPool();
            rChannels = new SocketChannel[config.replicas.size()];

            rf = rChannels.length;
            int i=0;
            for (InetSocketAddress r : config.replicas) {
                SocketChannel sc = SocketChannel.open(r);
                sc.configureBlocking(true);
                sc = NetSimUtil.setTcpParams(sc, tcp_params_value);
                rChannels[i++] = sc;
            }

            while (!shutDown) {
                final SocketChannel sc = ssc.accept();
                    System.err.println("New client connection:" + sc);
                requestExecutor.submit(new MasterTask(sc));
            }
        }

        /**
         * Each task reads messages from the client and forwards them to
         * replicas, returning an acknowledgement only after it gets an ack
         * from the replica. The number of tasks is determined by the degree
         * of parallelism configured by the client.
         *
         * TODO: The ack simulation needs to be async as described below.
         */
        class MasterTask implements Runnable {
            final SocketChannel sc;

            MasterTask(SocketChannel sc)
                throws IOException {

                this.sc = sc;
                sc.configureBlocking(true);
            }

            @Override
            public void run() {

                System.err.println("Master started on channel:" + sc);

                try {
                    while (!shutDown) {
                        ByteBuffer message = readMessage(sc);
                        message.rewind();
                        long messageId = message.getLong();

                        final int messageBytes = message.remaining();
                        int acks = 1; /* For the master. */

                        /**
                         * TODO: Make ack process async. That is, return
                         * after "first" simple majority acks, arranging to
                         * consume remaining acks.
                         *
                         * Also, gather replica ack statistics.
                         */

                        /* Simulate JE HA replication traffic. */
                        for (SocketChannel rsc : rChannels) {
                            synchronized (rsc) {
                                writeMessage(rsc, message);

                                readAck(rsc, messageId);
                                acks++;
                                if (acks * 2 > rf) {
                                    /* Ack the client. */
                                    writeAck(sc, messageId);
                                    /* ack just once. */
                                    acks = -rf;
                                }
                            }
                        }

                        if (messageBytes == 0) {
                            System.err.println("Requested shutdown");
                            ssc.close();
                            shutDown = true;
                            break;
                        }
                    }

                    System.err.println("Master task shutdown");
                } catch (Throwable e) {
                    e.printStackTrace(System.err);
                } finally {
                    shutDown = true;
                    try {
                        System.err.println("Master channel:" + sc + " closed");
                        sc.close();
                        ssc.close();
                    } catch (IOException e) {
                        System.err.println("Ignoring exception on close:" + e);
                    }

                }
            }
        }
    }

}
