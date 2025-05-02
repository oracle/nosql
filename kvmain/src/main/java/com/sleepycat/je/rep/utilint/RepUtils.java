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

package com.sleepycat.je.rep.utilint;

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MAX_WRITE_NANOS;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_WRITTEN;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_WRITE_NANOS;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.net.SimpleDataChannel;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

public class RepUtils {

    public static final boolean DEBUG_PRINT_THREAD = true;
    public static final boolean DEBUG_PRINT_TIME = true;

    /**
     * Maps from uppercase ReplicaConsistencyPolicy name to the policy's
     * format.
     */
    private static final Map<String, ConsistencyPolicyFormat<?>>
        consistencyPolicyFormats =
            new HashMap<>();
    static {
        addConsistencyPolicyFormat(TimeConsistencyPolicy.NAME,
                                   new TimeConsistencyPolicyFormat());
        addConsistencyPolicyFormat(NoConsistencyRequiredPolicy.NAME,
                                   new NoConsistencyRequiredPolicyFormat());
    }

    /*
     * Canonical channel instance used to indicate that this is the last
     * instance of a channel in a channel queue and that the queue is
     * effectively closed. This value is typically used during a soft shutdown
     * of a thread to cause the thread waiting on the queue to wake up and
     * take notice.
     */
    public final static DataChannel CHANNEL_EOF_MARKER;

    static {
        try {
            CHANNEL_EOF_MARKER = new SimpleDataChannel(SocketChannel.open());
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * If not null, called by openSocketChannel with the connect options before
     * opening the socket -- for unit testing.
     */
    public static volatile TestHook<ConnectOptions> openSocketChannelHook;

    /**
     * Define a new ConsistencyPolicyFormat.  Should only be called outside of
     * this class to add support custom policies for testing.  Must be called
     * when the system is quiescent, since the map is unsynchronized.
     *
     * @param name must be the first part of the policy string with a
     * non-letter delimiter following it, or must be the entire policy string.
     *
     * @param format to register.
     */
    public static void
        addConsistencyPolicyFormat(final String name,
                                   final ConsistencyPolicyFormat<?> format) {
        consistencyPolicyFormats.put
            (name.toUpperCase(java.util.Locale.ENGLISH), format);
    }

    /**
     * ReplicaConsistencyPolicy must be stored as a String for use with
     * ReplicationConfig and je.properties.  ConsistencyPolicyFormat is an
     * internal handler that formats and parses the string representation of
     * the policy.  Only a fixed number of string-representable policies are
     * supported. Other policies that are not string-representable can only be
     * used in TransactionConfig, not ReplicationConfig.  For testing only, we
     * allow defining new custom policies.
     */
    public interface
        ConsistencyPolicyFormat<T extends ReplicaConsistencyPolicy> {

        String policyToString(final T policy);

        T stringToPolicy(final String string);
    }

    private static class TimeConsistencyPolicyFormat
        implements ConsistencyPolicyFormat<TimeConsistencyPolicy> {

        @Override
        public String policyToString(final TimeConsistencyPolicy policy) {
            return policy.getName() +
                "(" + policy.getPermissibleLag(TimeUnit.MILLISECONDS) +
                " ms," + policy.getTimeout(TimeUnit.MILLISECONDS) +
                " ms)";
        }

        @Override
        public TimeConsistencyPolicy stringToPolicy(final String string) {
            /* Format: (<lag>, <timeout>) */
            String args =
                string.substring(TimeConsistencyPolicy.NAME.length());
            if (args.charAt(0) != '(' ||
                args.charAt(args.length()-1) != ')') {
                throw new IllegalArgumentException
                    ("Incorrect property value syntax: " + string);
            }
            int arg1 = args.indexOf(',');
            if (arg1 == -1) {
                throw new IllegalArgumentException
                    ("Incorrect property value syntax: " + string);
            }
            int lag = PropUtil.parseDuration(args.substring(1, arg1));
            int arg2 = args.indexOf(')');
            if (arg2 == -1) {
                throw new IllegalArgumentException
                    ("Incorrect property value syntax: " + string);
            }
            int timeout =
                PropUtil.parseDuration(args.substring(arg1 + 1, arg2));
            return new TimeConsistencyPolicy
                (lag, TimeUnit.MILLISECONDS, timeout, TimeUnit.MILLISECONDS);
        }
    }

    private static class NoConsistencyRequiredPolicyFormat
        implements ConsistencyPolicyFormat<NoConsistencyRequiredPolicy> {

        @Override
        public String
            policyToString(final NoConsistencyRequiredPolicy policy) {
            return NoConsistencyRequiredPolicy.NAME;
        }

        @Override
        public NoConsistencyRequiredPolicy
            stringToPolicy(final String string) {
            return NoConsistencyRequiredPolicy.NO_CONSISTENCY;
        }
    }

    /**
     * Converts a policy into a string suitable for use as a property value
     * in a je.properties file or elsewhere.
     *
     * @param policy the policy being converted.
     *
     * @return the formatted string representing the policy.
     *
     * @throws IllegalArgumentException if the specific policy does not have a
     * property value format, via ReplicationConfig(Properties) ctor and
     * setter.
     *
     * @see #getReplicaConsistencyPolicy(String)
     */
    @SuppressWarnings("unchecked")
    public static String getPropertyString(ReplicaConsistencyPolicy policy)
        throws IllegalArgumentException {

        @SuppressWarnings("rawtypes")
        ConsistencyPolicyFormat format =
            consistencyPolicyFormats.get(policy.getName().toUpperCase());
        if (format == null) {
            throw new IllegalArgumentException
                ("Policy: " + policy + " cannot be used as a property");
        }
        return format.policyToString(policy);
    }

    /**
     * Converts a property string into a policy instance.
     *
     * @param propertyValue the formatted string representing the policy.
     *
     * @return the policy computed from the string
     *
     * @throws IllegalArgumentException via ReplicationConfig(Properties) ctor
     * and setter.
     */
    public static ReplicaConsistencyPolicy
        getReplicaConsistencyPolicy(String propertyValue)
        throws IllegalArgumentException {

        final String upperCasePropertyValue =
            propertyValue.toUpperCase(java.util.Locale.ENGLISH);
        for (final Map.Entry<String, ConsistencyPolicyFormat<?>> entry :
                 consistencyPolicyFormats.entrySet()) {
            final String name = entry.getKey();
            if (upperCasePropertyValue.equals(name) ||
                (upperCasePropertyValue.startsWith(name) &&
                 upperCasePropertyValue.length() > name.length() &&
                 !Character.isLetter(
                     upperCasePropertyValue.charAt(name.length())))) {
                ConsistencyPolicyFormat<?> format = entry.getValue();
                return format.stringToPolicy(propertyValue);
            }
        }
        throw new IllegalArgumentException
            ("Invalid consistency policy: " + propertyValue);
    }

    /**
     * Like CountDownLatch, but makes provision in the await for the await, or
     * more specifically the new awaitOrException method to be exited via an
     * exception.
     */
    public static class ExceptionAwareCountDownLatch extends CountDownLatch {
        /* The environment that may need to be invalidated. */
        final EnvironmentImpl envImpl;

        /* The exception (if any) that caused the latch to be released */
        private final AtomicReference<DatabaseException> terminatingException =
            new AtomicReference<>();

        public ExceptionAwareCountDownLatch(EnvironmentImpl envImpl,
                                            int count) {
            super(count);
            this.envImpl = envImpl;
        }

        /**
         * The method used to free an await, ensuring that it throws an
         * exception at the awaitOrException.
         *
         * @param e the exception to be wrapped in a DatabaseException
         * and thrown.
         */
        public void releaseAwait(Exception e) {
            terminatingException.compareAndSet(
                null, prepareTerminatingException(e, envImpl));
            for (long count = getCount(); count > 0; count--) {
                countDown();
            }
            assert(getCount() == 0);
        }

        /**
         * Blocks, waiting for the latch to count down to zero, or until an
         * {@code Exception} is provided.  The exception is thrown in every
         * thread that is waiting in this method.
         *
         * @see #releaseAwait
         */
        public boolean awaitOrException(long timeout, TimeUnit unit)
            throws InterruptedException,
                   DatabaseException {

            boolean done = super.await(timeout, unit);
            if (!done) {
                return done;
            }
            final DatabaseException e = terminatingException.get();
            if (e != null) {
                throw addLocalStackTrace(e);
            }
            return done;
        }

        public void awaitOrException()
            throws InterruptedException,
                   DatabaseException {
            awaitOrException(Integer.MAX_VALUE, TimeUnit.SECONDS);
        }

        /**
         * DO NOT use this method. Use awaitOrException instead, so that any
         * outstanding exceptions are thrown.
         */
        @Override
        @SuppressWarnings("deprecation")
        @Deprecated
        public boolean await(long timeout, TimeUnit unit) {
            throw EnvironmentFailureException.unexpectedState
                ("Use awaitOrException() instead of await");
        }
    }

    /**
     * Like {@code LinkedBlockingQueue}, but provides a {@code
     * pollOrException()} method that should be used instead of {@code poll()},
     * so that callers don't have to treat exception cases specially.
     *
     * @see ExceptionAwareCountDownLatch
     */
    @SuppressWarnings("serial")
    public static class ExceptionAwareBlockingQueue<T>
        extends LinkedBlockingQueue<T> implements NotSerializable {

        final EnvironmentImpl envImpl;
        final T dummyValue;

        private final AtomicReference<DatabaseException> terminatingException =
            new AtomicReference<>();

        public ExceptionAwareBlockingQueue(EnvironmentImpl envImpl,
                                           T dummyValue) {
            super();
            this.envImpl = envImpl;
            this.dummyValue = dummyValue;
        }

        public void releasePoll(Exception e) {
            terminatingException.compareAndSet(
                null, prepareTerminatingException(e, envImpl));
            add(dummyValue);
        }

        public T pollOrException(long timeout, TimeUnit unit)
            throws InterruptedException,
            DatabaseException {

            T value = super.poll(timeout, unit);
            if (value == null) {
                return value;
            }
            final DatabaseException e = terminatingException.get();
            if (e != null) {
                throw addLocalStackTrace(e);
            }
            return value;
        }

        /**
         * (Use {@link #pollOrException} instead.
         */
        @Override
        @SuppressWarnings("deprecation")
        @Deprecated
        public T poll(long timeout, TimeUnit unit) {
            throw EnvironmentFailureException.unexpectedState
                ("Use pollOrException() instead of poll()");
        }
    }

    /**
     * The terminating exception is wrapped in an EFE if it is not already a
     * DatabaseException (which is unexpected).  Also text is added to the
     * message indicating it was thrown by an HA thread, since it will often be
     * re-thrown in an app thread and the stack trace may be confusing.
     */
    private static DatabaseException prepareTerminatingException(
        final Exception e,
        final EnvironmentImpl envImpl) {

        if (e == null) {
            return null;
        }

        final DatabaseException de =
            (e instanceof DatabaseException) ?
            ((DatabaseException) e) :
            EnvironmentFailureException.unexpectedException(envImpl, e);

        de.addErrorMessage(
            "Originally thrown by HA thread: " +
            Thread.currentThread().getName());

        return de;
    }

    /**
     * Ideally we should wrap the exception before rethrowing in a different
     * thread, but this confuses exception handlers that call getCause.  So
     * instead we add the per-thread local stack trace to the message.
     *
     * <p>To avoid the CIRCULAR REFERENCE problem (see SavedExceptionTest)
     * this approach should not be used when the exception is saved and may
     * be thrown more than once in a given thread.</p>
     */
    private static DatabaseException addLocalStackTrace(DatabaseException e) {
        e.addRethrownStackTrace();
        return e;
    }

    /**
     * Forces a shutdown of a blocking channel ignoring any errors that may be
     * encountered in the process.
     *
     * @param namedChannel the channel to be shutdown
     */
    public static void shutdownChannel(NamedChannel namedChannel) {
        if (namedChannel == null) {
            return;
        }
        shutdownChannel(namedChannel.getChannel());
    }

    public static void shutdownChannel(DataChannel channel) {
        if (channel == null) {
            return;
        }

        if (!channel.isBlocking()) {
            throw new IllegalStateException(
                    "Unexpected non-blocking channel for shutting down.");
        }

        /*
         * For SSL, shutting down the socket before shutting down the
         * channel is a no-no.  That results in SSLExceptions being
         * thrown due to missing close_notify alerts.
         */
        try {
            channel.close();
        } catch (IOException e) {
            /* Ignore */
        }
    }

    /**
     * Create a socket channel with the designated properties.
     *
     * The motivation and use of the localAddr parameter bears some
     * explanation. Explicit specification of the localAddr is not necessary
     * when the two nics are on separate subnets and the routing rules will
     * automatically choose the correct nic. In fact routing rules, could even
     * be used for such communication even when they are on the same subnet,
     * but such configuration can be brittle and hard to maintain.
     *
     * Consider the following illustrative example with two nodes
     * n1, n2, with two nics each labeled 0 and 1. We would like to constrain
     * all HA traffic between n1 and n2 to nics n1(1) and n2(1) leaving the
     * other two nics for application use, e.g. KV RMI requests.
     *
     * If n1 is the master and its ReplicationConfig.NODE_HOST_PORT specified
     * n1(1) it would listen on n1(1) for all HA traffic. n2 (the replica) has
     * two choices for sending traffic over to n2: n2(0) or n2(1), depending on
     * the routing rules that were set up. By binding conections locally (as is
     * done with this change) on the replica to n2(1) we guarantee that the
     * traffic to n1 will go over n2(1), so that connections always have the
     * desired endpoints n2(1) -> n1(1).
     *
     * @param addr the remote endpoint socket address
     * @param localAddr the local address the socket is bound to it may be null
     * @param connectOpts connect options to be applied to the channel
     * @return the connected channel
     *
     */
    public static SocketChannel openSocketChannel(InetSocketAddress addr,
                                                  InetSocketAddress localAddr,
                                                  ConnectOptions connectOpts)
        throws IOException {
        if (connectOpts.getDisableOpeningPort()) {
            throw new IOException("Unexpected request to open tcp/ip socket");
        }

        assert TestHookExecute.doHookIfSet(openSocketChannelHook, connectOpts);

        final SocketChannel channel = SocketChannel.open();
        final boolean blocking = connectOpts.getBlocking();
        channel.configureBlocking(blocking);

        final Socket socket = channel.socket();
        if (connectOpts.getReceiveBufferSize() != 0) {
            socket.setReceiveBufferSize(connectOpts.getReceiveBufferSize());
        }
        socket.setTcpNoDelay(connectOpts.getTcpNoDelay());
        socket.setSoTimeout(connectOpts.getReadTimeout());
        socket.setReuseAddress(connectOpts.getReuseAddr());
        socket.bind(localAddr);

        if (blocking) {
            socket.connect(addr, connectOpts.getOpenTimeout());
        } else {
            channel.connect(addr);
        }
        return channel;
    }

    /**
     * Create a Data channel with the designated properties
     *
     * @param addr the remote endpoint socket address
     * @param factory DataChannel factory for channel creation
     * @param connectOpts connect options to be applied to the channel
     * @return the connected channel
     *
     */
    public static DataChannel openBlockingChannel(InetSocketAddress addr,
                                                  DataChannelFactory factory,
                                                  ConnectOptions connectOpts)
        throws IOException {
        return factory.connect(addr, null, connectOpts);
    }

    /**
     * Return the remote host name or address associated with the channel.
     *
     * TODO: ISE should rarely be thrown and has only been observed once.
     *  If it becomes common, callers of this method may need to be changed
     *  to handle ISE appropriately. Currently callers do not do this, and
     *  the ISE will cause the environment to be invalidated.
     *
     * @throws IllegalStateException if an IO error occurs or the remote
     * address is null because the channel is not connected.
     */
    public static String getRemoteHost(DataChannel channel) {
        InetSocketAddress address;
        try {
            address = (InetSocketAddress)(channel.getRemoteAddress());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        if (address == null) {
            throw new IllegalStateException(
                    "Channel is not connected (null remote address)");
        }
        return address.getHostString();
    }

    /**
     * Chains an old outstanding exception to the tail of a new one, so it's
     * not lost.
     *
     * @param newt the new throwable
     * @param oldt the old throwable
     * @return the new throwable extended with the old cause
     */
    public static Throwable chainExceptionCause(Throwable newt,
                                                Throwable oldt) {
        /* Don't lose the original exception */
        Throwable tail = newt;
        while (tail.getCause() != null) {
            tail = tail.getCause();
        }
        tail.initCause(oldt);
        return newt;
    }

    public static String writeTimesString(StatGroup stats) {
        long nMessagesWritten = stats.getLong(N_MESSAGES_WRITTEN);
        long nWriteNanos = stats.getLong(N_WRITE_NANOS);
        long nMaxWriteNanos = stats.getLong(N_MAX_WRITE_NANOS);

        long avgWriteNanos =
            (nMessagesWritten <= 0) ? 0 : (nWriteNanos / nMessagesWritten);

        return String.format("write times -- total: %,d ms, average: %,d us, " +
                             "max %,d us",
                             nWriteNanos / 1000000, avgWriteNanos / 1000,
                             nMaxWriteNanos / 1000);
    }

    /**
     * Read replication access properties from a property file.
     *
     * @param props a Properties object into which the properties will be stored
     * @param accessPropsFile an abstract File naming a file containing property
     *        settings.
     * @return the input properties object, updated with the property settings
     *        found in the file.
     * @throws IllegalArgumentException if the accessPropsFile contains
     *        invalid settings.
     */
    public static Properties populateNetProps(Properties props,
                                              File accessPropsFile) {

        Properties rawProps = new Properties();
        DbConfigManager.applyFileConfig(accessPropsFile, rawProps,
                                        true); //forReplication

        /* filter out the properties that are not relevant */
        ReplicationNetworkConfig.applyRepNetProperties(rawProps, props);
        return props;
    }

    /**
     * A simple debugging utility used to obtain information about the
     * execution environment that's only available through some system utility,
     * like netstat, or jps, etc. It's up to the caller to ensure the
     * availability of the utility and ensure that it's on the search path.
     *
     * When calling this function, make sure that the given commands are
     * already listed in the APPROVED_COMMANDS array and the command
     * options are added to the APPROVED_OPTIONS array or the command
     * will not be executed.
     *
     * @param args the arguments to a ProcessBuilder with args[0] being the
     * command and args[1-...] being its args
     *
     * @return a string denoting the output from the command. Or a string
     * prefixed by the word EXCEPTION, if an exception was encountered,
     * followed by the exception class name and exception message.
     */
    public static String exec(String... args) {

        if (args == null || args.length < 1) {
            return "EXCEPTION: no command given.";
        }
        final ByteArrayOutputStream bao = new ByteArrayOutputStream(1024);
        final PrintStream output = new PrintStream(bao);

        // This is used to prevent command injection.
        final String[] APPROVED_COMMANDS = {
        		"jps",
        		"netstat",
        		"lsof" };
        final String[] APPROVED_OPTIONS = {
                "-v",
                "-lntp", "-antp",
                "-iTCP", "-P"
        };
        final String command = args[0];
        boolean approved = false;
        for (String cmd: APPROVED_COMMANDS) {
            if (cmd.equalsIgnoreCase(command)) {
                approved = true;
            }
        }
        if (!approved) {
            return "EXCEPTION: given command: " + command + " not approved" +
                "Approved commands are: " + APPROVED_COMMANDS;
        }
        for (int i = 1; i < args.length; i++) {
           approved = false;
           for (String option: APPROVED_OPTIONS) {
               if (option.equalsIgnoreCase(args[i])) {
                   approved = true;
               }
           }
           if (!approved) {
               return "EXCEPTION: given command option: " + args[i] +
                   " not approved Approved command options are: " +
                   APPROVED_OPTIONS;
           }
        }
        try {

            final ProcessBuilder builder = new ProcessBuilder(args);
            builder.redirectErrorStream(true);

            final Process process = builder.start();
            final InputStream is = process.getInputStream();
            final InputStreamReader isr = new InputStreamReader(is);
            final BufferedReader br = new BufferedReader(isr);

            String line;
            while ((line = br.readLine()) != null) {
                output.println(line);
            }

        } catch (Exception e) {
            output.println("EXCEPTION:" + " class:" + e.getClass().getName() +
                           " message:" + e.getMessage());
        }

        return bao.toString();
    }

    /*
     * Used to create deliberate clock skews for testing purposes. Replicator
     * code should use it instead of invoking TimeSupplier.currentTimeMillis()
     * directly.
     */
    public static class Clock {
        private final int skewMs;

        public Clock(int skewMs) {
            this.skewMs = skewMs;
        }

        public long currentTimeMillis() {
            return TimeSupplier.currentTimeMillis() + skewMs;
        }
    }

}
