/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collection;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.async.FutureUtils;
import oracle.kv.impl.async.FutureUtils.CheckedRunnable;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.ServiceUtils;

import com.sun.management.UnixOperatingSystemMXBean;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Test utility methods shared by KV tests.
 */
public class TestUtils {
    /**
     * An operation that throws an exception, for use by {@link
     * #checkException}.
     */
    public interface Operation {
        void call() throws Exception;
    }

    /**
     * Check that the operation throws an instance of the specified class.
     */
    public static <E extends Throwable> E checkException(
        Operation op, Class<E> exceptionClass)
    {
        return checkException(op, exceptionClass, null);
    }

    /**
     * Check that the operation throws an instance of the specified class, and
     * that a subsequence of the exception message matches the specified
     * regular expression pattern, if the specified value is not null.
     */
    public static <E extends Throwable> E checkException(
        Operation op, Class<E> exceptionClass, String messagePattern) {

        try {
            op.call();
        } catch (Throwable e) {
            return checkException(e, exceptionClass, messagePattern);
        }
        fail("Expected " + exceptionClass.getName());
        return null;
    }

    /**
     * Check that the exception is of the specified class.
     */
    public static <E extends Throwable> E checkException(
        Exception e, Class<E> exceptionClass)
    {
        return checkException(e, exceptionClass, null);
    }

    /**
     * Check that the exception is of the specified class, and that a
     * subsequence of the exception message matches the specified regular
     * expression pattern, if the specified value is not null.
     */
    public static <E extends Throwable> E checkException(
        Throwable e, Class<E> exceptionClass, String messagePattern)
    {
        if (e == null) {
            fail("Expected " + exceptionClass.getName() + ", got null");
            throw new AssertionError("Not reached");
        }
        if (!exceptionClass.isInstance(e)) {
            fail("Expected " + exceptionClass.getName() +
                 ", got " + CommonLoggerUtils.getStackTrace(e));
        }
        if (messagePattern != null) {
            final String msg = e.getMessage();
            if ((msg == null) ||
                !Pattern.compile(messagePattern).matcher(msg).find()) {
                fail("Expected to find pattern '" + messagePattern +
                     "', got '" + msg + "'"+
                     "\nStack trace:\n" + CommonLoggerUtils.getStackTrace(e));
            }
        }
        return exceptionClass.cast(e);
    }

    /**
     * Runs the future get call method, checks for expected exception types and
     * unwraps the exception if it is either CompletionException or
     * ExecutionException and returns the exception if there is any. Used to
     * test exceptions from Future#get calls.
     *
     * @param get the runnable implemented with a future get call
     * @return the exception, {@code null} if there is none
     */
    public static
        @Nullable Exception tryHandleFutureGetException(CheckedRunnable get)
    {
        try {
            get.run();
            return null;
        } catch (Throwable t) {
            try {
                return FutureUtils.handleFutureGetException(t);
            } catch (Exception e) {
                return e;
            }
        }
    }

    /**
     * Check that the cause of the exception is of the specified class.
     */
    public static <E extends Throwable> E checkCause(Throwable e,
                                                     Class<E> causeClass) {
        return checkCause(e, causeClass, null);
    }

    /**
     * Check that the cause of the exception is of the specified class, and
     * that a subsequence of the cause exception message matches the specified
     * regular expression pattern, if the specified value is not null.
     */
    public static <E extends Throwable> E checkCause(
        Throwable e, Class<E> causeClass, String causePattern)
    {
        assertNotNull("Expected cause to be " + causeClass.getName() +
                      ", got null" +
                      "\nStack trace:\n" + CommonLoggerUtils.getStackTrace(e),
                      e.getCause());
        return checkException(e.getCause(), causeClass, causePattern);
    }

    /**
     * Whether clients should use the async network protocol.
     */
    public static boolean useAsync() {
        final String propValue = System.getProperty(KVStoreConfig.USE_ASYNC);
        return (propValue != null) ?
            Boolean.parseBoolean(propValue) :
            KVStoreConfig.DEFAULT_USE_ASYNC;
    }

    /**
     * Checks that two objects have the same class, or are both null.
     */
    public static void assertEqualClasses(Object x, Object y) {
        assertEquals(getObjectClass(x), getObjectClass(y));
    }

    /**
     * Returns the class for an object, or null if the object is null.
     */
    public static Class<?> getObjectClass(Object obj) {
        return (obj == null) ? null : obj.getClass();
    }

    /**
     * Perform a series of operations specified as a {@link Stream}.
     *
     * @see #checkAll(Runnable...)
     */
    public static void checkAll(Stream<Runnable> ops) {
        checkAll(ops.collect(Collectors.toList()));
    }

    /**
     * Perform a series of operations specified as a {@link Collection}.
     *
     * @see #checkAll(Runnable...)
     */
    public static void checkAll(Collection<Runnable> ops) {
        checkAll(ops.toArray(new Runnable[ops.size()]));
    }

    /**
     * Perform a series of operations, returning successfully if all operations
     * return normally, printing any exceptions thrown if operations fail, and
     * throwing the last exception, if any. Use this method in tests that want
     * to do multiple checks, but want to report all failures, not just fail on
     * the first one.
     */
    public static void checkAll(Runnable... ops) {
        Throwable exception = null;
        for (Runnable op : ops) {
            try {
                op.run();
            } catch (Throwable e) {
                if (exception != null) {
                    System.err.println(exception);
                }
                exception = e;
            }
        }
        if (exception instanceof Error) {
            throw (Error) exception;
        }
        if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
        }
        if (exception != null) {
            throw new RuntimeException("Unexpected exception: " + exception,
                                       exception);
        }
    }

    /**
     * Returns the number of open file descriptors.
     */
    public static long getNumOpenFileDescriptors(){
        final OperatingSystemMXBean os =
            ManagementFactory.getOperatingSystemMXBean();
        if(os instanceof UnixOperatingSystemMXBean){
           return ((UnixOperatingSystemMXBean) os).
               getOpenFileDescriptorCount();
        }
        return 0;
    }

    /**
     * Check if repNode is stopped.
     */
    public static boolean isShutdown(RepNodeAdminAPI rna) {
        try {
            rna.ping();
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Check if repNode is running.
     */
    public static void isRunning(CreateStore createStore, StorageNodeId snId,
                                 RepNodeId rnId, Logger logger)
        throws Exception {

        ConfigurableService.ServiceStatus[] targetStatus = {
            ConfigurableService.ServiceStatus.RUNNING};
        ServiceUtils.waitForRepNodeAdmin(
            createStore.getStoreName(),
            createStore.getHostname(),
            createStore.getRegistryPort(snId),
            rnId,
            snId,
            createStore.
                getSNALoginManager(snId),
            40,
            targetStatus,
            logger);
    }
}
