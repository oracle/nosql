/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeString;
import static oracle.kv.impl.util.TestUtils.set;
import static oracle.kv.util.TestUtils.checkAll;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FastExternalizableException;
import oracle.kv.FaultException;
import oracle.kv.KeyRange;
import oracle.kv.RequestLimitConfig;
import oracle.kv.RequestLimitException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StaleStoreHandleException;
import oracle.kv.TestBase;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.security.PasswordExpiredException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.lob.KVLargeObject.LOBState;
import oracle.kv.lob.PartialLOBException;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.utilint.FormatUtil;

import org.junit.Test;

/**
 * Tests for {@link SerializeExceptionUtil}.
 *
 * <p>Exceptions that implement FastExternalizable should be tested by {@link
 * #testPublicExceptions} or {@link #testInternalExceptions}.
 */
public class SerializeExceptionUtilTest extends TestBase {

    private static final boolean debugDeserialization =
        Boolean.getBoolean("test.debugDeserialization");

    private static final Random random = new Random();

    /* Use a fixed stack so we get reproducible results */
    private static final StackTraceElement[] stack = {
        new StackTraceElement("cl1", "meth1", "file1", 42),
        new StackTraceElement("cl2", "meth2", "file2", 77)
    };

    /* Tests */

    @Test
    public void testStandardExceptions()
        throws Exception {

        testSerializationType(RuntimeException.class);
        testSerializationType(IOException.class);
        testSerializationType(OutOfMemoryError.class);
        testSerializationInstance(new AssertionError(createMessage()));
        Throwable t = new AssertionError(createMessage());
        t.initCause(new RuntimeException(createMessage()));
        testSerializationInstance(t);

        /*
         * Test that serializing a standard exception that lacks a standard
         * message string constructor produces a RuntimeException that mentions
         * the exception class name.
         */
        t = new UnsupportedFlavorException(null);
        Throwable t2 = serializeException(t);
        assertTrue(t2 instanceof RuntimeException);
        assertTrue(t2.getMessage().contains(
                       UnsupportedFlavorException.class.getName()));
    }

    @Test
    public void testPublicExceptions()
        throws Exception {

        testSerializationType(AuthenticationFailureException.class);
        testSerialization(
            new AuthenticationRequiredException(createMessage(), true),
            AuthenticationRequiredException::getIsReturnSignal);
        testSerialization(
            new AuthenticationRequiredException(createMessage(), false),
            AuthenticationRequiredException::getIsReturnSignal);
        testSerialization(
            new AuthenticationRequiredException(
                new RuntimeException(createMessage()), true),
            AuthenticationRequiredException::getIsReturnSignal);
        testSerialization(
            new AuthenticationRequiredException(
                new RuntimeException(createMessage()), false),
            AuthenticationRequiredException::getIsReturnSignal);
        testSerialization(
            new ConsistencyException(
                new ReplicaConsistencyException(
                    createMessage(),
                    new TimeConsistencyPolicy(33, MILLISECONDS, 78, SECONDS)),
                new Consistency.Time(33, MILLISECONDS, 78, SECONDS)),
            ConsistencyException::getConsistency,
            FaultException::wasLoggedRemotely,
            FaultException::getFaultClassName,
            FaultException::getRemoteStackTrace);
        testSerialization(
            new DurabilityException(
                new RuntimeException(createMessage()),
                Durability.ReplicaAckPolicy.ALL, 3, set("a", "b", "c")),
            DurabilityException::getCommitPolicy,
            DurabilityException::getRequiredNodeCount,
            DurabilityException::getAvailableReplicas,
            FaultException::wasLoggedRemotely,
            FaultException::getFaultClassName,
            FaultException::getRemoteStackTrace);
        testSerialization(new FaultException(createMessage(), true),
                          FaultException::wasLoggedRemotely,
                          FaultException::getFaultClassName,
                          FaultException::getRemoteStackTrace);
        testSerialization(new FaultException(createMessage(),
                                         new RuntimeException(createMessage()),
                                         true),
                          FaultException::wasLoggedRemotely,
                          FaultException::getFaultClassName,
                          FaultException::getRemoteStackTrace);
        testSerialization(RequestLimitException.create(
                              new RequestLimitConfig(7, 6, 5),
                              new RepNodeId(4, 3),
                              2, 1, true),
                          FaultException::wasLoggedRemotely,
                          FaultException::getFaultClassName,
                          FaultException::getRemoteStackTrace);
        testSerialization(new RequestTimeoutException(45, createMessage(),
                                                      null, true),
                          RequestTimeoutException::getTimeoutMs,
                          FaultException::wasLoggedRemotely,
                          FaultException::getFaultClassName,
                          FaultException::getRemoteStackTrace);
        testSerialization(new RequestTimeoutException(45, createMessage(),
                                                      new RuntimeException(
                                                          createMessage()),
                                                      true),
                          RequestTimeoutException::getTimeoutMs,
                          FaultException::wasLoggedRemotely,
                          FaultException::getFaultClassName,
                          FaultException::getRemoteStackTrace);
        testSerializationType(StaleStoreHandleException.class);
        testSerializationType(UnauthorizedException.class);
        testSerialization(new PartialLOBException(
                              createMessage(), LOBState.PARTIAL_APPEND, true),
                          PartialLOBException::getPartialState,
                          FaultException::wasLoggedRemotely,
                          FaultException::getFaultClassName,
                          FaultException::getRemoteStackTrace);
    }

    @Test
    public void testInternalExceptions()
        throws Exception {

        /*
         * Disable Clover instrumentation, to work around a problem with
         * Lambdas in Clover 4.0.1.
         */
        ///CLOVER:OFF
        testSerialization(
            new AdminFaultException(new RuntimeException(createMessage())),
            AdminFaultException::getCommandResult,
            InternalFaultException::getFaultClassName);
        testSerialization(
            new AdminFaultException(new RuntimeException(createMessage()),
                                    createMessage(),
                                    ErrorMessage.NOSQL_5000,
                                    CommandResult.STORE_CLEANUP),
            AdminFaultException::getCommandResult,
            InternalFaultException::getFaultClassName);
        testSerializationType(RNUnavailableException.class);
        testSerializationType(PasswordExpiredException.class);
        testSerializationType(SessionAccessException.class);
        testSerialization(new SessionAccessException(
                              new RuntimeException(createMessage()), false),
                          SessionAccessException::getIsReturnSignal);

        /*
         * Test that serializing a KVS exception that does not implement
         * FastExternalizable fails.
         */
        Throwable t = new DatabaseNotReadyException(createMessage());
        assertFalse(t instanceof FastExternalizable);
        try (final DataOutputStream out = new DataOutputStream(
                 new ByteArrayOutputStream())) {
            SerializeExceptionUtil.writeException(
                t, out, SerialVersion.CURRENT);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        ///CLOVER:ON
    }

    @Test
    public void testJEExceptions()
        throws Exception {

        DatabaseNotFoundException e =
            new DatabaseNotFoundException(createMessage());
        e.addErrorMessage(createMessage());
        e.addRethrownStackTrace();

        /*
         * Because of the way the JE DatabaseException class works, each time
         * the exception is constructed it will get the JE version number
         * prepended.  Java serialization works around that because it, in
         * effect, introduces new constructors that don't add the version
         * number.  It doesn't seem worth addressing that issue here, so just
         * ignore the extra prefix.
         */
        new CompareExceptionsContainsMessage<Throwable>()
            .testSerializationInternal(e);

        /*
         * Deserializing the exception fails if it has a cause because the
         * appropriate constructor is privde and the cause is limited the same
         * type as the exception, rather than the standard public
         * String/Throwable constructor.  Just punt on these.
         */
        e = (DatabaseNotFoundException) e.wrapSelf(
            createMessage(), FormatUtil.cloneBySerialization(e));
        try {
            serializeException(e);
        } catch (IOException ioe) {
        }
    }

    @Test
    public void testSerializeStackTraceElements()
        throws IOException
    {
        checkSerializeStackTraceElement(
            new StackTraceElement("class", "method", "file", 1000));
        checkSerializeStackTraceElement(
            new StackTraceElement("class", "method", "file", -1));
        checkSerializeStackTraceElement(
            new StackTraceElement("class", "method", null, 1000));
        checkSerializeStackTraceElement(
            new StackTraceElement("\u20ac-EuroClass", "method", "file", 1000));
    }

    @Test
    public void testSerialVersion() throws Exception {
        final Exception exceptionNoCause = new Exception();
        exceptionNoCause.setStackTrace(stack);

        final TestFastExternalException fastExternalNoCause =
            new TestFastExternalException("msg1", null);
        fastExternalNoCause.setStackTrace(stack);

        final Exception exceptionExceptionCause =
            new Exception("msg2", exceptionNoCause);
        exceptionExceptionCause.setStackTrace(stack);

        final Exception exceptionFastExternalCause =
            new Exception("msg3", fastExternalNoCause);
        exceptionFastExternalCause.setStackTrace(stack);

        final TestFastExternalException fastExternalExceptionCause =
            new TestFastExternalException("msg4", exceptionNoCause);
        fastExternalExceptionCause.setStackTrace(stack);

        final TestFastExternalException fastExternalFastExternalCause =
            new TestFastExternalException("msg5", fastExternalNoCause);
        fastExternalFastExternalCause.setStackTrace(stack);

        checkAll(
            Stream.of(
                serialVersionChecker(
                    exceptionNoCause,
                    0x206523cbfd28b957L),
                serialVersionChecker(
                    fastExternalNoCause,
                    0xf2009907101dc02aL),
                serialVersionChecker(
                    exceptionExceptionCause,
                    0x6df024e4a3a4aa14L),
                serialVersionChecker(
                    exceptionFastExternalCause,
                    0xfd944f7410745c02L),
                serialVersionChecker(
                    fastExternalExceptionCause,
                    0xe6953e736a5251b8L),
                serialVersionChecker(
                    fastExternalFastExternalCause,
                    0xf716b8ffc455e306L))
            .map(c -> c.writer(SerializeExceptionUtil::writeException))
            .map(c -> c.reader(SerializeExceptionUtil::readException))
            .map(c -> c.equalsChecker(
                     SerializeExceptionUtilTest::equalExceptions)));
    }

    /**
     * Test that serializing an FastExternalizable exception with a cause that
     * is in a KVS package but is not fast externalizable just leaves the cause
     * out. [KVSTORE-1528]
     */
    @Test
    public void testSerializeNonSerializableCauseStandard() {
        final Exception exception =
            new IOException(
                "msg",
                new InitialConnectIOException(
                    null, new IOException("cause msg")));
        exception.setStackTrace(stack);
        final Exception noCause =
            new IOException("msg\n\tCaused by: " + exception.getCause(),
                            null);
        noCause.setStackTrace(stack);
        serialVersionChecker(exception, 0xd5ffa9ac89793a83L)
            .writer(SerializeExceptionUtil::writeException)
            .reader(SerializeExceptionUtil::readException)
            .equalsChecker((expected, actual, sv) ->
                           equalExceptions(noCause, actual, sv))
            .check();
    }

    /**
     * Similar with testSerializeNonSerializableCauseStandard but with channel
     * description.
     */
    @Test
    public void testSerializeNonSerializableCauseStandardWithChannel() {
        final Exception exception =
            new IOException(
                "msg",
                new InitialConnectIOException(
                    () -> "channel", new IOException("cause msg")));
        exception.setStackTrace(stack);
        final Exception noCause =
            new IOException("msg\n\tCaused by: " + exception.getCause(),
                            null);
        noCause.setStackTrace(stack);
        serialVersionChecker(exception, 0xa5cabdd4163afb2cL)
            .writer(SerializeExceptionUtil::writeException)
            .reader(SerializeExceptionUtil::readException)
            .equalsChecker((expected, actual, sv) ->
                           equalExceptions(noCause, actual, sv))
            .check();
    }

    /** Also KVSTORE-1528 */
    @Test
    public void testSerializeNonSerializableCauseFastExternal() {
        final Exception cause = new InitialConnectIOException(
            null, new IOException("cause msg"));
        final Exception exception = new AuthenticationFailureException("msg");
        exception.initCause(cause);
        exception.setStackTrace(stack);
        final Exception noCause =
            new AuthenticationFailureException("msg\n\tCaused by: " + cause);
        noCause.setStackTrace(stack);
        serialVersionChecker(exception, 0xace6976f52ad54b2L)
            .writer(SerializeExceptionUtil::writeException)
            .reader(SerializeExceptionUtil::readException)
            .equalsChecker((expected, actual, sv) ->
                           equalExceptions(noCause, actual, sv))
            .check();
    }

    /**
     * Similar with testSerializeNonSerializableCauseFastExternal but with
     * channel description.
     */
    @Test
    public void testSerializeNonSerializableCauseFastExternalWithChannel() {
        final Exception cause = new InitialConnectIOException(
            () -> "channel", new IOException("cause msg"));
        final Exception exception = new AuthenticationFailureException("msg");
        exception.initCause(cause);
        exception.setStackTrace(stack);
        final Exception noCause =
            new AuthenticationFailureException("msg\n\tCaused by: " + cause);
        noCause.setStackTrace(stack);
        serialVersionChecker(exception, 0x1d661170ad8cc065L)
            .writer(SerializeExceptionUtil::writeException)
            .reader(SerializeExceptionUtil::readException)
            .equalsChecker((expected, actual, sv) ->
                           equalExceptions(noCause, actual, sv))
            .check();
    }

    /**
     * Test that attempting to deserialize an exception in JAVA_SERIAL format
     * using readException fails.
     */
    @Test
    public void testReadExceptionJavaSerialFormat() throws IOException {
        final ByteArrayOutputStream baos =
            new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);
        SerializeExceptionUtil.Format.JAVA_SERIAL.writeFastExternal(
            out, SerialVersion.CURRENT);
        writeNonNullString(out, SerialVersion.CURRENT,
                           Exception.class.getName());
        writeString(out, SerialVersion.CURRENT, "msg");
        out.writeBoolean(false /* no cause */);
        /* No stack trace */
        writeNonNullSequenceLength(out, 0);
        out.close();
        final DataInput in =
            new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        checkException(() -> SerializeExceptionUtil.readException(
                           in, SerialVersion.CURRENT),
                       IOException.class,
                       "Exception format is not permitted: JAVA_SERIAL");
    }

    /**
     * Test that attempting to deserialize an exception with
     * FAST_EXTERNALIZABLE format fails if the class does not extend Throwable.
     */
    @Test
    public void testReadExceptionFastExternalNotThrowable()
        throws IOException
    {
        final ByteArrayOutputStream baos =
            new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);

        /*
         * Write data as required for a FastExternalizable exception, but using
         * data for the KeyRange class, which is not an exception class.
         */
        SerializeExceptionUtil.Format.FAST_EXTERNALIZABLE
            .writeFastExternal(out, SerialVersion.CURRENT);
        SerializationUtil.writeNonNullString(
            out, SerialVersion.CURRENT, KeyRange.class.getName());
        new KeyRange("prefix")
            .writeFastExternal(out, SerialVersion.CURRENT);
        out.close();

        final DataInput in =
            new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        checkException(() -> SerializeExceptionUtil.readException(
                           in, SerialVersion.CURRENT),
                       IOException.class,
                       "non-exception");
    }

    /**
     * Test that attempting to deserialize an exception with
     * FAST_EXTERNALIZABLE format fails if the class does not implement
     * FastExternalizable.
     */
    @Test
    public void testReadExceptionFastExternalNotImplement()
        throws IOException
    {
        final ByteArrayOutputStream baos =
            new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);

        /*
         * Write data as for a FastExternalizable exception, but using the
         * NotFastExternalizableException class, which has the right
         * constructor but does not implement FastExternalizable.
         */
        SerializeExceptionUtil.Format.FAST_EXTERNALIZABLE
            .writeFastExternal(out, SerialVersion.CURRENT);
        SerializationUtil.writeNonNullString(
            out, SerialVersion.CURRENT,
            NotFastExternalizableException.class.getName());
        out.close();

        final DataInput in =
            new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        checkException(() -> SerializeExceptionUtil.readException(
                           in, SerialVersion.CURRENT),
                       IOException.class,
                       "does not implement FastExternalizable");
    }

    /**
     * An exception class that does not implement FastExternalizable but that
     * has a fast-externalizable-style constructor
     */
    private static class NotFastExternalizableException extends Exception {
        private static final long serialVersionUID = 1;
        @SuppressWarnings("unused")
        NotFastExternalizableException(DataInput in, short sv) {
            throw new RuntimeException("Shouldn't be called");
        }
    }

    /**
     * Test that attempting to deserialize a exception with STANDARD format
     * fails if the class does not extend Throwable.
     */
    @Test
    public void testReadExceptionStandardNotThrowable()
        throws IOException
    {
        final ByteArrayOutputStream baos =
            new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);

        /*
         * Write data as required for a STANDARD exception, but using data for
         * the class String class, which has a constructor with a String
         * parameter, one of the supported constructors.
         */
        SerializeExceptionUtil.Format.STANDARD.writeFastExternal(
            out, SerialVersion.CURRENT);
        writeNonNullString(out, SerialVersion.CURRENT, String.class.getName());
        writeString(out, SerialVersion.CURRENT, "msg");
        out.writeBoolean(false /* no cause */);
        /* No stack trace */
        writeNonNullSequenceLength(out, 0);
        out.close();
        final DataInput in =
            new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        Throwable exception =
            SerializeExceptionUtil.readException(in, SerialVersion.CURRENT);
        assertEquals(RuntimeException.class, exception.getClass());
        assertTrue("Got message: " + exception.getMessage(),
                   exception.getMessage().contains(
                       "Problem reading exception of type " +
                       String.class.getName() +
                       " with message 'msg'"));
    }

    /* Other methods and classes */

    /**
     * Checks if a deserialized exception equals an expected exception when
     * deserialized for the specified serial version. Note that this method
     * throws an exception if the check fails.
     */
    public static boolean equalExceptions(Throwable expected,
                                          Object deserialized,
                                          short serialVersion) {
        if (expected == null) {
            assertNull(deserialized);
            return true;
        }
        assertNotNull(deserialized);
        assertEquals(expected.getClass(), deserialized.getClass());
        final Throwable deserializedExcept = (Throwable) deserialized;
        assertEquals(expected.getMessage(), deserializedExcept.getMessage());
        equalExceptions(expected.getCause(), deserializedExcept.getCause(),
                        serialVersion);

        checkStackTrace(expected.getStackTrace(),
                        deserializedExcept.getStackTrace());
        return true;
    }

    /**
     * Test that serializing and deserializing exceptions of the specified type
     * preserves their messages and causes, using standard constructors.
     */
    private void testSerializationType(Class<? extends Throwable> cl)
        throws Exception {

        Constructor<? extends Throwable> cons = null;
        try {
            cons = cl.getConstructor(String.class);
        } catch (NoSuchMethodException e) {
        }
        if (cons != null) {
            Throwable t = cons.newInstance(createMessage());
            t.initCause(new RuntimeException(createMessage()));
            testSerializationInstance(t);
            testSerializationInstance(cons.newInstance(createMessage()));
            t = cons.newInstance((String) null);
            t.initCause(new IOException(createMessage()));
            testSerializationInstance(t);
            testSerializationInstance(cons.newInstance((String) null));
        } else {
            cons = cl.getConstructor(String.class, Throwable.class);
            testSerializationInstance(
                cons.newInstance(
                    createMessage(), new RuntimeException(createMessage())));
            testSerializationInstance(cons.newInstance(createMessage(), null));
            testSerializationInstance(
                cons.newInstance(
                    null, new RuntimeException(createMessage())));
            testSerializationInstance(cons.newInstance(null, null));
        }
    }

    private static String createMessage() {
        return "Random message " + random.nextInt();
    }

    /**
     * Test that serializing and deserializing the specified exception instance
     * preserves its messages and causes.
     */
    private static void testSerializationInstance(Throwable t)
        throws IOException {

        CompareExceptions.INSTANCE.testSerializationInternal(t);
    }

    /**
     * Test that serializing and deserializing the specified exception
     * preserves its messages and causes as well as the values returned by all
     * of the accessor methods.
     */
    @SafeVarargs
    @SuppressWarnings({"all", "varargs"})
    private static <T extends Throwable>
        void testSerialization(T t, Function<T, Object>... accessors)
        throws IOException {

        new CompareExceptions<T>(accessors).testSerializationInternal(t);
    }

    private static class CompareExceptions<T extends Throwable> {
        static final CompareExceptions<Throwable> INSTANCE =
            new CompareExceptions<>();
        final Function<T, Object>[] accessors;
        @SafeVarargs
        @SuppressWarnings({"all", "varargs"})
        CompareExceptions(Function<T, Object>... accessors) {
            this.accessors = accessors;
        }
        void testSerializationInternal(T t) throws IOException {
            check(t, serializeException(t));
        }
        void check(T expected, T actual) {
            if ((expected == null) && (actual == null)) {
                return;
            }
            if ((expected == null) || (actual == null)) {
                fail("Only one exception is null: " + expected + ", " +
                     actual);
            } else {
                assertEquals(expected.getClass(), actual.getClass());
                checkMessage(expected.getMessage(), actual.getMessage());
                checkCause(expected.getCause(), actual.getCause());
                checkStackTrace(expected.getStackTrace(),
                                actual.getStackTrace());
            }
            for (Function<T, Object> a : accessors) {
                assertEquals("Wrong value for accessor " + a,
                             a.apply(expected), a.apply(actual));
            }
        }
        void checkMessage(String expected, String actual) {
            assertEquals(expected, actual);
        }
        void checkCause(Throwable expected, Throwable actual) {
            INSTANCE.check(expected, actual);
        }
    }

    static void checkStackTrace(StackTraceElement[] expected,
                                StackTraceElement[] actual) {

        /*
         * Check that the actual stack trace has the expected stack trace as
         * its initial elements. The extra elements in the fast external
         * version represent the appended current stack trace.
         */
        assertTrue("Actual stack trace should be longer." +
                   " Expected " + expected.length +
                   ", found " + actual.length,
                   expected.length <= actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertStackTraceElementEquals("Stack trace element " + i,
                                          expected[i], actual[i]);
        }
    }

    private static void assertStackTraceElementEquals(
        String msg,
        StackTraceElement expected,
        StackTraceElement actual)
    {
        /*
         * Java 9 added support for class loader name, module name, and module
         * version. The Java 9 JVM initializes stack trace elements with a
         * class loader name of "app" for classes defined by the system class
         * loader. Since we still only require Java 8, we need to ignore the
         * class loader for now.
         */
        assertEquals(msg + ", class name",
                     expected.getClassName(), actual.getClassName());
        assertEquals(msg + ", method name",
                     expected.getMethodName(), actual.getMethodName());
        assertEquals(msg + ", file name",
                     expected.getFileName(), actual.getFileName());
        assertEquals(msg + ", line number",
                     expected.getLineNumber(), actual.getLineNumber());
    }

    private static class CompareExceptionsContainsMessage<T extends Throwable>
            extends CompareExceptions<T> {
        @Override
        void checkMessage(String expected, String actual) {
            if (expected == null) {
                assertEquals(null, actual);
            } else if (actual == null) {
                assertEquals(expected, null);
            } else if (!actual.contains(expected)) {
                fail("Expected message to contain: '" + expected +
                     "', found: '" + actual);
            }
        }
    }

    private static <T extends Throwable> T serializeException(T t)
        throws IOException {

        return serializeException(t, SerialVersion.CURRENT);
    }

    private static <T extends Throwable>
        T serializeException(T t, short serialVersion)
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(baos)) {
            SerializeExceptionUtil.writeException(t, out, serialVersion);
        }
        final byte[] bytes = baos.toByteArray();
        try (final DataInputStream in = new DataInputStream(
                 new ByteArrayInputStream(bytes))) {
            /* No easy way to check this */
            @SuppressWarnings("unchecked")
            final T result = (T) SerializeExceptionUtil.readException(
                in, serialVersion);
            assertEquals("Expected EOF after reading serialized object data",
                         -1, in.read());
            return result;
        } catch (IOException e) {
            if (debugDeserialization) {
                e = new IOException("Problem deserializing: " + e +
                                    "\nBytes: " + Arrays.toString(bytes),
                                    e);
            }
            throw e;
        }
    }

    private static void checkSerializeStackTraceElement(StackTraceElement ste)
        throws IOException
    {
        Exception e = new Exception();
        e.setStackTrace(new StackTraceElement[] { ste });
        testSerializationInstance(e);

        e = new ConsistencyException(new RuntimeException(),
                                     Consistency.ABSOLUTE);
        e.setStackTrace(new StackTraceElement[] { ste });
        testSerializationInstance(e);
    }

    public static class TestFastExternalException
            extends FastExternalizableException {
        private static final long serialVersionUID = 1;
        TestFastExternalException(String msg, Throwable cause) {
            super(msg, cause);
        }
        public TestFastExternalException(DataInput in, short sv)
            throws IOException
        {
            super(in, sv);
        }
    }
}
