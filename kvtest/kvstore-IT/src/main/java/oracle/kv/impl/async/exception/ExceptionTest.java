/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.util.function.Function;

import oracle.kv.TestBase;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.exception.ProtocolViolationException;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;

/**
 * Test the exception classes.
 */
public class ExceptionTest extends TestBase {

    private enum Check {
        TEMPORARY, PERSISTENT, UNKNOWN,
        REMOTE, LOCAL,
        HAS_SIDE_EFFECT, NO_SIDE_EFFECT,
    }

    @Test
    public void testExceptions() {
        /* DialogLimitExceededException */
        checkException(new DialogLimitExceededException(0),
                Check.UNKNOWN, Check.LOCAL, Check.NO_SIDE_EFFECT);
        /* DialogNoSuchTypeException */
        checkException(new DialogNoSuchTypeException(""),
                Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
        /* DialogException with ConnectionEndpointShutdownException */
        checkException((new ConnectionEndpointShutdownException(false, true, null, "")).
                getDialogException(false),
                Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException((new ConnectionEndpointShutdownException(false, true, null, "")).
                getDialogException(true),
                Check.PERSISTENT, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        checkException((new ConnectionEndpointShutdownException(true, true, null, "")).
                getDialogException(false),
                Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
        checkException((new ConnectionEndpointShutdownException(true, true, null, "")).
                getDialogException(true),
                Check.PERSISTENT, Check.REMOTE, Check.HAS_SIDE_EFFECT);
        /* DialogException with ConnectionHandlerShutdownException */
        checkException((new ConnectionHandlerShutdownException(null)).
                getDialogException(false),
                Check.TEMPORARY, Check.LOCAL, Check.NO_SIDE_EFFECT);
        try {
            checkException((new ConnectionHandlerShutdownException(null)).
                    getDialogException(true),
                    Check.TEMPORARY, Check.LOCAL, Check.NO_SIDE_EFFECT);
            fail("This DialogException should not have side effect.");
        } catch (IllegalArgumentException e) {
            /* do nothing */
        }
        /* DialogException with ConnectionIncompatibleException */
        checkException((new ConnectionIncompatibleException(false, null, "")).
                getDialogException(false),
                Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        try {
            checkException((new ConnectionIncompatibleException(false, null, "")).
                    getDialogException(true),
                    Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
            fail("This DialogException should not have side effect.");
        } catch (IllegalArgumentException e) {
            /* do nothing */
        }
        checkException((new ConnectionIncompatibleException(true, null, "")).
                getDialogException(false),
                Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
        try {
            checkException((new ConnectionIncompatibleException(true, null, "")).
                    getDialogException(true),
                    Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
            fail("This DialogException should not have side effect.");
        } catch (IllegalArgumentException e) {
            /* do nothing */
        }
        /* DialogException with InitialConnectIOException */
        checkException(
            (new InitialConnectIOException(null, new IOException())).
            getDialogException(false),
            Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        /* DialogException with ConnectionIOException */
        checkException(
            (new ConnectionIOException(true, null, new IOException())).
            getDialogException(false),
            Check.TEMPORARY, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException(
            (new ConnectionIOException(true, null, new IOException())).
            getDialogException(true),
            Check.TEMPORARY, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        /* DialogException with ConnectionNotEstablishedException */
        checkException((new ConnectionNotEstablishedException(null)).
                getDialogException(false),
                Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        try {
            checkException((new ConnectionNotEstablishedException(null)).
                    getDialogException(true),
                    Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
            fail("This DialogException should not have side effect.");
        } catch (IllegalArgumentException e) {
            /* do nothing */
        }
        /* DialogException with ConnectionTimeoutException */
        checkException((
            new ConnectionTimeoutException(false, true, false, null, "")).
                       getDialogException(false),
                       Check.TEMPORARY, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(false, true, false, null, "")).
                       getDialogException(true),
                       Check.TEMPORARY, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(true, true, false, null, "")).
                       getDialogException(false),
                       Check.TEMPORARY, Check.REMOTE, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(true, true, false, null, "")).
                       getDialogException(true),
                       Check.TEMPORARY, Check.REMOTE, Check.HAS_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(false, true, true, null, "")).
                       getDialogException(false),
                       Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(false, true, true, null, "")).
                       getDialogException(true),
                       Check.PERSISTENT, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(true, true, true, null, "")).
                       getDialogException(false),
                       Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionTimeoutException(true, true, true, null, "")).
                       getDialogException(true),
                       Check.PERSISTENT, Check.REMOTE, Check.HAS_SIDE_EFFECT);
        /* DialogException with ConnectionIdleException */
        checkException((
            new ConnectionIdleException(false, true, null, "")).
                       getDialogException(false),
                       Check.TEMPORARY, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionIdleException(false, true, null, "")).
                       getDialogException(true),
                       Check.TEMPORARY, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        checkException((
            new ConnectionIdleException(true, true, null, "")).
                       getDialogException(false),
                       Check.TEMPORARY, Check.REMOTE, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionIdleException(true, true, null, "")).
                       getDialogException(true),
                       Check.TEMPORARY, Check.REMOTE, Check.HAS_SIDE_EFFECT);
        /* DialogException with ConnectionUnknownException */
        checkException((
            new ConnectionUnknownException(true, null, new Error())).
                       getDialogException(false),
                       Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionUnknownException(true, null, new Error())).
                       getDialogException(true),
                       Check.PERSISTENT, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        checkException((
            new ConnectionUnknownException(true, null, "")).
                       getDialogException(false),
                       Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
        checkException((
            new ConnectionUnknownException(true, null, "")).
                       getDialogException(true),
                       Check.PERSISTENT, Check.REMOTE, Check.HAS_SIDE_EFFECT);
        /* ProtocolViolationException */
        checkException((
            new ProtocolViolationException(false, true, null, "")).
                       getDialogException(false),
                       Check.PERSISTENT, Check.LOCAL, Check.NO_SIDE_EFFECT);
        checkException((
            new ProtocolViolationException(false, true, null, "")).
                       getDialogException(true),
                       Check.PERSISTENT, Check.LOCAL, Check.HAS_SIDE_EFFECT);
        checkException((
            new ProtocolViolationException(true, true, null, "")).
                       getDialogException(false),
                       Check.PERSISTENT, Check.REMOTE, Check.NO_SIDE_EFFECT);
        checkException((
            new ProtocolViolationException(true, true, null, "")).
                       getDialogException(true),
                       Check.PERSISTENT, Check.REMOTE, Check.HAS_SIDE_EFFECT);
    }

    private void checkException(DialogException exception,
                                Check exceptionType,
                                Check localOrRemote,
                                Check hasSideEffect) {
        switch(exceptionType) {
        case PERSISTENT:
            assertInstanceOf(PersistentDialogException.class, exception);
            break;
        case TEMPORARY:
            assertInstanceOf(TemporaryDialogException.class, exception);
            break;
        case UNKNOWN:
            assertInstanceOf(DialogUnknownException.class, exception);
            break;
        default:
            throw new AssertionError();
        }
        switch(localOrRemote) {
        case LOCAL:
            assertEquals("fromRemote mismatch: " + exception,
                    false, exception.fromRemote());
            break;
        case REMOTE:
            assertEquals("fromRemote mismatch: " + exception,
                    true, exception.fromRemote());
            break;
        default:
            throw new AssertionError();
        }
        switch(hasSideEffect) {
        case HAS_SIDE_EFFECT:
            assertEquals("hasSideEffect mismatch: " + exception,
                    true, exception.hasSideEffect());
            break;
        case NO_SIDE_EFFECT:
            assertEquals("hasSideEffect mismatch: " + exception,
                    false, exception.hasSideEffect());
            break;
        default:
            throw new AssertionError();
        }
    }

    private void assertInstanceOf(Class<?> cls, Object obj) {
        assertTrue(String.format(
                    "Wrong exception type, expected=%s, got=%s",
                    cls, obj != null ? obj.getClass() : "null"),
                cls.isInstance(obj));
    }

    @Test
    public void testConnectionUserException() {
        final boolean runningAlone =
            isRunningAlone("testConnectionUserException");
        ConnectionException ce =
            new ConnectionEndpointShutdownException(true, true, null, "msg");
        assertInstanceOf(IOException.class, ce.getUserException());

        ce = new ConnectionHandlerShutdownException(null);
        assertInstanceOf(IOException.class, ce.getUserException());

        IOException ioe = new IOException("cause");
        ce = new ConnectionIOException(true, null, ioe);
        assertSame(ioe, ce.getUserException());

        ioe = new ConnectException("msg");
        final NetworkAddress addr = new InetNetworkAddress("host", 5000);
        ce = new ConnectionIOException(true, () -> addr.toString(), ioe);
        printUserExceptionIfRunningAlone(runningAlone, ce);
        assertInstanceOf(ConnectException.class, ce.getUserException());
        assertTrue(ce.getUserException().getMessage(),
                   ce.getUserException().getMessage().contains(
                       addr.toString()));

        ce = new ConnectionIdleException(true, true, null, "msg");
        assertInstanceOf(IOException.class, ce.getUserException());

        ce = new ConnectionIncompatibleException(true, null, "msg");
        assertInstanceOf(IllegalStateException.class, ce.getUserException());

        ce = new ConnectionNotEstablishedException(null);
        assertInstanceOf(IOException.class, ce.getUserException());

        ce = new ConnectionTimeoutException(true, true, true, null, "msg");
        assertInstanceOf(IOException.class, ce.getUserException());

        Throwable cause = new Error();
        ce = new ConnectionUnknownException(true, null, cause);
        assertSame(cause, ce.getUserException());

        cause = new Exception();
        ce = new ConnectionUnknownException(true, null, cause);
        assertSame(cause, ce.getUserException());

        ce = new ConnectionUnknownException(true, null, "msg");
        assertInstanceOf(IllegalStateException.class, ce.getUserException());

        ce = new InitialConnectIOException(
            () -> addr.toString(), new IOException("msg"));
        printUserExceptionIfRunningAlone(runningAlone, ce);
        assertInstanceOf(IOException.class, ce.getUserException());
        assertTrue(ce.getUserException().getMessage().
            contains(RegistryUtils.POSSIBLE_SECURITY_MISMATCH_MESSAGE));
        assertTrue(ce.getUserException().
            getMessage().contains(addr.toString()));

        ce = new InitialHandshakeIOException(
            () -> addr.toString(), new IOException("msg"));
        printUserExceptionIfRunningAlone(runningAlone, ce);
        assertInstanceOf(IOException.class, ce.getUserException());
        assertTrue(ce.getUserException().getMessage().
            contains(RegistryUtils.POSSIBLE_ASYNC_MISMATCH_MESSAGE));
        assertTrue(ce.getUserException().
            getMessage().contains(addr.toString()));
    }

    private void printUserExceptionIfRunningAlone(boolean runningAlone,
                                                  ConnectionException ce) {
        if (!runningAlone) {
            return;
        }
        System.out.println(
            String.format("%s -> %s",
                          ce.getClass().getSimpleName(),
                          ce.getUserException()));
    }

    @Test
    public void testDialogUnderlyingAndUserException() {
        testDialogUnderlyingAndUserExceptionWithCause(
            cause -> new PersistentDialogException(
                false, false, "msg", cause));

        DialogException de = new DialogLimitExceededException(1);
        assertInstanceOf(IllegalStateException.class, de.getUserException());

        testDialogUnderlyingAndUserExceptionWithCause(
            cause -> new TemporaryDialogException(false, false, "msg", cause));

        de = new DialogNoSuchTypeException("msg");
        assertInstanceOf(IOException.class, de.getUserException());

        testDialogUnderlyingAndUserExceptionWithCause(
            cause -> new DialogUnknownException(false, false, "msg", cause));
    }

    private void testDialogUnderlyingAndUserExceptionWithCause(
        Function<Throwable, DialogException> createException) {

        Throwable cause = null;
        DialogException de = createException.apply(cause);
        assertSame(de, de.getUnderlyingException());
        assertInstanceOf(IllegalStateException.class, de.getUserException());

        cause = new Error();
        de = createException.apply(cause);
        assertSame(cause, de.getUnderlyingException());
        assertSame(cause, de.getUserException());

        cause = new Exception();
        de = createException.apply(cause);
        assertSame(cause, de.getUnderlyingException());
        assertInstanceOf(IllegalStateException.class, de.getUserException());

        cause = new IOException();
        de = createException.apply(cause);
        assertSame(cause, de.getUnderlyingException());
        assertSame(cause, de.getUnderlyingException());

        cause = new ConnectionEndpointShutdownException(
            true, false, null, "Hi");
        de = createException.apply(cause);
        assertSame(de, de.getUnderlyingException());
        assertInstanceOf(IOException.class, de.getUserException());

        IOException cause2 = new IOException("cause2");
        cause = new ConnectionIOException(true, null, cause2);
        de = createException.apply(cause2);
        assertSame(cause2, de.getUnderlyingException());
        assertSame(cause2, de.getUserException());

        Throwable throwable = new Error("error");
        cause = new ConnectionUnknownException(true, null, throwable);
        de = createException.apply(cause);
        assertSame(throwable, de.getUnderlyingException());
        assertSame(throwable, de.getUserException());
    }

    @Test
    public void testContextUserException() {
        Throwable cause = new Error();
        ContextException ce = new ContextAbortedException(cause);
        assertSame(cause, ce.getUserException());

        cause = new Exception();
        ce = new ContextAbortedException(cause);
        assertSame(cause, ce.getUserException());

        ce = new ContextAbortedException(null);
        assertInstanceOf(IllegalStateException.class, ce.getUserException());

        IOException ioe = new IOException();
        ce = new ContextWriteIOException(ioe);
        assertSame(ioe, ce.getUserException());
    }

    @Test
    public void testContextWriteUserException() {
        ContextWriteException
            cwe = new ContextWriteExceedsLimitException(1, 2);
        assertInstanceOf(IllegalStateException.class, cwe.getUserException());

        cwe = new ContextWriteFinException();
        assertInstanceOf(IllegalStateException.class, cwe.getUserException());
    }

    @Test
    public void testConnectionExceptionMessage() {
        final Function<String, String> expectedMessageSupplier =
            (msg) -> String.format(
                "Problem with channel (channel): %s", msg);
        final String expectedMessage = expectedMessageSupplier.apply("msg");
        ConnectionException ce =
            new ConnectionEndpointShutdownException(
                true, true, () -> "channel", "msg");
        assertEquals(expectedMessage, ce.getMessage());

        ce = new ConnectionHandlerShutdownException(() -> "channel");
        assertEquals(
            expectedMessageSupplier.apply("Connection handler is unavailable"),
            ce.getMessage());

        IOException ioe = new IOException("msg");
        ce = new ConnectionIOException(true, () -> "channel", ioe);
        assertEquals(expectedMessage, ce.getMessage());

        ioe = new ConnectException("msg");
        ce = new ConnectionIOException(true, () -> "channel", ioe);
        assertEquals(expectedMessage, ce.getMessage());

        ce = new ConnectionIdleException(true, true, () -> "channel", "msg");
        assertEquals(expectedMessage, ce.getMessage());

        ce = new ConnectionIncompatibleException(true, () -> "channel", "msg");
        assertEquals(expectedMessage, ce.getMessage());

        ce = new ConnectionNotEstablishedException(() -> "channel");
        assertEquals(
            expectedMessageSupplier.apply(
                "No existing connection for responder endpoint"),
            ce.getMessage());

        ce = new ConnectionTimeoutException(
            true, true, true, () -> "channel", "msg");
        assertEquals(expectedMessage, ce.getMessage());

        Throwable cause = new Error("msg");
        ce = new ConnectionUnknownException(true, () -> "channel", cause);
        assertEquals(expectedMessage, ce.getMessage());

        cause = new Exception("msg");
        ce = new ConnectionUnknownException(true, () -> "channel", cause);
        assertEquals(expectedMessage, ce.getMessage());

        ce = new ConnectionUnknownException(true, () -> "channel", "msg");
        assertEquals(expectedMessage, ce.getMessage());

        ce = new InitialConnectIOException(
            () -> "channel", new IOException("msg"));
        assertEquals(expectedMessage, ce.getMessage());
    }

}
