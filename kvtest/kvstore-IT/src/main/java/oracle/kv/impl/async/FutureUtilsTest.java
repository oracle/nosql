/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.FutureUtils.checkedComplete;
import static oracle.kv.impl.async.FutureUtils.checkedCompleteExceptionally;
import static oracle.kv.impl.async.FutureUtils.checkedVoid;
import static oracle.kv.impl.async.FutureUtils.complete;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;
import static oracle.kv.impl.async.FutureUtils.thenApply;
import static oracle.kv.impl.async.FutureUtils.unwrapException;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.async.FutureUtils.whenComplete;
import static oracle.kv.impl.util.TestUtils.compareLevels;
import static oracle.kv.impl.util.TestUtils.removeIfCount;
import static oracle.kv.util.TestUtils.checkCause;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;

import oracle.kv.TestBase;
import oracle.kv.impl.async.FutureUtils.CheckedRunnable;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.Test;

public class FutureUtilsTest extends TestBase {

    private static final Formatter logFormatter = new SimpleFormatter();

    /**
     * Contains log records at level WARNING or higher that were logged during
     * the current test. Tests that expect WARNING or SEVERE logging should
     * filter out the expected records. Any remaining records will cause test
     * failures.
     */
    private static List<LogRecord> warnings = new ArrayList<>();

    private static final Handler warningsHandler = new StreamHandler() {
        @Override
        public synchronized void publish(@Nullable LogRecord record) {
            if ((record != null) &&
                (compareLevels(record.getLevel(), Level.WARNING) >= 0))
            {
                warnings.add(record);
            }
        }
    };

    @Override
    public void setUp() throws Exception {
        logger.addHandler(warningsHandler);
        warnings.clear();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        logger.removeHandler(warningsHandler);
        if (!warnings.isEmpty()) {
            fail("Unexpected warning or severe logging:\n" +
                 warnings.stream()
                 .map(logFormatter::format)
                 .collect(Collectors.joining()));
        }
    }

    /* Tests */

    @SuppressWarnings("null")
    @Test
    public void testFailedFuture() {
        checkCompletedExceptionally(failedFuture(null),
                                    IllegalArgumentException.class,
                                    "Exception must not be null");

        final Consumer<Throwable> test = t ->
            checkCompletedExceptionally(failedFuture(t),
                                        t.getClass(), t.getMessage());
        test.accept(new AssertionError("foo"));
        test.accept(new IOException("bar"));
        test.accept(new IllegalArgumentException("baz"));
        test.accept(new Throwable("quux"));
    }

    @SuppressWarnings("null")
    @Test
    public void testComplete() throws Exception {
        checkException(() -> complete(null, null, null),
                       NullPointerException.class);
        checkComplete(null, null);
        checkComplete("Hi", null);
        checkComplete(3, null);
        checkComplete(null, new AssertionError("foo"));
        checkComplete(null, new IOException("bar"));
        checkComplete(null, new IllegalArgumentException("baz"));
        checkComplete(null, new Throwable("quux"));
        checkComplete("a", new IllegalStateException("b"));
    }

    private static <T> void checkComplete(@Nullable T result,
                                          @Nullable Throwable exception)
        throws Exception
    {
        final CompletableFuture<T> future = new CompletableFuture<>();
        complete(future, result, exception);
        if (exception != null) {
            checkCompletedExceptionally(future, exception.getClass(),
                                        exception.getMessage());
        } else {
            assertEquals(result, future.get());
        }
    }

    @Test
    public void testCheckedComplete() throws Exception {
        final CompletableFuture<String> future = new CompletableFuture<>();
        assertTrue(checkedComplete(future, "furby", logger));
        assertEquals("furby", future.get());
        assertEquals(emptyList(), warnings);

        assertFalse(checkedComplete(future, "gumby", logger));
        assertEquals("furby", future.get());
        assertEquals(1, removeWarnings("future was completed"));

        final CompletableFuture<String> future2 = failedFuture(
            new IllegalArgumentException("furby"));
        assertFalse(checkedComplete(future2, "gumby", logger));
        checkCause(checkException(() -> future2.get(),
                                      ExecutionException.class),
                   IllegalArgumentException.class,
                   "furby");
        assertEquals(1, removeWarnings("future was completed exceptionally"));

        final CompletableFuture<String> future3 = new CompletableFuture<>();
        future3.cancel(false);
        assertFalse(checkedComplete(future3, "gumby", logger));
        checkException(() -> future3.get(),
                       CancellationException.class);
        assertEquals(1, removeWarnings("future was canceled"));
    }

    @Test
    public void testCheckedCompleteExceptionally() throws Exception {
        final CompletableFuture<String> future = new CompletableFuture<>();
        assertTrue(checkedCompleteExceptionally(
                       future, new IllegalArgumentException("furby"), logger));
        checkCause(checkException(() -> future.get(),
                                  ExecutionException.class),
                   IllegalArgumentException.class, "furby");
        assertEquals(emptyList(), warnings);

        assertFalse(checkedCompleteExceptionally(
                        future, new IllegalStateException("gumby"), logger));
        checkCause(checkException(() -> future.get(),
                                  ExecutionException.class),
                   IllegalArgumentException.class, "furby");
        assertEquals(1, removeWarnings("future was completed exceptionally"));

        final CompletableFuture<String> future2 = completedFuture("furby");
        assertFalse(checkedCompleteExceptionally(
                        future2, new IllegalStateException("gumby"), logger));
        assertEquals("furby", future2.get());
        assertEquals(1, removeWarnings("future was completed"));

        final CompletableFuture<String> future3 = new CompletableFuture<>();
        future3.cancel(false);
        assertFalse(checkedCompleteExceptionally(
                        future3, new IllegalStateException("gumby"), logger));
        checkException(() -> future3.get(),
                       CancellationException.class);
        assertEquals(1, removeWarnings("future was canceled"));
    }

    @Test
    public void testUnwrapException() {
        assertSame(3,
                   unwrapException((t, e) -> {
                           assertEquals("foo", t);
                           assertEquals(null, e);
                           return 3;
                       })
                   .apply("foo", null));
        assertSame(4,
                   unwrapException((t, e) -> {
                           assertEquals(null, t);
                           assertEquals(null, e);
                           return 4;
                       })
                   .apply(null, null));
        Throwable exception = new IllegalStateException("foo");
        assertSame(5,
                   unwrapException((t, e) -> {
                           assertEquals(null, t);
                           assertEquals(exception, e);
                           return 5;
                       })
                   .apply(null, exception));
        Throwable exception2 = new CompletionException(exception);
        assertSame(6,
                   unwrapException((t, e) -> {
                           assertEquals(null, t);
                           assertEquals(exception, e);
                           return 6;
                       })
                   .apply(null, exception2));
        checkException(
            () -> unwrapException((t, e) -> {
                    assertEquals(null, t);
                    assertEquals(exception, e);
                    throw new IllegalArgumentException("bar");
                })
            .apply(null, exception2),
            IllegalArgumentException.class, "bar");
    }

    @Test
    public void testUnwrapExceptionExceptOnly() {
        assertSame(3,
                   unwrapException(e -> {
                           assertEquals(null, e);
                           return 3;
                       })
                   .apply(null));
        Throwable exception = new IllegalStateException("foo");
        assertSame(4,
                   unwrapException(e -> {
                           assertEquals(exception, e);
                           return 4;
                       })
                   .apply(exception));
        Throwable exception2 = new CompletionException(exception);
        assertSame(5,
                   unwrapException(e -> {
                           assertEquals(exception, e);
                           return 5;
                       })
                   .apply(exception2));
        checkException(
            () -> unwrapException(e -> {
                    assertEquals(exception, e);
                    throw new IllegalArgumentException("bar");
                })
            .apply(exception2),
            IllegalArgumentException.class, "bar");
    }

    @Test
    public void testUnwrapExceptionVoid() {
        unwrapExceptionVoid((t, e) -> {
                assertEquals("foo", t);
                assertEquals(null, e);
            })
            .accept("foo", null);
        unwrapExceptionVoid((t, e) -> {
                assertEquals(null, t);
                assertEquals(null, e);
            })
            .accept(null, null);
        Throwable exception = new IllegalStateException("foo");
        unwrapExceptionVoid((t, e) -> {
                assertEquals(null, t);
                assertEquals(exception, e);
            })
            .accept(null, exception);
        Throwable exception2 = new CompletionException(exception);
        unwrapExceptionVoid((t, e) -> {
                assertEquals(null, t);
                assertEquals(exception, e);
            })
            .accept(null, exception2);
        checkException(
            () -> unwrapExceptionVoid((t, e) -> {
                    assertEquals(null, t);
                    assertEquals(exception, e);
                    throw new IllegalArgumentException("bar");
                })
            .accept(null, exception2),
            IllegalArgumentException.class, "bar");
    }

    @Test
    public void testUnwrapExceptionVoidExceptOnly() {
        unwrapExceptionVoid(e -> fail("Call unexpected"))
            .accept("foo", null);
        unwrapExceptionVoid(e -> fail("Call unexpected"))
            .accept(null, null);
        Throwable exception = new IllegalStateException("foo");
        unwrapExceptionVoid(e -> assertEquals(exception, e))
            .accept(null, exception);
        Throwable exception2 = new CompletionException(exception);
        unwrapExceptionVoid(e -> assertEquals(exception, e))
            .accept(null, exception2);
        checkException(
            () -> unwrapExceptionVoid(e -> {
                    assertEquals(exception, e);
                    throw new IllegalArgumentException("bar");
                })
            .accept(null, exception2),
            IllegalArgumentException.class, "bar");
    }

    @SuppressWarnings("null")
    @Test
    public void testHandleFutureGetExceptionNull() {
        checkException(() -> handleFutureGetException(null),
                       IllegalStateException.class,
                       "Unexpected exception");
    }

    @Test
    public void testHandleFutureGetExceptionCancellation() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        future.cancel(false);
        checkException(
            () -> {
                try {
                    future.get(1000, SECONDS);
                } catch (Throwable t) {
                    throw handleFutureGetException(t);
                }
            },
            CancellationException.class);
    }

    @Test
    public void testHandleFutureGetExceptionTimeout() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        checkException(
            () -> {
                try {
                    future.get(0, MILLISECONDS);
                } catch (Throwable t) {
                    throw handleFutureGetException(t);
                }
            },
            TimeoutException.class);
    }

    @Test
    public void testHandleFutureGetExceptionInterrupted() throws Exception {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final CompletableFuture<InterruptedException> thrown =
            new CompletableFuture<>();
        final Thread thread = new Thread(
            () -> {
                try {
                    try {
                        future.get(30, SECONDS);
                    } catch (Throwable t) {
                        throw handleFutureGetException(t);
                    }
                } catch (InterruptedException ie) {
                    thrown.complete(ie);
                } catch (Throwable t2) {
                    thrown.completeExceptionally(t2);
                }
            });
        thread.start();

        /*
         * Wait a little to make sure we interrupt the thread after it starts
         * waiting on the future -- no good way to coordinate this
         */
        Thread.sleep(1000);

        thread.interrupt();
        checkException(thrown.get(30, SECONDS), InterruptedException.class,
                       null);
    }

    @Test
    public void testHandleFutureGetException() {
        testHandleFutureGetException(
            new AssertionError("foo"),
            FutureUtilsTest::verifyHandleFutureGetThrows);

        testHandleFutureGetException(
            new IOException("bar"),
            FutureUtilsTest::verifyHandleFutureGetReturns);

        testHandleFutureGetException(
            new IllegalArgumentException("baz"),
            FutureUtilsTest::verifyHandleFutureGetThrows);

        testHandleFutureGetException(
            new Throwable("quux"),
            FutureUtilsTest::verifyHandleFutureGetUnexpected);
    }

    @Test
    public void testCheckedBiConsumer() {
        checkedVoid((t, u) -> {
                assertEquals("abc", t);
                assertEquals("def", u);
            })
            .accept("abc", "def");

        final BiConsumer<Throwable, Boolean> test = (e, wrapped) ->
            checkException(
                () -> {
                    try {
                        checkedVoid((t, u) -> { throw e; }).accept(null, null);
                    } catch (CompletionException ce) {
                        if (wrapped) {
                            final Throwable cause = ce.getCause();
                            if (cause instanceof Exception) {
                                throw (Exception) cause;
                            }
                        }
                        throw ce;
                    }
                },
                e.getClass(), e.getMessage());

        test.accept(new AssertionError("foo"), false);
        test.accept(new IOException("bar"), true);
        test.accept(new IllegalArgumentException("baz"), false);
        test.accept(new Throwable("quux"), true);
        test.accept(new CompletionException("huh", null), false);
    }

    @Test
    public void testCheckedConsumer() {
        checkedVoid(t -> assertEquals("abc", t)).accept("abc");

        final BiConsumer<Throwable, Boolean> test = (e, wrapped) ->
            checkException(
                () -> {
                    try {
                        checkedVoid(t -> { throw e; }).accept(null);
                    } catch (CompletionException ce) {
                        if (wrapped) {
                            final Throwable cause = ce.getCause();
                            if (cause instanceof Exception) {
                                throw (Exception) cause;
                            }
                        }
                        throw ce;
                    }
                },
                e.getClass(), e.getMessage());

        test.accept(new AssertionError("foo"), false);
        test.accept(new IOException("bar"), true);
        test.accept(new IllegalArgumentException("baz"), false);
        test.accept(new Throwable("quux"), true);
        test.accept(new CompletionException("huh", null), false);
    }

    @Test
    public void testCheckedBiFunction() {
        assertEquals("xyz",
                     checked((t, u) -> {
                             assertEquals("abc", t);
                             assertEquals("def", u);
                             return "xyz";
                         })
                     .apply("abc", "def"));

        final BiConsumer<Throwable, Boolean> test = (e, wrapped) ->
            checkException(
                () -> {
                    try {
                        checked((t, u) -> { throw e; })
                            .apply(null, null);
                    } catch (CompletionException ce) {
                        if (wrapped) {
                            final Throwable cause = ce.getCause();
                            if (cause instanceof Exception) {
                                throw (Exception) cause;
                            }
                        }
                        throw ce;
                    }
                },
                e.getClass(), e.getMessage());

        test.accept(new AssertionError("foo"), false);
        test.accept(new IOException("bar"), true);
        test.accept(new IllegalArgumentException("baz"), false);
        test.accept(new Throwable("quux"), true);
        test.accept(new CompletionException("huh", null), false);
    }

    @Test
    public void testCheckedFunction() {
        assertEquals("xyz",
                     checked(t -> {
                             assertEquals("abc", t);
                             return "xyz";
                         })
                     .apply("abc"));

        final BiConsumer<Throwable, Boolean> test = (e, wrapped) ->
            checkException(
                () -> {
                    try {
                        checked(t -> { throw e; }).apply(null);
                    } catch (CompletionException ce) {
                        if (wrapped) {
                            final Throwable cause = ce.getCause();
                            if (cause instanceof Exception) {
                                throw (Exception) cause;
                            }
                        }
                        throw ce;
                    }
                },
                e.getClass(), e.getMessage());

        test.accept(new AssertionError("foo"), false);
        test.accept(new IOException("bar"), true);
        test.accept(new IllegalArgumentException("baz"), false);
        test.accept(new Throwable("quux"), true);
        test.accept(new CompletionException("huh", null), false);
    }

    @Test
    public void testCheckedSupplier() {
        assertEquals("xyz",
                     checked(() -> "xyz").get());

        final BiConsumer<Throwable, Boolean> test = (e, wrapped) ->
            checkException(
                () -> {
                    try {
                        checked(() -> { throw e; }).get();
                    } catch (CompletionException ce) {
                        if (wrapped) {
                            final Throwable cause = ce.getCause();
                            if (cause instanceof Exception) {
                                throw (Exception) cause;
                            }
                        }
                        throw ce;
                    }
                },
                e.getClass(), e.getMessage());

        test.accept(new AssertionError("foo"), false);
        test.accept(new IOException("bar"), true);
        test.accept(new IllegalArgumentException("baz"), false);
        test.accept(new Throwable("quux"), true);
        test.accept(new CompletionException("huh", null), false);
    }

    @Test
    public void testCheckedRunnable() {
        checkedVoid(() -> { }).run();

        final BiConsumer<Throwable, Boolean> test = (e, wrapped) ->
            checkException(
                () -> {
                    try {
                        checkedVoid(() -> { throw e; }).run();
                    } catch (CompletionException ce) {
                        if (wrapped) {
                            final Throwable cause = ce.getCause();
                            if (cause instanceof Exception) {
                                throw (Exception) cause;
                            }
                        }
                        throw ce;
                    }
                },
                e.getClass(), e.getMessage());

        test.accept(new AssertionError("foo"), false);
        test.accept(new IOException("bar"), true);
        test.accept(new IllegalArgumentException("baz"), false);
        test.accept(new Throwable("quux"), true);
        test.accept(new CompletionException("huh", null), false);
    }

    @SuppressWarnings("null")
    @Test
    public void testThenApply() throws Exception {
        checkException(() -> thenApply(
                           null,
                           s -> {
                               fail("Method should not be called");
                               return null;
                           }),
                       NullPointerException.class);

        final CompletableFuture<String> f = new CompletableFuture<>();
        checkException(() -> thenApply(f, null), NullPointerException.class);

        f.complete("hi");
        assertEquals("hi there", thenApply(f, s -> s + " there").get());

        assertEquals("there hi",
                     thenApply(f,
                               new Function<Object, Object>() {
                                   @Override
                                   public Object apply(Object o) {
                                       return "there " + o;
                                   }
                               })
                     .get());

        thenApply(failedFuture(new IllegalStateException("msg")),
                  s -> {
                      fail("Method should not be called");
                      return null;
                  })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IllegalStateException.class, "msg");
                    return null;
                })
            .get();

        thenApply(completedFuture("hi")
                  .whenComplete((s, e) -> {
                          assertEquals("hi", s);
                          assertEquals(null, e);
                          throw new IllegalStateException("msg");
                      }),
                  s -> {
                      fail("Method should not be called");
                      return null;
                  })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IllegalStateException.class, "msg");
                    return null;
                })
            .get();

        /* ThenApply doesn't call the function if the future failed */
        thenApply(failedFuture(new IllegalStateException("foo")),
                  s -> {
                      fail("Method should not be called");
                      return null;
                  })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IllegalStateException.class, "foo");
                    return null;
                })
            .get();

        thenApply(failedFuture(new IOException("msg")),
                  s -> {
                      fail("Method should not be called");
                      return null;
                  })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IOException.class, "msg");
                    return null;
                })
            .get();

        thenApply(completedFuture("hi"),
                  checked(s -> {
                          assertEquals("hi", s);
                          throw new IOException("msg");
                      }))
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IOException.class, "msg");
                    return null;
                })
            .get();
    }

    @SuppressWarnings("null")
    @Test
    public void testWhenComplete() throws Exception {
        checkException(() -> whenComplete(
                           null,
                           (t, e) -> fail("Method should not be called")),
                       NullPointerException.class);

        CompletableFuture<String> f = new CompletableFuture<>();
        checkException(() -> whenComplete(f, null),
                       NullPointerException.class);

        f.complete("hi");
        assertEquals("hi",
                     whenComplete(f, (s, e) -> {
                             assertEquals("hi", s);
                             assertEquals(null, e);
                         }).get());

        assertEquals("hi",
                     whenComplete(f,
                                  new BiConsumer<Object, Object>() {
                                      @Override
                                      public void accept(Object s,
                                                         Object e) {
                                          assertEquals("hi", s);
                                          assertEquals(null, e);
                                      }
                                  })
                     .get());

        whenComplete(failedFuture(new IllegalStateException("msg")),
                     (s, e) -> {
                         assertEquals(null, s);
                         checkException(e, IllegalStateException.class, "msg");
                     })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IllegalStateException.class, "msg");
                    return null;
                })
            .get();

        whenComplete(completedFuture("hi")
                     .whenComplete((s, e) -> {
                             assertEquals("hi", s);
                             assertEquals(null, e);
                             throw new IllegalStateException("msg");
                         }),
                     (s, e) -> { })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IllegalStateException.class, "msg");
                    return null;
                })
            .get();

        /*
         * WhenComplete calls the action if the future failed and completes
         * with the action's exception
         */
        whenComplete(failedFuture(new IllegalStateException("foo")),
                     (s, e) -> {
                         assertEquals(null, s);
                         checkException(e, IllegalStateException.class, "foo");
                         throw new IllegalStateException("bar");
                     })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IllegalStateException.class, "bar");
                    return null;
                })
            .get();

        whenComplete(failedFuture(new IOException("msg")),
                     (s, e) -> {
                         assertEquals(null, s);
                         checkException(e, IOException.class, "msg");
                     })
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IOException.class, "msg");
                    return null;
                })
            .get();

        whenComplete(completedFuture("hi"),
                     checkedVoid((s, e) -> {
                             assertEquals("hi", s);
                             assertEquals(null, e);
                             throw new IOException("msg");
                         }))
            .handle((s, e) -> {
                    assertEquals(null, s);
                    checkException(e, IOException.class, "msg");
                    return null;
                })
            .get();
    }

    /* Other methods and classes */

    /**
     * Check that future completed exceptionally with an exception of the
     * specified class.
     */
    public static void
        checkCompletedExceptionally(CompletableFuture<?> future,
                                    Class<? extends Throwable> exceptionClass)
    {
        checkCompletedExceptionally(future, exceptionClass, null);
    }

    /**
     * Check that future completed exceptionally with an exception of the
     * specified class, and that the exception message contains the specified
     * substring, if the specified value is not null.
     */
    public static void
        checkCompletedExceptionally(CompletableFuture<?> future,
                                    Class<? extends Throwable> exceptionClass,
                                    @Nullable String messageSubstring) {
        assertTrue("Future should be done", future.isDone());
        assertTrue("Future should be completed exceptionally",
                   future.isCompletedExceptionally());
        try {
            future.getNow(null);
            fail("Expected exception");
        } catch (CompletionException e) {
            checkException(e.getCause(), exceptionClass, messageSubstring);
        }
    }

    private static
        void testHandleFutureGetException(Throwable exception,
                                          VerifyHandleGetFutureException
                                          verifier) {
        verifier.verify(() -> doFutureGet(exception), exception);
        verifier.verify(() -> doFutureGetNow(exception), exception);
        verifier.verify(() -> doFutureHandleGet(exception), exception);
        verifier.verify(() -> doFutureHandleGetNow(exception), exception);
    }

    private static void doFutureGet(Throwable e) throws Exception {
        failedFuture(e).get();
    }

    private static void doFutureGetNow(Throwable e) {
        failedFuture(e).getNow(null);
    }

    private static void doFutureHandleGet(Throwable e) throws Exception {
        failedFuture(e)
            .handle(checked((v, t) -> { throw t; }))
            .get();
    }

    private static void doFutureHandleGetNow(Throwable e) {
        failedFuture(e)
            .handle(checked((v, t) -> { throw t; }))
            .getNow(null);
    }

    interface VerifyHandleGetFutureException {
        void verify(CheckedRunnable get,
                    Throwable exception);
    }

    private static void verifyHandleFutureGetThrows(CheckedRunnable get,
                                                    Throwable exception) {
        checkException(() -> tryHandleFutureGetException(get),
                       exception.getClass(), exception.getMessage());
    }

    private static void verifyHandleFutureGetReturns(CheckedRunnable get,
                                                     Throwable exception) {
        try {
            checkException(tryHandleFutureGetException(get),
                           exception.getClass(), exception.getMessage());
        } catch (InterruptedException|TimeoutException e) {
            throw new IllegalStateException("Unexpected exception: " + e, e);
        }
    }

    private static void verifyHandleFutureGetUnexpected(CheckedRunnable get,
                                                        Throwable exception) {
        checkCause(checkException(() -> tryHandleFutureGetException(get),
                                  IllegalStateException.class,
                                  "Unexpected exception"),
                   exception.getClass(), exception.getMessage());
    }

    private static
        @Nullable Exception tryHandleFutureGetException(CheckedRunnable get)
        throws InterruptedException, TimeoutException
    {
        try {
            get.run();
            return null;
        } catch (Throwable t) {
            return handleFutureGetException(t);
        }
    }

    private int removeWarnings(String message) {
        return removeIfCount(
            warnings,
            record -> record.getMessage().contains(message) &&
            (record.getLevel() == Level.WARNING));
    }
}
