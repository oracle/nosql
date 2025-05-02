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

package oracle.kv.impl.async;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities for use with CompletableFuture, including various methods to help
 * with handling exceptions.
 *
 * <h3>Rules for exception handling when using CompletableFuture</h3>
 *
 * <p>CompletableFuture provides a way to return results, including exceptional
 * results, that is distinct from returning the result, or throwing the
 * exception, directly. To write dependable code, callers need to know whether
 * exceptions can be thrown directly or will always be returned via the
 * CompletableFuture. Rather than need to deal with both kinds of delivery
 * mechanisms, it seems simpler to require that exceptions always be delivered
 * through the CompletableFuture if one is provided.
 *
 * <p>CompletableFutures can be used by a method in one of two ways.
 *
 * <p>The first way is to have a method return a CompletableFuture. In this
 * mode, the method uses the future to return a result asynchronously. The
 * method implementation should guarantee that all exceptions are supplied via
 * a future rather than being thrown directly.
 *
 * <p>For example, suppose we have a method with this signature:
 *
 * {@literal
 * CompletableFuture<Integer> increment(int i);
 * }
 *
 * <p>To make sure that the method does not throw any exceptions directly, the
 * implementation could be written as:
 *
 * {@literal
 * CompletableFuture<Integer> increment(int i) {
 *     try {
 *         // Do the actual work and return the value
 *         return completedFuture(...);
 *     } catch (Throwable e) {
 *         // Return any exceptions
 *         return failedFuture(e);
 *     }
 * }
 * }
 *
 * <p>If this method is being called by another method that returns a
 * CompletableFuture, then the caller still needs to worry about exceptions
 * thrown before the method is called, including when generating its arguments:
 *
 * {@literal
 * CompletableFuture<Integer> callIncrement(int i) {
 *     try {
 *         // Need to do some work before calling increment...
 *        return increment(someComputation(i));
 *     } catch (Throwable e) {
 *         // ... so make sure to catch any exceptions it throws
 *         return failedFuture(e);
 *     }
 * }
 * }
 *
 * <p>There is no need to catch exceptions for the various future chaining
 * methods, which all arrange to pass exceptions down to the final future. For
 * example:
 *
 * {@literal
 * CompletableFuture<Integer> callIncrement(int i) {
 *      return increment(i)
 *         .whenComplete(this::afterIncrement);
 * }
 * }
 *
 * <p>Note that exceptions generated later in the chain of stages are not
 * delivered to earlier stages. If you want a caller to notice later failures,
 * then make sure to return the very last stage to the caller, or attach
 * whatever final action is needed to the last stage.
 *
 * <p>Note that unboxing can result in hidden exceptions, so deciding whether
 * exception handling is needed may require careful analysis.
 *
 * <p>The second way that a method might refer to a CompletableFuture is when
 * one is passed to a method so that the method can complete it when some
 * computation is done. In this case, the caller depends on the fact that the
 * method will complete the future in all cases, including exceptional ones.
 * For example:
 *
 * {@literal
 * void callWhenDone(CompletableFuture<Void> done) {
 *     try {
 *         // Do some work...
 *         // ...
 *         // ... then mark done...
 *         done.completed(null);
 *     } catch (Throwable e) {
 *         // ... and note any exceptions thrown
 *         done.failedFuture(e);
 *     }
 * }
 * }
 *
 * <p>Note that methods that do not accept or return CompletableFuture can
 * throw exceptions in the normal way. If these methods are called to operate
 * on CompletableFutures, the calling methods will insure that exceptions are
 * passed correctly.
 *
 * <p>For example, if following the earlier example, we have:
 *
 * {@literal
 * void afterIncrement(Integer i, Throwable e) {
 *     if (i < 0) {
 *         throw new IllegalArgumentException("Negative");
 *     }
 * }
 * }
 *
 * <h3>Handling wrapped exceptions when using CompletableFuture</h3>
 *
 * When methods on CompletableFuture are chained and pass exceptions to the
 * next stage in the computation, for example when using the exceptionally,
 * handle, or whenComplete methods, exceptions thrown by the handler for the
 * previous stage will be wrapped in CompletionException. To give the stages
 * direct access to the thrown exceptions, these CompletionExceptions need to
 * be unwrapped to replace them with the underlying cause. To make it
 * convenient to do this translation in a systematic way, callers of these
 * chaining methods can use the unwrapException (for functions) or
 * unwrapExceptionVoid (for consumers) methods.
 *
 * <p>Callers of CompletableFuture.get need to deal with additional exceptions
 * and may find the handleFutureGetException method useful.
 *
 * <h3>Handling checked exceptions</h3>
 *
 * The CompletableFuture methods do not throw checked exceptions. To handle
 * checked exceptions more conveniently, this class provides a set of wrapper
 * interfaces (CheckedBiConsumer, CheckedConsumer, etc.) that wrap checked
 * exceptions in CompletionException. It is easiest to use these wrapper
 * interfaces by calling the associated set of convenience methods: checked
 * (for functions and suppliers) and checkedVoid (for consumers).
 *
 * <h3>Noticing if futures are completed more than once</h3>
 *
 * CompletableFutures remember the first value that they are completed with,
 * ignoring subsequent values or exceptional results. To help check for coding
 * errors that might result in the same future being set more than once,
 * callers can use the checkedComplete or checkedCompleteExceptionally methods,
 * which log warnings if the future is set multiple times.
 */
/*
 * TODO: Consider using CompletableFuture.exceptionallyCompose when it becomes
 * available in Java 12
 */
public class FutureUtils {

    /**
     * Returns a new CompletableFuture that is already completed exceptionally
     * with the given exception.
     *
     * @param <U> the type of the value
     * @param ex the exception
     * @return the exceptionally completed CompletableFuture
     */
    /*
     * TODO: Use CompletableFuture.failedFuture when it becomes available in
     * Java 9.
     */
    /*
     * Null checking should prevent the exception from being null, but put a
     * null check in the code anyway just to be sure, and suppress the
     * resulting dead code warning
     */
    @SuppressWarnings("unused")
    public static <U> CompletableFuture<U> failedFuture(Throwable ex) {
        final CompletableFuture<U> future = new CompletableFuture<>();
        if (ex == null) {
            ex = new IllegalArgumentException("Exception must not be null");
        }
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Completes the future argument exceptionally with the specified exception
     * if exception is non-null, or else completes it normally with the
     * specified result.
     *
     * @param <T> the type of the future value
     * @param future the future to complete
     * @param result the result to complete with normally if exception is null
     * @param exception the exception to complete with exceptionally if
     * non-null
     * @return true if this invocation caused this CompletableFuture to
     * transition to a completed state, else false
     */
    public static <T> boolean complete(CompletableFuture<T> future,
                                       @Nullable T result,
                                       @Nullable Throwable exception) {
        return (exception != null) ?
            future.completeExceptionally(exception) :
            future.complete(result);
    }

    /**
     * Sets the result of the future to the specified value and logs a warning
     * if the future is already complete.
     *
     * @param <T> the type of the future value
     * @param future the future to complete
     * @param value the result value
     * @param logger the logger for logging warnings
     * @return true if this invocation caused this CompletableFuture to
     * transition to a completed state, else false
     */
    public static <T> boolean checkedComplete(CompletableFuture<T> future,
                                              @Nullable T value,
                                              Logger logger) {
        final boolean set = future.complete(value);
        if (!set) {
            final StringBuilder msg = new StringBuilder();
            msg.append("Failed to complete future using value ")
                .append(value)
                .append(", ");
            getAlreadyCompletedMessage(future, msg);
            logger.log(Level.WARNING, msg.toString(), new Throwable());
        }
        return set;
    }

    /**
     * Sets the exceptional result of the future to the specified exception and
     * logs a warning if the future is already complete.
     *
     * @param future the future to complete
     * @param exception the exceptional result
     * @param logger the logger for logging warnings
     * @return true if this invocation caused this CompletableFuture to
     * transition to a completed state, else false
     */
    public static
        boolean checkedCompleteExceptionally(CompletableFuture<?> future,
                                             Throwable exception,
                                             Logger logger) {
        final boolean set = future.completeExceptionally(exception);
        if (!set) {
            final StringBuilder msg = new StringBuilder();
            msg.append("Failed to complete future using exception ")
                .append(exception)
                .append(", ");
            getAlreadyCompletedMessage(future, msg);
            logger.log(Level.WARNING, msg.toString(), new Throwable());
        }
        return set;
    }

    private static void getAlreadyCompletedMessage(CompletableFuture<?> future,
                                                   StringBuilder msg) {
        if (future.isCancelled()) {
            msg.append("future was canceled");
        } else {
            try {
                final Object oldValue = future.getNow(null);
                msg.append("future was completed: ")
                    .append(oldValue);
            } catch (Throwable e) {
                msg.append("future was completed exceptionally: ")
                    .append(unwrapException(e));
            }
        }
    }

    /**
     * If the argument is a CompletionException, and its cause is not null,
     * return its cause, otherwise return the argument. Use this method in
     * places that want to handle any exceptions thrown during completion using
     * the original exception. Note that this method only unwraps one level of
     * CompletionException because CompletableFuture should only wrap
     * exceptions once in CompletionException.
     *
     * @param e the exception
     * @return the possibly unwrapped exception
     */
    public static Throwable unwrapException(Throwable e) {
        if ((e instanceof CompletionException) &&
            (e.getCause() != null)) {
            e = e.getCause();
        }
        return e;
    }

    /**
     * If the argument is a CompletionException, return its cause, and
     * otherwise return the argument, both of which can be null. Use this
     * method in places that want to handle any exceptions thrown during
     * completion using the original exception. Note that this method only
     * unwraps one level of CompletionException because CompletableFuture
     * should only wrap exceptions once in CompletionException.
     *
     * @param e the exception or null
     * @return the possibly unwrapped exception or null
     */
    private static
        @Nullable Throwable unwrapExceptionOrNull(@Nullable Throwable e)
    {
        return (e instanceof CompletionException) ? e.getCause() : e;
    }

    /**
     * Returns a function that unwraps CompletionExceptions before passing them
     * on to the specified function. Use this method, for example, to wrap a
     * function passed to CompletableFunction.handle so that it will receive
     * unwrapped exceptions.
     *
     * @param <T> the future result type
     * @param <U> the function return type
     * @param function the function
     * @return a function that unwraps exceptions
     */
    public static <T, U> BiFunction<T, Throwable, U>
        unwrapException(BiFunction<T, Throwable, U> function)
    {
        return (t, e) -> function.apply(t, unwrapExceptionOrNull(e));
    }

    /**
     * Returns a function that unwraps CompletionExceptions before passing them
     * on to the specified function. Use this method, for example, to wrap a
     * function passed to CompletableFunction.exceptionally so that it will
     * receive unwrapped exceptions.
     *
     * @param <T> the function return type
     * @param function the function
     * @return a function that unwraps exceptions
     */
    public static <T> Function<Throwable, T>
        unwrapException(Function<Throwable, T> function)
    {
        return e -> function.apply(unwrapExceptionOrNull(e));
    }

    /**
     * Returns a consumer that unwraps CompletionExceptions before passing them
     * to the specified consumer. Use this method, for example, to wrap a
     * consumer passed to CompletableFuture.whenComplete so that it will
     * receive unwrapped exceptions.
     *
     * @param <T> the future result type
     * @param consumer the consumer
     * @return a consumer that unwraps exceptions
     */
    public static <T> BiConsumer<T, Throwable>
        unwrapExceptionVoid(BiConsumer<T, Throwable> consumer)
    {
        return (t, e) -> consumer.accept(t, unwrapExceptionOrNull(e));
    }

    /**
     * Returns a consumer that unwraps CompletionExceptions before passing them
     * on to the specified consumer, doing nothing if the exception is null.
     * Use this method, for example, to wrap a consumer only interested in
     * exceptions passed to CompletableFunction.whenComplete so that it will
     * receive unwrapped exceptions.
     *
     * @param <T> the future result type
     * @param consumer the exception consumer
     * @return a consumer that unwraps exceptions and only passes non-null
     * exceptions to the argument
     */
    public static <T> BiConsumer<T, Throwable>
        unwrapExceptionVoid(Consumer<Throwable> consumer)
    {
        return (t, e) -> {
            if (e != null) {
                consumer.accept(unwrapException(e));
            }
        };
    }

    /**
     * Handles exceptions thrown by a call to CompletableFuture.get(long,
     * TimeUnit). Throws the exception if it is an InterruptedException or
     * TimeoutException thrown while waiting for the result. Otherwise, checks
     * if the exception is a CompletionException and replaces it with the cause
     * if it is non-null. Then checks if the resulting exception is an
     * ExecutionException and replaces it with the cause if it is non-null.
     * Throws the resulting exception if it is a RuntimeException or Error,
     * returns it if it is a checked Exception, or else throws
     * IllegalStateException.
     *
     * @param exception the exception
     * @throws CancellationException if the future was canceled
     * @throws InterruptedException if the current thread was interrupted while
     * waiting
     * @throws TimeoutException if the wait timed out
     * @return the translated Exception
     */
    public static Exception handleFutureGetException(Throwable exception)
        throws InterruptedException, TimeoutException
    {
        if (exception instanceof InterruptedException) {
            throw (InterruptedException) exception;
        }
        if (exception instanceof TimeoutException) {
            throw (TimeoutException) exception;
        }
        if (exception instanceof CompletionException) {
            final Throwable cause = exception.getCause();
            if (cause != null) {
                exception = cause;
            }
        }
        if (exception instanceof ExecutionException) {
            final Throwable cause = exception.getCause();
            if (cause != null) {
                exception = cause;
            }
        }
        if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
        }
        if (exception instanceof Error) {
            throw (Error) exception;
        }
        if (exception instanceof Exception) {
            return (Exception) exception;
        }
        throw new IllegalStateException(
            "Unexpected exception: " + exception, exception);
    }

    /* Interfaces for wrapping checked exceptions in CompletionException */

    /**
     * Like {@link BiConsumer} but wraps all exceptions other than {@link
     * RuntimeException} and {@link Error} in {@link CompletionException} so
     * that this class can be used with {@link CompletableFuture}.
     *
     * @see #checkedVoid(CheckedBiConsumer)
     */
    @FunctionalInterface
    public interface CheckedBiConsumer<T, U> extends BiConsumer<T, U> {
        @Override
        default void accept(T t, U u) {
            try {
                acceptChecked(t, u);
            } catch (RuntimeException|Error e) {
                throw e;
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
        void acceptChecked(T t, U u) throws Throwable;
    }

    /**
     * Like {@link Consumer} but wraps all exceptions other than {@link
     * RuntimeException} and {@link Error} in {@link CompletionException} so
     * that this class can be used with {@link CompletableFuture}.
     *
     * @see #checkedVoid(CheckedConsumer)
     */
    @FunctionalInterface
    public interface CheckedConsumer<T> extends Consumer<T> {
        @Override
        default void accept(T t) {
            try {
                acceptChecked(t);
            } catch (RuntimeException|Error e) {
                throw e;
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
        void acceptChecked(T t) throws Throwable;
    }

    /**
     * Like {@link BiFunction} but wraps all exceptions other than {@link
     * RuntimeException} and {@link Error} in {@link CompletionException} so
     * that this class can be used with {@link CompletableFuture}.
     *
     * @see #checked(CheckedBiFunction)
     */
    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> extends BiFunction<T, U, R> {
        @Override
        default R apply(T t, U u) {
            try {
                return applyChecked(t, u);
            } catch (RuntimeException|Error e) {
                throw e;
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
        R applyChecked(T t, U u) throws Throwable;
    }

    /**
     * Like {@link Function} but wraps all exceptions other than {@link
     * RuntimeException} and {@link Error} in {@link CompletionException} so
     * that this class can be used with {@link CompletableFuture}.
     *
     * @see #checked(CheckedFunction)
     */
    @FunctionalInterface
    public interface CheckedFunction<T, R> extends Function<T, R> {
        @Override
        default R apply(T t) {
            try {
                return applyChecked(t);
            } catch (RuntimeException|Error e) {
                throw e;
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
        R applyChecked(T t) throws Throwable;
    }

    /**
     * Like {@link Supplier} but wraps all exceptions other than {@link
     * RuntimeException} and {@link Error} in {@link CompletionException} so
     * that this class can be used with {@link CompletableFuture}.
     *
     * @see #checked(CheckedSupplier)
     */
    @FunctionalInterface
    public interface CheckedSupplier<T> extends Supplier<T> {
        @Override
        default T get() {
            try {
                return getChecked();
            } catch (RuntimeException|Error e) {
                throw e;
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
        T getChecked() throws Throwable;
    }

    /**
     * Like {@link Runnable} but wraps all exceptions other than {@link
     * RuntimeException} and {@link Error} in {@link CompletionException} so
     * that this class can be used with {@link CompletableFuture}.
     *
     * @see #checkedVoid(CheckedRunnable)
     */
    @FunctionalInterface
    public interface CheckedRunnable extends Runnable {
        @Override
        default void run() {
            try {
                runChecked();
            } catch (RuntimeException|Error e) {
                throw e;
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
        void runChecked() throws Throwable;
    }

    /*
     * Convenience methods to convert checked exceptions
     *
     * Note that methods that return a value are all named "checked" and ones
     * that do not return a value are called "checkedVoid". The different names
     * are needed because the Java parser is not able to distinguish between
     * returning and non-returning overloadings when the implementation throws
     * an exception.
     */

    /**
     * Converts a {@link CheckedBiConsumer} to a {@link BiConsumer} so that an
     * instance that throws exceptions can be converted to an instance
     * that wraps checked exceptions in {@link CompletionException}.
     *
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     * @param consumer the CheckedBiConsumer
     * @return the consumer as a BiConsumer
     */
    public static <T, U> BiConsumer<T, U> checkedVoid(CheckedBiConsumer<T, U>
                                                      consumer) {
        return consumer;
    }

    /**
     * Converts a {@link CheckedConsumer} to a {@link Consumer} so that an
     * instance that throws exceptions can be converted to an instance that
     * wraps checked exceptions in {@link CompletionException}.
     *
     * @param <T> the type of the argument
     * @param consumer the CheckedConsumer
     * @return the consumer as a Consumer
     */
    public static <T> Consumer<T> checkedVoid(CheckedConsumer<T> consumer) {
        return consumer;
    }

    /**
     * Converts a {@link CheckedBiFunction} to a {@link BiFunction} so that an
     * instance that throws exceptions can be converted to an instance
     * that wraps checked exceptions in {@link CompletionException}.
     *
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     * @param <R> the type of the return value
     * @param function the CheckedBiFunction
     * @return the function as a BiFunction
     */
    public static <T, U, R>
        BiFunction<T, U, R> checked(CheckedBiFunction<T, U, R> function)
    {
        return function;
    }

    /**
     * Converts a {@link CheckedFunction} to a {@link Function} so that an
     * instance that throws exceptions can be converted to an instance that
     * wraps checked exceptions in {@link CompletionException}.
     *
     * @param <T> the type of the argument
     * @param <R> the type of the return value
     * @param function the CheckedFunction
     * @return the function as a Function
     */
    public static <T, R> Function<T, R> checked(CheckedFunction<T, R>
                                                function) {
        return function;
    }

    /**
     * Converts a {@link CheckedSupplier} to a {@link Supplier} so that an
     * instance that throws exceptions can be converted to an instance that
     * wraps checked exceptions in {@link CompletionException}.
     *
     * @param <T> the type of the return value
     * @param supplier the CheckedSupplier
     * @return the supplier as a Supplier
     */
    public static <T> Supplier<T> checked(CheckedSupplier<T> supplier) {
        return supplier;
    }

    /**
     * Converts a {@link CheckedRunnable} to a {@link Runnable} so that an
     * instance that throws exceptions can be converted to an instance that
     * wraps checked exceptions in {@link CompletionException}.
     *
     * @param runnable the CheckedRunnable
     * @return the runnable as a Runnable
     */
    public static Runnable checkedVoid(CheckedRunnable runnable) {
        return runnable;
    }

    /* End convenience methods to convert checked exceptions */

    /**
     * Returns a new CompletionFuture that, when the specified future completes
     * normally, is executed with the future's result as the argument to the
     * supplied function. If the specified future completes exceptionally, the
     * new future will complete exceptionally with the same exception, not one
     * wrapped in CompletionException.
     *
     * <p>This method is just like {@link CompletableFuture#thenApply}, but
     * makes sure that the exceptions are unwrapped before using them to
     * complete the resulting future exceptionally.
     *
     * @param <T> the result type of the supplied future
     * @param <R> the result type of the returned future
     * @param future the future whose results the function should be applied to
     * @param function the function to apply to the results of the supplied
     * future
     * @return the new future
     */
    public static <T, R>
        CompletableFuture<R> thenApply(CompletableFuture<T> future,
                                       Function<? super T, R> function) {
        requireNonNull(function);
        final CompletableFuture<R> result = new CompletableFuture<>();
        future.whenComplete(
            (t, e) -> {
                if (e != null) {
                    result.completeExceptionally(unwrapException(e));
                } else {
                    try {
                        result.complete(function.apply(t));
                    } catch (Throwable e2) {
                        result.completeExceptionally(unwrapException(e2));
                    }
                }
            });
        return result;
    }

    /**
     * Returns a new CompletableFuture with the same result or an unwrapped
     * version of the exception as the specified future that executes the given
     * action when the specified future completes. If the specified action
     * throws an exception, the new future will complete with that exception,
     * not one wrapped in CompletionException. If the action throws
     * CompletionException, then the new future will complete with that
     * exception's cause. Note that this method is only needed if the return
     * value of the whenComplete call is being used. If whenComplete is only
     * being called for side effect, then the CompletableFuture method is
     * sufficient.
     *
     * <p>When this stage is complete, the given action is invoked with the
     * result (or {@code null} if none) and the unwrapped exception (or {@code
     * null} if none) of this stage as arguments. The returned stage is
     * completed when the action returns. If the supplied action itself
     * encounters an exception, then the returned stage exceptionally completes
     * with this exception unless this stage also completed exceptionally.
     *
     * @param <T> the result type of the supplied future
     * @param action the action to perform
     * @return the new future
     */
    public static <T> CompletableFuture<T>
        whenComplete(CompletableFuture<T> future,
                     BiConsumer<? super T, ? super Throwable> action) {
        requireNonNull(action);
        final CompletableFuture<T> result = new CompletableFuture<>();
        future.whenComplete(
            (t, e) -> {
                e = unwrapExceptionOrNull(e);
                try {
                    action.accept(t, e);
                    complete(result, t, e);
                } catch (Throwable e2) {
                    result.completeExceptionally(unwrapException(e2));
                }
            });
        return result;
    }
}
