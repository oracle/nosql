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

package com.sleepycat.je.util;

import java.util.concurrent.Callable;

/*This class is mostly useful for easier way to handle the 
 * exceptions in the stream.
 * Basically it converts the error to a failure object and 
 * then send it to downstream.
 * Don't use this method, if we want to throw a exception while 
 * stream processing 
 * Based on the VAVR try construct 
 */

public abstract class TryStream<T> {

    public static <T> TryStream<T> of(Callable<T> callable) {
        try {
            return new Success<>(callable.call());
        } catch (Throwable e) {
            return new Failure<>(e);
        }
    }

    public abstract boolean isSuccess();

    public abstract boolean isFailure();

    public abstract T get() throws Throwable;

    public abstract Throwable getException();

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {
        R apply(T t) throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws Throwable;
    }

    public <R> TryStream<R> map(ThrowingFunction<T, R> mapper) {
        if (isSuccess()) {
            try {
                return new Success<>(mapper.apply(get()));
            } catch (Throwable e) {
                return new Failure<>(e);
            }
        }
        // Propagate the same error downstream
        return new Failure<>(getException());
    }

    static class Success<T> extends TryStream<T> {
        private final T res;

        Success(T res) {
            this.res = res;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public boolean isFailure() {
            return false;
        }

        @Override
        public T get() {
            return res;
        }

        @Override
        public Exception getException() {
            throw new UnsupportedOperationException("Success cannot have exceptions");
        }
    }

    static class Failure<T> extends TryStream<T> {
        private final Throwable t;

        Failure(Throwable t) {
            this.t = t;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isFailure() {
            return true;
        }

        @Override
        public T get() throws Throwable {
            throw t;
        }

        @Override
        public Throwable getException() {
            return t;
        }
    }
}
