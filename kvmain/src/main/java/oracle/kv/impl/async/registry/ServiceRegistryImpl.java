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

package oracle.kv.impl.async.registry;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.util.SerialVersion;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Server implementation of the {@link ServiceRegistry} interface.
 *
 * @see ServiceRegistryAPI
 */
public class ServiceRegistryImpl implements ServiceRegistry {

    private final Logger logger;

    private final Map<String, ServiceEndpoint> registry =
        Collections.synchronizedMap(new HashMap<>());

    /**
     * Creates an instance of this class.
     *
     * @param logger for debug logging
     */
    public ServiceRegistryImpl(Logger logger) {
        logger.log(Level.FINE, "Created {0}", this);
        this.logger = logger;
    }

    /**
     * Returns a responder (server-side) dialog handler for this instance.
     */
    public DialogHandler createDialogHandler() {
        return new ServiceRegistryResponder(this, logger);
    }

    @Override
    public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                     long timeoutMillis) {

        return completedFuture(SerialVersion.CURRENT);
    }

    @Override
    public CompletableFuture<ServiceEndpoint> lookup(short serialVersion,
                                                     @Nullable String name,
                                                     long timeout) {
        try {
            if (name == null) {
                throw new IllegalArgumentException(
                    "Name parameter must not be null");
            }
            final ServiceEndpoint result = registry.get(name);
            logger.log(Level.FINEST,
                       () -> String.format(
                           "ServiceRegistryImpl.lookup" +
                           " this=%s" +
                           " name=%s" +
                           " result=%s",
                           this,
                           name,
                           result));
            return completedFuture(result);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> bind(short serialVersion,
                                        String name,
                                        ServiceEndpoint endpoint,
                                        long timeout) {
        try {
            logger.log(Level.FINEST,
                       () -> String.format(
                           "ServiceRegistryImpl.bind" +
                           " this=%s" +
                           " name=%s" +
                           " endpoint=%s",
                           this,
                           name,
                           endpoint));
            if ((name == null) || (endpoint == null)) {
                throw new IllegalArgumentException(
                    "Name and endpoint parameters must not be null");
            }
            registry.put(name, endpoint);
            return completedFuture(null);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> unbind(short serialVersion,
                                          @Nullable String name,
                                          long timeout) {
        try {
            logger.log(Level.FINEST,
                       () -> String.format(
                           "ServiceRegistryImpl.unbind" +
                           " this=%s" +
                           " name=%s",
                           this,
                           name));
            if (name == null) {
                throw new IllegalArgumentException(
                    "Name parameter must not be null");
            }
            registry.remove(name);
            return completedFuture(null);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> list(short serialVersion,
                                                long timeout) {
        try {
            final List<String> result;
            synchronized (registry) {
                result = new ArrayList<>(registry.keySet());
            }
            logger.log(Level.FINEST,
                       () -> String.format(
                           "ServiceRegistryImpl.list" +
                           "this=%s" +
                           "result=%s",
                           this,
                           result));
            return completedFuture(result);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    @Override
    public String toString() {
        return "ServiceRegistryImpl@" + Integer.toHexString(hashCode());
    }
}
