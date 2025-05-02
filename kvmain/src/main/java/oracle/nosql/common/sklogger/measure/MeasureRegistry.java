/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger.measure;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.sklogger.ScheduleStart;

/**
 * Manages obtaining measure results from registered elements and processing
 * the results.
 *
 * <p>For each registered measure element, a watcher obtaining task can be
 * started which periodically obtains the result. Measure result processors
 * can be associated with the watcher so that the obtained results can be
 * processed.
 */
public class MeasureRegistry {

    /* The executor */
    protected final ScheduledExecutorService scheduledExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            (r) -> {
                final Thread t = new Thread(r, String.format(
                    "%sThread", MeasureRegistry.class.getSimpleName()));
                t.setDaemon(true);
                return t;
            });

    /* Registered measure elements */
    protected final Map<String, MeasureElement<?>> elementMap =
        new ConcurrentHashMap<>();
    /*
     * Watchers obtaining tasks, access under synchronization block of this
     * object.
     */
    protected final Map<String, Future<?>> obtainTasks =
        new HashMap<>();
    /* Processors associated with watchers */
    protected final Map<String, KeySetView<MeasureResultProcessor, Boolean>>
        processorMap = new ConcurrentHashMap<>();

    /**
     * Registers a {@link MeasureElement}.
     */
    public <E extends MeasureElement<?>> E register(String name, E element) {
        final MeasureElement<?> prev = elementMap.putIfAbsent(name, element);
        if (prev != null) {
            throw new IllegalArgumentException(
                String.format("Already registered element: %s", name));
        }
        return element;
    }

    /**
     * Unregisters an element.
     */
    public void unregister(String name) {
        elementMap.remove(name);
    }

    /**
     * Clear and stop all settings and processors from the registry.
     * This will stop any running tasks.
     * This is typically only used in tests.
     */
    public void clear() {
        for (String s : obtainTasks.keySet()) {
            stop(s);
        }
        obtainTasks.clear();
        processorMap.clear();
        elementMap.clear();
    }

    /**
     * Adds a {@link MeasureResultProcessor} to the watcher's processors.
     */
    public void add(String watcherName, MeasureResultProcessor processor) {
        getProcessors(watcherName).add(processor);
    }

    private KeySetView<MeasureResultProcessor, Boolean>
        getProcessors(String watcherName) {

        return processorMap.computeIfAbsent(
            watcherName,
            (k) -> ConcurrentHashMap.newKeySet());
    }

    /**
     * Unregisters a processor.
     */
    public void remove(String watcherName, MeasureResultProcessor processor) {
        getProcessors(watcherName).remove(processor);
    }

    /**
     * Starts a obtaining task for {@code watcherName}.
     *
     * <p>The task obtains the results from all elements with the specified
     * {@code watcherName}. It then feeds the results to all the processors
     * associated with the watcher.
     */
    public void start(String watcherName, long configuredIntervalMs) {
        synchronized(this) {
            /*
             * Removes the previous one if exists. We need to remove it becuase
             * it might have a different interval.
             */
            final Future<?> previous = obtainTasks.remove(watcherName);
            if (previous != null) {
                previous.cancel(true);
            }
            final long delayMs =
                ScheduleStart.calculateDelay(configuredIntervalMs);
            final Future<?> current = scheduledExecutor.scheduleAtFixedRate(
                new ObtainTask(watcherName),
                delayMs, configuredIntervalMs, TimeUnit.MILLISECONDS);
            obtainTasks.put(watcherName, current);
        }
    }

    public void stop(String watcherName) {
        synchronized(this) {
            final Future<?> future = obtainTasks.get(watcherName);
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    /**
     * Obtains results from registered elements and processes the results.
     */
    private class ObtainTask implements Runnable {

        private final String watcherName;

        private ObtainTask(String watcherName) {
            this.watcherName = watcherName;
        }

        @Override
        public void run() {
            final long timeMillis = System.currentTimeMillis();
            final KeySetView<MeasureResultProcessor, Boolean> processors =
                getProcessors(watcherName);

            final List<JsonSerializable> results;
            try {
                results = elementMap.values().stream()
                    .map((e) -> e.obtain(watcherName))
                    .filter((e) -> !e.isDefault())
                    .collect(Collectors.toList());
            } catch (Throwable t) {
                runProcessors(processors, (p) -> p.process(t), timeMillis);
                return;
            }
            runProcessors(
                processors,
                (p) ->  results.stream().forEach((r) -> p.process(r)),
                timeMillis);
        }

        private void runProcessors(
            Collection<MeasureResultProcessor> processors,
            Consumer<MeasureResultProcessor> consumer,
            long timeMillis) {

            processors.stream().forEach((p) -> {
                try {
                    p.prepare(timeMillis);
                    consumer.accept(p);
                } catch (Throwable t) {
                    p.process(t);
                } finally {
                    p.finish();
                }
            });
        }
    }
}
