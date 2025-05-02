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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Maintains the map of dialog handler factories.
 */
public class DialogHandlerFactoryMap {

    /** The active registered dialog handler factories */
    private final Map<Integer, DialogHandlerFactory> activeFactories =
        new ConcurrentHashMap<>();
    /**
     * The known dialog types. This is used to distinguish between dialog types
     * that are unknown which indicates a bug or imcompatibility issue and that
     * are just deregistered due to service shutting down.
     */
    private final Set<Integer> allKnownDialogTypes =
        ConcurrentHashMap.newKeySet();

    public void put(int dialogType, DialogHandlerFactory factory) {
        activeFactories.put(dialogType, factory);
        allKnownDialogTypes.add(dialogType);
    }

    public @Nullable DialogHandlerFactory get(int dialogType) {
        return activeFactories.get(dialogType);
    }

    public void remove(int dialogType) {
        activeFactories.remove(dialogType);
    }

    public boolean isKnown(int dialogType) {
        return allKnownDialogTypes.contains(dialogType);
    }

    public boolean isActive(int dialogType) {
        return activeFactories.containsKey(dialogType);
    }

    public Collection<DialogHandlerFactory> getActiveFactories() {
        return activeFactories.values();
    }

    public Set<Integer> getActiveDialogTypes() {
        return activeFactories.keySet();
    }

    public boolean hasActiveDialogTypes() {
        return !activeFactories.isEmpty();
    }

    public String describeActiveDialogTypes() {
        return activeFactories.keySet().stream().map((k) -> k.toString())
            .collect(Collectors.joining(",", "[", "]"));
    }
}
