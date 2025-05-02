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

package oracle.kv.impl.param;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a simple class that tracks ParameterListener objects that register
 * interest in notification that a parameter change has occurred.  A service
 * that has parameters that can be modified at run time will contain one of
 * these objects, and portions of that service can register ParameterListener
 * objects to be notified when a change occurs.
 *
 * See Admin.java and RepNodeService.java for examples of usage
 */
public class ParameterTracker {

    /**
     * Use a set to avoid the need to handle duplicates (LoggerUtils, notably).
     */
    private final Set<ParameterListener> listeners;

    public ParameterTracker() {
        listeners =
            Collections.synchronizedSet(new HashSet<ParameterListener>());
    }

    public void addListener(ParameterListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ParameterListener listener) {
        listeners.remove(listener);
    }

    public void notifyListeners(ParameterMap oldMap, ParameterMap newMap) {
        for (ParameterListener listener : listeners) {
            listener.newParameters(oldMap, newMap);
        }
    }
}
