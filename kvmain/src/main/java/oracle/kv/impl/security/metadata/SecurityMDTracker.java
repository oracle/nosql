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

package oracle.kv.impl.security.metadata;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a simple class that tracks SecurityMDListener objects that register
 * interest in notification that security metadata change has occurred.
 */
public class SecurityMDTracker {

    /**
     * Use a set to avoid the need to handle duplicates.
     */
    private final Set<SecurityMDListener> listeners;

    public SecurityMDTracker() {
        listeners =
            Collections.synchronizedSet(new HashSet<SecurityMDListener>());
    }

    public void addListener(SecurityMDListener listener) {
        listeners.add(listener);
    }

    public void removeListener(SecurityMDListener listener) {
        listeners.remove(listener);
    }

    public void notifyListeners(SecurityMDChange mdChange) {
        notify(mdChange);
    }

    private void notify(SecurityMDChange mdChange) {
        synchronized(listeners) {
            for (SecurityMDListener listener : listeners) {
                listener.notifyMetadataChange(mdChange);
            }
        }
    }
}
