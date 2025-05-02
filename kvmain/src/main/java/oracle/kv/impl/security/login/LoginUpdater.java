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
package oracle.kv.impl.security.login;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;

/**
 * A class to help update the global and service-based security configuration
 * of components once there are changes of global and service parameters.
 */
public class LoginUpdater {

    private final Set<GlobalParamsUpdater> gpUpdaters =
        Collections.synchronizedSet(new HashSet<GlobalParamsUpdater>());

    private final Set<ServiceParamsUpdater> spUpdaters =
        Collections.synchronizedSet(new HashSet<ServiceParamsUpdater>());

    public void addGlobalParamsUpdaters(GlobalParamsUpdater... updaters) {
        for (final GlobalParamsUpdater updater : updaters) {
            gpUpdaters.add(updater);
        }
    }

    public void addServiceParamsUpdaters(ServiceParamsUpdater... updaters) {
        for (final ServiceParamsUpdater updater : updaters) {
            spUpdaters.add(updater);
        }
    }

    /**
     * Global parameter change listener. Registered globalParams updaters will
     * be notified of the changes.
     */
    public class GlobalParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            if (oldMap != null) {
                final ParameterMap filtered =
                    oldMap.diff(newMap, true /* notReadOnly */).
                        filter(EnumSet.of(ParameterState.Info.SECURITY));
                if (filtered.isEmpty()) {
                    return;
                }
            }
            for (final GlobalParamsUpdater gpUpdater : gpUpdaters) {
                gpUpdater.newGlobalParameters(newMap);
            }
        }
    }

    /**
     * Service parameter change listener. Registered serviceParams updaters
     * will be notified of the changes.
     */
    public class ServiceParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            for (final ServiceParamsUpdater spUpdater : spUpdaters) {
                spUpdater.newServiceParameters(newMap);
            }
        }
    }

    /**
     * Updater interface for global parameters changes.
     */
    public interface GlobalParamsUpdater {
        void newGlobalParameters(ParameterMap map);
    }

    /**
     * Updater interface for service parameters changes.
     */
    public interface ServiceParamsUpdater {
        void newServiceParameters(ParameterMap map);
    }
}
