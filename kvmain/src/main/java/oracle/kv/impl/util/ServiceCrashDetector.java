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

package oracle.kv.impl.util;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A special service status tracker listener that detects when a service
 * restarts after a crash. The detector creates a file in the specified
 * directory, using the service ID and a distinguished suffix, when the service
 * reaches the STARTING state. The file is deleted when reaching the STOPPED,
 * ERROR_RESTARTING, or ERROR_NO_RESTART states. If the listener finds the file
 * already present when reaching the STARTING state, it sets the
 * startingAfterCrash field which causes getStartingAfterCrash to return true.
 *
 * <p>Callers should plan to use the logging directory to store the crash
 * detection file. For simplicity, only perform crash detection for configured
 * stores so that we can rely on using the same logging directory.
 */
@NonNullByDefault
public class ServiceCrashDetector implements ServiceStatusTracker.Listener {
    private final File filename;
    private final Logger logger;
    private volatile boolean startingAfterCrash;

    /**
     * Creates an instance.
     *
     * @param serviceId the resource ID of the service for which we are
     * detecting crashes
     * @param directory the directory for storing the crash detection file
     * @param logger the logger for logging debugging messages
     */
    public ServiceCrashDetector(ResourceId serviceId,
                                File directory,
                                Logger logger) {
        filename = new File(directory,
                            serviceId.getFullName() + "." +
                            FileNames.CRASH_DETECT_FILE_SUFFIX);
        this.logger = logger;
    }

    @Override
    public void update(@Nullable ServiceStatusChange prevChange,
                       @Nullable ServiceStatusChange newChange) {
        final ServiceStatus newStatus =
            checkNull("newChange", newChange).getStatus();
        if ((prevChange != null) && (prevChange.getStatus() == newStatus)) {
            return;
        }
        switch (newStatus) {
        case STARTING:
            startingAfterCrash = starting();
            break;
        case STOPPED:
        case ERROR_RESTARTING:
        case ERROR_NO_RESTART:
            startingAfterCrash = false;
            stopped();
            break;
        default:
            startingAfterCrash = false;
            break;
        }
    }

    /** Returns the pathname of the crash detection file. */
    public File getFilename() {
        return filename;
    }

    /**
     * Notify the detector that the service is starting. If a file with the
     * filename specified to the constructor is found, returns true to mean
     * that the service is restarting after a crash that prevented the service
     * from shutting down cleanly.
     */
    boolean starting() {
        try {
            return !filename.createNewFile();
        } catch (IOException e) {
            logger.log(Level.INFO,
                       "Failed to create service crash detector file: " + e,
                       e);
            return false;
        }
    }

    /**
     * Notify the detector that the service has been stopped cleanly, returning
     * whether the crash detection file was deleted successfully.
     */
    boolean stopped() {
        if (filename.delete()) {
            return true;
        }
        logger.info("Unable to delete service crash detector file");
        return false;
    }

    /**
     * Returns whether the last update call to the detector was for a
     * transition to the STARTING state where the detector noticed that the
     * service had crashed.
     */
    public boolean getStartingAfterCrash() {
        return startingAfterCrash;
    }
}
