/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test the {@link ServiceCrashDetector} class. */
public class ServiceCrashDetectorTest extends TestBase {
    private File directory;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (directory != null) {
            FileUtils.deleteDirectory(directory);
        }
    }

    @Test
    public void testUpdate() throws Exception {

        /*
         * Find a unique directory pathname, but don't create the directory yet
         */
        directory = File.createTempFile("crash-detect", ".tmp");
        directory.deleteOnExit();
        assertTrue(directory.delete());

        final ServiceCrashDetector detector =
            new ServiceCrashDetector(new RepNodeId(1, 3), directory, logger);

        assertEquals(new File(directory, "/rg1-rn3.up"),
                     detector.getFilename());

        /* Test with non-existent directory */
        assertFalse(detector.getStartingAfterCrash());
        assertFalse(detector.starting());
        assertFalse(detector.getStartingAfterCrash());
        assertFalse(detector.stopped());
        assertFalse(detector.getStartingAfterCrash());
        detector.update(new ServiceStatusChange(ServiceStatus.INITIAL),
                        new ServiceStatusChange(ServiceStatus.STARTING));
        assertFalse(detector.getStartingAfterCrash());
        detector.update(new ServiceStatusChange(ServiceStatus.STARTING),
                        new ServiceStatusChange(ServiceStatus.RUNNING));
        assertFalse(detector.getStartingAfterCrash());
        detector.update(new ServiceStatusChange(ServiceStatus.RUNNING),
                        new ServiceStatusChange(ServiceStatus.STOPPING));
        assertFalse(detector.getStartingAfterCrash());
        detector.update(new ServiceStatusChange(ServiceStatus.STOPPING),
                        new ServiceStatusChange(ServiceStatus.STOPPED));
        assertFalse(detector.getStartingAfterCrash());

        /* Now create directory and test that way */
        assertTrue(directory.mkdir());

        /* Check all transitions going in order */
        ServiceStatusChange prevChange = null;
        for (final ServiceStatus newStatus : ServiceStatus.values()) {
            final ServiceStatusChange newChange =
                new ServiceStatusChange(newStatus);
            detector.update(prevChange, newChange);
            assertFalse(detector.getStartingAfterCrash());
            prevChange = newChange;
        }

        /* Check a duplicate transition */
        detector.update(new ServiceStatusChange(ServiceStatus.INITIAL),
                        new ServiceStatusChange(ServiceStatus.STARTING));
        assertFalse(detector.getStartingAfterCrash());
        detector.update(new ServiceStatusChange(ServiceStatus.STARTING),
                        new ServiceStatusChange(ServiceStatus.STARTING));
        assertFalse(detector.getStartingAfterCrash());

        /*
         * Test transitions from STARTING to other statuses, then to STARTING
         * again, checking for whether it represents starting after a crash
         */
        final Set<ServiceStatus> cleanStop = new HashSet<>();
        cleanStop.add(ServiceStatus.STOPPED);
        cleanStop.add(ServiceStatus.ERROR_RESTARTING);
        cleanStop.add(ServiceStatus.ERROR_NO_RESTART);
        for (final ServiceStatus status : ServiceStatus.values()) {
            detector.update(new ServiceStatusChange(ServiceStatus.STARTING),
                            new ServiceStatusChange(status));
            detector.update(new ServiceStatusChange(ServiceStatus.INITIAL),
                            new ServiceStatusChange(ServiceStatus.STARTING));
            final boolean clean = cleanStop.contains(status);
            assertEquals("status: " + status + " clean: " + clean,
                         !clean,
                         detector.getStartingAfterCrash());
            detector.update(new ServiceStatusChange(ServiceStatus.STARTING),
                            new ServiceStatusChange(ServiceStatus.RUNNING));
            assertFalse("status: " + status + " clean: " + clean,
                        detector.getStartingAfterCrash());
        }
    }
}
