/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.sna.ManagedService;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test service status and monitor event logging output, and the associated
 * check-node-killer-logs and check-service-monitor scripts.
 */
public class LifecycleLoggingTest extends TestBase {

    private static final String testDir = TestUtils.getTestDir().toString();

    /**
     * Regular expression for service status change events: 2023-06-21
     * 14:45:43.006 UTC INFO [sn1] LIFECYCLE Service status change:
     * from=WAITING_FOR_DEPLOY to=STARTING
     */
    private static Pattern SERVICE_PATTERN =
        Pattern.compile("(?<date>[-0-9]+ [0-9:.]+ UTC) " +
                        "(?<level>INFO) " +
                        "\\[(?<service>[-a-zA-Z0-9]+)\\] " +
                        "LIFECYCLE Service status change: " +
                        "(?<msg>.*$)");

    /**
     * Regular expression for service monitor events: 2023-06-21 14:56:37.928
     * UTC INFO [sn1] LIFECYCLE Service monitor event: service=admin1
     * event=CREATE
     */
    private static Pattern MONITOR_PATTERN =
        Pattern.compile("(?<date>[-0-9]+ [0-9:.]+ UTC) " +
                        "(?<level>INFO) " +
                        "(?<snTag>\\[sn[1-9]\\]) " +
                        "LIFECYCLE Service monitor event: " +
                        "service=(?<service>[-a-zA-Z0-9]+) " +
                        "(?<msg>.*$)");

    /**
     * Regular expression for whether output known to be a service monitor
     * event represents an unexpected event: 2023-06-26 12:51:50.555 UTC INFO
     * [sn2] LIFECYCLE Service monitor event: service=rg1-rn2 event=EXIT
     * cause=FAULT exitCode=137/kill-9 restart=YES
     */
    private static Pattern UNEXPECTED_MONITOR_PATTERN =
        Pattern.compile(".* event=EXIT cause=FAULT .*");

    private CreateStore createStore;
    final List<Event> serviceEvents = new ArrayList<>();
    final List<Event> monitorEvents = new ArrayList<>();
    final List<Event> nonVerboseMonitorEvents = new ArrayList<>();

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
        }
    }

    /* Tests */

    @Test
    public void testEventEquals() {
        assertEquals(new Event("sn1", "msg1"), new Event("sn1", "msg1"));
        assertNotEquals(new Event("sn1", "msg1"), new Event("sn2", "msg1"));
        assertNotEquals(new Event("sn1", "msg1"), new Event("sn1", "msg2"));
        assertEquals(new Event("sn1", "msg.*", true),
                     new Event("sn1", "msg1 and more stuff"));
        assertNotEquals(new Event("sn1", "msg.*", true),
                        new Event("sn1", "something else"));
        assertEquals(new Event("sn1", "msg1 and more stuff"),
                     new Event("sn1", "msg.*", true));
        assertNotEquals(new Event("sn1", "something else"),
                        new Event("sn1", "msg.*", true));
        checkException(() -> new Event("sn1", "msg", true).equals(
                           new Event("sn1", "msg", true)),
                       IllegalArgumentException.class,
                       "Both regexs");
    }

    @Test
    public void testServiceEvents() {
        final Event event = serviceEventFromLine(
            "2023-06-21 14:45:43.006 UTC INFO [sn1]" +
            " LIFECYCLE Service status change:" +
            " from=WAITING_FOR_DEPLOY to=STARTING");
        assertEquals("sn1", event.serviceId);
        assertEquals("from=WAITING_FOR_DEPLOY to=STARTING", event.msg);
        checkException(() -> serviceEventFromLine("foo"),
                       IllegalArgumentException.class,
                       "Line doesn't match");
    }

    @Test
    public void testMonitorEvents() {
        checkException(() -> monitorEventFromLine(
                           "No faults found",
                           false /* unexpectedFailures */,
                           false /* verbose */),
                       IllegalArgumentException.class,
                       "Expected no output");
        monitorEventFromLine("No faults found",
                             false /* unexpectedFailures */,
                             true /* verbose */);
        checkException(
            () -> monitorEventFromLine("No faults found",
                                       true /* unexpectedFailures */,
                                       false /* verbose */),
            IllegalArgumentException.class,
            "Expected: Found faults");
        checkException(
            () -> monitorEventFromLine("No faults found",
                                       true /* unexpectedFailures */,
                                       true /* verbose */),
            IllegalArgumentException.class,
            "Expected: Found faults");

        checkException(
            () -> monitorEventFromLine("Found faults",
                                       false /* unexpectedFailures */,
                                       false /* verbose */),
            IllegalArgumentException.class,
            "Expected no output");
        checkException(
            () -> monitorEventFromLine("Found faults",
                                       false /* unexpectedFailures */,
                                       true /* verbose */),
            IllegalArgumentException.class,
            "Expected: No faults found");
        monitorEventFromLine("Found faults", true /* unexpectedFailures */,
                             false /* verbose */);
        monitorEventFromLine("Found faults", true /* unexpectedFailures */,
                             true /* verbose */);

        final String text = "2023-06-21 14:56:37.928 UTC INFO [sn1]" +
            " LIFECYCLE Service monitor event: service=admin1 event=CREATE";
        checkException(() -> monitorEventFromLine(
                           text, false /* unexpectedFailures */,
                           false /* verbose */),
                       IllegalArgumentException.class,
                       "Unexpected output");
        Event event = monitorEventFromLine(
            text, false /* unexpectedFailures */, true /* verbose */);
        assertEquals("admin1", event.serviceId);
        assertEquals("event=CREATE", event.msg);
        checkException(() -> monitorEventFromLine(
                           text, true /* unexpectedFailures */,
                           false /* verbose */),
                       IllegalArgumentException.class,
                       "Unexpected output");
        event = monitorEventFromLine(
            text, true /* unexpectedFailures */, true /* verbose */);
        assertEquals("admin1", event.serviceId);
        assertEquals("event=CREATE", event.msg);

        final String unexpectedText = "2023-06-26 12:51:50.555 UTC" +
            " INFO [sn2] LIFECYCLE Service monitor event: service=rg1-rn2" +
            " event=EXIT cause=FAULT exitCode=137/kill-9 restart=YES";
        checkException(() -> monitorEventFromLine(
                           unexpectedText, false /* unexpectedFailures */,
                           false /* verbose */),
                       IllegalArgumentException.class,
                       "Unexpected output");
        checkException(() -> monitorEventFromLine(
                           unexpectedText, false /* unexpectedFailures */,
                           true /* verbose */),
                       IllegalArgumentException.class,
                       "Unexpected output");
        event = monitorEventFromLine(unexpectedText,
                                     true /* unexpectedFailures */,
                                     false /* verbose */);
        assertEquals("rg1-rn2", event.serviceId);
        assertEquals("event=EXIT cause=FAULT exitCode=137/kill-9 restart=YES",
                     event.msg);
        event = monitorEventFromLine(unexpectedText,
                                     true /* unexpectedFailures */,
                                     true /* verbose */);
        assertEquals("rg1-rn2", event.serviceId);
        assertEquals("event=EXIT cause=FAULT exitCode=137/kill-9 restart=YES",
                     event.msg);
    }

    @Test
    public void testLifecycle() throws Exception {

        /* Deploy store */
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      4, /* numStorageNodes */
                                      2, /* replicationFactor */
                                      30, /* numPartitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN, /* memoryMB */
                                      false /* useThreads */,
                                      null /* mgmtImpl */);

        /* Reserve 1 SN for elasticity */
        createStore.setPoolSize(3);
        createStore.setAllowArbiters(true);
        createStore.start();

        /*
         * Tally expected events from store creation. Note that all 4 SNs get
         * started even though only 3 are being used in the topology at first.
         */
        for (int i = 1; i <= 4; i++) {
            serviceEvents.add(
                new Event("sn" + i, "from=WAITING_FOR_DEPLOY to=STARTING"));
            serviceEvents.add(
                new Event("sn" + i, "from=STARTING to=RUNNING"));
        }

        /*
         * There are service status BootstrapAdmin events for the bootstrap
         * admin associated with each SN, but the fact that they are all
         * reported for the same service name makes checking those events
         * undependable, so we will filter them out. The admin1 startup events
         * are bootstrap only.
         */
        monitorEvents.add(new Event("admin1", "event=CREATE"));
        for (final String svc : new String[] {
                "admin2", "admin3", "rg1-rn1", "rg1-rn2", "rg1-an1" }) {
            monitorEvents.add(new Event(svc, "event=CREATE"));
            monitorEvents.add(new Event(svc, "event=START"));
            serviceEvents.add(new Event(svc, "from=INITIAL to=STARTING"));
            serviceEvents.add(new Event(svc, "from=STARTING to=RUNNING"));
        }

        /* Restart SN */
        createStore.shutdownSNA(0 /* snaIndex */, false /* force */);
        createStore.startSNA(0);
        serviceEvents.add(
            new Event("sn1", "from=RUNNING to=STOPPING reason=For testing"));
        serviceEvents.add(
            new Event("sn1", "from=STOPPING to=STOPPED reason=For testing"));
        serviceEvents.add(new Event("sn1", "from=INITIAL to=STARTING"));
        serviceEvents.add(new Event("sn1", "from=STARTING to=RUNNING"));

        serviceEvents.add(
            new Event("rg1-rn1",
                      "from=RUNNING to=STOPPING reason=For testing"));
        serviceEvents.add(
            new Event("rg1-rn1",
                      "from=STOPPING to=STOPPED reason=For testing"));
        monitorEvents.add(new Event("rg1-rn1", "event=EXIT cause=STOP"));
        monitorEvents.add(new Event("rg1-rn1", "event=START"));
        serviceEvents.add(new Event("rg1-rn1", "from=INITIAL to=STARTING"));
        serviceEvents.add(new Event("rg1-rn1", "from=STARTING to=RUNNING"));
        serviceEvents.add(
            new Event("admin1",
                      "from=RUNNING to=STOPPING reason=For testing"));
        serviceEvents.add(
            new Event("admin1",
                      "from=STOPPING to=STOPPED reason=For testing"));
        monitorEvents.add(new Event("admin1", "event=EXIT cause=STOP"));
        monitorEvents.add(new Event("admin1", "event=START"));
        serviceEvents.add(new Event("admin1", "from=INITIAL to=STARTING"));
        serviceEvents.add(new Event("admin1", "from=STARTING to=RUNNING"));

        /* Restart other services */
        restartService(new RepNodeId(1, 1));
        restartService(new AdminId(1));
        restartService(new ArbNodeId(1, 1));

        /* Inject faults */
        injectProcessExit(new RepNodeId(1, 2));
        injectProcessExit(new ArbNodeId(1, 1));
        injectProcessExit(new AdminId(2));

        /* Expand store */
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.copyCurrentTopology("topo2");
        cs.redistributeTopology("topo2", "AllStorageNodes");
        executePlan(
            cs.createDeployTopologyPlan("deployNewTopo", "topo2", null));
        final String[] svcs = { "rg2-rn1", "rg2-rn2", "rg2-an1" };
        for (final String svc : svcs) {
            monitorEvents.add(new Event(svc, "event=CREATE"));
            monitorEvents.add(new Event(svc, "event=START"));
            serviceEvents.add(new Event(svc, "from=INITIAL to=STARTING"));
            serviceEvents.add(new Event(svc, "from=STARTING to=RUNNING"));
        }
        checkEventOutput(false /* unexpectedFailures */);

        /* Kill a process unexpectedly */
        ManagedService.killManagedProcesses(
            createStore.getStoreName(), "rg1-rn2", logger);
        createStore.getRepNodeAdmin(new RepNodeId(1, 2), 10 /* timeoutSec */);
        serviceEvents.add(new Event("rg1-rn2",
                                    "from=INITIAL to=STARTING afterCrash"));
        serviceEvents.add(new Event("rg1-rn2", "from=STARTING to=RUNNING"));
        Event event = new Event(
            "rg1-rn2",
            "event=EXIT cause=FAULT exitCode=137/kill-9 restart=YES");
        monitorEvents.add(event);
        nonVerboseMonitorEvents.add(event);
        monitorEvents.add(new Event("rg1-rn2", "event=START"));
        checkEventOutput(true /* unexpectedFailures */);
    }

    /* Other methods and classes */

    /**
     * Inject a failure into the specified service and wait for it to restart.
     */
    private void injectProcessExit(ResourceId serviceId) throws Exception {
        final RegistryUtils ru = new RegistryUtils(
            createStore.getAdmin().getTopology(), null, logger);
        final RemoteTestAPI testAPI;
        if (serviceId instanceof RepNodeId) {
            testAPI = ru.getRepNodeTest((RepNodeId) serviceId);
        } else if (serviceId instanceof ArbNodeId) {
            testAPI = ru.getArbNodeTest((ArbNodeId) serviceId);
        } else {
            final int adminId = ((AdminId) serviceId).getAdminInstanceId();
            testAPI = ru.getAdminTest(new StorageNodeId(adminId));
        }
        try {
            testAPI.processExit(true /* restart */);
        } catch (RemoteException e) {
        }
        if (serviceId instanceof RepNodeId) {
            createStore.getRepNodeAdmin((RepNodeId) serviceId,
                                        10 /* timeoutSec */);
        } else if (serviceId instanceof ArbNodeId) {
            createStore.getArbNodeAdmin((ArbNodeId) serviceId,
                                        10 /* timeoutSec */);
        } else {
            final int adminId = ((AdminId) serviceId).getAdminInstanceId();
            createStore.getAdmin(adminId - 1);
        }
        serviceEvents.add(
            new Event(serviceId,
                      "from=RUNNING" +
                      " to=INJECTED_FAULT_RESTARTING" +
                      " reason=RemoteTestInterface.processExit"));
        monitorEvents.add(
            new Event(serviceId,
                      "event=EXIT" +
                      " cause=INJECTED_FAULT" +
                      " exitCode=203/INJECTED_FAULT_RESTART restart=YES"));
        monitorEvents.add(new Event(serviceId, "event=START"));
        serviceEvents.add(new Event(serviceId,
                                    "from=INITIAL to=STARTING afterCrash"));
        serviceEvents.add(new Event(serviceId, "from=STARTING to=RUNNING"));
    }

    /**
     * Check output for check-node-killer-logs and check-service-monitor
     * scripts.
     */
    private void checkEventOutput(boolean unexpectedFailures)
        throws Exception {

        /* testDir represents the ${project.build.directory}/sandbox  */
        /* in this case it is kv/kvtest/kvstore-IT/target/sandbox */
        final String kvstoreTestDir = testDir + "/../../../../tests/";

        String[] args = {
            kvstoreTestDir + "standalone/stress/check-node-killer-logs",
            testDir
        };
        Process process = new ProcessBuilder(args)
            .redirectErrorStream(true)
            .start();
        final List<Event> serviceOutput = new ArrayList<>();
        final InputStream serviceInput = process.getInputStream();
        CompletableFuture<Void> processOutputResult =
            CompletableFuture.runAsync(
                () -> processServiceOutput(serviceInput, serviceOutput));
        assertTrue(process.waitFor(30, SECONDS));
        int processStatus = process.exitValue();
        processOutputResult.get(30, SECONDS);
        serviceEvents.sort(Event::compare);
        serviceOutput.sort(Event::compare);
        if (!serviceEvents.equals(serviceOutput)) {
            fail("Expected service events:\n" +
                 stringWithNewlines(serviceEvents) +
                 "\nbut was:\n" + stringWithNewlines(serviceOutput));
        }
        assertEquals(0, processStatus);

        args = new String[] {
            kvstoreTestDir + "scripts/check-service-monitor",
            "-v",
            testDir
        };
        process = new ProcessBuilder(args)
            .redirectErrorStream(true)
            .start();
        final List<Event> monitorOutput = new ArrayList<>();
        final InputStream monitorInput = process.getInputStream();
        processOutputResult = CompletableFuture.runAsync(
            () -> processMonitorOutput(monitorInput, monitorOutput,
                                       unexpectedFailures,
                                       true /* verbose */));
        assertTrue(process.waitFor(30, SECONDS));
        processStatus = process.exitValue();
        processOutputResult.get(30, SECONDS);
        monitorEvents.sort(Event::compare);
        monitorOutput.sort(Event::compare);
        if (!monitorEvents.equals(monitorOutput)) {
            fail("Expected monitor events:\n" +
                 stringWithNewlines(monitorEvents) +
                 "\nbut was:\n" + stringWithNewlines(monitorOutput));
        }
        assertEquals(unexpectedFailures ? 1 : 0, processStatus);

        args = new String[] {
            kvstoreTestDir + "scripts/check-service-monitor",
            testDir
        };
        process = new ProcessBuilder(args)
            .redirectErrorStream(true)
            .start();
        final List<Event> nonVerboseMonitorOutput = new ArrayList<>();
        final InputStream nonVerboseMonitorInput = process.getInputStream();
        processOutputResult = CompletableFuture.runAsync(
            () -> processMonitorOutput(nonVerboseMonitorInput,
                                       nonVerboseMonitorOutput,
                                       unexpectedFailures,
                                       false /* verbose */));
        assertTrue(process.waitFor(30, SECONDS));
        processStatus = process.exitValue();
        processOutputResult.get(30, SECONDS);
        nonVerboseMonitorEvents.sort(Event::compare);
        nonVerboseMonitorOutput.sort(Event::compare);
        if (!nonVerboseMonitorEvents.equals(nonVerboseMonitorOutput)) {
            fail("Expected non-verbose monitor events:\n" +
                 stringWithNewlines(monitorEvents) +
                 "\nbut was:\n" + stringWithNewlines(monitorOutput));
        }
        assertEquals(unexpectedFailures ? 1 : 0, processStatus);
    }

    /**
     * Convert a collection to a string by separating the elements with
     * newlines
     */
    private static String stringWithNewlines(List<?> list) {
        return list.stream().map(Object::toString).collect(
            Collectors.joining("\n"));
    }

    private void processServiceOutput(InputStream input,
                                      List<Event> collectedEvents) {
        new BufferedReader(new InputStreamReader(input)).lines().forEach(
            line -> {
                final Event event = serviceEventFromLine(line);
                if (event != null) {
                    collectedEvents.add(event);
                }
            });
    }

    /**
     * Take a line of output from check-node-killer-logs and return an event if
     * it specifies a service status change. Returns null if the event is for a
     * bootstrap admin. Throws IllegalArgumentException if the line is
     * unexpected.
     */
    Event serviceEventFromLine(String line) {
        final Matcher matcher = SERVICE_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Line doesn't match: " + line);
        }
        final String service = matcher.group("service");

        /*
         * Skip BootstrapAdmin events since they are actually for
         * multiple services and so can't be ordered dependably
         */
        if ("BootstrapAdmin".equals(service)) {
            return null;
        }
        final String msg = matcher.group("msg");
        return new Event(service, msg);
    }

    private void processMonitorOutput(InputStream input,
                                      List<Event> collectedEvents,
                                      boolean unexpectedFailures,
                                      boolean verbose) {
        new BufferedReader(new InputStreamReader(input)).lines().forEach(
            line -> {
                final Event event =
                    monitorEventFromLine(line, unexpectedFailures, verbose);
                if (event != null) {
                    collectedEvents.add(event);
                }
            });
    }

    /**
     * Take a line of output from check-service-monitor and return an event if
     * it specifies a monitor event. Otherwise, check to see if the line
     * matches the expected failure/no failure result and return null. Throws
     * IllegalArgumentException if the line is unexpected.
     */
    Event monitorEventFromLine(String line,
                               boolean unexpectedFailures,
                               boolean verbose) {
        final Matcher matcher = MONITOR_PATTERN.matcher(line);
        if (matcher.matches()) {
            final String service = matcher.group("service");
            final String msg = matcher.group("msg");

            final boolean foundUnexpected =
                UNEXPECTED_MONITOR_PATTERN.matcher(line).matches();
            if (!verbose && !foundUnexpected) {
                throw new IllegalArgumentException(
                    "Unexpected output: " + line);
            }
            if (foundUnexpected && !unexpectedFailures) {
                throw new IllegalArgumentException(
                    "Unexpected output: " + line);
            }

            return new Event(service, msg);
        }

        if (!unexpectedFailures && !verbose) {
            throw new IllegalArgumentException(
                "Expected no output, found: " + line);
        }
        final String expected =
            unexpectedFailures ? "Found faults" : "No faults found";
        if (!expected.equals(line)) {
            throw new IllegalArgumentException(
                "Expected: " + expected + ", found: " + line);
        }
        return null;
    }

    private void restartService(ResourceId serviceId) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        executePlan(
            cs.createStopServicesPlan(
                "Stop " + serviceId.getFullName(),
                Collections.singleton(serviceId)));
        executePlan(
            cs.createStartServicesPlan(
                "Restart " + serviceId.getFullName(),
                Collections.singleton(serviceId)));
        serviceEvents.add(
            new Event(serviceId, "from=RUNNING to=STOPPING reason=Plan .*",
                      true /* isRegex */));
        serviceEvents.add(
            new Event(serviceId, "from=STOPPING to=STOPPED reason=Plan .*",
                      true /* isRegex */));
        monitorEvents.add(new Event(serviceId, "event=EXIT cause=STOP"));
        monitorEvents.add(new Event(serviceId, "event=START"));
        serviceEvents.add(new Event(serviceId, "from=INITIAL to=STARTING"));
        serviceEvents.add(new Event(serviceId, "from=STARTING to=RUNNING"));
    }

    private void executePlan(int planId) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.approvePlan(planId);
        cs.executePlan(planId, false /* force */);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    /** A service status change or monitor event */
    private static class Event {
        private static int nextIndex;
        final String serviceId;
        final String msg;
        final boolean isRegex;

        /**
         * Track the order of event creation so we can sort events by time for
         * a given service.
         */
        final int index;

        Event(ResourceId serviceId, String msg) {
            this(serviceId, msg, false /* isRegex */);
        }

        Event(ResourceId serviceId, String msg, boolean isRegex) {
            this(serviceId.getFullName(), msg, isRegex);
        }

        Event(String serviceId, String msg) {
            this(serviceId, msg, false /* isRegex */);
        }

        Event(String serviceId, String msg, boolean isRegex) {
            this.serviceId = serviceId;
            this.msg = msg;
            this.isRegex = isRegex;
            index = nextIndex++;
        }

        /**
         * Compare events first by service ID and then by creation index.
         */
        int compare(Event other) {
            int result = serviceId.compareTo(other.serviceId);
            if (result == 0) {
                result = index - other.index;
            }
            return result;
        }

        /**
         * Handle comparing events that specify a regular expression, but only
         * allow one of the two events to be a regex.
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Event)) {
                return false;
            }
            final Event other = (Event) obj;
            if (!serviceId.equals(other.serviceId)) {
                return false;
            }
            if (isRegex && other.isRegex) {
                throw new IllegalArgumentException("Both regexs");
            }
            if (!isRegex && !other.isRegex) {
                return msg.equals(other.msg);
            }
            return isRegex ? other.msg.matches(msg) : msg.matches(other.msg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serviceId, msg, isRegex);
        }

        @Override
        public String toString() {
            return serviceId + ": " + msg + (isRegex ? " (regex)" : "");
        }
    }
}
