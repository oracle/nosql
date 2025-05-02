/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.util.TestUtils.assertMatch;
import static oracle.kv.util.CompareTo.greaterThanEqual;
import static oracle.kv.util.PingTest.scan;
import static oracle.nosql.common.json.JsonUtils.getArray;
import static oracle.nosql.common.json.JsonUtils.getAsText;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.admin.topo.Validations.HelperParameters;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.StoreUtils.KeyMismatchException;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingUtils;
import oracle.nosql.common.json.JsonNode;

import org.junit.Assert;


/**
 * Utilities shared between DeployTopo* tests.
 */
public class DeployUtils {

    /**
     * Check that each RN has the most optimal helper host setting, where its
     * helper param has been set to the node hosts of all its peers in the rep
     * group.
     */
    private static void checkRNParams(Topology topo,
                                      Parameters params,
                                      int repFactor,
                                      boolean stringentCheck) {

        for (RepGroup rg: topo.getRepGroupMap().getAll()) {

            /* Find the HA node host addresses for each member of the shard. */
            Set<String> nodeHosts = new HashSet<String>();
            for (RepNode rn: rg.getRepNodes()) {
                nodeHosts.add(params.get(rn.getResourceId()).getJENodeHostPort());
            }
            for (ArbNode an: rg.getArbNodes()) {
                nodeHosts.add(params.get(an.getResourceId()).getJENodeHostPort());
            }

            /*
             * Each RN should have a helper host setting that lists repfactor-1
             * number of helpers, where the helpers are the nodeHostPorts of the
             * other nodes, unless there is only one RN in the group (because of
             * an incomplete execution of a plan.
             */
            boolean singleton = (rg.getRepNodes().size() == 1);
            for (RepNode rn: rg.getRepNodes()) {
                RepNodeParams rnp =  params.get(rn.getResourceId());
                String nodeHostPort = rnp.getJENodeHostPort();
                String helpers = rnp.getJEHelperHosts();

                /*
                 * Check the number of helpers. Note that a half-run plan
                 * may have not have the proper number of helpers.
                 */
                StringTokenizer t = new StringTokenizer(helpers, ",");
                if (stringentCheck) {
                    int nHelpersExpected =
                        repFactor - 1 + rg.getArbNodes().size();
                    Assert.assertEquals
                        ("Number of helpers not correct. Helpers=" +
                         helpers, nHelpersExpected, t.countTokens());
                }

                /* Each helper should be the nodeHost of a peer. */
                while (t.hasMoreElements()) {
                    String oneHelper = t.nextToken().trim();
                    if (singleton) {
                        /*
                         * If there's 1 node in the group, it must be its own
                         * helper.
                         */
                        assertEquals(nodeHostPort, oneHelper);
                    } else {
                        if (stringentCheck) {
                            Assert.assertFalse("helpers=" + helpers +
                                               " oneHelper=" +
                                               oneHelper + " nodeHostPort=" +
                                               nodeHostPort,
                                               oneHelper.equals(nodeHostPort));

                        }
                    }
                    Assert.assertTrue("rn=" + rn + " nodeHosts=" + nodeHosts +
                                      " oneHelper=" + oneHelper,
                                      nodeHosts.contains(oneHelper));
                }
            }

            for (ArbNode an: rg.getArbNodes()) {
                ArbNodeParams anp =  params.get(an.getResourceId());
                String helpers = anp.getJEHelperHosts();

                /*
                 * Check the number of helpers. Note that a half-run plan
                 * may have not have the proper number of helpers.
                 */
                StringTokenizer t = new StringTokenizer(helpers, ",");

                /* Each helper should be the nodeHost of a peer. */
                while (t.hasMoreElements()) {
                    String oneHelper = t.nextToken();
                    Assert.assertTrue("an=" + an + " nodeHosts=" + nodeHosts +
                                      " oneHelper=" + oneHelper,
                                      nodeHosts.contains(oneHelper));
                }
            }
        }
    }

    /**
     * Check that the topology contains the specified number of components.
     * Number of Arbiters is zero.
     */
    public static void checkTopo(Topology topo,
                                 int numDCs,
                                 int numSNs,
                                 int numRGs,
                                 int numRNs,
                                 int numPartitions) {
        checkTopo(topo, numDCs, numSNs, numRGs, numRNs,
                  0 /* num ANs */,
                  numPartitions);
    }

    /**
     * Check that the topology contains the specified number of components.
     */
    public static void checkTopo(Topology topo,
                                 int numDCs,
                                 int numSNs,
                                 int numRGs,
                                 int numRNs,
                                 int numARBs,
                                 int numPartitions) {
        Assert.assertEquals("NumDCs wrong", numDCs,
                            topo.getDatacenterMap().size());
        Assert.assertEquals("NumSNs wrong", numSNs,
                            topo.getStorageNodeMap().size());
        Assert.assertEquals("NumRGs wrong", numRGs,
                            topo.getRepGroupMap().size());
        Assert.assertEquals("NumRNs wrong",numRNs,
                            topo.getSortedRepNodes().size());
        Assert.assertEquals("NumPartitions wrong", numPartitions,
                            topo.getPartitionMap().size());
        Assert.assertEquals("NumARBs wrong",numARBs,
                            topo.getArbNodeIds().size());
    }

    /**
     * Check the structure of the current deployed store against its topology.
     * Do an approximate check, the topology may be in flux.
     * @param startingProblems are the violations that existed before the
     * test case.
     * are ok.
     * @param permissibleProblems are the type of violations and warnings that
     * are ok.
     * @throws RemoteException
     */
    public static void checkDeployment(CommandServiceAPI cs,
                                       int repFactor,
                                       final Set<Problem> startingProblems,
                                       final Set<Class<?>> permissibleProblems,
                                       Logger logger)
        throws RemoteException {

        Topology topo = cs.getTopology();
        Parameters params = cs.getParameters();

        /* Check that the RNs have updated helper host values. */
        if (!permissibleProblems.contains(HelperParameters.class)) {
            checkRNParams(topo, params, repFactor, false);
        }

        /* Check that all RNs have valid mount points. */
        checkMountPoints(topo, params);
        /* Check that all RNs have valid log mount points. */
        checkRNLogMountPoints(topo, params);
        /* Check that store have valid admin mount points. */
        checkAdminMountPoints(topo, params);

        /*
         * Validate the store, see if the violations and warnings existed
         * before, or are permissible.
         */
        VerifyResults results = verifyConfiguration(cs, topo, params, logger);
        Set<Problem> scratchCopy = new HashSet<Problem>(startingProblems);

        for (Problem p: results.getViolations()) {
            boolean existed = scratchCopy.remove(p);
            if (existed) {
                logger.fine(p + " existed originally");
            } else {
                Assert.assertTrue(p + " (" + p.getClass().getName() + ")" +
                                  " is not a permitted problem",
                                  permissibleProblems.contains(p.getClass()));
            }
        }

        for (Problem p: results.getWarnings()) {
            boolean existed = scratchCopy.remove(p);
            if (existed) {
                logger.fine(p + " existed originally");
            } else {
                Assert.assertTrue(p + " (" + p.getClass().getName() + ")" +
                                  " is not a permitted problem",
                                  permissibleProblems.contains(p.getClass()));
            }
        }
    }

    /**
     * Call CommandServiceAPI.verifyConfiguration, return the results, and
     * check the output format as a side effect.
     */
    private static VerifyResults verifyConfiguration(CommandServiceAPI cs,
                                                     Topology topo,
                                                     Parameters params,
                                                     Logger logger)
        throws RemoteException {

        /* Verify all output formats available via the CLI */
        VerifyResults results = cs.verifyConfiguration(true, true, true);
        logger.fine("Verify JSON results:\n" + results.display());
        checkVerifyResultsJsonOutput(results, topo, params, true);
        results = cs.verifyConfiguration(true, true, false);
        logger.fine("Verify human-readable results:\n" + results.display());
        checkVerifyResultsHumanOutput(results, topo, params, true, true);
        results = cs.verifyConfiguration(false, true, false);
        logger.fine("Verify silent human-readable results:\n" +
                    results.display());
        checkVerifyResultsHumanOutput(results, topo, params, false, true);
        return results;
    }

    /** Check the output of the verify command. */
    public static void checkVerifyResultsJsonOutput(VerifyResults results,
                                                    Topology topo,
                                                    Parameters params,
                                                    boolean listAll) {

        final JsonNode json = PingUtils.checkJsonOutput(
            results.display(), topo, params, !listAll);

        /* Problems */
        final Iterable<JsonNode> jsonViolations = getArray(json, "violations");
        int numViolations = 0;
        for (final JsonNode jsonViolation : jsonViolations) {
            assertNotNull("violation " + numViolations + " resourceId",
                          getAsText(jsonViolation, "resourceId"));
            numViolations++;
        }
        assertEquals("numViolations", results.numViolations(), numViolations);
        final Iterable<JsonNode> jsonWarnings = getArray(json, "warnings");
        int numWarnings = 0;
        for (final JsonNode jsonWarning : jsonWarnings) {
            assertNotNull("warning " + numWarnings + " resourceId",
                          getAsText(jsonWarning, "resourceId"));
            numWarnings++;
        }
        assertEquals("numWarnings", results.numWarnings(), numWarnings);
    }

    /**
     * Check the output of the available Storage in verify command
     * output.
     */
    static void checkVerifyAvailableStorageOutput(VerifyResults results,
                                                  Topology topo,
                                                  Parameters params,
                                                  int expectedViolations,
                                                  int expectedWarnings) {
        checkVerifyAvailableStorageOutput(results, topo, params,
                expectedViolations + expectedWarnings,
                expectedViolations, expectedViolations,
                expectedWarnings, expectedWarnings);
    }

    /**
     * Check the output of the available Storage in verify command
     * output.
     * Allow for a range of expected violations and warnings.
     */
    static void checkVerifyAvailableStorageOutput(VerifyResults results,
                                                  Topology topo,
                                                  Parameters params,
                                                  int expectedTotal,
                                                  int expectedViolationsMin,
                                                  int expectedViolationsMax,
                                                  int expectedWarningsMin,
                                                  int expectedWarningsMax) {

        final JsonNode json = PingUtils.checkJsonOutput(
            results.display(), topo, params, false);

        /* check for available Storage */
        int violations = 0;
        int warnings = 0;

        final Iterable<JsonNode> jsonSNs = getArray(json, "snStatus");
        for (final JsonNode jsonSN : jsonSNs) {
            final Iterable<JsonNode> jsonRNsStatus =
               getArray(jsonSN, "rnStatus");
            for (final JsonNode jsonRNstatus : jsonRNsStatus) {
                String availableStorage =
                    getAsText(jsonRNstatus, "availableStorageSize");
                if (availableStorage.startsWith("-")) {
                    violations++;
                } else {
                    warnings++;
                }
            }
        }

        if (violations < expectedViolationsMin ||
            violations > expectedViolationsMax ||
            warnings < expectedWarningsMin ||
            warnings > expectedWarningsMax ||
            (violations + warnings) != expectedTotal) {
            StringBuilder sb = new StringBuilder();
            sb.append("Expecting: violations=" + expectedViolationsMin);
            if (expectedViolationsMin != expectedViolationsMax) {
                sb.append("-" + expectedViolationsMax);
            }
            sb.append(", warnings=" + expectedWarningsMin);
            if (expectedWarningsMin != expectedWarningsMax) {
                sb.append("-" + expectedWarningsMax);
            }
            sb.append(", total=" + expectedTotal);
            sb.append("\n");
            sb.append("Actual: violations=" + violations);
            sb.append(", warnings=" + warnings + ", total=");
            sb.append((violations + warnings) + "\n");
            assertTrue(sb.toString(), false);
        }
    }

    /**
     * Check the output of the verify command.
     */
    private static void checkVerifyResultsHumanOutput(VerifyResults results,
                                                      Topology topo,
                                                      Parameters params,
                                                      boolean showProgress,
                                                      boolean listAll) {

        final String output = results.display();
        final List<String> lines = new ArrayList<String>();
        final String[] tlines = output.split("\n");
        for (int i = 0; i < tlines.length; i++) {
            tlines[i] = trimEnd(tlines[i]);
        }
        Collections.addAll(lines, tlines);

        /* Topology overview */
        assertMatch("Topology overview line 1",
                    "Verify: starting verification of store " +
                    topo.getKVStoreName() +
                    " based upon topology sequence #\\d+",
                    lines.remove(0));
        assertMatch("Topology overview line 2",
                    "\\d+ partitions and \\d+ storage nodes",
                    lines.remove(0));
        assertMatch("Topology overview line 3", "Time: .*   Version: .*",
                    lines.remove(0));
        assertMatch("Topology overview line 4",
                    "See .* for progress messages", lines.remove(0));

        if (showProgress) {

            /* Shard status */
            assertMatch("shard summary",
                        "Verify: Shard Status: healthy: (\\d+) .*",
                        lines.remove(0));

            /* Admin status */
            if (lines.get(0).matches("Verify: Admin Status: .*")) {
                lines.remove(0);
            }

            /* Zone status */
            final List<DatacenterId> zoneIds = new ArrayList<DatacenterId>(
                topo.getDatacenterMap().getAllIds());
            Collections.sort(zoneIds);
            for (final DatacenterId zoneId : zoneIds) {
                assertMatch(zoneId.toString(),
                            "Verify: Zone \\[name=.* id=" + zoneId +
                            " type=.*\\]" + "   RN Status: .*",
                            lines.remove(0));
            }

            if (listAll) {

                /* SN status */
                final List<StorageNodeId> snIds = topo.getStorageNodeIds();
                Collections.sort(snIds);
                for (final StorageNodeId snId : snIds) {
                    assertMatch(snId.toString(),
                                "Verify: == checking storage node " + snId +
                                " ==",
                                lines.remove(0));
                    checkProblemsProgress(snId, results, lines);
                    assertMatch(snId.toString(),
                                "Verify: Storage Node \\[" + snId + "\\].*",
                                lines.remove(0));

                    /* Admin status */
                    AdminId adminId = null;
                    for (final AdminId aId : params.getAdminIds()) {
                        if (snId.equals(params.get(aId).getStorageNodeId())) {
                            adminId = aId;
                            break;
                        }
                    }
                    if (adminId != null) {
                        checkProblemsProgress(adminId, results, lines);
                        assertMatch(adminId.toString(),
                                    "Verify: \tAdmin \\[" + adminId + "\\].*",
                                    lines.remove(0));
                    }

                    /* RN status */
                    final List<RepNodeId> rnIds = new ArrayList<RepNodeId>(
                        topo.getHostedRepNodeIds(snId));
                    Collections.sort(rnIds);
                    for (final RepNodeId rnId : rnIds) {
                        checkProblemsProgress(rnId, results, lines);
                        assertMatch(rnId.toString(),
                                    "Verify: \tRep Node \\[" + rnId + "\\].*",
                                    lines.remove(0));
                    }

                    /* AN status */
                    final List<ArbNodeId> anIds = new ArrayList<ArbNodeId>(
                        topo.getHostedArbNodeIds(snId));
                    Collections.sort(anIds);
                    for (final ArbNodeId anId : anIds) {
                        checkProblemsProgress(anId, results, lines);
                        assertMatch(anId.toString(),
                                    "Verify: \tArb Node \\[" + anId + "\\].*",
                                    lines.remove(0));
                    }
                }
            }

            assertEquals("Blank line before summary", "", lines.remove(0));
        }

        /* Summary */
        final int numViolations = results.numViolations();
        final int numWarnings = results.numWarnings();
        if ((numViolations == 0) && (numWarnings == 0)) {
            assertEquals("Summary",
                         "Verification complete, no violations.",
                         lines.remove(0));
            assertEquals("No more output", 0, lines.size());
            return;
        }

        assertEquals("Summary",
                     "Verification complete, " + numViolations +
                     (numViolations == 1 ? " violation, " : " violations, ") +
                     numWarnings + (numWarnings == 1 ? " note" : " notes") +
                     " found.",
                     lines.remove(0));

        /* Problems */
        checkProblems("violation", numViolations, lines);
        checkProblems("note", numWarnings, lines);

        assertEquals("No more output", 0, lines.size());
    }

    private static void checkProblems(String problemType,
                                      int problemCount,
                                      List<String> lines) {
        String lastId = null;
        for (int i = 0; i < problemCount; i++) {
            final String line = lines.remove(0);
            final String regex =
                "Verification " + problemType + ": \\[([a-z0-9]*).*";
            assertMatch(problemType + " " + i, regex, line);
            final String id = scan(line, regex);
            if (lastId != null) {
                assertThat("Next " + problemType + " ID",
                           id,
                           greaterThanEqual(lastId));
            }
            lastId = id;

            /*
             * Treat additional lines that start with whitespace or tabs as
             * continuation lines for the current problem
             */
            while (!lines.isEmpty() && lines.get(0).matches("^[ \t].*")) {
                lines.remove(0);
            }
        }
    }

    private static void checkProblemsProgress(ResourceId resourceId,
                                                VerifyResults results,
                                                List<String> lines) {
        final int violationCount =
            countProblems(resourceId, results.getViolations());
        for (int i = 0; i < violationCount; i++) {
            assertMatch(resourceId + " violation " + i,
                        "Verify:         " + resourceId + ": .*",
                        lines.remove(0));

            /*
             * Treat additional lines that start with whitespace or tabs as
             * continuation lines for the current problem
             */
            while (!lines.isEmpty() && lines.get(0).matches("^[ \t].*")) {
                lines.remove(0);
            }
        }
        final int warningCount =
            countProblems(resourceId, results.getWarnings());
        for (int i = 0; i < warningCount; i++) {
            assertMatch(resourceId + " violation " + i,
                        "Verify:         " + resourceId + ": .*",
                        lines.remove(0));
            while (!lines.isEmpty() && lines.get(0).matches("^[ \t].*")) {
                lines.remove(0);
            }
        }
    }

    private static int countProblems(ResourceId resourceId,
                                     List<Problem> problems) {
        int count = 0;
        for (final Problem problem : problems) {
            if (problem.getResourceId().equals(resourceId)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Check the structure of the current deployed store against its topology,
     * checking for specific counts of arbitrary verification problems.  The
     * countsAndProblemClasses parameter should contain an even number of
     * elements, where the first element of each pair specifies an integer
     * count, and the second specifies the subclass of problem expected.  The
     * check will assert that the specified problem counts match all of the
     * warnings and violations found in the current topology.
     *
     * Note: if tests randomly fail using this method, you may try using
     *       checkDeploymentWithRanges() instead.
     *
     * @throws RemoteException
     */
    public static void checkDeployment(final CommandServiceAPI cs,
                                       final int repFactor,
                                       final int timeout,
                                       final Logger logger,
                                       final Object... countsAndProblemClasses)
        throws RemoteException {

        final Topology topo = cs.getTopology();
        final Parameters params = cs.getParameters();
        final Set<Class<?>> expectedProblems = new HashSet<Class<?>>();
        for (int i = 0; i < countsAndProblemClasses.length; i += 2) {
            expectedProblems.add((Class<?>) countsAndProblemClasses[i+1]);
        }

        /* Check that the RNs have updated helper host values. */
        if (!expectedProblems.contains(HelperParameters.class)) {
            checkRNParams(topo, params, repFactor, false);
        }

        /* Check that all RNs have valid mount points. */
        checkMountPoints(topo, params);
        /* Check that all RNs have valid log mount points. */
        checkRNLogMountPoints(topo, params);
        /* Check that store have valid admin mount points. */
        checkAdminMountPoints(topo, params);

        checkProblemsWithWait(cs, timeout, logger, countsAndProblemClasses);

    }


    /**
     * Check for specific counts of arbitrary verification problems after
     * waiting for store to settle down. The check will assert that the
     * specified problem counts match all of the warnings and violations found
     * in the current topology.  The countsAndProblemClasses parameter should
     * contain an even number of elements, where the first element of each pair
     * specifies an integer count, and the second specifies the subclass of
     * problem expected.
      * @throws RemoteException
      */

    public static void checkProblemsWithWait(final CommandServiceAPI cs,
                                       final int timeout,
                                       final Logger logger,
                                      final Object... countsAndProblemClasses)
        throws RemoteException {

        if (countsAndProblemClasses.length % 2 != 0) {
            throw new IllegalArgumentException(
                "The countsAndProblemClasses argument must have an even" +
                " number of elements");
        }
        final Map<Class<?>, Integer> expectedProblems =
            new HashMap<Class<?>, Integer>();
        for (int i = 0; i < countsAndProblemClasses.length; i += 2) {
            expectedProblems.put((Class<?>) countsAndProblemClasses[i+1],
                                 (Integer) countsAndProblemClasses[i]);
        }

        final Topology topo = cs.getTopology();
        final Parameters params = cs.getParameters();

        /* Check for problems, waiting for store to settle down */
        final boolean validated = new PollCondition(1000, timeout) {
            @Override
            protected boolean condition() {
                try {
                    return expectedProblems.equals(
                        classifyProblems(
                            verifyGetProblems(cs, topo, params, logger)));
                } catch (RemoteException e) {
                    return false;
                }
            }}.await();
        if (!validated) {
            /* Check one last time */
            final Collection<Problem> problems =
                verifyGetProblems(cs, topo, params, logger);
            assertEquals(problems.toString(), expectedProblems,
                         classifyProblems(problems));
        }

        /* Display the deployed history */
        List<String> history = cs.getTopologyHistory(false /* concise */);
        logger.fine("test check deployment: Dump of history");
        for (String s : history) {
            logger.fine(s);
        }
    }

    /**
     * Verify the configuration of the store, and return a collection of all of
     * the problems found.
     */
    private static Collection<Problem> verifyGetProblems(
        final CommandServiceAPI cs,
        Topology topo,
        Parameters params,
        Logger logger)
        throws RemoteException {

        final VerifyResults results =
            verifyConfiguration(cs, topo, params, logger);
        final Collection<Problem> problems =
            new ArrayList<Problem>(results.getViolations());
        problems.addAll(results.getWarnings());
        return problems;
    }

    /**
     * Convert a collection of problems to a map that maps the class of each
     * problem to the number of instances of that class.
     */
    private static Map<Class<? extends Problem>, Integer> classifyProblems(
        final Collection<Problem> problems) {

        final Map<Class<? extends Problem>, Integer> result =
            new HashMap<Class<? extends Problem>, Integer>();
        for (final Problem problem : problems) {
            final Integer count = result.get(problem.getClass());
            result.put(problem.getClass(), (count == null ? 1 : count + 1));
        }
        return result;
    }

    /**
     * Check the structure of the current deployed store against its topology,
     * checking for a range of counts of arbitrary verification problems.  The
     * countsAndProblemClasses parameter should contain a multiple of 3 number of
     * elements, where the first element is the min count, the second is the max
     * count, and the third specifies the subclass of problem expected.  The
     * check will assert that the specified problem counts match all of the
     * warnings and violations found in the current topology.
     * @param minProblems the minimum number of problems expected overall
     * @param maxProblems the maximum number of problems expected (can be same as min)
     * @throws RemoteException
     */
    public static void checkDeploymentWithRanges(final CommandServiceAPI cs,
                                       final int repFactor,
                                       final int timeout,
                                       final Logger logger,
                                       final int minProblems,
                                       final int maxProblems,
                                       final Object... countsAndProblemClasses)
        throws RemoteException {

        final Topology topo = cs.getTopology();
        final Parameters params = cs.getParameters();

        boolean expectsHelperProblem = false;
        for (int i = 0; i < countsAndProblemClasses.length; i += 3) {
            if (countsAndProblemClasses[i+2] instanceof HelperParameters) {
                expectsHelperProblem = true;
                break;
            }
        }

        /* Check that the RNs have updated helper host values. */
        if (expectsHelperProblem == false) {
            checkRNParams(topo, params, repFactor, false);
        }

        /* Check that all RNs have valid mount points. */
        checkMountPoints(topo, params);
        /* Check that all RNs have valid log mount points. */
        checkRNLogMountPoints(topo, params);
        /* Check that store have valid admin mount points. */
        checkAdminMountPoints(topo, params);

        checkProblemsWithRangesWait(cs, timeout, logger, minProblems, maxProblems, countsAndProblemClasses);

    }

    private static class MinMax {
        int min;
        int max;
        MinMax(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }

    private static boolean rangeCompare(MinMax totals, Map<Class<?>, MinMax> expectedProblems, Collection<Problem> problems, StringBuilder sb) {

        boolean ret = true;
        int totalProblems = 0;

        Map<Class<? extends Problem>, Integer> problemClasses = classifyProblems(problems);

        // walk expected, find actual and compare with range
        for (Map.Entry<Class<?>, MinMax> e : expectedProblems.entrySet()) {
            MinMax mm = e.getValue();
            Integer i = problemClasses.get(e.getKey());
            int val = 0;
            if (i != null) {
                val = i.intValue();
                totalProblems += val;
            }
            if (val < mm.min || val > mm.max) {
                ret = false;
                if (sb != null) {
                    sb.append("\nExpected " + mm.min);
                    if (mm.min != mm.max) {
                        sb.append("-" + mm.max);
                    }
                    sb.append(" " + e.getKey() +
                        " problems, found " + val + "\n");
                }
            }
        }

        // walk actual, make sure all are listed in expected
        for (Map.Entry<Class<? extends Problem>, Integer> e : problemClasses.entrySet()) {
            if (expectedProblems.get(e.getKey()) == null) {
                ret = false;
                if (sb != null) {
                    sb.append("\nExpected 0 " + e.getKey() + " problems, found " +
                        e.getValue() + "\n");
                }
            }
        }

        // verify total # of problems is in range
        if (totalProblems < totals.min || totalProblems > totals.max) {
            ret = false;
            if (sb != null) {
                sb.append("\nExpected " + totals.min);
                if (totals.min != totals.max) {
                    sb.append("-" + totals.max);
                }
                sb.append(" total problems, found " + totalProblems + "\n");
            }
        }

        if (ret == false && sb != null) {
            // dump # and type of problem classes found
            sb.append("\nProblem classes found:\n");
            for (Map.Entry<Class<? extends Problem>, Integer> e : problemClasses.entrySet()) {
                sb.append(e.getKey() + "=" + e.getValue() + "\n");
            }
            // dump list of problems found
            sb.append("\nActual problems found:\n");
            for (Problem p : problems) {
                sb.append(p + "\n");
            }
        }

        return ret;
    }

    /**
     * Check for a range of counts of arbitrary verification problems after
     * waiting for store to settle down. The check will assert that the
     * specified problem counts match all of the warnings and violations found
     * in the current topology.
     * The countsAndProblemClasses parameter should contain a multiple of 3 number of
     * elements, where the first element is the min count, the second is the max
     * count, and the third specifies the subclass of problem expected.  The
     * check will assert that the specified problem counts match all of the
     * warnings and violations found in the current topology.
     * @param minProblems the minimum number of problems expected overall
     * @param maxProblems the maximum number of problems expected (can be same as min)
     * @throws RemoteException
     */
    public static void checkProblemsWithRangesWait(final CommandServiceAPI cs,
                                       final int timeout,
                                       final Logger logger,
                                       final int minProblems,
                                       final int maxProblems,
                                       final Object... countsAndProblemClasses)
        throws RemoteException {

        if (countsAndProblemClasses.length % 3 != 0) {
            throw new IllegalArgumentException(
                "The countsAndProblemClasses argument must have a multiple of 3 " +
                " number of elements");
        }

        final MinMax totals = new MinMax(minProblems, maxProblems);
        final Map<Class<?>, MinMax> expectedProblems =
            new HashMap<Class<?>, MinMax>();
        for (int i = 0; i < countsAndProblemClasses.length; i += 3) {
            MinMax range = new MinMax(
                ((Integer)countsAndProblemClasses[i]).intValue(),
                ((Integer)countsAndProblemClasses[i+1]).intValue());
            expectedProblems.put((Class<?>) countsAndProblemClasses[i+2], range);
        }

        final Topology topo = cs.getTopology();
        final Parameters params = cs.getParameters();

        /* Check for problems, waiting for store to settle down */
        final boolean validated = new PollCondition(1000, timeout) {
            @Override
            protected boolean condition() {
                try {
                    final Collection<Problem> problems =
                        verifyGetProblems(cs, topo, params, logger);
                    return rangeCompare(totals, expectedProblems, problems, null);
                } catch (RemoteException e) {
                    return false;
                }
            }}.await();
        if (!validated) {
            /* Check one last time */
            final Collection<Problem> problems =
                verifyGetProblems(cs, topo, params, logger);
            StringBuilder sb = new StringBuilder();
            boolean ret = rangeCompare(totals, expectedProblems, problems, sb);
            if (ret == false) {
                List<String> history = cs.getTopologyHistory(false /* concise */);
                sb.append("\nTopology History:\n");
                for (String s : history) {
                    sb.append(s);
                    sb.append("\n");
                }
            }
            assertTrue(sb.toString(), ret);
        }

        /* Display the deployed history */
        List<String> history = cs.getTopologyHistory(false /* concise */);
        logger.fine("test check deployment: Dump of history");
        for (String s : history) {
            logger.fine(s);
        }
    }

    /**
     * Make sure all the mount points are valid.
     */
    private static void checkMountPoints(Topology topo, Parameters params) {
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            String mountPoint = rnp.getStorageDirectoryPath();
            if (mountPoint == null) {
                continue;
            }

            StorageNodeId snId = rnp.getStorageNodeId();
            boolean found = false;
            for (String mp : params.get(snId).getStorageDirPaths()) {
                if (mp.equals(mountPoint)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new RuntimeException
                    (rnId + " assigned to storage directory " + mountPoint +
                     " but " + snId + " has these storage directories " +
                     params.get(snId).getStorageDirPaths());
            }
        }
    }

    /**
     * Make sure all the RN log mount points are valid.
     */
    private static void checkRNLogMountPoints(Topology topo,
                                              Parameters params) {
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            String mountPoint = rnp.getLogDirectoryPath();
            if (mountPoint == null) {
                continue;
            }

            StorageNodeId snId = rnp.getStorageNodeId();
            boolean found = false;
            for (String mp : params.get(snId).getRNLogDirPaths()) {
                if (mp.equals(mountPoint)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new RuntimeException
                    (rnId + " assigned to RN log directory " + mountPoint +
                     " but " + snId + " has these RN log directories " +
                     params.get(snId).getRNLogDirPaths());
            }
        }
    }

    /**
     * Make sure all the Admin log mount points are valid.
     */
    @SuppressWarnings("unused")
    private static void checkAdminMountPoints(Topology topo,
                                              Parameters params) {

        /*
         * TODO : For this check, Need to incorporate admin Dir
         * path in AdminParams and then cross verify with
         * storageNodeParams
         */
    }

    /**
     * This method is used to trim whitespace at the end of the line.
     * It is used instead of trim() because the output checks assume that
     * a line that stats with a space is part of the previous output line.
     */
    private static String trimEnd(String source) {
        int pos = source.length() - 1;
        while ((pos >= 0) && Character.isWhitespace(source.charAt(pos))) {
            pos--;
        }
        pos++;
        return (pos < source.length()) ? source.substring(0, pos) : source;
    }

    /**
     * Check that all keys still exist in the store.
     */
    public static void checkContents(StoreUtils su,
                                     List<Key> expectedKeys,
                                     int numExpectedRecords) {
        /*
         * because of statistics data, the actual number of records is more
         * than the number of expected number which is the number of loaded
         * key/value pairs
         */
        Assert.assertTrue(numExpectedRecords <= su.numRecords());
        try {
            su.keysExist(expectedKeys);
        } catch (KeyMismatchException e) {
            KVStoreImpl storeImpl = (KVStoreImpl) su.getKVStore();
            throw new RuntimeException
                ("key from partition " + storeImpl.getPartitionId(e.getKey()) +
                 " is missing", e);
        }
    }

    public static void printCandidate(CommandServiceAPI cs,
                                      String candidateName,
                                      Logger logger)
        throws RemoteException {

        TopologyCandidate tc = cs.getTopologyCandidate(candidateName);

        /*
         * Execute the printTopology outside the logging call, to be sure it
         * executes and is exercised.
         */
        String printResult =
            TopologyPrinter.printTopology(tc, cs.getParameters(),
                                          true);
        logger.info(candidateName + " candidate> " + printResult);
    }

    public static void printCurrentTopo(CommandServiceAPI cs,
                                        String label,
                                        Logger logger)
        throws RemoteException {

        /*
         * Execute the printTopology outside the logging call, to be sure it
         * executes and is exercised.
         */
        String printResult =
            TopologyPrinter.printTopology(cs.getTopology(), cs.getParameters(),
                                          true);
        logger.info(label + " deployed> " + printResult);
    }

    public static void printPreview(String candidateName,
                                    boolean expectNoChange,
                                    CommandServiceAPI cs,
                                    Logger logger)
        throws RemoteException {

        /*
         * Note: be sure to execute preview, so call it explicitly outside the
         * call to the logger, which may not execute depending on the level
         * of logging.
         */
        String previewInfo = cs.preview(candidateName, null, true);
        Assert.assertEquals("preview=" + previewInfo,
                            expectNoChange,
                            TopologyDiff.NO_CHANGE.equals(previewInfo));

        logger.log(Level.INFO, "preview of {0}: {1}",
                   new Object[] {candidateName, previewInfo});
    }

    public static void checkArbBalance(Topology topo) {
        Set<RepGroupId> rgIds = topo.getRepGroupIds();
        List<StorageNodeId> snIds = topo.getStorageNodeIds();
        int averageAN = rgIds.size()/snIds.size();
        if (averageAN == 0) {
            averageAN = 1;
        }

        HashMap<StorageNodeId, Integer> anBySn =
            new HashMap<StorageNodeId, Integer>();
        for(ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode an = topo.get(anId);
            StorageNodeId snId = an.getStorageNodeId();
            Integer cnt = anBySn.get(snId);
            if (cnt == null) {
                cnt = Integer.valueOf(1);
            } else {
                cnt = Integer.valueOf(cnt.intValue() + 1);
                Assert.assertFalse("Number of ANs is above the average " +
                                   "[" + averageAN +"]" +
                                   "on SN "+ snId + " number of ANs is " +
                                   cnt, cnt.intValue() > averageAN);
            }
            anBySn.put(snId, cnt);
        }
    }

    /**
     * Generate a separate thread that will request plan status for a running
     * plan, to be run concurrently while the plan is executing.
     */
    public static Timer spawnStatusThread(CommandServiceAPI cs,
                                          int planNum,
                                          long options,
                                          Logger logger,
                                          long period) {
        Timer timer = new Timer("TestPlanStatusThread");
        timer.schedule(new GetPlanStatus(cs, planNum, options, logger),
                       0, period);
        return timer;
    }

    /**
     * Runnable to retrieve plan status.
     */
    private static class GetPlanStatus extends TimerTask {
        private final CommandServiceAPI cs;
        private final int planNum;
        private final long options;
        private final Logger logger;

        GetPlanStatus(CommandServiceAPI cs,
                      int planNum,
                      long options,
                      Logger logger) {
            this.cs = cs;
            this.planNum = planNum;
            this.options = options;
            this.logger = logger;
        }

        @Override
        public void run() {
            try {
                String status = cs.getPlanStatus(planNum, options,
                                                 false /* json */);
                logger.severe("Plan status for plan " + planNum + "\n" +
                              status);
            } catch (RemoteException e) {
                logger.severe("Problems getting plan status " +
                              LoggerUtils.getStackTrace(e));
            }
        }
    }


    /*
     * A test hook that throws a RuntimeException after a counter has been
     * counted down to 0. The idea is in plan execution 1, the fake error is
     * injected at count 1, plan execution 2 incurs an error at count 2, etc.
     */
    static class FaultCounter implements TestHook<Integer> {
        private final int faultCount;
        private final AtomicInteger counter;

        FaultCounter(int faultCount) {
            this.faultCount = faultCount;
            this.counter = new AtomicInteger(faultCount);
        }

        @Override
            public void doHook(Integer unused) {
            if (counter.decrementAndGet() == 0) {
                throw new RuntimeException("Injecting fault at point " +
                                           faultCount);
            }
        }
    }

    /*
     * A test hook that throws a RuntimeException when the hook value matches
     * the target value.
     */
    public static class TaskHook implements TestHook<Integer> {
        private final int target;

        TaskHook(int target) {
            this.target = target;
        }

        @Override
            public void doHook(Integer value) {
            if (target == value) {
                throw new RuntimeException("Injecting fault at point " + value);
            }
        }
    }
}
