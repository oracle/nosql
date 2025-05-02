/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.tif.esclient.esRequest.CreateIndexRequest;
import oracle.kv.impl.tif.esclient.esResponse.CreateIndexResponse;
import oracle.kv.impl.tif.esclient.restClient.ESAdminClient;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.VersionUtil;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for ElasticSearchHandler.getAllTransports.
 */
public class GetAllTransportsTest extends TextIndexFeederTestBase {

    //standalone test already checks the whole es cluster
    private static final int NNODES = 1;
    private static final String clusterName = "tif-test-es-cluster";

    private int[] httpPorts = new int[NNODES];
    private int emptyHttpPort;

    @BeforeClass
    public static void staticSetUp() throws Exception {
         Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        final FreePortLocator locator =
            new FreePortLocator("localhost", 29000, 29100);
        for (int i = 0; i < NNODES; i++) {
            httpPorts[i] = Integer.parseInt(System.getProperty("es.http.port"));
        }

        emptyHttpPort = locator.next();
    }

    @Test
    public void testGetAllTransports() throws Exception {

        HostPort hp = new HostPort("mxyzptlck7u7", emptyHttpPort);
        boolean thrown = false;

        try {

            ElasticsearchHandler.getAllTransports(clusterName, hp, null, false,
                                                  null, false, logger);

        } catch (IllegalCommandException e) {
            thrown = true;
        }

        assertTrue("Expected host not found exception was not thrown.", thrown);

        hp = new HostPort("localhost", emptyHttpPort);
        thrown = false;

        try {
            ElasticsearchHandler.getAllTransports(clusterName, hp, null, false,
                                                  null, false, logger);
        } catch (IllegalCommandException e) {
            thrown = true;
        }
        assertTrue("Expected lack of connection was not detected.", thrown);

        /* Check getAllTransports against the first node; result should include
         * all three nodes.
         */
        hp = new HostPort("localhost", httpPorts[0]);
        final String s =
            ElasticsearchHandler.getAllTransports(clusterName, hp, null, false,
                                                  null, false, logger);
        /* All three ports should be in the string returned. */
        for (int i = 0; i < NNODES; i++) {
            assertTrue
                ("Port " + httpPorts[i] +
                 " missing from string returned by getAllTransports on " +
                 hp.toString(),
                 s.contains(Integer.toString(httpPorts[i])));
        }

        /* Test detection and removal of existing ES index. */

        final String storeName = "randomStore";
        final String indexName =
            TextIndexFeeder.deriveESIndexName(storeName,
                                              null, /* namespace */
                                              "randomTable",
                                              "randomIndex");

        final ESAdminClient esAdminClient =
            ElasticsearchHandler.createESRestClient(
                clusterName, "localhost:" + httpPorts[0], logger).admin();

        final CreateIndexRequest crIdxRq = new CreateIndexRequest(indexName);
        final CreateIndexResponse createResponse =
            esAdminClient.createIndex(crIdxRq);

        if (!createResponse.isAcknowledged()) {
            fail("Failed to create ES index " + indexName);
        }

        thrown = false;
        try {
            ElasticsearchHandler.getAllTransports(clusterName, hp, storeName,
                                                  false, null, false, logger);
        } catch (IllegalCommandException e) {
            thrown = true;
        }
        assertTrue("Detection of pre-existing index failed.", thrown);

        /* Try again with automatic removal enabled. */

        ElasticsearchHandler.getAllTransports(clusterName, hp, storeName,
                                              false, null, true, logger);

        assertFalse(ElasticsearchHandler.existESIndex(indexName,
                                                      esAdminClient));
    }
}
