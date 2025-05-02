/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static oracle.kv.impl.async.UnixDomainNetworkAddress.UNIX_DOMAIN_SOCKETS_JAVA_VERSION;
import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;
import static org.junit.Assume.assumeFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

/**
 * Base class for KVLocal tests that can run in INET_SECURE, INET_NONSECURE,
 * and UNIX_DOMAIN modes.
 */
public class KVLocalTestModeBase extends TestBase {
    final TestMode testMode;

    enum TestMode {
        /** TCP sockets using security */
        INET_SECURE,
        /** TCP sockets without security */
        INET_NONSECURE,
        /** Unix domain sockets */
        UNIX_DOMAIN
    }

    KVLocalTestModeBase(TestMode testMode) {
        this.testMode = testMode;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        checkUnixDomainSocketsSupported(testMode);
    }

    @Parameters(name="kvlocalMode={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            final List<Object[]> params = new ArrayList<>();
            for (Object[] array : PARAMS_OVERRIDE) {
                params.add(
                    new Object[] { TestMode.valueOf((String) array[0]) });
            }
            return params;
        }
        return Arrays.asList(new Object[][]{
                {TestMode.INET_SECURE},
                {TestMode.INET_NONSECURE},
                {TestMode.UNIX_DOMAIN}});
    }

    /**
     * Skips the test if the Java version means that Unix domain sockets are
     * not supported and are needed for the specified test mode.
     */
    static void checkUnixDomainSocketsSupported(TestMode testMode) {
        assumeFalse("Skipping Unix domain sockets test with Java version " +
                    getJavaMajorVersion(),
                    (getJavaMajorVersion() < UNIX_DOMAIN_SOCKETS_JAVA_VERSION)
                    && (testMode == TestMode.UNIX_DOMAIN));
    }

    /**
     * Returns a KVLocalConfig builder that is appropriate for the current
     * test mode.
     */
    KVLocalConfig.Builder getConfigBuilder(String rootDir) {
        return getConfigBuilder(rootDir, testMode);
    }

    /**
     * Returns a KVLocalConfig builder that is appropriate for the specified
     * test mode.
     */
    static KVLocalConfig.Builder getConfigBuilder(String rootDir,
                                                  TestMode testMode) {
        final KVLocalConfig.Builder builder =
            (testMode == TestMode.UNIX_DOMAIN) ?
            new KVLocalConfig.UnixDomainBuilder(rootDir) :
            new KVLocalConfig.InetBuilder(rootDir);
        if (testMode == TestMode.INET_NONSECURE) {
            builder.isSecure(false);
        }
        return builder;
    }
}
