/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.security.ssl.SSLConfig.CLIENT_AUTHENTICATOR;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.SERVER_HOST_VERIFIER;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import oracle.kv.FastExternalizableException;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.SSLTestUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.runners.Parameterized.Parameters;

/**
 * A base class for tests of asynchronous remote calls
 */
public abstract class AsyncVersionedRemoteTestBase
        extends UncaughtExceptionTestBase {

    public static final long TIMEOUT = 5000;

    protected final boolean secure;
    protected @Nullable EndpointGroup endpointGroup;
    protected @Nullable ListenerConfig listenerConfig;

    protected AsyncVersionedRemoteTestBase(boolean secure) {
        this.secure = secure;
    }

    @Parameters(name="secure={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        endpointGroup = new NioEndpointGroup(logger, 2);
        listenerConfig = createListenerConfigBuilder().build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (endpointGroup != null) {
            endpointGroup.shutdown(true);
        }
    }

    public ListenerConfig getListenerConfig() {
        return checkNull("listenerConfig", listenerConfig);
    }

    public EndpointGroup getEndpointGroup() {
        return checkNull("endpointGroup", endpointGroup);
    }

    public CreatorEndpoint createEndpoint(
        final DialogType dialogType,
        final AsyncVersionedRemoteDialogResponder responder)
        throws Exception {

        return createEndpoint(listen(dialogType, responder));
    }

    public EndpointGroup.ListenHandle listen(
        final DialogType dialogType,
        final AsyncVersionedRemoteDialogResponder responder)
        throws IOException
    {
        return getEndpointGroup().listen(
            getListenerConfig(), dialogType.getDialogTypeId(),
            () -> responder);
    }

    public CreatorEndpoint createEndpoint(
        final EndpointGroup.ListenHandle listenHandle)
        throws Exception
    {
        final NetworkAddress localAddress =
            checkNull("localAddress",
                      listenHandle.getLocalAddress().toNetworkAddress());
        return getEndpointGroup().getCreatorEndpoint(
            "perfName",
            localAddress,
            InetNetworkAddress.ANY_LOCAL_ADDRESS,
            createEndpointConfigBuilder(false).build());
    }

    public static class LocalException extends FastExternalizableException {
        private static final long serialVersionUID = 0;
        public LocalException(String msg) {
            super(msg);
        }
        public LocalException(DataInput in, short serialVersion)
            throws IOException {

            super(in, serialVersion);
        }
    }

    public EndpointConfigBuilder createEndpointConfigBuilder(boolean server)
        throws Exception {

        final EndpointConfigBuilder builder = new EndpointConfigBuilder();
        if (secure) {
            builder.sslControl(
                createSSLConfig().makeSSLControl(server, logger));
        }
        return builder;
    }

    public ListenerConfigBuilder createListenerConfigBuilder()
        throws Exception {

        return new ListenerConfigBuilder()
            .portRange(new ListenerPortRange(6000, 8000))
            .endpointConfigBuilder(createEndpointConfigBuilder(true));
    }

    public SSLConfig createSSLConfig() {
        final File sslDir = SSLTestUtils.getTestSSLDir();
        final String clientAuthenticator = "dnmatch(CN=Unit Test)";
        final String serverhostVerifier = "dnmatch(CN=Unit Test)";
        final String keystorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_KS_NAME).getPath();
        final String truststorePath =
            new File(sslDir.getPath(), SSLTestUtils.SSL_CTS_NAME).getPath();

        final Properties props = new Properties();
        props.setProperty(CLIENT_AUTHENTICATOR, clientAuthenticator);
        props.setProperty(SERVER_HOST_VERIFIER, serverhostVerifier);
        props.setProperty(KEYSTORE_FILE, keystorePath);
        props.setProperty(TRUSTSTORE_FILE, truststorePath);

        final SSLConfig config = new SSLConfig(props);
        config.setKeystorePassword("unittest".toCharArray());
        return config;
    }
}
