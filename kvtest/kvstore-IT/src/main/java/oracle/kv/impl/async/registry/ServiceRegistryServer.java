/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.registry;

import static oracle.kv.impl.async.StandardDialogTypeFamily.SERVICE_REGISTRY_DIALOG_TYPE;

import java.util.logging.Logger;

import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.async.ListenerPortRange;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;

/**
 * A {@link ServiceRegistry} server.
 */
public class ServiceRegistryServer {

    private static final Logger logger =
        Logger.getLogger(ServiceRegistryServer.class.getName());

    private static final String USAGE =
        "java " + ServiceRegistryServer.class.getName() + " <args>\n" +
        "\n" +
        "Arguments:\n" +
        "  -port <port> (default 0 for anonymous port)\n";

    /**
     * Runs a ServiceRegistry server.
     */
    public static void main(String[] args) throws Exception {
        int port = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
            case "-port":
                port = Integer.parseInt(args[++i]);
                break;
            default:
                throw new IllegalArgumentException(
                    "Unexpected argument: " + args[i] + "\n" + USAGE);
            }
        }
        final ListenHandle serverHandle = server(port);
        final NetworkAddress localAddress =
            serverHandle.getLocalAddress().toNetworkAddress();
        System.out.println("Started service registry on port " +
                           ((localAddress != null) ?
                            localAddress.getPort() :
                            "Null address"));
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Runs a ServiceRegistry server on the specified port.
     *
     * @param port the server port, or 0 for an anonymous port
     * @return the listen handle for the server
     */
    public static ListenHandle server(int port) throws Exception {
        final EndpointGroup endpointGroup = new NioEndpointGroup(logger, 2);
        final ServiceRegistryImpl registry =
            new ServiceRegistryImpl(logger);
        final ListenerConfigBuilder listenerConfigBuilder =
            new ListenerConfigBuilder();
        listenerConfigBuilder.portRange(
            (port == 0) ?
            new ListenerPortRange() :
            new ListenerPortRange(port, port));
        return endpointGroup.listen(
            listenerConfigBuilder.build(),
            SERVICE_REGISTRY_DIALOG_TYPE.getDialogTypeId(),
            () -> new ServiceRegistryResponder(registry, logger));
    }
}
