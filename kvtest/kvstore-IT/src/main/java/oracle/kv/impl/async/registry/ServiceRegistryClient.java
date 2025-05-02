/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.registry;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT;
import static oracle.kv.impl.async.FutureUtils.failedFuture;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;

import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link ServiceRegistry} client.
 */
public class ServiceRegistryClient {

    private static final Logger logger =
        Logger.getLogger(ServiceRegistryClient.class.getName());

    private static final String USAGE =
        "java " + ServiceRegistryClient.class.getName() + " <args>\n" +
        "\n" +
        "Arguments:\n" +
        "  -host <host> (defaults to 'localhost')\n" +
        "  -port <port> (required)\n" +
        "  -timeout <long> (in milliseconds, defaults to '5000')\n" +
        "  -security <security-properties-file> (optional)\n" +
        "  <command and args>\n" +
        "\n" +
        "Commands:\n" +
        "  lookup <name>\n" +
        "  bind <name> <endpoint>\n" +
        "  unbind <name>\n" +
        "  list\n" +
        "\n" +
        "Where:\n" +
        "  <endpoint> = <address>|<dialog type>\n" +
        "  <address> = <host>:<port>\n" +
        "  <dialog type> = <int>";

    /**
     * Calls a ServiceRegistry client command.
     */
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        boolean portSpecified = false;
        int port = 0;
        long timeoutArg = 5000;
        Properties securityProperties = null;
        String command = null;
        Object[] commandArgs = new Object[2];
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
            case "-host":
                host = args[++i];
                break;
            case "-port":
                port = Integer.parseInt(args[++i]);
                portSpecified = true;
                break;
            case "-timeout":
                timeoutArg = Long.parseLong(args[++i]);
                break;
            case "-security":
                securityProperties =
                    KVStoreLogin.createSecurityProperties(args[++i]);
                break;
            case "lookup":
                command = "lookup";
                commandArgs[0] = args[++i];
                break;
            case "bind":
                command = "bind";
                commandArgs[0] = args[++i];
                commandArgs[1] = toEndpoint(args[++i]);
                break;
            case "unbind":
                command = "unbind";
                commandArgs[0] = args[++i];
                break;
            case "list":
                command = "list";
                break;
            default:
                throw new IllegalArgumentException(
                    "Unexpected argument: " + args[i] + "\n" + USAGE);
            }
        }
        if (!portSpecified) {
            throw new IllegalArgumentException(
                "Port must be specified" + "\n" + USAGE);
        }
        if (command == null) {
            throw new IllegalArgumentException(
                "Command must be specified" + "\n" + USAGE);
        }
        final long timeout = timeoutArg;
        final Function<ServiceRegistryAPI, CompletableFuture<Void>>
            registryHandler;
        switch (command) {
        case "lookup":
        {
            final String name = (String) commandArgs[0];
            registryHandler = reg -> lookup(reg, name, timeout);
            break;
        }
        case "bind":
        {
            final String name = (String) commandArgs[0];
            final ServiceEndpoint endpoint = (ServiceEndpoint) commandArgs[1];
            registryHandler = reg -> bind(reg, name, endpoint, timeout);
            break;
        }
        case "unbind":
        {
            final String name = (String) commandArgs[0];
            registryHandler = reg -> unbind(reg, name, timeout);
            break;
        }
        case "list":
            registryHandler = reg -> list(reg, timeout);
            break;
        default:
            throw new AssertionError();
        }
        getRegistry(host, port, timeout, securityProperties)
            .thenCompose(
                registry -> {
                    if (registry == null) {
                        throw new IllegalArgumentException(
                            "Registry is null");
                    }
                    return registryHandler.apply(registry);
                })
            .get(timeout, TimeUnit.MILLISECONDS);
    }

    public static CompletableFuture<ServiceRegistryAPI>
        getRegistry(String host,
                    int port,
                    long timeout,
                    @Nullable Properties securityProperties) {
        try {
            final EndpointConfigBuilder endpointConfigBuilder =
                new EndpointConfigBuilder();
            if (securityProperties != null) {
                endpointConfigBuilder.sslControl(
                    new SSLConfig(securityProperties)
                    .makeSSLControl(false /* server */, logger));
            }
            final CreatorEndpoint ce = new NioEndpointGroup(logger, 2).
                getCreatorEndpoint(
                    "perfName", new InetNetworkAddress(host, port),
                    InetNetworkAddress.ANY_LOCAL_ADDRESS,
                    endpointConfigBuilder.build());
            return ServiceRegistryAPI.wrap(
                ce, null /* clientId */, timeout,
                DEFAULT_REGISTRY_READ_TIMEOUT, logger);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    public static String endpointString(@Nullable ServiceEndpoint endpoint) {
        if (endpoint == null) {
            return "none";
        }
        return endpoint.getNetworkAddress() + "|" + endpoint.getDialogType();
    }

    public static ServiceEndpoint toEndpoint(String string) {
        final int bar = string.indexOf('|');
        if (bar < 0) {
            throw new IllegalArgumentException(
                "Illegal service endpoint: '" + string + "'");
        }
        final NetworkAddress address = toAddress(string.substring(0, bar));
        final String dialogTypeString = string.substring(bar + 1);
        final DialogType dialogType =
            new DialogType(Integer.parseInt(dialogTypeString));
        return new ServiceEndpoint(
            address, dialogType,
            new ClearClientSocketFactory("AsyncRegistryClient", 0, 0, null));
    }

    public static NetworkAddress toAddress(String string) {
        final int colon = string.indexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException(
                "Illegal network address: '" + string + "'");
        }
        return new InetNetworkAddress(
            string.substring(0, colon),
            Integer.parseInt(string.substring(colon + 1)));
    }

    private static
        CompletableFuture<Void> lookup(ServiceRegistryAPI registry,
                                       String name,
                                       long timeout) {
        return registry.lookup(name, null, timeout)
            .thenAccept(
                endpoint -> {
                    System.out.println(name + ": " + endpointString(endpoint));
                });
    }

    private static
        CompletableFuture<Void> bind(ServiceRegistryAPI registry,
                                     String name,
                                     ServiceEndpoint endpoint,
                                     long timeout) {
        return registry.bind(name, endpoint, timeout)
            .thenRun(() -> System.out.println("Set binding for: " + name +
                                              ": " + endpoint));
    }

    private static
        CompletableFuture<Void> unbind(ServiceRegistryAPI registry,
                                       String name,
                                       long timeout) {
        return registry.unbind(name, timeout)
            .thenRun(() -> {
                    System.out.println("Removed binding for: " + name);
                });
    }

    private static
        CompletableFuture<Void> list(ServiceRegistryAPI registry,
                                     long timeout) {
        return registry.list(timeout)
            .thenCompose(
                list -> {
                    if (list == null) {
                        throw new IllegalArgumentException("List is null");
                    }
                    return printEntries(list, registry, timeout);
                });
    }

    private static
        CompletableFuture<Void> printEntries(List<String> names,
                                             ServiceRegistryAPI registry,
                                             long timeout) {
        try {
            if (names.isEmpty()) {
                System.out.println("No entries");
                return completedFuture(null);
            }
            final CompletableFuture<Void> future = new CompletableFuture<>();
            new Runnable() {
                @Override
                public void run() {
                    final String name = names.remove(0);
                    registry.lookup(name, null, timeout)
                        .whenComplete((endpoint, e) -> {
                                if (endpoint != null) {
                                    System.out.println(
                                        name + ": " +
                                        endpointString(endpoint));
                                } else {
                                    System.out.println(name + ": throws " + e);
                                }
                                if (names.isEmpty()) {
                                    future.complete(null);
                                } else {
                                    CompletableFuture.runAsync(this::run);
                                }
                            });
                }
            }.run();
            return future;
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }
}
