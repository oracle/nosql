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

package oracle.kv.impl.as;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Tiny http server for delivering health status of a service to
 * an orchestration service such as Kubernetes.<p>
 *
 * Usage: Define one or more HealthCheckers to test the health of the service;
 * add them to a HealthServer, and call start().  GET Requests to the path
 * "/healthz" return a response code 200 if the checkers all return true;
 * 500 otherwise.  See the main() method for sample usage.
 *
 * @see <a href="https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/%23defining-a-liveness-http-request">
 * https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/#defining-a-liveness-http-request</a>
 */
public class HealthServer {

    /* The path of the GET request. */
    static final String path = "/healthz";

    @FunctionalInterface
    public interface HealthChecker {

        /**
         * @return true:  service is healthy;
         *         false: service is unhealthy and should be restarted.
         */
        boolean doCheck();
    }

    private final List<HealthChecker> checkers;
    private final HttpServer server;

    public HealthServer(int port) throws IOException {
        checkers = new ArrayList<>();
        InetSocketAddress isa = new InetSocketAddress(port);
        server = HttpServer.create(isa, 0);
        server.createContext(path, new Handler());
    }

    public void addChecker(HealthChecker c) {
        checkers.add(c);
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    class Handler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                boolean result = true;
                for (HealthChecker c : checkers) {
                    if (! c.doCheck()) {
                        result = false;
                        break;
                    }
                }
                if (result) {
                    exchange.sendResponseHeaders(200, 0);
                } else {
                    exchange.sendResponseHeaders(500, 0);
                }
                exchange.getResponseBody().close();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        final HealthServer h = new HealthServer(8080);

        h.addChecker(new HealthChecker() {
                private int counter = 1;
                @Override
                public boolean doCheck() {
                    /* We are alternately healthy and unhealthy */
                    return (counter++ % 2 == 1);
                }
            });

        h.start();
    }
}
