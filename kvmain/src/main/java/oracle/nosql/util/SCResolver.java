/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.util.HttpRequest.ConnectionHandler;
import oracle.nosql.util.ssl.SSLConfig;
import oracle.nosql.util.ssl.SSLConnectionHandler;

/**
 * A utility for all cloud components to get the address of the
 * ServiceController. In environments where the SC can be found via a discovery
 * mechanism like a load balancer or DNS, the SC's address is a well known
 * name, specified via a system property or environment variable. In the
 * absence of a discovery mechanism, the cloud component is given a set of
 * possible hosts that might be hosting this SC.
 *
 * The SCResolver is given SC host information through an environment variable,
 * a system property, or a file. The host information is in the helper hosts
 * format. This is provided in the IP1:port,IP2:port... format. e.g.:
 * 127.0.0.1:8000,127.0.0.1:8001. If the port is omitted,
 * ToolConstants.SC_SERVICE_PORT is used.
 *
 * If an input file is used, the input file is seeded by the SC,
 * and the resolver is ignorant of the originator of the file.
 *
 * The resolver holds no state.
 */
public class SCResolver {

    /**
     * The default location of the hosts file.
     */
    public static final Path SC_HOSTS_FILE_PATH_DEFAULT = Paths
            .get("/tmp/sc_hosts");

    /**
     * The path of the file containing the SC host IPs (and/or ports)
     * in the host:1port,host2:port..
     */
    private final Path hostsFilePath;

    /**
     * Environment variable to set with the SC host name in case the SC is
     * resolved using the load balancer, DNS, or containers. The SC service
     * port will assumed to be the default port.
     */
    public static final String SC_HOSTPORT_ENV_PARAM = "SC_HOSTPORT";

    public SCResolver () {
        this.hostsFilePath = SC_HOSTS_FILE_PATH_DEFAULT;
    }

    public SCResolver (String hostFilePath) {
        this.hostsFilePath = Paths.get(hostFilePath);
    }

    public Path getHostsFilePath() {
        return hostsFilePath;
    }

    /**
     * Gets the SC in IP:port format. Makes an http call to an SC's /health
     * to check if it has return code 200 and returns that SC host:port.
     *
     * @return null if the file doesn't exist or is empty or is no SC is
     * available
     * @throws IllegalArgumentException if the file has bad contents.
     */
    public HostPort getSCHostPort(SkLogger logger) {

        /*
         * Check if the SC HOST:PORT is set in the environment or in a system
         * property.  Otherwise read it from the hosts file.
         */
        String scHosts = System.getenv(SC_HOSTPORT_ENV_PARAM);
        boolean useSSL = SSLConfig.isInternalSSLEnabled();
        logInfo(logger, "SCResolver: environment variable: " + scHosts +
                        (useSSL ? ", SSL enabled" : ", SSL disabled"));

        if (scHosts == null) {
            scHosts = System.getProperty(SC_HOSTPORT_ENV_PARAM);
            logInfo(logger, "SCResolver: system variable: " + scHosts);
        }

        if (scHosts == null) {
            /* Return null if file doesn't exist. */
            File f = new File(hostsFilePath.toString());
            if (!f.exists() || f.isDirectory()) {
                logInfo(logger, "SCResolver: can't find hosts file: " + hostsFilePath);
                return null;
            }

            /* Return null if file could not be read. */
            try {
                scHosts = new String(Files.readAllBytes(hostsFilePath))
                        .trim();
            } catch (Exception e) {
                logInfo(logger, "SCResolver: invalid content in hosts file: " +
                        hostsFilePath);
                return null;
            }

            /* Return null if file is empty. */
            if (scHosts == null || scHosts.length() == 0) {
                logInfo(logger, "SCResolver: not content in hosts file: " +
                        hostsFilePath);
                return null;
            }
        }

        /* Throw an exception if the host info is bad. */
        List<HostPort> scTargets = null;
        try {
            scTargets = parseHosts(scHosts);
        } catch (Exception e) {
            logInfo(logger, "SCResolver: exception parsing hosts string: "
                    + scHosts + ": " + e.getMessage());
            throw new IllegalArgumentException(
                    "Could not parse hosts info: " + scHosts + ":" + e);

        }

        /*
         * Do an http call to get sc health. If the response is 200, we have a
         * good sc.
         */
        HttpRequest request = new HttpRequest();
        ConnectionHandler sslHandler = null;
        String schema = null;
        if (useSSL) {
            request.disableHostVerification();
            sslHandler = SSLConnectionHandler.getOCICertHandler(logger);
            schema = "https://";
        } else {
            schema = "http://";
        }

        HostPort foundSC = null;
        for (HostPort scAddr : scTargets) {
            try {
                HttpResponse resp = request.doHttpGet(
                        schema + scAddr.toString() + "/V0/health",
                        sslHandler, null);
                if (resp.getStatusCode() == HttpURLConnection.HTTP_OK) {
                    foundSC = scAddr;
                    logger.fine("SCResolver: contacted SC, response OK");
                    break;
                }
                logInfo(logger, "SCResolver: reached " + scAddr +
                            " but did not find a responding SC " + resp);
            } catch (Exception ignore) {
                /* Can't reach this SC, ignore this and try the next one */
                logInfo(logger, "SCResolver: tried to reach " + scAddr +
                            " but got " + getStackTrace(ignore));
            }
        }

        return foundSC;
    }

    private void logInfo(SkLogger logger, String msg) {
        if (logger != null) {
            logger.info(msg);
        }
    }

    private static List<HostPort> parseHosts(String scHosts)
            throws UnknownHostException {
        List<HostPort> scTargets = new ArrayList<>();
        for (String sc : scHosts.split(",")) {
            String[] hostports = sc.split(":");
            /* See if the IP address parses */
            InetAddress.getByName(hostports[0]);
            int port = Integer.parseInt(hostports[1]);
            scTargets.add(new HostPort(hostports[0], port));
        }
        return scTargets;
    }

    /*
     * Create the default hosts file (or overwrite it) and put the hostPorts
     * inside it. To be used by the SC. Likely to be modified to take the
     * file name.
     */
    public void addToHostsFile(String hostPorts)
            throws IOException {

        /* Check if the hostPorts is correctly parseable. */
        parseHosts(hostPorts);
        FileOutputStream fop = new FileOutputStream(
                hostsFilePath.toString(), false);
        fop.write(hostPorts.getBytes());
        fop.close();

    }


    /**
     * Return a String version of a stack trace. TODO: put in SKLogger.
     */
    private String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        return stackTrace;
    }
}
