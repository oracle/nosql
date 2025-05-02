/*-
 * Copyright (C) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 */

package oracle.nosql.util;

import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.util.ph.HealthStatus;
import oracle.nosql.util.ssl.SSLConfig;
import oracle.nosql.util.ssl.SSLConnectionHandler;

import java.io.File;
import java.util.List;

/**
 * A helper class using HttpURLConnection to issue HTTP request to check
 * HTTP server is alive.
 */
public class HttpServerHealth {
    private static final int HTTP_REQUEST_TIME_OUT = 2000;

    private final String healthUrl;
    private final String expectedResp;
    private final HttpRequest request;
    private final HttpRequest.ConnectionHandler sslHandler;

    public HttpServerHealth(int port,
                            String healthPath,
                            String expectedResp) {
        this("localhost", port,
             SSLConfig.isInternalSSLEnabled() ?
                 SSLConfig.getInternalSSLCert() : null,
             healthPath, expectedResp);
    }

    public HttpServerHealth(String host,
                            int port,
                            File sslCert,
                            String healthPath,
                            String expectedResp) {
        this(getFullUrl(host, port, sslCert != null, healthPath),
             sslCert, expectedResp, HTTP_REQUEST_TIME_OUT);
    }

    public HttpServerHealth(String fullHealthUrl,
                            File sslCert,
                            String expectedResp,
                            int requestTimeout) {
        this.healthUrl = fullHealthUrl;
        this.expectedResp = expectedResp;
        request = new HttpRequest(
            100, requestTimeout, requestTimeout, requestTimeout, false);
        if (sslCert != null) {
            request.disableHostVerification();
            sslHandler = new SSLConnectionHandler.SSLHanlder(
                "NoSQLCert", sslCert);
        } else {
            sslHandler = null;
        }
    }

    /**
     * Add error to the errors list and return status: GREEN, YELLOW or RED.
     */
    public HealthStatus checkHealth(List<String> errors,
                                    SkLogger logger) {
        try {
            HttpResponse resp = request.doHttpGet(healthUrl, sslHandler, null);
            int code = resp.getStatusCode();
            if (code != 200) {
                errors.add(healthUrl + " response code is " + code);
                return HealthStatus.RED;
            }
            if (expectedResp == null || expectedResp.isBlank()) {
                return HealthStatus.GREEN;
            }
            String output = resp.getOutput();
            if (output.contains(expectedResp)) {
                return HealthStatus.GREEN;
            }
            errors.add(healthUrl + " response error: " + output);
            return HealthStatus.RED;
        } catch (Exception e) {
            String errorMsg = healthUrl + " server health check error: " + e;
            logger.warning(errorMsg);
            errors.add(errorMsg);
            return HealthStatus.RED;
        }
    }

    private static String getFullUrl(String host,
                                     int port,
                                     boolean useSSL,
                                     String healthPath) {
        String fullUrl = new HostPort(host, port).toUrl(useSSL);
        if (healthPath == null || healthPath.isEmpty()) {
            return fullUrl;
        }
        if (healthPath.charAt(0) != '/') {
            fullUrl += "/";
        }
        fullUrl += healthPath;
        return fullUrl;
    }
}
