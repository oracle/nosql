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

package oracle.kv.impl.tif.esclient.httpClient;

import static oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods.HttpDelete;
import static oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods.HttpGet;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.UnknownHostException; 
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException; 
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

import oracle.kv.impl.tif.esclient.esResponse.ESException;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

/**
 *
 * The base level HttpClient on which RestClient and MonitorClient are built.
 *
 */
public class ESHttpClient {
    //Sets a timeout for the request.
    private static final int SOCKET_TIMEOUT_MILLIS = 30000;
    private final Logger logger;
    //why volatile, what happens if reference is cached ?
    private volatile HttpClient client;
    private volatile boolean closed = false;
    /*
     * The ESNodeMonitor would assign a new list to this variable, if there are
     * changes in available ES Nodes.
     *
     * TODO: AuthCache needs to change for the new ES Node getting added to
     * availableESHttpNodes.
     *
     * Currently this client does not use BasicAuthentication.
     */
    private volatile List<HttpHost> availableESHttpNodes;
    private Map<HttpHost, HttpHost> allESHttpNodes =
        new ConcurrentHashMap<HttpHost, HttpHost>();
    private final long maxRetryTimeoutMillis;
    private final String[] defaultHeaders;
    private final String pathPrefix;
    private FailureListener failureListener;
    private final long checkForESConnectionTimeoutMillis = 30 * 60 * 1000;

    public static final Comparator<HttpHost> httpHostComparator =
        new Comparator<HttpHost>() {

            @Override
            public int compare(HttpHost o1, HttpHost o2) {
                if (o1.getHostName()
                      .compareToIgnoreCase(o1.getHostName()) == 0) {
                    if (o1.getPort() > o2.getPort()) {
                        return 1;
                    } else if (o1.getPort() < o2.getPort()) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
                return o1.getHostName().compareToIgnoreCase(o1.getHostName());
            }

        };

    public ESHttpClient(HttpClient httpClient,
            int maxRetryTimeout,
            String[] defaultHeaders,
            List<HttpHost> esNodes,
            String pathPrefix,
            FailureListener failureListener,
            Logger logger) {
        this.client = httpClient;
        this.maxRetryTimeoutMillis = maxRetryTimeout;
        this.availableESHttpNodes = esNodes;
        populateAllHttpNodes(esNodes);
        this.pathPrefix = pathPrefix;
        this.failureListener = failureListener;
        this.defaultHeaders = defaultHeaders;
        this.logger = logger;
    }

    /*
     * AvailableNodes are changed in ESNodeMonitor. A new list should be
     * created and assigned here. Get method prepares a copy of this list. This
     * method should be called under a lock.
     */
    public synchronized void setAvailableNodes(List<HttpHost> availableNodes) {
        this.availableESHttpNodes = availableNodes;
        populateAllHttpNodes(availableNodes);
    }

    public synchronized List<HttpHost> getAvailableNodes() {
        final List<HttpHost> copyofAvailableNodes = new ArrayList<HttpHost>();
        for (HttpHost host : availableESHttpNodes) {
            copyofAvailableNodes.add(host);
        }
        return copyofAvailableNodes;
    }

    /* Not thread safe. This method should be called under a lock. */
    private void populateAllHttpNodes(List<HttpHost> availableESNodes) {
        for (HttpHost host : availableESNodes) {
            allESHttpNodes.put(host, host);
        }
    }

    public List<HttpHost> getAllESHttpNodes() {

        final List<HttpHost> allNodes = new ArrayList<HttpHost>();

        for (HttpHost host : allESHttpNodes.keySet()) {
            allNodes.add(host);
        }
        return allNodes;
    }

    public ESHttpClient.FailureListener getFailureListener() {
        return failureListener;
    }

    /*
     * ESNodeMonitor is a FailureListener, however ESNodeMonitor can be
     * instantiated only after ESHttpClient is constructed. So this
     * failureListener is set later in the ESHttpClientBuilder.
     */
    public void
            setFailureListener(ESHttpClient.FailureListener failureListener) {
        this.failureListener = failureListener;
    }

    public RestResponse executeSync(String method, String endpoint)
        throws IOException, ESException {
        return executeSync(method, endpoint,
                           Collections.<String, String>emptyMap());
    }

    public RestResponse
            executeSync(String method, String endpoint, Map<String, String> headers)
                throws IOException, ESException {
        return executeSync(method, endpoint,
                           Collections.<String, String>emptyMap(), null,
                           headers);
    }

    public RestResponse executeSync(
                                    String method,
                                    String endpoint,
                                    Map<String, String> params,
                                    Map<String, String> headers)
        throws IOException, ESException {
        return executeSync(method, endpoint, params, (byte[]) null,
                           headers);
    }

    public RestResponse executeSync(
                                    String method,
                                    String endpoint,
                                    Map<String, String> params,
                                    byte[] entity,
                                    Map<String, String> headers)
        throws IOException, ESException {
        final ESSyncResponseListener listener =
            new ESSyncResponseListener(maxRetryTimeoutMillis);
        executeAsync(method, endpoint, params, entity, listener, headers);
        return listener.get();
    }

    public void executeAsync(
                             String method,
                             String endpoint,
                             Map<String, String> params,
                             ESResponseListener responseListener,
                             Map<String, String> headers) {
        executeAsync(method, endpoint, params, null, responseListener,
                     headers);
    }

    public void executeAsync(
                             String method,
                             String endpoint,
                             Map<String, String> params,
                             byte[] entity,
                             ESResponseListener responseListener,
                             Map<String, String> headers) {
        try {
            Objects.requireNonNull(params, "params must not be null");
            final Map<String, String> requestParams = new HashMap<>(params);
            final HttpHost host = availableESHttpNodes.get(0);
            final URI uri = buildUri(host, pathPrefix, endpoint, requestParams);
             
            final HttpRequest.Builder hBuilder =
                createHttpRequest(method, new URI(uri.toASCIIString()),
                                  entity, headers);
            final Set<String> requestNames = headers != null ? headers.keySet():
                                             Collections.emptySet();   
            for (String defaultHeader : defaultHeaders) {
                if (!requestNames.contains(defaultHeader)) {
                    hBuilder.headers(defaultHeader);
                }
            }
            final long startTime = System.nanoTime();
            executeAsync(startTime, hBuilder.build(), responseListener, 0);
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }

    private void executeAsync(
                              final long startTime,
                              final HttpRequest request,
                              final ESResponseListener listener,
                              final int attempt) {
        rotateNodes(availableESHttpNodes);
        /* Make sure that availableESHttpNodes should at least have one node
         * before calling this method.
         */
        final HttpHost host = availableESHttpNodes.get(0);
        if (!request.uri().toString().toLowerCase()
                    .contains("_nodes")) {
            logger.finest("ESHttpClient.executeAsync: send request = " +
                          request + " to host = " + host);

        }

        /* Java httpclient is designed to be self managed. if its mismanaging 
         * resources, then we might need to set our own executor and shutdown
         * when client closes (renable this code)
        if (!closed) {
            try {
                synchronized (this) {
                    final HttpClient httpclient = HttpClient.newBuilder()
                               .connectTimeout(Duration.ofSeconds(5)).executor(executor)
                               .build();
                    client = httpclient;
                }
            } catch (Exception e) {
                logger.warning("ESHttpClient.executeAsync: Closed Client -" +
                        " Could not create a new one and start it");
            }
            logger.warning("ESHttpClient.executeAsync: Found closed client." +
                    " New Client started: State - isRunning : ");
        }
        */

        // we assume all the publishers to the request will be String
        CompletableFuture<HttpResponse<String>> resp = 
            client.sendAsync(request, BodyHandlers.ofString());
        if (resp.isCancelled()) {
            onRequestCancelled(listener);
        }
        resp.exceptionally(ex -> {
                                  onResponseFailed(ex, request, startTime, 
                                                   listener, attempt);
                                  return null;
                          }).thenAccept(response -> {
                   onResponseCompleted(response, startTime, listener, attempt);
                });
    }

    private void onRequestCancelled(final ESResponseListener listener) {
        listener.onFailure(new ExecutionException(
                    "request" + " was cancelled", null));
    }

    private void retryIfPossible(Throwable exception,
                                 final HttpRequest req,
                                 final long startTime,
                                 final int attempt,
                                 final ESResponseListener listener) {
        if (closed) {
            return;
        }
        if (availableESHttpNodes.size() >= 2) {
            rotateNodes(availableESHttpNodes);
        }
        if (exception instanceof ConnectException) {
            /*
             * Connection is refused. Try again with a
             * longer retry timeout.
             */
            logger.fine("Connection Refused when" +
                    " availableNodes: " +
                    availableESHttpNodes + " and" +
                    " allNodes: " + allESHttpNodes);
            try {
                Thread.sleep(
                        1 + 500 * attempt * attempt);
            } catch (InterruptedException e) {
                logger.finest("Thread waiting due" +
                        " to connection" +
                        " refused, got interrupted");
            }
            retryRequestWithTimeout(req,
                    checkForESConnectionTimeoutMillis, startTime, attempt,
                    listener);
        } else {
            retryRequestWithTimeout(req,
                    maxRetryTimeoutMillis, startTime, attempt, listener);
        }
    }

    private void retryRequestWithTimeout(final HttpRequest req,
                                         long timeoutSettingMillis,
                                         final long startTime,
                                         final int attempt,
                                         final ESResponseListener listener) {
        if (closed) {
            return;
        }
        final long timeElapsedMillis =
            TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startTime);
        final long timeout =
            timeoutSettingMillis - timeElapsedMillis;
        if (timeout <= 0) {
            final IOException retryTimeoutException =
                new IOException(
                        "request retries" +
                        " exceeded max timeout for" +
                        " ES to come up [" +
                        checkForESConnectionTimeoutMillis +
                        "]");
            listener.onFailure(retryTimeoutException);
        } else {
            if (!req.uri().toString()
                    .toLowerCase().contains(
                        "_nodes")) {
                logger.fine(" Retrying request:" +
                        req);
            }
            executeAsync(startTime, req,
                    listener, attempt + 1);
        }
    }

    private void onResponseCompleted(HttpResponse<String> httpResponse,
                                     final long startTime,
                                     final ESResponseListener listener,
                                     final int attempt) {
        try {
            if (httpResponse == null) {
                logger.finest("ES Cluster may be down or not yet deployed");
                return;
            }
            HttpRequest req = httpResponse.request();
            final HttpHost host = availableESHttpNodes.get(0);
            final RestResponse restResponse =
                new RestResponse(host, httpResponse);
            final int status = httpResponse.statusCode();
            if (!req.uri().toString()
                    .toLowerCase()
                    .contains("_nodes")) {
                logger.finest(
                        "ESHttpClient.executeAsync: " +
                        "response status = " + status +
                        ", for request = " +
                        req);
            }

            if (httpResponse.statusCode() < 300) {
                logger.finest(
                        "ESHttpClient.executeAsync: " +
                        "STATUS OK [request = " +
                        req + "]");
                restResponse.success(true);
                listener.onSuccess(restResponse);

            } else {

                String httpResponseStr = httpResponse.body();
                if (null != httpResponseStr) {
                    if (logger.isLoggable
                            (Level.FINEST)) {
                        logger.finest("ESHttpClient." +
                                "executeAsync: request = " +
                                req + ", response = " +
                                httpResponseStr);
                    }
                }

                final ESException responseException =
                    new ESException(restResponse);

                if (responseException.errorType() ==
                        null &&
                        responseException.errorStatus() ==
                        null &&
                        status ==
                        RestStatus.NOT_FOUND.getStatus()) {

                    logger.finest("ESHttpClient." +
                            "executeAsync: " +
                            "errorType = null, " +
                            "errorStatus = null, " +
                            "restStatus = NOT_FOUND");

                    switch (req
                            .method()
                            .toUpperCase(Locale.ROOT)) {
                        case HttpDelete:
                        case HttpGet:
                            /*
                             * Trying to access a
                             * document which does not
                             * exist.
                             */
                            listener.onSuccess(
                                    restResponse);
                            logger.finest(
                                    "ESHttpClient." +
                                    "executeAsync: " +
                                    "REQUEST = " + req +
                                    " - trying to access " +
                                    "document that does " +
                                    "not exist");
                            return;
                        default:
                            break;

                    }
                }

                /*
                 * Parse the response and use its
                 * contents to populate the exception.
                 * See SR27091.
                 */
                final Map<String, String>
                    errorResponseMap =
                    ESJsonUtil.parseFailedHttpResponse(
                            httpResponseStr);

                if (errorResponseMap != null &&
                        errorResponseMap.size() > 0) {

                    responseException.setReason(
                            errorResponseMap.get("reason"));
                    responseException.setErrorType(
                            errorResponseMap.get("type"));
                    int errStatus = status;
                    try {
                        errStatus = Integer.parseInt(
                                errorResponseMap.get(
                                    "status"));
                    } catch (NumberFormatException nfe)
                    /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                    responseException.setErrorStatus(
                            RestStatus.fromCode(errStatus));
                }

                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning(
                            "ESHttpClient.executeAsync: " +
                            "failed response - status " +
                            "code = " + status +
                            " [errorStatus = " +
                            responseException.errorStatus() +
                            ", errorType = " +
                            responseException.errorType() +
                            ", errorReason = " +
                            responseException.reason() +
                            "] from request = " +
                            req);
                }

                if (isRetriable(status)) {
                    logger.info(" Retriable response" +
                            " with status: " + status);
                    restResponse.retriable(true);
                    /*
                     * mark host dead and retry against
                     * next one
                     */
                    if (failureListener != null) {
                        failureListener.onFailure(host);
                    }
                    logger.finest(
                            "ESHttpClient.executeAsync: " +
                            responseException + " - " +
                            "retry if possible");
                    retryIfPossible(responseException, req,
                                    startTime, attempt, listener);
                } else {
                    /*
                     * mark host alive and don't retry,
                     * as the error should be a request
                     * problem
                     */
                    logger.fine(
                            "ESHttpClient.executeAsync: " +
                            "host marked alive but won't " +
                            "retry - error in request?");
                    listener.onFailure(
                            responseException);
                }
            }
        } catch (Exception e) {
            logger.warning(
                    "ESHttpClient.executeAsync:  could " +
                    "not process the completed Async " +
                    "Event successfully");
            listener.onFailure(e);
        }
    }

    private void onResponseFailed(final Throwable asynclientException,
                                  final HttpRequest req,
                                  final long startTime,
                                  final ESResponseListener listener,
                                  final int attempt) {
        /*
         * The problem could be with the ES Node the request
         * went to. Populate availableESNodes once more.
         */
        final HttpHost host = availableESHttpNodes.get(0);
        try {
            if (asynclientException instanceof
                    SSLPeerUnverifiedException) {
                final IOException e =
                    new IOException(
                            "SSL Exception: " +
                            asynclientException + "\n" +
                            "Please verify the Subject " +
                            "Alternative Name dns/ip " +
                            "X.509 extension is specified " +
                            "in the public certificate of " +
                            "each ES Node.");
                logger.warning("Peer not verified." +
                        " Exception:" + e.getMessage());
                listener.onFailure(e);
                return;
            } else if (asynclientException instanceof
                    SSLException) {
                /*
                 * Problems with SSL Layer. No point
                 * retrying.
                 */
                final IOException e =
                    new IOException(
                            "SSL Connection had issues." +
                            asynclientException);
                logger.warning("SSL Exception:" +
                        e.getMessage());

                listener.onFailure(e);
                return;
            } else if (asynclientException instanceof
                    UnknownHostException) {
                final IOException e =
                    new IOException(
                            "Unknown Host." +
                            "One of The ES Host " +
                            "registered " +
                            "can not be resolved." +
                            asynclientException);
                logger.warning("Unknown Host:" +
                        e.getMessage());
                listener.onFailure(e);
                return;
            }

            if (failureListener != null &&
                    !failureListener.isClosed()) {
                logger.fine(
                        "ES Http Client with nodes: " +
                        availableESHttpNodes +
                        " request: " +
                        req +
                        " failed due to " +
                        asynclientException +
                        " Failure Listener will handle" +
                        " and will be retried.");
                failureListener.onFailure(host);
                retryIfPossible(asynclientException, req,
                                startTime, attempt, listener);
            } else {
                logger.fine("ES Http Client with " +
                        "nodes: " +
                        availableESHttpNodes +
                        "request: " +
                        req +
                        " failed due to " +
                        asynclientException +
                        ". Failure Listener is not" +
                        " available for this client" +
                        " and this request will be" +
                        " retried.");
                retryIfPossible(asynclientException, req,
                                startTime, attempt, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
  
    private synchronized void rotateNodes(List<HttpHost> availableNodes) {
        Collections.rotate(availableNodes, 1);
    }

    public synchronized void close() {
        try {
            if (client !=null) {
                logger.info("ES Http Client - Connection to ES Nodes:" +
                        availableESHttpNodes + " going down. " +
                        "ES Http Client closing ..."); 
                if (failureListener != null) {
                    failureListener.close();
                }
                /* is this efficient? */
                client = null;
                closed = true;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "ESHttpClient closing exception", e);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private static boolean isRetriable(int status) {
        switch (status) {
            case 408:
            case 502:
            case 503:
            case 504:
                return true;
            default:
                break;
        }
        return false;
    }

    private static URI buildUri(final HttpHost host,
                                String pathPrefix,
                                String path,
                                Map<String, String> params) {

        Objects.requireNonNull(path, "path must not be null");
        try {
            final StringBuilder sb = new StringBuilder(host.toURI());
            String fullPath;
            if (pathPrefix != null) {
                if (path.startsWith("/")) {
                    fullPath = pathPrefix + path;
                } else {
                    fullPath = pathPrefix + "/" + path;
                }
            } else {
                fullPath = path;
            }
            sb.append(fullPath);
            if (params.size() > 0) {
                sb.append("?");
            }
            String encodedURL = params.keySet().stream()
                .map(key -> {
                        try {
                            key += "=" + URLEncoder.encode(params.get(key),
                                         StandardCharsets.UTF_8.toString());
                            return key;
                        } catch (UnsupportedEncodingException e) {
                            throw new IllegalArgumentException(e.getMessage(), e);
                        }
                })
                .collect(Collectors.joining("&", sb.toString(), ""));
            return new URI(encodedURL);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static HttpRequest.Builder
            createHttpRequest(String method, URI newUri, byte[] entity, 
                              Map<String, String> requestHeaders) {
        HttpRequest.Builder hBuilder = HttpRequest.newBuilder();
        hBuilder.uri(newUri);
        hBuilder.timeout(Duration.ofMillis(SOCKET_TIMEOUT_MILLIS));
        if (method != null) {
            if (entity == null) {
                hBuilder.method(method.toUpperCase(Locale.ROOT),
                        BodyPublishers.noBody());
            } else {
                hBuilder.method(method.toUpperCase(Locale.ROOT),
                        BodyPublishers.
                        ofByteArray(entity));
            }
        }
        if (requestHeaders != null) {
            requestHeaders.forEach((k, v) -> hBuilder.headers(k, v));
        }
        return hBuilder;
     }

    public static class FailureListener {

        /*
         * The ESNodeMonitor serves as FailureListener. That will override this
         * method. There is no default implementation currently.
         */
        public void onFailure(@SuppressWarnings("unused") HttpHost host) {
        }

        public void start() {
        }

        public void close() {
        }

        public boolean isClosed() {
            return false;
        }
    }

    static class ESSyncResponseListener implements ESResponseListener {
        private volatile Exception supressedExceptions;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<RestResponse> response =
            new AtomicReference<>();
        private final AtomicReference<Exception> failureException =
            new AtomicReference<>();

        private final long timeout;

        ESSyncResponseListener(long timeout) {
            assert timeout > 0;
            this.timeout = timeout;
        }

        @Override
        public void onSuccess(RestResponse response1) {
            Objects.requireNonNull(response1, "response must not be null");
            final boolean wasResponseNull =
                this.response.compareAndSet(null, response1);
            if (!wasResponseNull) {
                throw new IllegalStateException("response is already set");
            }

            latch.countDown();
        }

        @Override
        public void onFailure(Exception failureException1) {
            Objects.requireNonNull(failureException1,
                                   "exception must not be null");
            final boolean wasExceptionNull =
                this.failureException.compareAndSet(null, failureException1);
            if (!wasExceptionNull) {
                throw new IllegalStateException("exception is already set");
            }
            latch.countDown();
        }

        /**
         * Waits (up to a timeout) for some result of the request: either a
         * response, or an exception.
         *
         * @throws ESException
         */
        RestResponse get() throws IOException, ESException {
            try {
                /*
                 * providing timeout is just a safety measure to prevent
                 * everlasting waits. the different client timeouts should
                 * already do their jobs
                 */
                if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    throw new IOException("listener timeout after" +
                                          " waiting for [" +
                                          timeout + "] ms");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("thread waiting for the response" +
                                           " was interrupted",
                                           e);
            }

            final Exception exception = failureException.get();

            final RestResponse resp = this.response.get();

            if (exception != null) {
                if (resp != null) {
                    final IllegalStateException e =
                        new IllegalStateException("response and exception" +
                                                  " are unexpectedly set " +
                                                  "at the same time");
                    e.addSuppressed(exception);
                    throw e;
                }
                /*
                 * Do not want to handle too many types of Exceptions. Two
                 * categories of Exception will be handled.
                 *
                 * Rest will result in RunTimeException.
                 */
                if (exception instanceof IOException) {
                    throw (IOException) exception;
                } else if (exception instanceof ESException) {
                    throw (ESException) exception;
                }

                /*
                 * TODO: Handle retry timeout exception. ExecutionException
                 * from async client.
                 */

                throw new RuntimeException("error while performing request",
                                           exception);
            }

            if (resp == null) {
                throw new IllegalStateException("response not set and no " +
                                                "exception caught either");
            }
            return resp;
        }

        @Override
        public void onRetry(Exception retryException) {
            if (supressedExceptions == null) {
                this.supressedExceptions = retryException;
            } else {
                supressedExceptions.addSuppressed(retryException);
            }

        }
    }

    public static enum Scheme {
        HTTP, HTTPS;

        public static Scheme getScheme(String scheme) {
            for (Scheme s : values()) {
                if (valueOf(scheme.toUpperCase(Locale.ENGLISH)) == s) {
                    return s;
                }
            }
            return null;
        }

        public String getProtocol() {

            return name().toLowerCase(Locale.ENGLISH);
        }
    }

}
